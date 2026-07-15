"""
transform_silver.py

Capa de TRANSFORMACION del pipeline Medallion: Bronze -> Silver.

Aplica las reglas de calidad de datos definidas en docs/data_dictionary.md.
Filosofia: no se descartan datos silenciosamente. Los registros con
problemas se conservan y se marcan con data_quality_flag, excepto los
duplicados exactos por llave primaria, que si se eliminan.

Lee siempre la particion mas reciente (load_date) de cada tabla en Bronze.

Uso:
    python src/transform_silver.py
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("transform_silver")

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"


def crear_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HospitalDB-Transform-Silver")
        .master("local[*]")
        .getOrCreate()
    )


def leer_ultima_particion_bronze(spark: SparkSession, nombre_tabla: str) -> DataFrame:
    """Lee la particion de load_date mas reciente de una tabla en Bronze."""
    ruta_tabla = BRONZE_DIR / nombre_tabla
    particiones = sorted(ruta_tabla.glob("load_date=*"))

    if not particiones:
        raise FileNotFoundError(
            f"No hay particiones en Bronze para '{nombre_tabla}'. "
            f"Corre extract.py primero."
        )

    ultima_particion = particiones[-1]
    logger.info(f"Leyendo {nombre_tabla} desde {ultima_particion.name}")
    return spark.read.parquet(str(ultima_particion))


def eliminar_duplicados(df: DataFrame, nombre_tabla: str, llave_primaria: str) -> DataFrame:
    """Elimina duplicados exactos por llave primaria, logueando cuantos se quitaron."""
    n_antes = df.count()
    df_limpio = df.dropDuplicates([llave_primaria])
    n_despues = df_limpio.count()
    n_eliminados = n_antes - n_despues

    if n_eliminados > 0:
        logger.warning(f"{nombre_tabla}: se eliminaron {n_eliminados} duplicados por {llave_primaria}")

    return df_limpio


def transformar_pacientes(df: DataFrame) -> DataFrame:
    """Limpia y valida la tabla de pacientes."""
    df = eliminar_duplicados(df, "pacientes", "id_paciente")

    # Regex simple de validacion de email
    patron_email = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    df = df.withColumn(
        "flag_email_invalido",
        F.when(~F.col("email").rlike(patron_email), F.lit("email_invalido"))
    ).withColumn(
        "flag_fecha_nacimiento",
        F.when(
            (F.col("fecha_nacimiento") >= F.current_date())
            | (F.col("fecha_nacimiento") < F.lit("1900-01-01")),
            F.lit("fecha_nacimiento_invalida")
        )
    )

    df = df.withColumn(
        "data_quality_flag",
        F.array_except(
            F.array(F.col("flag_email_invalido"), F.col("flag_fecha_nacimiento")),
            F.array(F.lit(None).cast("string")),
        )
    ).drop("flag_email_invalido", "flag_fecha_nacimiento")

    return df


def transformar_doctores(df: DataFrame) -> DataFrame:
    """Limpia y valida la tabla de doctores."""
    df = eliminar_duplicados(df, "doctores", "id_doctor")

    df = df.withColumn(
        "flag_experiencia",
        F.when(
            (F.col("anios_experiencia") < 0) | (F.col("anios_experiencia") > 60),
            F.lit("experiencia_fuera_de_rango")
        )
    )

    df = df.withColumn(
        "data_quality_flag",
        F.array_except(F.array(F.col("flag_experiencia")), F.array(F.lit(None).cast("string")))
    ).drop("flag_experiencia")

    return df


def transformar_citas(df: DataFrame) -> DataFrame:
    """Limpia y valida la tabla de citas. Aqui viven las fechas nulas a proposito."""
    df = eliminar_duplicados(df, "citas", "id_cita")

    df = df.withColumn(
        "flag_fecha_hora",
        F.when(F.col("fecha_hora").isNull(), F.lit("missing_fecha_hora"))
    ).withColumn(
        # Corregimos tiempos de espera negativos (no deberian existir, pero por
        # seguridad los normalizamos a 0 y lo marcamos)
        "flag_tiempo_espera",
        F.when(F.col("tiempo_espera_minutos") < 0, F.lit("tiempo_espera_corregido"))
    ).withColumn(
        "tiempo_espera_minutos",
        F.when(F.col("tiempo_espera_minutos") < 0, 0).otherwise(F.col("tiempo_espera_minutos"))
    )

    df = df.withColumn(
        "data_quality_flag",
        F.array_except(
            F.array(F.col("flag_fecha_hora"), F.col("flag_tiempo_espera")),
            F.array(F.lit(None).cast("string")),
        )
    ).drop("flag_fecha_hora", "flag_tiempo_espera")

    return df


def transformar_resultados_lab(df: DataFrame) -> DataFrame:
    """Limpia y valida la tabla de resultados de laboratorio."""
    df = eliminar_duplicados(df, "resultados_lab", "id_resultado")

    df = df.withColumn(
        "flag_resultado",
        F.when(
            (F.col("resultado") < 0) | F.col("resultado").isNull(),
            F.lit("resultado_fuera_de_rango")
        )
    )

    df = df.withColumn(
        "data_quality_flag",
        F.array_except(F.array(F.col("flag_resultado")), F.array(F.lit(None).cast("string")))
    ).drop("flag_resultado")

    return df


def resumen_calidad(df: DataFrame, nombre_tabla: str) -> None:
    """Loguea cuantas filas tienen al menos un flag de calidad."""
    total = df.count()
    con_problemas = df.filter(F.size(F.col("data_quality_flag")) > 0).count()
    pct = (con_problemas / total * 100) if total > 0 else 0
    logger.info(
        f"{nombre_tabla:<20} | {total:>6,} filas totales | "
        f"{con_problemas:>5,} con flags ({pct:.1f}%)"
    )


TRANSFORMACIONES = {
    "pacientes": transformar_pacientes,
    "doctores": transformar_doctores,
    "citas": transformar_citas,
    "resultados_lab": transformar_resultados_lab,
}


def main():
    logger.info("=== Iniciando transformacion: Bronze -> Silver ===")
    spark = crear_spark_session()

    try:
        for nombre_tabla, funcion_transform in TRANSFORMACIONES.items():
            df_bronze = leer_ultima_particion_bronze(spark, nombre_tabla)
            df_silver = funcion_transform(df_bronze)

            ruta_destino = SILVER_DIR / nombre_tabla
            df_silver.write.mode("overwrite").parquet(str(ruta_destino))

            resumen_calidad(df_silver, nombre_tabla)
            logger.info(f"  -> Guardado en {ruta_destino}")
    finally:
        spark.stop()

    logger.info("=== Transformacion Silver finalizada ===")


if __name__ == "__main__":
    main()
