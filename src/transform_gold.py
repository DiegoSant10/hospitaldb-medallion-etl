"""
transform_gold.py

Capa de MODELADO DIMENSIONAL del pipeline Medallion: Silver -> Gold.

Construye un esquema estrella:
    - dim_paciente
    - dim_doctor
    - dim_tiempo
    - fact_citas

Decision de diseño: las citas con fecha_hora nula (marcadas en Silver
con el flag missing_fecha_hora) NO se descartan aqui tampoco. En vez de
eso, se enlazan a un registro especial en dim_tiempo (id_tiempo = -1,
"Fecha Desconocida"). Esto es la tecnica estandar de Kimball para evitar
nulls en las llaves foraneas de una tabla de hechos, manteniendo la
trazabilidad de esos registros en vez de perderlos silenciosamente.

Uso:
    python src/transform_gold.py
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("transform_gold")

BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

ID_TIEMPO_DESCONOCIDO = -1


def crear_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HospitalDB-Transform-Gold")
        .master("local[*]")
        .getOrCreate()
    )


def leer_silver(spark: SparkSession, nombre_tabla: str) -> DataFrame:
    ruta = SILVER_DIR / nombre_tabla
    logger.info(f"Leyendo Silver: {nombre_tabla}")
    return spark.read.parquet(str(ruta))


def construir_dim_paciente(pacientes_silver: DataFrame) -> DataFrame:
    """
    Construye dim_paciente. Calcula edad a partir de fecha_nacimiento.
    No se incluyen datos sensibles como email o telefono en Gold:
    esta capa esta pensada para analitica, no para operacion diaria.
    """
    return pacientes_silver.select(
        "id_paciente",
        "nombre",
        F.floor(F.datediff(F.current_date(), F.col("fecha_nacimiento")) / 365.25)
            .cast(IntegerType()).alias("edad"),
        "genero",
        "ciudad",
    )


def construir_dim_doctor(doctores_silver: DataFrame) -> DataFrame:
    return doctores_silver.select(
        "id_doctor",
        "nombre",
        "especialidad",
        "anios_experiencia",
    )


def construir_dim_tiempo(citas_silver: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Genera un calendario completo cubriendo el rango de fechas de las citas,
    mas un registro especial para "Fecha Desconocida" (id_tiempo = -1).
    """
    rango = citas_silver.filter(F.col("fecha_hora").isNotNull()).agg(
        F.min(F.to_date("fecha_hora")).alias("fecha_min"),
        F.max(F.to_date("fecha_hora")).alias("fecha_max"),
    ).collect()[0]

    fecha_min, fecha_max = rango["fecha_min"], rango["fecha_max"]

    calendario = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{fecha_min}'), to_date('{fecha_max}'), interval 1 day
        )) AS fecha
    """)

    calendario = calendario.withColumn(
        "id_tiempo", F.date_format("fecha", "yyyyMMdd").cast(IntegerType())
    ).withColumn(
        "dia_semana", F.date_format("fecha", "EEEE")
    ).withColumn(
        "mes", F.month("fecha")
    ).withColumn(
        "trimestre", F.quarter("fecha")
    ).withColumn(
        "anio", F.year("fecha")
    ).withColumn(
        "es_fin_de_semana", F.dayofweek("fecha").isin([1, 7])
    ).select(
        "id_tiempo", "fecha", "dia_semana", "mes", "trimestre", "anio", "es_fin_de_semana"
    )

    # Fila especial para citas sin fecha valida (tecnica Kimball).
    # Se define el esquema explicitamente con nullable=True en cada campo,
    # porque calendario.schema hereda columnas no-nulables de la funcion
    # sequence() de Spark SQL, y esta fila si necesita nulls.
    from pyspark.sql.types import StructType, StructField, DateType, StringType, BooleanType

    schema_desconocido = StructType([
        StructField("id_tiempo", IntegerType(), True),
        StructField("fecha", DateType(), True),
        StructField("dia_semana", StringType(), True),
        StructField("mes", IntegerType(), True),
        StructField("trimestre", IntegerType(), True),
        StructField("anio", IntegerType(), True),
        StructField("es_fin_de_semana", BooleanType(), True),
    ])

    fila_desconocida = spark.createDataFrame(
        [(ID_TIEMPO_DESCONOCIDO, None, "Desconocido", None, None, None, None)],
        schema=schema_desconocido,
    )

    # Forzamos el mismo esquema (todo nullable=True) en calendario antes
    # de unir, para que coincida exactamente con fila_desconocida.
    calendario_nullable = spark.createDataFrame(calendario.rdd, schema=schema_desconocido)

    return calendario_nullable.unionByName(fila_desconocida)


def construir_fact_citas(citas_silver: DataFrame) -> DataFrame:
    """
    Construye fact_citas. Las citas con fecha_hora nula se enlazan al
    registro id_tiempo = -1 en vez de perderse.
    """
    return citas_silver.withColumn(
        "id_tiempo",
        F.when(
            F.col("fecha_hora").isNotNull(),
            F.date_format("fecha_hora", "yyyyMMdd").cast(IntegerType()),
        ).otherwise(F.lit(ID_TIEMPO_DESCONOCIDO)),
    ).withColumn(
        "tiene_flag_calidad", F.size(F.col("data_quality_flag")) > 0
    ).select(
        "id_cita",
        "id_paciente",
        "id_doctor",
        "id_tiempo",
        "duracion_minutos",
        "tiempo_espera_minutos",
        "costo",
        "estado",
        "tiene_flag_calidad",
    )


def validar_llaves_huerfanas(fact: DataFrame, dim: DataFrame, columna_fk: str, columna_pk: str, nombre_dim: str) -> None:
    """Verifica que no existan llaves foraneas en fact que no existan en la dimension."""
    huerfanas = fact.join(dim, fact[columna_fk] == dim[columna_pk], "left_anti").count()
    if huerfanas > 0:
        logger.warning(f"fact_citas tiene {huerfanas} filas con {columna_fk} huerfano respecto a {nombre_dim}")
    else:
        logger.info(f"Integridad OK: todas las llaves {columna_fk} existen en {nombre_dim}")


def main():
    logger.info("=== Iniciando modelado dimensional: Silver -> Gold ===")
    spark = crear_spark_session()

    try:
        pacientes_silver = leer_silver(spark, "pacientes")
        doctores_silver = leer_silver(spark, "doctores")
        citas_silver = leer_silver(spark, "citas")

        dim_paciente = construir_dim_paciente(pacientes_silver)
        dim_doctor = construir_dim_doctor(doctores_silver)
        dim_tiempo = construir_dim_tiempo(citas_silver, spark)
        fact_citas = construir_fact_citas(citas_silver)

        # Validacion de integridad referencial antes de guardar
        validar_llaves_huerfanas(fact_citas, dim_paciente, "id_paciente", "id_paciente", "dim_paciente")
        validar_llaves_huerfanas(fact_citas, dim_doctor, "id_doctor", "id_doctor", "dim_doctor")
        validar_llaves_huerfanas(fact_citas, dim_tiempo, "id_tiempo", "id_tiempo", "dim_tiempo")

        tablas_gold = {
            "dim_paciente": dim_paciente,
            "dim_doctor": dim_doctor,
            "dim_tiempo": dim_tiempo,
            "fact_citas": fact_citas,
        }

        for nombre, df in tablas_gold.items():
            ruta_destino = GOLD_DIR / nombre
            df.write.mode("overwrite").parquet(str(ruta_destino))
            logger.info(f"{nombre:<15} | {df.count():>6,} filas -> {ruta_destino}")

    finally:
        spark.stop()

    logger.info("=== Modelado Gold finalizado ===")


if __name__ == "__main__":
    main()
