"""
test_silver.py

Valida que la capa Silver cumpla las reglas de calidad definidas en
docs/data_dictionary.md: sin duplicados por llave primaria, y presencia
correcta de la columna data_quality_flag.

Requiere que Silver ya exista (correr src/transform_silver.py antes).
"""

from pathlib import Path

from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"

TABLAS_Y_LLAVES = {
    "pacientes": "id_paciente",
    "doctores": "id_doctor",
    "citas": "id_cita",
    "resultados_lab": "id_resultado",
}


def test_silver_existe():
    """Verifica que las 4 tablas de Silver se hayan generado."""
    for tabla in TABLAS_Y_LLAVES:
        ruta = SILVER_DIR / tabla
        assert ruta.exists(), f"No existe la tabla Silver: {tabla}. Corre transform_silver.py primero."


def test_sin_duplicados_por_llave_primaria(spark):
    """
    Regla de docs/data_dictionary.md: ninguna tabla Silver debe tener
    filas duplicadas por su llave primaria.
    """
    for tabla, llave in TABLAS_Y_LLAVES.items():
        df = spark.read.parquet(str(SILVER_DIR / tabla))
        total = df.count()
        distintos = df.select(llave).distinct().count()
        assert total == distintos, (
            f"{tabla}: hay {total - distintos} duplicados por {llave} "
            f"(total={total}, distintos={distintos})"
        )


def test_columna_data_quality_flag_existe():
    """Todas las tablas Silver deben tener la columna estandar data_quality_flag."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()

    for tabla in TABLAS_Y_LLAVES:
        df = spark.read.parquet(str(SILVER_DIR / tabla))
        assert "data_quality_flag" in df.columns, (
            f"{tabla} no tiene la columna data_quality_flag"
        )


def test_citas_con_fecha_nula_estan_marcadas(spark):
    """
    Regla especifica: toda cita con fecha_hora nula debe tener el flag
    'missing_fecha_hora' en data_quality_flag (no debe perderse la marca).
    """
    df = spark.read.parquet(str(SILVER_DIR / "citas"))

    citas_sin_fecha = df.filter(F.col("fecha_hora").isNull())
    citas_sin_fecha_sin_flag = citas_sin_fecha.filter(
        ~F.array_contains(F.col("data_quality_flag"), "missing_fecha_hora")
    )

    n_sin_flag = citas_sin_fecha_sin_flag.count()
    assert n_sin_flag == 0, (
        f"Hay {n_sin_flag} citas con fecha_hora nula que NO tienen el flag "
        f"missing_fecha_hora — se estaria perdiendo trazabilidad."
    )


def test_no_se_perdieron_filas_de_bronze_a_silver(spark):
    """
    Regla de negocio del proyecto: como la filosofia es "marcar, no descartar",
    el conteo de filas de citas en Silver no deberia ser menor al de Bronze
    (excepto por duplicados legitimos, que si se eliminan).
    """
    BRONZE_DIR = BASE_DIR / "data" / "bronze"
    particiones = sorted((BRONZE_DIR / "citas").glob("load_date=*"))
    assert particiones, "No hay datos en Bronze para comparar. Corre extract.py primero."

    bronze_citas = spark.read.parquet(str(particiones[-1]))
    silver_citas = spark.read.parquet(str(SILVER_DIR / "citas"))

    n_bronze = bronze_citas.select("id_cita").distinct().count()
    n_silver = silver_citas.count()

    assert n_silver == n_bronze, (
        f"Silver tiene {n_silver} citas pero Bronze (sin duplicados) tiene "
        f"{n_bronze}. Se perdieron filas que no deberian haberse descartado."
    )
