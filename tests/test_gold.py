"""
test_gold.py

Valida el modelo dimensional en Gold: integridad referencial (sin llaves
foraneas huerfanas), presencia del registro "Fecha Desconocida" en
dim_tiempo, y que fact_citas nunca tenga id_tiempo nulo.

Requiere que Gold ya exista (correr src/transform_gold.py antes).
"""

from pathlib import Path

from pyspark.sql import functions as F

BASE_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = BASE_DIR / "data" / "gold"

ID_TIEMPO_DESCONOCIDO = -1


def test_gold_existe():
    """Verifica que las 4 tablas de Gold se hayan generado."""
    for tabla in ["dim_paciente", "dim_doctor", "dim_tiempo", "fact_citas"]:
        ruta = GOLD_DIR / tabla
        assert ruta.exists(), f"No existe la tabla Gold: {tabla}. Corre transform_gold.py primero."


def test_fact_citas_sin_id_tiempo_nulo(spark):
    """
    Regla clave del diseño: ninguna fila de fact_citas debe tener id_tiempo
    nulo. Las citas sin fecha valida deben enlazarse al registro especial
    id_tiempo = -1, nunca quedar como NULL.
    """
    fact = spark.read.parquet(str(GOLD_DIR / "fact_citas"))
    n_nulos = fact.filter(F.col("id_tiempo").isNull()).count()
    assert n_nulos == 0, (
        f"Hay {n_nulos} filas en fact_citas con id_tiempo NULL. "
        f"Deberian estar enlazadas a id_tiempo={ID_TIEMPO_DESCONOCIDO}."
    )


def test_dim_tiempo_tiene_registro_desconocido(spark):
    """dim_tiempo debe incluir el registro especial para fechas desconocidas."""
    dim_tiempo = spark.read.parquet(str(GOLD_DIR / "dim_tiempo"))
    existe = dim_tiempo.filter(F.col("id_tiempo") == ID_TIEMPO_DESCONOCIDO).count() == 1
    assert existe, (
        f"dim_tiempo no tiene el registro id_tiempo={ID_TIEMPO_DESCONOCIDO} "
        f"('Fecha Desconocida')."
    )


def test_integridad_referencial_doctor(spark):
    """Toda cita en fact_citas debe apuntar a un doctor que existe en dim_doctor."""
    fact = spark.read.parquet(str(GOLD_DIR / "fact_citas"))
    dim_doctor = spark.read.parquet(str(GOLD_DIR / "dim_doctor"))

    huerfanas = fact.join(dim_doctor, "id_doctor", "left_anti").count()
    assert huerfanas == 0, f"{huerfanas} citas apuntan a un id_doctor que no existe en dim_doctor"


def test_integridad_referencial_paciente(spark):
    """Toda cita en fact_citas debe apuntar a un paciente que existe en dim_paciente."""
    fact = spark.read.parquet(str(GOLD_DIR / "fact_citas"))
    dim_paciente = spark.read.parquet(str(GOLD_DIR / "dim_paciente"))

    huerfanas = fact.join(dim_paciente, "id_paciente", "left_anti").count()
    assert huerfanas == 0, f"{huerfanas} citas apuntan a un id_paciente que no existe en dim_paciente"


def test_integridad_referencial_tiempo(spark):
    """Toda cita en fact_citas debe apuntar a un id_tiempo que existe en dim_tiempo."""
    fact = spark.read.parquet(str(GOLD_DIR / "fact_citas"))
    dim_tiempo = spark.read.parquet(str(GOLD_DIR / "dim_tiempo"))

    huerfanas = fact.join(dim_tiempo, "id_tiempo", "left_anti").count()
    assert huerfanas == 0, f"{huerfanas} citas apuntan a un id_tiempo que no existe en dim_tiempo"


def test_dim_paciente_no_expone_datos_sensibles(spark):
    """
    Regla de diseño documentada en el README: dim_paciente no debe incluir
    email ni telefono, ya que Gold es para analitica, no operacion diaria.
    """
    dim_paciente = spark.read.parquet(str(GOLD_DIR / "dim_paciente"))
    columnas_prohibidas = {"email", "telefono"}
    columnas_presentes = columnas_prohibidas & set(dim_paciente.columns)
    assert not columnas_presentes, (
        f"dim_paciente expone columnas sensibles: {columnas_presentes}"
    )
