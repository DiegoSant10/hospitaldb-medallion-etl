"""
extract.py

Capa de EXTRACCION del pipeline Medallion.

Lee los datos "de origen" (data/source_mock/, que simulan lo que vendria
de HospitalDB_ITVH en MariaDB) y los guarda tal cual, sin transformar,
en la capa Bronze como archivos Parquet particionados por fecha de carga.

Decision de diseño: full load (se recarga todo cada vez que se corre).
En un entorno de produccion real, esto se haria via conexion JDBC a
MariaDB con carga incremental (usando una columna updated_at o un
mecanismo de CDC), para no tener que releer todas las tablas completas
en cada corrida. Aqui se documenta esa limitacion a proposito.

Uso:
    python src/extract.py
"""

import logging
import time
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("extract")

BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_DIR = BASE_DIR / "data" / "source_mock"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

# Nombre logico de la tabla -> nombre del archivo CSV origen
TABLAS = {
    "pacientes": "pacientes.csv",
    "doctores": "doctores.csv",
    "citas": "citas.csv",
    "resultados_lab": "resultados_lab.csv",
}

LOAD_DATE = date.today().isoformat()  # ej. "2026-07-14"


def crear_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HospitalDB-Extract-Bronze")
        .master("local[*]")
        .getOrCreate()
    )


def extraer_tabla(spark: SparkSession, nombre_tabla: str, archivo_csv: str) -> int:
    """
    Extrae una tabla desde el CSV origen y la guarda en Bronze.
    Retorna el numero de filas extraidas (para el resumen de log).
    """
    ruta_origen = SOURCE_DIR / archivo_csv

    if not ruta_origen.exists():
        logger.error(f"No se encontro el archivo origen: {ruta_origen}")
        return 0

    inicio = time.time()

    # header=True porque nuestros CSV traen encabezado
    # inferSchema=True porque en Bronze no forzamos tipos todavia:
    # el objetivo de Bronze es preservar el dato "tal como llego"
    df = spark.read.csv(str(ruta_origen), header=True, inferSchema=True)

    n_filas = df.count()

    ruta_destino = BRONZE_DIR / nombre_tabla / f"load_date={LOAD_DATE}"

    df.write.mode("overwrite").parquet(str(ruta_destino))

    duracion = time.time() - inicio
    logger.info(
        f"{nombre_tabla:<20} | {n_filas:>6,} filas | {duracion:>5.2f}s | -> {ruta_destino}"
    )
    return n_filas


def main():
    logger.info("=== Iniciando extraccion: source_mock -> Bronze ===")
    logger.info(f"Fecha de carga (load_date): {LOAD_DATE}")

    spark = crear_spark_session()

    resumen = {}
    try:
        for nombre_tabla, archivo_csv in TABLAS.items():
            resumen[nombre_tabla] = extraer_tabla(spark, nombre_tabla, archivo_csv)
    finally:
        spark.stop()

    total_filas = sum(resumen.values())
    logger.info("=== Extraccion finalizada ===")
    logger.info(f"Total de filas extraidas: {total_filas:,}")

    for tabla, filas in resumen.items():
        estado = "OK" if filas > 0 else "FALLO (0 filas)"
        logger.info(f"  - {tabla}: {filas:,} filas [{estado}]")


if __name__ == "__main__":
    main()
