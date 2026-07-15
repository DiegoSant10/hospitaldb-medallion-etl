"""
run_pipeline.py

Orquestador del pipeline completo Medallion: Bronze -> Silver -> Gold.

Corre las 3 fases en orden, respetando dependencias: si una fase falla,
las siguientes NO se ejecutan (no tiene sentido transformar datos que
no se extrajeron correctamente, o modelar en Gold datos que no pasaron
por limpieza en Silver).

Esto es una version simplificada, de un solo archivo, de lo que en
produccion se haria con un orquestador real como Airflow o Fabric
Pipelines: definicion de tareas, dependencias entre ellas, reintentos,
y alertas si algo falla. Aqui se documenta esa limitacion a proposito,
como siguiente paso natural del proyecto.

Uso:
    python src/run_pipeline.py
"""

import logging
import subprocess
import sys
import time
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("run_pipeline")

BASE_DIR = Path(__file__).resolve().parent
PYTHON_EXE = sys.executable  # usa el mismo interprete de Python que corre este script

# Orden de ejecucion: cada fase depende de que la anterior haya terminado bien
FASES = [
    ("Extraccion (source_mock -> Bronze)", BASE_DIR / "extract.py"),
    ("Transformacion (Bronze -> Silver)", BASE_DIR / "transform_silver.py"),
    ("Modelado dimensional (Silver -> Gold)", BASE_DIR / "transform_gold.py"),
]


def ejecutar_fase(nombre: str, script: Path) -> tuple[bool, float]:
    """
    Ejecuta un script de fase como subproceso independiente.
    Retorna (exito, duracion_segundos).

    Se usa subprocess en vez de importar y llamar las funciones directamente
    porque cada fase crea y cierra su propia SparkSession; mezclarlas en el
    mismo proceso puede causar conflictos de contexto de Spark.
    """
    logger.info(f"--- Iniciando fase: {nombre} ---")
    inicio = time.time()

    resultado = subprocess.run(
        [PYTHON_EXE, str(script)],
        capture_output=False,  # deja que el log de cada fase se vea en vivo
    )

    duracion = time.time() - inicio
    exito = resultado.returncode == 0

    if exito:
        logger.info(f"--- Fase completada: {nombre} ({duracion:.1f}s) ---")
    else:
        logger.error(f"--- Fase FALLIDA: {nombre} (codigo {resultado.returncode}) ---")

    return exito, duracion


def main():
    logger.info("=" * 60)
    logger.info("INICIANDO PIPELINE COMPLETO: Bronze -> Silver -> Gold")
    logger.info("=" * 60)

    inicio_total = time.time()
    resumen = []

    for nombre, script in FASES:
        if not script.exists():
            logger.error(f"No se encontro el script: {script}")
            resumen.append((nombre, False, 0.0))
            break

        exito, duracion = ejecutar_fase(nombre, script)
        resumen.append((nombre, exito, duracion))

        if not exito:
            logger.error(
                f"Pipeline detenido: '{nombre}' fallo. "
                f"Las fases siguientes no se ejecutaron."
            )
            break

    duracion_total = time.time() - inicio_total

    logger.info("=" * 60)
    logger.info("RESUMEN DEL PIPELINE")
    logger.info("=" * 60)
    for nombre, exito, duracion in resumen:
        estado = "OK" if exito else "FALLO"
        logger.info(f"  [{estado:>5}] {nombre} ({duracion:.1f}s)")

    logger.info(f"Tiempo total: {duracion_total:.1f}s")

    todas_exitosas = all(exito for _, exito, _ in resumen)
    if todas_exitosas:
        logger.info("Pipeline completado exitosamente. Datos listos en data/gold/")
    else:
        logger.error("Pipeline finalizado con errores. Revisa el log de la fase fallida arriba.")
        sys.exit(1)


if __name__ == "__main__":
    main()
