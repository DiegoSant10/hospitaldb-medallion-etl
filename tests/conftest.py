"""
conftest.py

Fixtures compartidas para los tests del pipeline.
"""

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"


@pytest.fixture(scope="session")
def spark():
    """
    SparkSession compartida entre todos los tests (scope='session' evita
    crear/destruir una JVM por cada test individual, lo cual seria lento).
    """
    session = (
        SparkSession.builder
        .appName("HospitalDB-Tests")
        .master("local[*]")
        .getOrCreate()
    )
    yield session
    session.stop()
