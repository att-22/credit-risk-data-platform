"""Condiciones para ejecutar los tests de Spark (PySpark, delta-spark y Java).

Está en su propio módulo (y no en conftest.py) porque hay dos conftest.py en
tests/: importar "conftest" por nombre podría traer el que no es.
"""

import os
import shutil

import pytest


def saltar_sin_spark() -> None:
    """Salta el módulo que la llama si no se puede arrancar Spark con Delta."""
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    if not (os.getenv("JAVA_HOME") or shutil.which("java")):
        pytest.skip("Spark necesita Java", allow_module_level=True)
