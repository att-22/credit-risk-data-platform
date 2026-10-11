"""Configuración común de los tests de Spark: UNA sesión para todos.

Spark corre en una JVM, y los jars disponibles (el classpath) se fijan al
arrancarla. Si un módulo de tests creara una sesión sin Delta y otro la pidiera
después con Delta, Spark reutilizaría la JVM ya arrancada y los jars de Delta
llegarían tarde ("Cannot find catalog plugin class ... DeltaCatalog"). Por eso
hay una sola sesión, con Delta, compartida por todos los tests (scope="session").

Cada módulo de tests decide si se salta (requisitos_spark.saltar_sin_spark): un
salto dentro de conftest.py pytest lo trataría como un error.
"""

import os
import sys
from pathlib import Path

import pytest

# Los trabajos de Spark no son parte del paquete credit_risk: se importan por ruta
# (en el repositorio, spark_jobs/; en el contenedor de Spark, donde se monten)
TRABAJOS = Path(__file__).resolve().parents[2] / "spark_jobs"
sys.path.insert(0, os.getenv("SPARK_JOBS_DIR", str(TRABAJOS)))


@pytest.fixture(scope="session")
def spark():
    from agregados import sesion  # aquí dentro: este fichero se importa aunque no haya Spark

    sesion_spark = sesion("tests", particiones=2, delta=True)
    yield sesion_spark
    sesion_spark.stop()
