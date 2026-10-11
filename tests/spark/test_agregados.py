"""Tests de los trabajos de Spark (spark_jobs/agregados.py) con datos mínimos.

Necesitan PySpark y Java: se saltan si no están (p. ej. el job de tests normal
de la CI o un Windows sin Java). Se ejecutan en el job "Spark" de la CI y, en
local, dentro del contenedor de Spark:

    docker compose run --rm spark python3 -m pytest /opt/spark-tests
"""

import os
import shutil
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if not (os.getenv("JAVA_HOME") or shutil.which("java")):
    pytest.skip("Spark necesita Java", allow_module_level=True)

# Los trabajos de Spark no son parte del paquete credit_risk: se importan por ruta
# (en el repositorio, spark_jobs/; en el contenedor de Spark, donde se monten)
TRABAJOS = Path(__file__).resolve().parents[2] / "spark_jobs"
sys.path.insert(0, os.getenv("SPARK_JOBS_DIR", str(TRABAJOS)))

from agregados import (  # noqa: E402
    agregar_por_cliente,
    bureau_balance_por_credito,
    con_comportamiento,
    sesion,
)


@pytest.fixture(scope="module")
def spark():
    # Arrancar Spark cuesta segundos: una sola sesión para todos los tests del módulo
    sesion_spark = sesion("tests", particiones=2)
    yield sesion_spark
    sesion_spark.stop()


def test_resume_el_historial_de_cada_credito(spark):
    balance = spark.createDataFrame(
        [(10, 0, "0"), (10, -1, "1"), (10, -2, "C"), (10, -3, "5"), (20, 0, "X")],
        "SK_ID_BUREAU long, MONTHS_BALANCE long, STATUS string",
    )

    filas = {r.SK_ID_BUREAU: r for r in bureau_balance_por_credito(balance).collect()}

    assert filas[10].BB_MESES == 4
    assert filas[10].BB_PROP_IMPAGO == pytest.approx(0.5)  # "1" y "5" son impago; "0" y "C" no
    assert filas[20].BB_PROP_IMPAGO == 0.0  # "X" (desconocido) no cuenta como impago


def test_comportamiento_de_pago_con_casos_limite(spark):
    pagos = spark.createDataFrame(
        [
            (1, -10.0, -12.0, 100.0, 100.0),
            (1, -10.0, -5.0, 0.0, 50.0),
            (1, -10.0, None, 80.0, None),
        ],
        "SK_ID_CURR long, DAYS_INSTALMENT double, DAYS_ENTRY_PAYMENT double, "
        "AMT_INSTALMENT double, AMT_PAYMENT double",
    )

    filas = con_comportamiento(pagos).collect()

    assert (filas[0].DIAS_ADELANTO, filas[0].DIAS_RETRASO) == (2.0, 0.0)  # pagó 2 días antes
    assert (filas[1].DIAS_RETRASO, filas[1].PAGO_RATIO) == (5.0, None)  # cuota 0: ratio NULL
    assert filas[2].DIAS_RETRASO is None  # sin pago: NULL, no 0


def test_agrega_solo_las_columnas_numericas_que_no_son_claves(spark):
    df = spark.createDataFrame(
        [(1, 7, 10.0, "a"), (1, 8, 30.0, "b"), (2, 9, 5.0, "c")],
        "SK_ID_CURR long, SK_ID_PREV long, IMPORTE double, TEXTO string",
    )

    agregado = agregar_por_cliente(df, "SK_ID_CURR", "T")
    fila = {r.SK_ID_CURR: r for r in agregado.collect()}[1]

    assert sorted(agregado.columns) == sorted(
        ["SK_ID_CURR", "T_NUM_REGISTROS"]
        + [f"T_IMPORTE_{s}" for s in ("MEAN", "MAX", "MIN", "SUM")]
    )  # ni SK_ID_PREV (identificador) ni TEXTO (no numérica)
    assert (fila.T_NUM_REGISTROS, fila.T_IMPORTE_MEAN, fila.T_IMPORTE_SUM) == (2, 20.0, 40.0)
