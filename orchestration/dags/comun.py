"""Piezas compartidas por los DAGs de credit-risk: rutas, Assets y entorno.

No define ningún DAG (está en .airflowignore para que el dag-processor no lo
lea como tal); los DAGs lo importan porque Airflow añade la carpeta dags/ al
path de Python.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.sdk import Asset

PROYECTO = Path("/opt/credit-risk")
BIN = PROYECTO / ".venv" / "bin"
DBT_DIR = PROYECTO / "transform"

# Pool de 1 hueco (lo crea airflow-init en compose.yaml): DuckDB solo admite un
# proceso escribiendo, así que las tareas que abren warehouse.duckdb van de una en una.
POOL_DUCKDB = "duckdb"

# Assets: los DATOS que producen y consumen los DAGs. Un DAG declara que
# actualiza un Asset (outlets) y otro DAG se programa "cuando cambie ese Asset"
# (schedule=[...]). Airflow une ambos en el grafo de Assets.
BRONZE = Asset(name="home_credit_bronze", group="credit_risk")
MART = Asset(name="mart_scoring__clientes", group="credit_risk")

DATOS_VALIDOS = ["muestra", "completo"]

# Periodo de entregas que genera el sistema origen simulado (simulate-source:
# 30 días desde el 1 de septiembre). La ingesta diaria se programa en ese rango.
PRIMERA_ENTREGA = datetime(2026, 9, 1)
ULTIMA_ENTREGA = datetime(2026, 9, 30)

# Ficheros de cada entrega (contrato con el origen). Es la misma lista que
# credit_risk.config.TABLES, que aquí no se puede importar: el paquete vive en
# otro entorno virtual. Un test comprueba que no se desincronizan.
FICHEROS_ENTREGA = [
    "application_train.csv.gz",
    "application_test.csv.gz",
    "bureau.csv.gz",
    "bureau_balance.csv.gz",
    "previous_application.csv.gz",
    "POS_CASH_balance.csv.gz",
    "installments_payments.csv.gz",
    "credit_card_balance.csv.gz",
]


def entorno(datos: str) -> dict[str, str]:
    """Variables de entorno de las tareas para un conjunto de datos.

    `datos` es una expresión Jinja que Airflow evalúa en cada ejecución (por
    ejemplo "params.datos"), así que el mismo DAG sirve para la muestra y para
    los datos completos. Cada conjunto escribe en su propio lago.
    """
    muestra = PROYECTO / "muestra"
    return {
        "DATA_DIR": "{{ '/data' if " + datos + f" == 'completo' else '{muestra}' }}}}",
        "LAKE_DIR": "/opt/airflow/lake/{{ " + datos + " }}",
        "OUTPUTS_DIR": "/opt/airflow/lake/{{ " + datos + " }}/outputs",
    }


def landing(datos: str) -> Path:
    """Landing zone de un conjunto de datos (LANDING_DIR por defecto: LAKE_DIR/landing)."""
    return Path("/opt/airflow/lake") / datos / "landing"
