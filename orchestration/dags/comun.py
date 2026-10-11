"""Piezas compartidas por los DAGs de credit-risk: rutas, Assets y entorno.

No define ningún DAG (está en .airflowignore para que el dag-processor no lo
lea como tal); los DAGs lo importan porque Airflow añade la carpeta dags/ al
path de Python.
"""

from __future__ import annotations

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
