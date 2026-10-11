"""Pipeline completo de la plataforma: CSV -> bronze -> dbt (staging, intermediate, gold).

Cada tarea ejecuta un comando del proyecto, instalado en su propio entorno
virtual dentro de la imagen (ver orchestration/Dockerfile):

    verify_data -> ingest_bronze -> dbt_source_freshness -> dbt_build

Parámetro `datos` (se elige al lanzar el DAG con "Trigger"):
- "muestra": la muestra sintética incluida en la imagen (~10 s en total).
- "completo": los CSV de Kaggle montados en /data (varios minutos).
Cada conjunto escribe en su propio lago (/opt/airflow/lake/<datos>), así una
ejecución con la muestra nunca pisa los datos completos.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

PROYECTO = "/opt/credit-risk"
BIN = f"{PROYECTO}/.venv/bin"

# Variables de entorno de las tareas. Son plantillas Jinja: Airflow las rellena
# en cada ejecución con el valor del parámetro `datos`.
ENTORNO = {
    "DATA_DIR": "{{ '/data' if params.datos == 'completo' else '" + PROYECTO + "/muestra' }}",
    "LAKE_DIR": "/opt/airflow/lake/{{ params.datos }}",
    "OUTPUTS_DIR": "/opt/airflow/lake/{{ params.datos }}/outputs",
}


@dag(
    dag_id="credit_risk_pipeline",
    description="CSV -> bronze -> dbt build (staging, intermediate, gold)",
    doc_md=__doc__,
    schedule=None,  # de momento solo se lanza a mano
    start_date=datetime(2026, 10, 1),
    catchup=False,
    params={
        "datos": Param(
            "muestra",
            enum=["muestra", "completo"],
            description="Muestra sintética (rápida) o datos completos de Kaggle.",
        )
    },
    default_args={
        "retries": 1,  # un fallo puntual (p. ej. un bloqueo de fichero) se reintenta
        "retry_delay": timedelta(seconds=30),
        "env": ENTORNO,
        "append_env": True,  # además de ENTORNO, conservar el entorno del contenedor
    },
    tags=["credit-risk", "dbt"],
)
def credit_risk_pipeline():
    verify_data = BashOperator(
        task_id="verify_data",
        bash_command=f"{BIN}/verify-data",
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{BIN}/ingest-bronze",
    )

    # Si la ingesta no ha refrescado bronze, se para aquí antes de transformar.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {PROYECTO}/transform && {BIN}/dbt source freshness",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROYECTO}/transform && {BIN}/dbt build",
    )

    verify_data >> ingest_bronze >> dbt_source_freshness >> dbt_build


credit_risk_pipeline()
