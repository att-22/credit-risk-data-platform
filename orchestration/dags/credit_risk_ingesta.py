"""Ingesta: CSV de Home Credit -> capa bronze (Parquet).

    verify_data -> ingest_bronze -> publicar_bronze  ==>  Asset home_credit_bronze

Al terminar, `publicar_bronze` registra un evento en el Asset
`home_credit_bronze` con el conjunto de datos procesado (`datos`) en sus
metadatos. Ese evento es el que dispara el DAG `credit_risk_transformacion`:
este DAG no necesita saber que existe el otro, solo que ha actualizado bronze.

Parámetro `datos`: "muestra" (la sintética de la imagen) o "completo" (los CSV
de Kaggle montados en /data). Las ejecuciones programadas usan la muestra.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from comun import BIN, BRONZE, DATOS_VALIDOS, entorno

ENTORNO = entorno("params.datos")


@dag(
    dag_id="credit_risk_ingesta",
    description="CSV -> bronze; publica el Asset home_credit_bronze",
    doc_md=__doc__,
    schedule="0 6 * * *",  # todos los días a las 06:00 UTC
    start_date=datetime(2026, 10, 1),
    catchup=False,  # no recuperar los días pasados desde start_date
    max_active_runs=1,  # una ejecución a la vez: dos escribiendo en el mismo lago chocarían
    params={
        "datos": Param(
            "muestra",
            enum=DATOS_VALIDOS,
            description="Muestra sintética (rápida) o datos completos de Kaggle.",
        )
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["credit-risk", "ingesta"],
)
def credit_risk_ingesta():
    verify_data = BashOperator(
        task_id="verify_data",
        bash_command=f"{BIN}/verify-data",
        env=ENTORNO,
        append_env=True,  # además de ENTORNO, conservar el entorno del contenedor
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{BIN}/ingest-bronze",
        env=ENTORNO,
        append_env=True,
    )

    @task(outlets=[BRONZE])
    def publicar_bronze(params=None, outlet_events=None):
        """Registra el evento del Asset con el conjunto de datos en sus metadatos."""
        outlet_events[BRONZE].extra = {"datos": params["datos"]}

    verify_data >> ingest_bronze >> publicar_bronze()


credit_risk_ingesta()
