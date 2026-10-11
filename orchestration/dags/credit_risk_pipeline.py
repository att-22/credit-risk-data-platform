"""Pipeline completo de la plataforma: CSV -> bronze -> dbt (staging, intermediate, gold).

    verify_data -> ingest_bronze -> dbt_source_freshness -> dbt (una tarea por modelo)

Las tareas de Python ejecutan comandos del proyecto, instalado en su propio
entorno virtual dentro de la imagen (ver orchestration/Dockerfile). El grupo
`dbt` lo genera astronomer-cosmos a partir del manifest de dbt: una tarea por
modelo que ejecuta `dbt build --select <modelo>` (el modelo y sus tests), con
las mismas dependencias que el linaje de dbt.

Parámetro `datos` (se elige al lanzar el DAG con "Trigger"):
- "muestra": la muestra sintética incluida en la imagen.
- "completo": los CSV de Kaggle montados en /data.
Cada conjunto escribe en su propio lago (/opt/airflow/lake/<datos>), así una
ejecución con la muestra nunca pisa los datos completos.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, TestBehavior

PROYECTO = Path("/opt/credit-risk")
BIN = PROYECTO / ".venv" / "bin"
DBT_DIR = PROYECTO / "transform"

# Variables de entorno de las tareas. Son plantillas Jinja: Airflow las rellena
# en cada ejecución con el valor del parámetro `datos`.
ENTORNO = {
    "DATA_DIR": "{{ '/data' if params.datos == 'completo' else '" + f"{PROYECTO}/muestra" + "' }}",
    "LAKE_DIR": "/opt/airflow/lake/{{ params.datos }}",
    "OUTPUTS_DIR": "/opt/airflow/lake/{{ params.datos }}/outputs",
}

# Pool de 1 hueco (lo crea airflow-init en compose.yaml): DuckDB solo admite un
# proceso escribiendo, así que las tareas que abren warehouse.duckdb van de una en una.
POOL_DUCKDB = "duckdb"


@dag(
    dag_id="credit_risk_pipeline",
    description="CSV -> bronze -> dbt (una tarea por modelo)",
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
    },
    tags=["credit-risk", "dbt"],
)
def credit_risk_pipeline():
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

    # Si la ingesta no ha refrescado bronze, se para aquí antes de transformar.
    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {BIN}/dbt source freshness",
        env=ENTORNO,
        append_env=True,
        pool=POOL_DUCKDB,
    )

    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_DIR,
            # Generado con `dbt parse` al construir la imagen: leerlo es instantáneo.
            manifest_path=DBT_DIR / "target" / "manifest.json",
            # Los paquetes (dbt_utils, dbt_expectations) ya están en la imagen.
            install_dbt_deps=False,
            copy_dbt_packages=True,
            # Mismas variables que las tareas de Python (plantillas con params.datos).
            env_vars=ENTORNO,
        ),
        profile_config=ProfileConfig(
            profile_name="credit_risk",
            target_name="dev",
            profiles_yml_filepath=DBT_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            # dbt vive en el venv del proyecto, no en el de Airflow: se llama
            # como un proceso aparte (SUBPROCESS) por su ruta.
            dbt_executable_path=str(BIN / "dbt"),
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            # BUILD: una tarea por modelo con `dbt build` (modelo + sus tests).
            test_behavior=TestBehavior.BUILD,
            # Los tests que dependen de VARIOS modelos (relationships, tests singulares
            # como el de filas del mart) van en una tarea propia que espera a todos sus
            # padres. Si no, `dbt build --select <modelo>` los lanzaría con el primero
            # de ellos, antes de que existieran los demás.
            should_detach_multiple_parents_tests=True,
            # Cosmos puede publicar cada modelo como Asset de Airflow; lo veremos más adelante.
            emit_datasets=False,
        ),
        operator_args={
            "append_env": True,
            "pool": POOL_DUCKDB,
        },
    )

    verify_data >> ingest_bronze >> dbt_source_freshness >> dbt


credit_risk_pipeline()
