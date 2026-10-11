"""Transformación: bronze -> dbt (staging, intermediate, gold).

    [Asset home_credit_bronze] ==> resolver_datos -> dbt_source_freshness -> dbt
                                   -> publicar_mart ==> Asset mart_scoring__clientes

Se ejecuta SOLO cuando hay datos nuevos en bronze: está programado sobre el
Asset `home_credit_bronze` (lo actualiza `credit_risk_ingesta`), no sobre un
horario. El grupo `dbt` lo genera astronomer-cosmos a partir del manifest de
dbt: una tarea por modelo con `dbt build --select <modelo>`.

Los agregados y el mart son incrementales: cada ejecución solo recalcula los
clientes con datos nuevos. El parámetro `reconstruir` fuerza --full-refresh.

¿Muestra o completo? `resolver_datos` lo lee de los metadatos del evento que
disparó la ejecución. Si se lanza a mano (sin evento), usa el parámetro `datos`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from comun import BIN, BRONZE, DATOS_VALIDOS, DBT_DIR, MART, POOL_DUCKDB, entorno
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, TestBehavior

# Las tareas leen el conjunto de datos del resultado (XCom) de resolver_datos.
ENTORNO = entorno("ti.xcom_pull(task_ids='resolver_datos')")


@dag(
    dag_id="credit_risk_transformacion",
    description="bronze -> dbt (una tarea por modelo); se dispara con el Asset de bronze",
    doc_md=__doc__,
    schedule=[BRONZE],  # programación por datos: cuando se actualice bronze
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,  # una ejecución a la vez: dos escribiendo en el mismo lago chocarían
    params={
        "datos": Param(
            "muestra",
            enum=DATOS_VALIDOS,
            description="Solo para ejecuciones manuales (sin evento de bronze).",
        ),
        "reconstruir": Param(
            False,
            type="boolean",
            description="Reconstruir los modelos incrementales desde cero (--full-refresh).",
        ),
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["credit-risk", "dbt"],
)
def credit_risk_transformacion():
    @task
    def resolver_datos(params=None, triggering_asset_events=None) -> str:
        """Conjunto de datos a transformar: el del último evento de bronze o el parámetro."""
        eventos = triggering_asset_events.get(BRONZE, []) if triggering_asset_events else []
        datos = eventos[-1].extra.get("datos") if eventos else params["datos"]
        if datos not in DATOS_VALIDOS:
            raise ValueError(f"Conjunto de datos desconocido en el evento: {datos!r}")
        print(f"Conjunto de datos: {datos} ({'evento de bronze' if eventos else 'manual'})")
        return datos

    # Si bronze no está al día, se para aquí antes de transformar.
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
            # Un Asset por modelo sería demasiado detalle: publicamos solo el mart.
            emit_datasets=False,
        ),
        operator_args={
            "append_env": True,
            "pool": POOL_DUCKDB,
            # Los modelos incrementales solo procesan lo nuevo; con reconstruir=True
            # se recalculan enteros (Cosmos convierte el texto "True"/"False" a booleano).
            "full_refresh": "{{ params.reconstruir }}",
        },
    )

    @task(outlets=[MART])
    def publicar_mart(datos: str, outlet_events=None):
        """Registra que el mart está actualizado (lo podrán consumir otros DAGs)."""
        outlet_events[MART].extra = {"datos": datos}

    datos = resolver_datos()
    datos >> dbt_source_freshness >> dbt >> publicar_mart(datos)


credit_risk_transformacion()
