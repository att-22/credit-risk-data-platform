"""Tests de integridad de los DAGs de credit-risk.

No ejecutan el pipeline: cargan los DAGs como lo haría el dag-processor de
Airflow y comprueban su estructura. Detectan en la CI errores que, si no, solo
se verían al arrancar Airflow (un import roto, una dependencia mal puesta, una
tarea que se salta el pool de DuckDB, un Asset que no conecta los DAGs...).

Necesitan Airflow y Cosmos, así que se ejecutan DENTRO de la imagen de Airflow
(ver el job "Airflow DAGs" en .github/workflows/ci.yml), no con el pytest del
proyecto (pyproject.toml solo recoge la carpeta tests/).
"""

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from comun import BRONZE, FICHEROS_ENTREGA, MART

DAGS_DIR = Path("/opt/airflow/dags")
MANIFEST = Path("/opt/credit-risk/transform/target/manifest.json")
ORIGEN = "credit_risk_origen"
INGESTA = "credit_risk_ingesta"
TRANSFORMACION = "credit_risk_transformacion"
TODOS = [ORIGEN, INGESTA, TRANSFORMACION]


@pytest.fixture(scope="module")
def dagbag():
    # scope="module": los DAGs se cargan una sola vez para todos los tests
    return DagBag(dag_folder=DAGS_DIR)


@pytest.fixture(scope="module")
def ingesta(dagbag):
    return dagbag.dags[INGESTA]


@pytest.fixture(scope="module")
def transformacion(dagbag):
    return dagbag.dags[TRANSFORMACION]


@pytest.fixture(scope="module")
def tareas_duckdb(transformacion):
    """Tareas que abren warehouse.duckdb: freshness y todo el grupo de Cosmos."""
    return [t for t in transformacion.tasks if t.task_id.startswith("dbt")]


def test_los_dags_se_importan_sin_errores(dagbag):
    assert dagbag.import_errors == {}
    # comun.py está en .airflowignore: solo hay los tres DAGs
    assert set(dagbag.dag_ids) == set(TODOS)


# --- Assets: la conexión entre los dos DAGs -----------------------------------


def test_la_ingesta_publica_bronze_al_final(ingesta):
    publicar = ingesta.get_task("publicar_bronze")
    assert BRONZE in publicar.outlets
    assert {"esperar_entrega", "ingest_bronze"} <= publicar.get_flat_relative_ids(upstream=True)


# --- Ingesta diaria -----------------------------------------------------------


def test_la_ingesta_es_diaria_en_el_periodo_simulado(ingesta):
    assert ingesta.catchup is True
    assert ingesta.start_date == datetime(2026, 9, 1, tzinfo=UTC)
    assert ingesta.end_date == datetime(2026, 9, 30, tzinfo=UTC)
    assert ingesta.timetable.expression == "0 0 * * *"  # "@daily" en formato cron


def test_la_ingesta_espera_la_entrega_e_ingiere_su_dia(ingesta):
    sensor = ingesta.get_task("esperar_entrega")
    assert sensor.mode == "reschedule"  # no ocupa un hueco mientras espera
    assert sensor.downstream_task_ids == {"ingest_bronze"}
    assert "--fecha {{ ds }}" in ingesta.get_task("ingest_bronze").bash_command


def test_los_ficheros_esperados_coinciden_con_el_proyecto():
    # comun.py no puede importar credit_risk (otro entorno virtual): se pregunta
    # al Python del proyecto y se compara con la lista de los DAGs.
    salida = subprocess.run(
        [
            "/opt/credit-risk/.venv/bin/python",
            "-c",
            "from credit_risk.config import TABLES; print(','.join(TABLES.values()))",
        ],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    assert FICHEROS_ENTREGA == [f + ".gz" for f in salida.split(",")]


def test_el_origen_simula_las_entregas(dagbag):
    origen = dagbag.dags[ORIGEN]
    assert origen.get_task("verify_data").downstream_task_ids == {"simulate_source"}


def test_la_transformacion_se_programa_con_bronze(transformacion):
    # schedule=[BRONZE] crea un horario "por Assets" cuya condición es ese Asset
    assert type(transformacion.timetable).__name__ == "AssetTriggeredTimetable"
    assert tuple(transformacion.timetable.asset_condition.objects) == (BRONZE,)


def test_la_transformacion_publica_el_mart_al_final(transformacion):
    publicar = transformacion.get_task("publicar_mart")
    assert MART in publicar.outlets
    assert "dbt.mart_scoring__clientes_model_build" in publicar.get_flat_relative_ids(upstream=True)


# --- Estructura de la transformación ------------------------------------------


def test_una_tarea_por_cada_modelo_dbt(transformacion):
    nodos = json.loads(MANIFEST.read_text())["nodes"].values()
    modelos = {n["name"] for n in nodos if n["resource_type"] == "model"}
    tareas = {t.task_id for t in transformacion.tasks if t.task_id.endswith("_model_build")}
    assert tareas == {f"dbt.{m}_model_build" for m in modelos}


def test_dbt_espera_a_la_comprobacion_de_frescura(tareas_duckdb):
    for tarea in tareas_duckdb:
        if tarea.task_id != "dbt_source_freshness":
            assert "dbt_source_freshness" in tarea.get_flat_relative_ids(upstream=True)


def test_las_tareas_de_duckdb_usan_el_pool(tareas_duckdb):
    # DuckDB solo admite un proceso escribiendo: sin el pool, Airflow lanzaría
    # varios modelos a la vez y fallarían por el bloqueo del fichero.
    assert tareas_duckdb
    assert {t.pool for t in tareas_duckdb} == {"duckdb"}


def test_la_transformacion_no_reconstruye_por_defecto(transformacion):
    assert transformacion.params.get_param("reconstruir").value is False
    mart = transformacion.get_task("dbt.mart_scoring__clientes_model_build")
    assert mart.full_refresh == "{{ params.reconstruir }}"


def test_toda_la_transformacion_lee_bronze_hasta_el_mismo_corte(tareas_duckdb):
    # Si no, la ingesta escribiendo a la vez hace que cada tarea vea datos distintos
    dbt = [t for t in tareas_duckdb if t.task_id.startswith("dbt.")]
    assert {t.vars["corte_ingesta"] for t in dbt} == {"{{ dag_run.start_date }}"}


# --- Comunes a los dos DAGs -----------------------------------------------------


@pytest.mark.parametrize("dag_id", TODOS)
def test_todas_las_tareas_tienen_reintentos(dagbag, dag_id):
    assert all(t.retries >= 1 for t in dagbag.dags[dag_id].tasks)


@pytest.mark.parametrize("dag_id", TODOS)
def test_una_sola_ejecucion_activa(dagbag, dag_id):
    # Dos ejecuciones a la vez escribirían en el mismo lago
    assert dagbag.dags[dag_id].max_active_runs == 1


@pytest.mark.parametrize("dag_id", TODOS)
def test_parametro_datos_por_defecto_es_la_muestra(dagbag, dag_id):
    param = dagbag.dags[dag_id].params.get_param("datos")
    assert param.value == "muestra"
    assert param.schema["enum"] == ["muestra", "completo"]
