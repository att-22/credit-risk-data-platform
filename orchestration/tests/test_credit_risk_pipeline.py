"""Tests de integridad del DAG credit_risk_pipeline.

No ejecutan el pipeline: cargan el DAG como lo haría el dag-processor de Airflow
y comprueban su estructura. Detectan en la CI errores que, si no, solo se verían
al arrancar Airflow (un import roto, una dependencia mal puesta, una tarea que
se salta el pool de DuckDB...).

Necesitan Airflow y Cosmos, así que se ejecutan DENTRO de la imagen de Airflow
(ver el job "Airflow DAGs" en .github/workflows/ci.yml), no con el pytest del
proyecto (pyproject.toml solo recoge la carpeta tests/).
"""

import json
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path("/opt/airflow/dags")
MANIFEST = Path("/opt/credit-risk/transform/target/manifest.json")
DAG_ID = "credit_risk_pipeline"


@pytest.fixture(scope="module")
def dagbag():
    # scope="module": el DAG se carga una sola vez para todos los tests
    return DagBag(dag_folder=DAGS_DIR)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags[DAG_ID]


@pytest.fixture(scope="module")
def tareas_duckdb(dag):
    """Tareas que abren warehouse.duckdb: freshness y todo el grupo de Cosmos."""
    return [t for t in dag.tasks if t.task_id.startswith("dbt")]


def test_los_dags_se_importan_sin_errores(dagbag):
    assert dagbag.import_errors == {}
    assert DAG_ID in dagbag.dags


def test_tareas_de_python_en_orden(dag):
    assert dag.get_task("verify_data").downstream_task_ids == {"ingest_bronze"}
    assert dag.get_task("ingest_bronze").downstream_task_ids == {"dbt_source_freshness"}


def test_una_tarea_por_cada_modelo_dbt(dag):
    nodos = json.loads(MANIFEST.read_text())["nodes"].values()
    modelos = {n["name"] for n in nodos if n["resource_type"] == "model"}
    tareas = {t.task_id for t in dag.tasks if t.task_id.endswith("_model_build")}
    assert tareas == {f"dbt.{m}_model_build" for m in modelos}


def test_dbt_espera_a_la_comprobacion_de_frescura(dag, tareas_duckdb):
    for tarea in tareas_duckdb:
        if tarea.task_id != "dbt_source_freshness":
            assert "dbt_source_freshness" in tarea.get_flat_relative_ids(upstream=True)


def test_las_tareas_de_duckdb_usan_el_pool(tareas_duckdb):
    # DuckDB solo admite un proceso escribiendo: sin el pool, Airflow lanzaría
    # varios modelos a la vez y fallarían por el bloqueo del fichero.
    assert tareas_duckdb
    assert {t.pool for t in tareas_duckdb} == {"duckdb"}


def test_todas_las_tareas_tienen_reintentos(dag):
    assert all(t.retries >= 1 for t in dag.tasks)


def test_parametro_datos_por_defecto_es_la_muestra(dag):
    param = dag.params.get_param("datos")
    assert param.value == "muestra"
    assert param.schema["enum"] == ["muestra", "completo"]
