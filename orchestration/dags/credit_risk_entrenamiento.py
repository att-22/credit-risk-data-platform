"""Entrenamiento del modelo de scoring sobre el mart.

    entrenar -> publicar_modelo ==> Asset modelo_scoring

Entrena LightGBM sobre el mart (comando train-model), registra la ejecución en
MLflow y, si el AUC de prueba alcanza `auc_minimo`, publica el modelo como
vigente. Si no lo alcanza, la tarea falla y sigue vigente el modelo anterior.

Se lanza a mano: reentrenar es una decisión, no algo que deba pasar cada vez que
llegan datos. Al publicar un modelo se actualiza el Asset `modelo_scoring`, y eso
dispara `credit_risk_scoring`, que vuelve a puntuar con el modelo nuevo.

Parámetros: `datos` (con la muestra se usan hiperparámetros rápidos de prueba) y
`auc_minimo`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from comun import BIN, DATOS_VALIDOS, MODELO, POOL_DUCKDB, entorno

# La muestra tiene unos cientos de filas: los hiperparámetros reales no aprenderían nada
PRUEBA = "{{ '--prueba' if params.datos == 'muestra' else '' }}"


@dag(
    dag_id="credit_risk_entrenamiento",
    description="Entrena el modelo de scoring sobre el mart y lo publica si es bueno",
    doc_md=__doc__,
    schedule=None,  # se lanza a mano
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "datos": Param("muestra", enum=DATOS_VALIDOS, description="Mart sobre el que entrenar."),
        "auc_minimo": Param(
            0.0,
            type="number",
            minimum=0,
            maximum=1,
            description="AUC de prueba por debajo del cual el modelo no se publica "
            "(con los datos completos, 0.78).",
        ),
    },
    # Repetirlo es seguro: cada entrenamiento deja su propia carpeta
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["credit-risk", "ml"],
)
def credit_risk_entrenamiento():
    entrenar = BashOperator(
        task_id="entrenar",
        bash_command=f"{BIN}/train-model --auc-minimo {{{{ params.auc_minimo }}}} {PRUEBA}",
        env=entorno("params.datos"),
        append_env=True,
        pool=POOL_DUCKDB,  # lee warehouse.duckdb
    )

    @task(outlets=[MODELO])
    def publicar_modelo(params=None, outlet_events=None):
        """Registra que hay un modelo vigente nuevo (lo usará credit_risk_scoring)."""
        outlet_events[MODELO].extra = {"datos": params["datos"]}

    entrenar >> publicar_modelo()


credit_risk_entrenamiento()
