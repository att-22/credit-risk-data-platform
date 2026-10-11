"""Sistema origen SIMULADO: genera las entregas diarias en la landing zone.

    verify_data -> simulate_source

En la realidad este DAG no existiría: los ficheros los dejaría el sistema del
banco. Aquí hace ese papel para que la ingesta diaria tenga algo que recoger.
Se lanza a mano; genera de una vez las 30 entregas (1-30 de septiembre de 2026)
y la ingesta las va recogiendo día a día, como si llegaran con el tiempo.

Parámetro `datos`: "muestra" (la sintética de la imagen) o "completo" (los CSV
de Kaggle montados en /data).
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from comun import BIN, DATOS_VALIDOS, entorno

ENTORNO = entorno("params.datos")


@dag(
    dag_id="credit_risk_origen",
    description="Sistema origen simulado: CSV -> landing zone (entregas diarias)",
    doc_md=__doc__,
    schedule=None,  # se lanza a mano
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "datos": Param(
            "muestra",
            enum=DATOS_VALIDOS,
            description="Muestra sintética (rápida) o datos completos de Kaggle.",
        )
    },
    default_args={"retries": 1},
    tags=["credit-risk", "origen"],
)
def credit_risk_origen():
    verify_data = BashOperator(
        task_id="verify_data",
        bash_command=f"{BIN}/verify-data",
        env=ENTORNO,
        append_env=True,
    )

    simulate_source = BashOperator(
        task_id="simulate_source",
        bash_command=f"{BIN}/simulate-source",
        env=ENTORNO,
        append_env=True,
    )

    verify_data >> simulate_source


credit_risk_origen()
