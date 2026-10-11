"""Actividad del core bancario: la aplicación del banco trabaja sobre su Postgres.

    simular_actividad

Simula que, a lo largo de un día, la aplicación del banco inserta las solicitudes
recibidas y las hace avanzar (en revisión, aprobada o rechazada; algunas se
borran porque el cliente desiste) en la tabla core.solicitudes del servicio
`core-bancario`.

Este DAG NO lee esos cambios: los captura Debezium del WAL de Postgres y los
escribe en bronze el servicio `consumidor-cdc`. Los modelos de dbt del CDC
(carpeta transform/models/cdc, `--vars '{con_cdc: true}'`) reconstruyen después
el estado actual y el historial de cada solicitud.

Parámetros: `datos` (conjunto del almacén del banco: lanzar antes
credit_risk_origen) y `fecha` de las solicitudes.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from comun import BIN, DATOS_VALIDOS

ARGUMENTOS = "--fecha {{ params.fecha }} --conjunto {{ params.datos }}"


@dag(
    dag_id="credit_risk_core",
    description="La aplicación del banco escribe en su Postgres (lo captura el CDC)",
    doc_md=__doc__,
    schedule=None,  # se lanza a mano
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "datos": Param(
            "muestra", enum=DATOS_VALIDOS, description="Conjunto del almacén del banco."
        ),
        "fecha": Param("2026-09-07", type="string", format="date", description="Día a simular."),
    },
    default_args={"retries": 1},  # repetirlo es seguro: los INSERT usan ON CONFLICT DO NOTHING
    tags=["credit-risk", "cdc"],
)
def credit_risk_core():
    BashOperator(
        task_id="simular_actividad",
        bash_command=f"{BIN}/simulate-core {ARGUMENTOS}",
        append_env=True,  # CORE_DSN y ORIGEN_RAIZ vienen del contenedor
    )


credit_risk_core()
