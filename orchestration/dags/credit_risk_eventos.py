"""Eventos en tiempo real: el sistema origen publica las solicitudes de un día en Kafka.

    publicar_eventos

Simula que el banco emite un evento por cada solicitud nueva en el topic
`solicitudes` de Redpanda. NO los consume este DAG: los consume el servicio
`consumidor-stream` (compose.yaml), que corre siempre y escribe micro-lotes en
bronze/stream_<tabla>/. Airflow orquesta trabajos que empiezan y terminan; un
consumidor de streaming es un servicio permanente.

Parámetros: `datos` (conjunto del almacén del banco: lanzar antes
credit_risk_origen), `fecha` de las solicitudes y `malos` (eventos inválidos
añadidos a propósito para ver la cola de errores, `solicitudes.dlq`).
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from comun import BIN, DATOS_VALIDOS

ARGUMENTOS = "--fecha {{ params.fecha }} --conjunto {{ params.datos }} --malos {{ params.malos }}"


@dag(
    dag_id="credit_risk_eventos",
    description="El origen publica las solicitudes de un día como eventos en Kafka",
    doc_md=__doc__,
    schedule=None,  # se lanza a mano
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "datos": Param(
            "muestra", enum=DATOS_VALIDOS, description="Conjunto del almacén del banco."
        ),
        "fecha": Param("2026-09-07", type="string", format="date", description="Día a publicar."),
        "malos": Param(5, type="integer", minimum=0, description="Eventos inválidos (a la DLQ)."),
    },
    default_args={"retries": 0},  # reintentar republicaría el día: mejor que lo decida alguien
    tags=["credit-risk", "streaming"],
)
def credit_risk_eventos():
    BashOperator(
        task_id="publicar_eventos",
        bash_command=f"{BIN}/produce-events {ARGUMENTOS}",
        append_env=True,  # KAFKA_BOOTSTRAP y ORIGEN_RAIZ vienen del contenedor
    )


credit_risk_eventos()
