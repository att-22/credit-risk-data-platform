"""Sistema origen SIMULADO (el "banco"): genera sus entregas diarias y las publica.

    verify_data -> generar_almacen -> exportar_ficheros

En la realidad este DAG no existiría: todo esto lo haría el sistema del banco.
Aquí hace ese papel para que la ingesta tenga de dónde leer:

- generar_almacen: las 30 entregas (1-30 de septiembre de 2026) en el almacén del
  banco, ORIGEN_RAIZ/<datos>/. Es lo que sirve la API REST (servicio api-origen).
- exportar_ficheros: el banco también deja una copia en nuestra landing zone,
  para el canal "ficheros" de la ingesta.

Se lanza a mano. Parámetro `datos`: "muestra" (la sintética de la imagen) o
"completo" (los CSV de Kaggle montados en /data); en la API es el `conjunto`.
"""

from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from comun import BIN, DATOS_VALIDOS, ORIGEN_RAIZ, entorno

ENTORNO = entorno("params.datos")


@dag(
    dag_id="credit_risk_origen",
    description="Sistema origen simulado: almacén del banco (API) + ficheros en la landing",
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

    generar_almacen = BashOperator(
        task_id="generar_almacen",
        bash_command=f"{BIN}/simulate-source --destino {ORIGEN_RAIZ}/{{{{ params.datos }}}}",
        env=ENTORNO,
        append_env=True,
    )

    # Copia atómica: se prepara en .tmp y se sustituye la landing de una vez
    exportar_ficheros = BashOperator(
        task_id="exportar_ficheros",
        bash_command=(
            "set -e; L=/opt/airflow/lake/{{ params.datos }}/landing; "
            f"rm -rf $L.tmp; cp -r {ORIGEN_RAIZ}/{{{{ params.datos }}}} $L.tmp; "
            "rm -rf $L; mv $L.tmp $L"
        ),
    )

    verify_data >> generar_almacen >> exportar_ficheros


credit_risk_origen()
