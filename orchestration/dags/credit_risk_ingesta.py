"""Ingesta diaria: la entrega de cada día en la landing zone -> bronze.

    esperar_entrega -> ingest_bronze -> publicar_bronze  ==>  Asset home_credit_bronze

Cada ejecución procesa SU día: el de su fecha lógica (`{{ ds }}`). La ejecución
del 2026-09-05 ingiere landing/fecha=2026-09-05 con `ingest-bronze --fecha`, y
volver a ejecutarla (Clear) reingiere exactamente ese día (idempotente).

- `esperar_entrega` es un sensor: comprueba cada 30 s si la entrega del día
  está completa (los 8 ficheros) y, entretanto, libera su hueco de ejecución
  (mode="reschedule"). Si en 1 hora no llega, falla: alguien debe enterarse.
- catchup=True: al activar el DAG, Airflow crea una ejecución por cada día del
  periodo (start_date -> end_date) que aún no se haya ejecutado. Para reprocesar
  un rango de días ya ejecutados se usa un backfill.
- Al terminar, publica el Asset de bronze (con el día y el conjunto de datos en
  sus metadatos), que dispara la transformación.

Parámetro `datos`: "muestra" o "completo". Las ejecuciones programadas usan la
muestra; para los datos completos, lanzar un backfill con datos=completo.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from comun import (
    BIN,
    BRONZE,
    DATOS_VALIDOS,
    FICHEROS_ENTREGA,
    PRIMERA_ENTREGA,
    ULTIMA_ENTREGA,
    entorno,
    landing,
)

ENTORNO = entorno("params.datos")


@dag(
    dag_id="credit_risk_ingesta",
    description="Ingesta diaria de la entrega del día a bronze",
    doc_md=__doc__,
    schedule="@daily",  # una ejecución por día, a las 00:00 UTC
    start_date=PRIMERA_ENTREGA,
    end_date=ULTIMA_ENTREGA,  # el periodo simulado; en producción no tendría fin
    catchup=True,  # ejecutar también los días del periodo que ya han pasado
    max_active_runs=1,  # un día detrás de otro: todos escriben la tabla de control
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
    @task.sensor(poke_interval=30, timeout=timedelta(hours=1), mode="reschedule")
    def esperar_entrega(ds=None, params=None) -> bool:
        """¿Ha dejado el origen los 8 ficheros de la entrega de este día?"""
        entrega = landing(params["datos"]) / f"fecha={ds}"
        faltan = [f for f in FICHEROS_ENTREGA if not (entrega / f).exists()]
        if faltan:
            print(f"Entrega {entrega} incompleta, faltan: {faltan}")
        return not faltan

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{BIN}/ingest-bronze --fecha {{{{ ds }}}}",
        env=ENTORNO,
        append_env=True,  # además de ENTORNO, conservar el entorno del contenedor
    )

    @task(outlets=[BRONZE])
    def publicar_bronze(ds=None, params=None, outlet_events=None):
        """Registra el evento del Asset con el día y el conjunto de datos."""
        outlet_events[BRONZE].extra = {"datos": params["datos"], "fecha_entrega": ds}

    esperar_entrega() >> ingest_bronze >> publicar_bronze()


credit_risk_ingesta()
