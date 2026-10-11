"""Ingesta diaria: la entrega de cada día -> landing zone -> bronze.

                    ┌─> esperar_entrega (canal=ficheros) ─┐
    elegir_canal ───┤                                     ├─> ingest_bronze -> publicar_bronze
                    └─> extraer_api     (canal=api)     ──┘        ==>  Asset home_credit_bronze

Cada ejecución procesa SU día: el de su fecha lógica (`{{ ds }}`). La ejecución
del 2026-09-05 trae y ingiere la entrega del 2026-09-05, y volver a ejecutarla
(Clear) la reprocesa exactamente (idempotente).

- Canal "ficheros": `esperar_entrega` es un sensor que comprueba cada 30 s si el
  origen ha dejado los 8 ficheros del día en la landing y, entretanto, libera su
  hueco (mode="reschedule"). Si en 1 hora no llegan, falla.
- Canal "api": `extraer_api` descarga la entrega de la API REST del origen
  (paginación, reintentos ante 429/5xx) y la deja en la landing con el mismo
  contrato; a partir de ahí todo es igual.
- `elegir_canal` es una bifurcación (@task.branch): solo se ejecuta la rama del
  canal elegido y la otra queda "skipped". `ingest_bronze` usa la regla
  none_failed_min_one_success para ejecutarse tras cualquiera de las dos.
- catchup=True: al activar el DAG, Airflow crea una ejecución por cada día del
  periodo (start_date -> end_date). Para reprocesar días ya ejecutados, un backfill.
- Al terminar, publica el Asset de bronze (con el día y el conjunto de datos en
  sus metadatos), que dispara la transformación.

Parámetros: `datos` ("muestra" o "completo") y `canal` ("ficheros" o "api").
Las ejecuciones programadas usan los valores por defecto.
"""

from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from comun import (
    BIN,
    BRONZE,
    CANALES,
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
    description="Ingesta diaria de la entrega del día a bronze (por ficheros o por API)",
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
        ),
        "canal": Param(
            "ficheros",
            enum=CANALES,
            description="Ficheros en la landing zone o extracción de la API del origen.",
        ),
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["credit-risk", "ingesta"],
)
def credit_risk_ingesta():
    @task.branch
    def elegir_canal(params=None) -> str:
        """Devuelve el task_id de la rama a ejecutar; la otra se salta."""
        return "extraer_api" if params["canal"] == "api" else "esperar_entrega"

    @task.sensor(poke_interval=30, timeout=timedelta(hours=1), mode="reschedule")
    def esperar_entrega(ds=None, params=None) -> bool:
        """¿Ha dejado el origen los 8 ficheros de la entrega de este día?"""
        entrega = landing(params["datos"]) / f"fecha={ds}"
        faltan = [f for f in FICHEROS_ENTREGA if not (entrega / f).exists()]
        if faltan:
            print(f"Entrega {entrega} incompleta, faltan: {faltan}")
        return not faltan

    extraer_api = BashOperator(
        task_id="extraer_api",
        bash_command=f"{BIN}/extract-api --fecha {{{{ ds }}}} --conjunto {{{{ params.datos }}}}",
        env=ENTORNO,
        append_env=True,  # ORIGEN_API_URL y ORIGEN_API_KEY vienen del contenedor
    )

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=f"{BIN}/ingest-bronze --fecha {{{{ ds }}}}",
        env=ENTORNO,
        append_env=True,  # además de ENTORNO, conservar el entorno del contenedor
        # Por defecto una tarea exige que TODAS sus anteriores hayan ido bien; aquí
        # una de las dos ramas siempre queda "skipped": basta con que una funcione.
        trigger_rule="none_failed_min_one_success",
    )

    @task(outlets=[BRONZE])
    def publicar_bronze(ds=None, params=None, outlet_events=None):
        """Registra el evento del Asset con el día, el conjunto y el canal."""
        outlet_events[BRONZE].extra = {
            "datos": params["datos"],
            "fecha_entrega": ds,
            "canal": params["canal"],
        }

    canal = elegir_canal()
    canal >> [esperar_entrega(), extraer_api] >> ingest_bronze >> publicar_bronze()


credit_risk_ingesta()
