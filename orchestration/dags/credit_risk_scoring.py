"""Puntuación por lotes: el modelo vigente puntúa las solicitudes sin resolver.

    [Asset mart_scoring__clientes] ==>
                                       resolver_datos -> hay_modelo -> puntuar
    [Asset modelo_scoring]         ==>                  -> publicar_scoring ==> Asset scoring

Cierra el ciclo de la plataforma sin intervención: cuando la transformación
actualiza el mart (datos nuevos) o el entrenamiento publica un modelo (modelo
nuevo), se vuelven a puntuar las solicitudes sin TARGET y el resultado queda en el
lago (comando score), listo para quien decide sobre los créditos.

Si todavía no hay ningún modelo publicado, `hay_modelo` corta la ejecución (las
tareas siguientes quedan omitidas, no fallidas): primero hay que lanzar
`credit_risk_entrenamiento`.

¿Muestra o completo? Como en la transformación: del evento que disparó la
ejecución o, si se lanza a mano, del parámetro `datos`.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from comun import BIN, DATOS_VALIDOS, MART, MODELO, POOL_DUCKDB, SCORING, entorno

ENTORNO = entorno("ti.xcom_pull(task_ids='resolver_datos')")


@dag(
    dag_id="credit_risk_scoring",
    description="Puntúa las solicitudes sin resolver cuando cambia el mart o el modelo",
    doc_md=__doc__,
    schedule=(MART | MODELO),  # programación por datos: cualquiera de los dos Assets
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "datos": Param(
            "muestra",
            enum=DATOS_VALIDOS,
            description="Solo para ejecuciones manuales (sin evento del mart o del modelo).",
        ),
    },
    # Repetirlo es seguro: puntuar con el mismo modelo sobrescribe su partición
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["credit-risk", "ml"],
)
def credit_risk_scoring():
    @task
    def resolver_datos(params=None, triggering_asset_events=None) -> str:
        """Conjunto de datos a puntuar: el del evento más reciente o el parámetro."""
        eventos = [
            evento
            for asset in (MART, MODELO)
            for evento in (triggering_asset_events or {}).get(asset, [])
        ]
        if eventos:
            datos = max(eventos, key=lambda e: e.timestamp).extra.get("datos")
        else:
            datos = params["datos"]
        if datos not in DATOS_VALIDOS:
            raise ValueError(f"Conjunto de datos desconocido en el evento: {datos!r}")
        print(f"Conjunto de datos: {datos} ({'evento' if eventos else 'manual'})")
        return datos

    @task.short_circuit
    def hay_modelo(datos: str) -> bool:
        """Sin modelo publicado no hay nada que hacer: se omite el resto."""
        modelo = Path("/opt/airflow/lake") / datos / "outputs/modelos/vigente/modelo.txt"
        if not modelo.exists():
            print(f"No hay modelo vigente ({modelo}): lanza antes credit_risk_entrenamiento.")
        return modelo.exists()

    puntuar = BashOperator(
        task_id="puntuar",
        bash_command=f"{BIN}/score",
        env=ENTORNO,
        append_env=True,
        pool=POOL_DUCKDB,  # lee warehouse.duckdb
    )

    @task(outlets=[SCORING])
    def publicar_scoring(datos: str, outlet_events=None):
        """Registra que las puntuaciones están actualizadas."""
        outlet_events[SCORING].extra = {"datos": datos}

    datos = resolver_datos()
    hay_modelo(datos) >> puntuar >> publicar_scoring(datos)


credit_risk_scoring()
