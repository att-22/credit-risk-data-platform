"""Comprueba que la réplica construida con el CDC es idéntica a la tabla de Postgres.

Pasos:
  1. Consume los cambios pendientes de Debezium (consume-cdc) hasta que dejan de
     llegar nuevos (Debezium publica con unos segundos de retraso).
  2. Construye los modelos de CDC de dbt (--select tag:cdc, con con_cdc=true).
  3. Compara core_solicitudes_actual con core.solicitudes en Postgres, fila a fila.
  4. Comprueba el historial: con el ciclo de vida del simulador, cada solicitud
     viva tiene exactamente 3 versiones (recibida, en_revision, decisión). Una
     versión de menos delataría un cambio perdido aunque el estado final cuadre.

Uso (con core-bancario, debezium y redpanda arrancados y cambios ya simulados):
    uv run python scripts/comprobar_replica_cdc.py
"""

from __future__ import annotations

import subprocess
import sys

import duckdb
import psycopg

from credit_risk.cdc import consumir_cdc
from credit_risk.config import BRONZE_DIR, CORE_DSN, LAKE_DIR, ROOT


def main() -> None:
    # 1) Consumir hasta que dos rondas seguidas no traigan nada nuevo
    total, vacias = 0, 0
    while vacias < 2:
        escritos = consumir_cdc(BRONZE_DIR, segundos=1, hasta_vaciar=True)["escritos"]
        total += escritos
        vacias = vacias + 1 if escritos == 0 else 0
    print(f"Cambios consumidos: {total:,}")

    # 2) Modelos de CDC de dbt
    subprocess.run(
        ["dbt", "build", "--select", "tag:cdc", "--vars", "{con_cdc: true}"],
        cwd=ROOT / "transform",
        check=True,
    )

    # 3) Réplica frente a Postgres
    with duckdb.connect((LAKE_DIR / "warehouse.duckdb").as_posix(), read_only=True) as almacen:
        replica = {
            fila[0]: fila[1:]
            for fila in almacen.execute(
                "SELECT sk_id_curr, estado, importe::DOUBLE, canal FROM core_solicitudes_actual"
            ).fetchall()
        }
        versiones = almacen.execute("""
            SELECT count(*) FILTER (WHERE n <> 3) FROM (
                SELECT sk_id_curr, count(*) AS n FROM int_core__solicitudes_historial
                GROUP BY ALL HAVING bool_or(es_actual))
        """).fetchone()[0]
    with psycopg.connect(CORE_DSN) as pg:
        real = {
            fila[0]: fila[1:]
            for fila in pg.execute(
                "SELECT sk_id_curr, estado, importe::float8, canal FROM core.solicitudes"
            ).fetchall()
        }

    problemas = []
    if replica != real:
        faltan, sobran = len(real.keys() - replica.keys()), len(replica.keys() - real.keys())
        distintas = sum(replica[k] != real[k] for k in real.keys() & replica.keys())
        problemas.append(
            f"réplica distinta: faltan {faltan}, sobran {sobran}, distintas {distintas}"
        )
    if versiones:
        problemas.append(f"{versiones} solicitudes vivas sin sus 3 versiones (¿cambios perdidos?)")

    print(f"Postgres: {len(real):,} solicitudes | réplica dbt: {len(replica):,}")
    if problemas:
        sys.exit("ERROR: " + "; ".join(problemas))
    print("OK: la réplica del CDC es idéntica a Postgres y el historial está completo.")


if __name__ == "__main__":
    main()
