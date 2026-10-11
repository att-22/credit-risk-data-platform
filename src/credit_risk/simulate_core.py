"""Simula la aplicación del banco trabajando sobre su base de datos operacional.

Para las solicitudes de un día (del almacén del banco), escribe en Postgres
(core.solicitudes) su ciclo de vida, como haría la aplicación real:

    recibida  ->  en_revision  ->  aprobada | rechazada
        └──────────────┴────────->  (borrada: el cliente desiste)

Cada paso es una transacción distinta (INSERT, UPDATE o DELETE): justo lo que
Debezium captura del WAL y publica como eventos de CDC. Las decisiones son
deterministas (hash del identificador): repetirlo da los mismos cambios.

Uso:
    uv run simulate-core --fecha 2026-09-07 --conjunto completo
"""

from __future__ import annotations

import argparse
import hashlib
import time
from datetime import date

import duckdb
import psycopg

from credit_risk.config import CORE_DSN, ORIGEN_RAIZ, TABLES

CANALES = ["oficina", "web", "app", "telefono"]


def azar(sk_id: int, paso: str) -> float:
    """Número determinista en [0, 1) para un cliente y una decisión."""
    resumen = hashlib.md5(f"{sk_id}/{paso}".encode()).hexdigest()
    return int(resumen[:8], 16) / 0x1_0000_0000


def solicitudes_del_dia(conjunto: str, dia: date) -> list[tuple[int, float]]:
    """(SK_ID_CURR, AMT_CREDIT) de las solicitudes de un día del almacén del banco."""
    carpeta = ORIGEN_RAIZ / conjunto / f"fecha={dia.isoformat()}"
    ficheros = [
        (carpeta / f"{TABLES[t]}.gz").as_posix() for t in ("application_train", "application_test")
    ]
    return duckdb.sql(f"""
        SELECT SK_ID_CURR::BIGINT, AMT_CREDIT::DOUBLE
        FROM read_csv({ficheros}, union_by_name=true, hive_partitioning=false)
        ORDER BY 1
    """).fetchall()


def simular(dsn: str, solicitudes: list[tuple[int, float]]) -> dict[str, int]:
    """Aplica el ciclo de vida de las solicitudes. Devuelve cuántos cambios de cada tipo."""
    cambios = {"insert": 0, "update": 0, "delete": 0}
    with psycopg.connect(
        dsn, autocommit=True
    ) as con:  # autocommit: cada sentencia, una transacción
        for sk_id, importe in solicitudes:
            canal = CANALES[int(azar(sk_id, "canal") * len(CANALES))]
            con.execute(
                """INSERT INTO core.solicitudes (sk_id_curr, estado, importe, canal)
                   VALUES (%s, 'recibida', %s, %s)
                   ON CONFLICT (sk_id_curr) DO NOTHING""",
                (sk_id, importe, canal),
            )
            cambios["insert"] += 1
        for sk_id, _ in solicitudes:
            if azar(sk_id, "desiste") < 0.05:  # 5 %: el cliente desiste antes de la revisión
                con.execute("DELETE FROM core.solicitudes WHERE sk_id_curr = %s", (sk_id,))
                cambios["delete"] += 1
                continue
            for estado in (
                "en_revision",
                "aprobada" if azar(sk_id, "decision") < 0.7 else "rechazada",
            ):
                con.execute(
                    """UPDATE core.solicitudes SET estado = %s, actualizado_en = now()
                       WHERE sk_id_curr = %s""",
                    (estado, sk_id),
                )
                cambios["update"] += 1
    return cambios


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--fecha", type=date.fromisoformat, required=True)
    parser.add_argument("--conjunto", default="completo")
    args = parser.parse_args()

    inicio = time.perf_counter()
    solicitudes = solicitudes_del_dia(args.conjunto, args.fecha)
    cambios = simular(CORE_DSN, solicitudes)
    print(
        f"{len(solicitudes):,} solicitudes del {args.fecha}: {cambios} "
        f"en {time.perf_counter() - inicio:.1f} s"
    )


if __name__ == "__main__":
    main()
