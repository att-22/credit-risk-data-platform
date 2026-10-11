"""Consumidor de CDC: cambios del core bancario (Debezium) -> micro-lotes Parquet en bronze.

Debezium publica en `cdc.core.solicitudes` un evento por cada cambio en la tabla
core.solicitudes de Postgres:

    {"op": "u",                           # c=insert, u=update, d=delete, r=lectura inicial
     "before": {...fila anterior...},     # null en los insert
     "after":  {...fila nueva...},        # null en los delete
     "source": {"lsn": 26658072, "txId": 764, "ts_ms": ...}}

Cada cambio se guarda como una fila (con la fila anterior en los delete) en:

    bronze/cdc_solicitudes/fecha_captura=AAAA-MM-DD/p<partición>_o<ini>-<fin>.parquet

Bronze guarda el REGISTRO DE CAMBIOS completo, no solo el estado final: con él,
dbt reconstruye el estado actual y el historial de cada solicitud (SCD tipo 2).
El LSN (posición en el WAL de Postgres) ordena los cambios y sirve para
deduplicar: el consumo es at-least-once (el mismo bucle que consumidor.py).

Uso:
    uv run consume-cdc                    # servicio: corre hasta que lo paren
    uv run consume-cdc --hasta-vaciar     # procesa lo pendiente y sale
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pandas as pd

from credit_risk.config import BRONZE_DIR, KAFKA_BOOTSTRAP, TOPIC_CDC
from credit_risk.consumidor import bucle_consumo

GRUPO = "bronze-cdc"
OPERACIONES = {"c", "u", "d", "r"}
COLUMNAS = ["sk_id_curr", "estado", "importe", "canal", "actualizado_en"]


def interpretar(valor: bytes | None) -> tuple[dict | None, str | None]:
    """Convierte un mensaje de Debezium en una fila de cambio.

    Devuelve (fila, None) si es válido, (None, motivo) si no, o (None, None) para
    las "lápidas" (mensajes con valor nulo que Kafka usa tras un delete).
    """
    if valor is None:
        return None, None
    try:
        evento = json.loads(valor)
        op = evento["op"]
        if op not in OPERACIONES:
            return None, f"operación desconocida: {op!r}"
        # En un delete solo existe la fila anterior; en el resto, la nueva
        fila = evento["before"] if op == "d" else evento["after"]
        fuente = evento["source"]
        return {
            "op": op,
            **{c: fila[c] for c in COLUMNAS},
            "lsn": fuente["lsn"],
            "tx_id": fuente["txId"],
            "capturado_en": datetime.fromtimestamp(fuente["ts_ms"] / 1000, UTC),
        }, None
    except (ValueError, KeyError, TypeError) as error:
        return None, f"evento de CDC mal formado: {type(error).__name__}: {error}"[:300]


def escribir_cambios(bronze_dir: Path, lote: list[tuple[int, int, dict]]) -> int:
    """Escribe un micro-lote de cambios: un fichero por (día de captura, partición)."""
    grupos = defaultdict(list)
    for particion, offset, fila in lote:
        grupos[(fila["capturado_en"].date(), particion)].append((offset, fila))
    con = duckdb.connect()
    for (dia, particion), filas in grupos.items():
        df = pd.DataFrame([f for _, f in filas])
        df["_kafka_particion"] = particion
        df["_kafka_offset"] = [o for o, _ in filas]
        con.register("lote", df)
        destino = (
            bronze_dir
            / "cdc_solicitudes"
            / f"fecha_captura={dia.isoformat()}"
            / f"p{particion}_o{filas[0][0]:012d}-{filas[-1][0]:012d}.parquet"
        )
        destino.parent.mkdir(parents=True, exist_ok=True)
        temporal = destino.with_name(destino.name + ".tmp")
        con.execute(f"""
            COPY (
                SELECT op, sk_id_curr::BIGINT AS sk_id_curr, estado,
                       importe::DECIMAL(14, 2) AS importe, canal,
                       actualizado_en::TIMESTAMPTZ AS actualizado_en,
                       lsn::BIGINT AS lsn, tx_id::BIGINT AS tx_id,
                       capturado_en::TIMESTAMPTZ AS capturado_en,
                       _kafka_particion::INTEGER AS _kafka_particion,
                       _kafka_offset::BIGINT AS _kafka_offset,
                       current_timestamp AS _ingested_at
                FROM lote
            ) TO '{temporal.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """)
        con.unregister("lote")
        temporal.replace(destino)
    con.close()
    return len(lote)


def consumir_cdc(bronze_dir: Path = BRONZE_DIR, bootstrap: str = KAFKA_BOOTSTRAP, **kw) -> dict:
    return bucle_consumo(
        TOPIC_CDC,
        f"{TOPIC_CDC}.dlq",
        validar=lambda mensaje: interpretar(mensaje.value()),
        escribir=lambda lote: escribir_cambios(bronze_dir, lote),
        bootstrap=bootstrap,
        grupo=kw.pop("grupo", GRUPO),
        **kw,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hasta-vaciar", action="store_true", help="salir al no quedar cambios")
    parser.add_argument("--bronze", type=Path, default=BRONZE_DIR, help="carpeta de bronze")
    parser.add_argument("--segundos", type=float, default=5.0)
    args = parser.parse_args()

    print(f"Consumiendo '{TOPIC_CDC}' de {KAFKA_BOOTSTRAP} (grupo '{GRUPO}')")
    totales = consumir_cdc(args.bronze, segundos=args.segundos, hasta_vaciar=args.hasta_vaciar)
    print(json.dumps(totales))


if __name__ == "__main__":
    main()
