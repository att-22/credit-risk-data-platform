"""Consumidor de streaming: eventos de Kafka -> micro-lotes Parquet en bronze.

Lee el topic `solicitudes` como parte de un grupo de consumidores y escribe los
eventos válidos en bronze, agrupados en micro-lotes:

    bronze/stream_<tabla>/fecha_entrega=AAAA-MM-DD/p<partición>_o<offset ini>-<offset fin>.parquet

Garantías y decisiones:
- At-least-once (al menos una vez): el offset se confirma (commit) DESPUÉS de
  escribir el lote. Si el proceso muere entre medias, al volver relee desde el
  último commit: nada se pierde, pero puede escribirse dos veces. Los duplicados
  se eliminan al leer, por la clave natural (_kafka_partition, _kafka_offset).
- Validación contra el contrato de esquema de bronze: un evento inválido no
  bloquea el flujo; se envía a la cola de errores (DLQ, topic `solicitudes.dlq`)
  con el motivo en una cabecera, para revisarlo después.
- Micro-lotes: se escribe cada `tam_lote` eventos o cada `segundos`, lo que
  ocurra antes. Más grande = menos ficheros pequeños; más pequeño = menos latencia.
- Parada ordenada: ante SIGTERM (p. ej. `docker stop`) termina el lote en curso,
  confirma y sale, en lugar de morir a medias.

Uso:
    uv run consume-events                       # servicio: corre hasta que lo paren
    uv run consume-events --hasta-vaciar        # procesa lo pendiente y sale
"""

from __future__ import annotations

import argparse
import json
import signal
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
from confluent_kafka import Consumer, KafkaError, Producer

from credit_risk.config import BRONZE_DIR, KAFKA_BOOTSTRAP, TOPIC_DLQ, TOPIC_SOLICITUDES
from credit_risk.eventos import Evento, asegurar_topics, validar
from credit_risk.ingest import ESQUEMAS

GRUPO = "bronze-solicitudes"


class FalloSimulado(Exception):
    """Caída provocada tras escribir y antes de confirmar (para probar at-least-once)."""


def escribir_lote(bronze_dir_de, lote: list[tuple[int, int, Evento]]) -> int:
    """Escribe un micro-lote en Parquet: un fichero por (conjunto, tabla, día, partición)."""
    grupos = defaultdict(list)
    for particion, offset, evento in lote:
        grupos[(evento.conjunto, evento.tabla, evento.fecha_entrega, particion)].append(
            (offset, evento)
        )
    con = duckdb.connect()
    for (conjunto, tabla, dia, particion), filas in grupos.items():
        esquema = ESQUEMAS[tabla]
        df = pd.DataFrame([e.datos for _, e in filas], columns=list(esquema), dtype="string")
        df["_kafka_particion"] = particion
        df["_kafka_offset"] = [o for o, _ in filas]
        df["_evento_id"] = [e.evento_id for _, e in filas]
        df["_publicado_en"] = [e.publicado_en for _, e in filas]
        con.register("lote", df)
        # Los tipos los dicta el contrato de esquema, igual que en la ingesta por lotes
        columnas = ", ".join(f'CAST("{c}" AS {t}) AS "{c}"' for c, t in esquema.items())
        destino = (
            bronze_dir_de(conjunto)
            / f"stream_{tabla}"
            / f"fecha_entrega={dia.isoformat()}"
            / f"p{particion}_o{filas[0][0]:012d}-{filas[-1][0]:012d}.parquet"
        )
        destino.parent.mkdir(parents=True, exist_ok=True)
        temporal = destino.with_name(destino.name + ".tmp")
        con.execute(f"""
            COPY (
                SELECT {columnas}, _kafka_particion::INTEGER AS _kafka_particion,
                       _kafka_offset::BIGINT AS _kafka_offset, _evento_id,
                       _publicado_en::TIMESTAMPTZ AS _publicado_en,
                       current_timestamp AS _ingested_at
                FROM lote
            ) TO '{temporal.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """)
        con.unregister("lote")
        temporal.replace(destino)  # atómico: o está el fichero entero o no está
    con.close()
    return len(lote)


def consumir(
    bronze_dir_de=lambda conjunto: BRONZE_DIR,
    bootstrap: str = KAFKA_BOOTSTRAP,
    grupo: str = GRUPO,
    tam_lote: int = 5_000,
    segundos: float = 5.0,
    hasta_vaciar: bool = False,
    fallar_tras_escribir: bool = False,
) -> dict[str, int]:
    """Bucle del consumidor. Devuelve cuántos eventos se escribieron y cuántos fueron a la DLQ."""
    consumidor = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": grupo,
            "enable.auto.commit": False,  # el commit lo hacemos nosotros, tras escribir
            "auto.offset.reset": "earliest",  # un grupo nuevo empieza por el principio
        }
    )
    dlq = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    consumidor.subscribe([TOPIC_SOLICITUDES])

    parar = False

    def al_recibir_senal(*_):
        nonlocal parar
        parar = True

    signal.signal(signal.SIGTERM, al_recibir_senal)

    lote: list[tuple[int, int, Evento]] = []
    sin_confirmar = 0  # mensajes leídos (válidos o no) desde el último commit
    totales = {"escritos": 0, "dlq": 0}
    ultimo_volcado = time.monotonic()
    vacios = 0
    try:
        while not parar:
            mensajes = consumidor.consume(num_messages=500, timeout=1.0)
            vacios = 0 if mensajes else vacios + 1
            for mensaje in mensajes:
                if mensaje.error():
                    if mensaje.error().code() != KafkaError._PARTITION_EOF:
                        raise RuntimeError(mensaje.error())
                    continue
                sin_confirmar += 1
                evento, motivo = validar(mensaje.value())
                if evento:
                    lote.append((mensaje.partition(), mensaje.offset(), evento))
                else:
                    origen = f"{mensaje.topic()}/{mensaje.partition()}/{mensaje.offset()}"
                    dlq.produce(
                        TOPIC_DLQ,
                        key=mensaje.key(),
                        value=mensaje.value(),
                        headers={"motivo": motivo, "origen": origen},
                    )
                    totales["dlq"] += 1

            vaciado = hasta_vaciar and vacios >= 3
            toca = len(lote) >= tam_lote or time.monotonic() - ultimo_volcado >= segundos
            if sin_confirmar and (toca or vaciado or parar):
                totales["escritos"] += escribir_lote(bronze_dir_de, lote) if lote else 0
                dlq.flush(10)
                if fallar_tras_escribir:
                    raise FalloSimulado("caída entre la escritura y el commit")
                consumidor.commit(asynchronous=False)  # ahora sí: el lote está a salvo
                print(f"[lote] {len(lote):>6} escritos · {totales['dlq']} en DLQ en total")
                lote, sin_confirmar, ultimo_volcado = [], 0, time.monotonic()
            if vaciado:
                break
    finally:
        consumidor.close()  # abandona el grupo: sus particiones pasan a otro consumidor
    return totales


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--hasta-vaciar", action="store_true", help="salir al no quedar eventos")
    parser.add_argument(
        "--raiz-lagos",
        type=Path,
        help="escribir en <raiz>/<conjunto>/bronze (un lago por conjunto, como en Airflow)",
    )
    parser.add_argument("--tam-lote", type=int, default=5_000)
    parser.add_argument("--segundos", type=float, default=5.0)
    args = parser.parse_args()

    bronze_dir_de = (
        (lambda conjunto: args.raiz_lagos / conjunto / "bronze")
        if args.raiz_lagos
        else (lambda conjunto: BRONZE_DIR)
    )
    asegurar_topics()
    print(f"Consumiendo '{TOPIC_SOLICITUDES}' de {KAFKA_BOOTSTRAP} (grupo '{GRUPO}')")
    totales = consumir(
        bronze_dir_de,
        tam_lote=args.tam_lote,
        segundos=args.segundos,
        hasta_vaciar=args.hasta_vaciar,
    )
    print(json.dumps(totales))


if __name__ == "__main__":
    main()
