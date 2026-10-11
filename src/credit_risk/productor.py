"""Productor de eventos: el sistema origen publica las solicitudes de un día en Kafka.

Lee las solicitudes (application_train y application_test) de un día del almacén
del banco (ORIGEN_RAIZ/<conjunto>/) y publica una por evento en el topic
`solicitudes`, con SK_ID_CURR como clave: todos los eventos de un cliente van a
la misma partición, así que se consumen en orden.

Configuración del productor:
- acks=all: el broker solo confirma cuando el mensaje está guardado.
- enable.idempotence=true: si un envío se reintenta por un fallo de red, el
  broker descarta el duplicado (cada mensaje lleva un número de secuencia).

`--malos N` publica además N eventos inválidos a propósito, para ver cómo el
consumidor los manda a la cola de errores (DLQ).

Uso:
    uv run produce-events --fecha 2026-09-07 --conjunto completo
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
from confluent_kafka import Producer

from credit_risk.config import KAFKA_BOOTSTRAP, ORIGEN_RAIZ, TABLES, TOPIC_SOLICITUDES
from credit_risk.eventos import TABLAS_EVENTO, asegurar_topics


def eventos_del_dia(raiz: Path, conjunto: str, dia: date) -> list[dict]:
    """Construye los eventos de las solicitudes de un día del almacén del banco."""
    eventos = []
    for tabla in TABLAS_EVENTO:
        ruta = raiz / conjunto / f"fecha={dia.isoformat()}" / f"{TABLES[tabla]}.gz"
        # all_varchar: el evento lleva el texto original, como las entregas por fichero
        opciones = "all_varchar=true, hive_partitioning=false"
        relacion = duckdb.sql(f"SELECT * FROM read_csv('{ruta.as_posix()}', {opciones})")
        for fila in relacion.fetchall():
            datos = dict(zip(relacion.columns, fila, strict=True))
            eventos.append(
                {
                    "evento_id": f"{dia}/{tabla}/{datos['SK_ID_CURR']}",
                    "tabla": tabla,
                    "conjunto": conjunto,
                    "fecha_entrega": dia.isoformat(),
                    "publicado_en": datetime.now(UTC).isoformat(),
                    "datos": datos,
                }
            )
    return eventos


def eventos_malos(n: int, dia: date) -> list[dict]:
    """Eventos inválidos de varios tipos, para ejercitar la cola de errores."""
    tipos = [
        {"evento_id": "malo/sin-campos"},  # faltan campos obligatorios
        {
            "evento_id": "malo/id",
            "tabla": "application_test",
            "conjunto": "x",
            "fecha_entrega": dia.isoformat(),
            "publicado_en": datetime.now(UTC).isoformat(),
            "datos": {"SK_ID_CURR": "no-es-un-numero"},
        },  # identificador no entero
    ]
    return [tipos[i % len(tipos)] | {"evento_id": f"malo/{i}"} for i in range(n)]


def publicar(eventos: list[dict], bootstrap: str = KAFKA_BOOTSTRAP, malformados: int = 0) -> int:
    """Publica los eventos (y N bytes que ni siquiera son JSON). Devuelve los confirmados."""
    productor = Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "linger.ms": 50,
        }  # agrupa mensajes durante 50 ms: menos peticiones, más rendimiento
    )
    errores = []

    def al_entregar(error, _mensaje):
        if error is not None:
            errores.append(error)

    for evento in eventos:
        clave = (evento.get("datos") or {}).get("SK_ID_CURR") or evento["evento_id"]
        productor.produce(
            TOPIC_SOLICITUDES, key=str(clave), value=json.dumps(evento), on_delivery=al_entregar
        )
        productor.poll(0)  # atiende las confirmaciones pendientes sin bloquear
    for i in range(malformados):
        productor.produce(TOPIC_SOLICITUDES, key=f"basura/{i}", value=b"{esto no es json")
    pendientes = productor.flush(30)
    if errores or pendientes:
        raise RuntimeError(f"{len(errores)} errores de entrega, {pendientes} sin confirmar")
    return len(eventos) + malformados


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--fecha", type=date.fromisoformat, required=True)
    parser.add_argument("--conjunto", default="completo")
    parser.add_argument("--malos", type=int, default=0, help="eventos inválidos a añadir")
    args = parser.parse_args()

    asegurar_topics()
    inicio = time.perf_counter()
    eventos = eventos_del_dia(ORIGEN_RAIZ, args.conjunto, args.fecha)
    eventos += eventos_malos(args.malos, args.fecha)
    n = publicar(eventos, malformados=1 if args.malos else 0)
    print(
        f"Publicados {n:,} eventos en '{TOPIC_SOLICITUDES}' ({KAFKA_BOOTSTRAP}) "
        f"en {time.perf_counter() - inicio:.1f} s"
    )


if __name__ == "__main__":
    main()
