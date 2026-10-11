"""Tests del CDC: interpretación de eventos de Debezium y captura real de Postgres.

Los de integración necesitan Postgres (core bancario), Debezium y Redpanda; se
saltan si no están definidas CORE_DSN_TEST y KAFKA_BOOTSTRAP_TEST (en la CI hay un
job que levanta los tres; en local: `docker compose up -d core-bancario debezium`).
"""

import json
import os
import random
import time
import uuid

import duckdb
import psycopg
import pytest

from credit_risk.cdc import consumir_cdc, escribir_cambios, interpretar
from credit_risk.simulate_core import simular

BROKER = os.getenv("KAFKA_BOOTSTRAP_TEST")
CORE = os.getenv("CORE_DSN_TEST")
integracion = pytest.mark.skipif(
    not (BROKER and CORE), reason="sin Postgres/Debezium/Redpanda (CORE_DSN_TEST)"
)

FILA = {
    "sk_id_curr": 7,
    "estado": "recibida",
    "importe": "1000.50",
    "canal": "web",
    "actualizado_en": "2026-10-11T19:00:00Z",
}
FUENTE = {"lsn": 100, "txId": 5, "ts_ms": 1_791_745_914_129}


def mensaje(op, before=None, after=None):
    return json.dumps({"op": op, "before": before, "after": after, "source": FUENTE}).encode()


# --- Interpretación de los eventos de Debezium (sin infraestructura) -----------------


def test_un_insert_toma_la_fila_nueva():
    fila, motivo = interpretar(mensaje("c", after=FILA))
    assert motivo is None
    assert (fila["op"], fila["estado"], fila["lsn"], fila["tx_id"]) == ("c", "recibida", 100, 5)


def test_un_update_toma_la_fila_nueva():
    fila, _ = interpretar(mensaje("u", before=FILA, after=FILA | {"estado": "aprobada"}))
    assert fila["estado"] == "aprobada"


def test_un_delete_toma_la_fila_anterior():
    # En un delete, "after" es null: lo que se guarda es la fila que se borró
    fila, _ = interpretar(mensaje("d", before=FILA))
    assert (fila["op"], fila["sk_id_curr"], fila["estado"]) == ("d", 7, "recibida")


def test_las_lapidas_se_ignoran_sin_ir_a_la_dlq():
    assert interpretar(None) == (None, None)


@pytest.mark.parametrize(
    "valor",
    [b"{no es json", mensaje("x", after=FILA), json.dumps({"op": "c"}).encode()],
)
def test_los_eventos_mal_formados_van_a_la_dlq(valor):
    fila, motivo = interpretar(valor)
    assert fila is None and motivo


def test_escribe_los_cambios_con_sus_tipos(tmp_path):
    lote = [
        (0, 3, interpretar(mensaje("c", after=FILA))[0]),
        (0, 4, interpretar(mensaje("d", before=FILA))[0]),
    ]

    assert escribir_cambios(tmp_path, lote) == 2

    ficheros = f"{(tmp_path / 'cdc_solicitudes').as_posix()}/*/*.parquet"
    tipos = {c: t for c, t, *_ in duckdb.sql(f"DESCRIBE SELECT * FROM '{ficheros}'").fetchall()}
    assert tipos["importe"] == "DECIMAL(14,2)" and tipos["lsn"] == "BIGINT"
    assert duckdb.sql(f"SELECT list(op ORDER BY _kafka_offset) FROM '{ficheros}'").fetchone()[
        0
    ] == ["c", "d"]


# --- Integración: Postgres -> Debezium -> Redpanda -> bronze -------------------------


@integracion
def test_la_captura_reproduce_todos_los_cambios_de_postgres(tmp_path):
    # Solicitudes con identificadores únicos para no mezclarse con otras pruebas
    base = random.randint(10**9, 2 * 10**9)
    solicitudes = [(base + i, 1000.0 + i) for i in range(40)]
    cambios = simular(CORE, solicitudes)
    esperados = sum(cambios.values())
    ids = [sk for sk, _ in solicitudes]

    # Debezium tarda un poco en publicar: se consume hasta ver todos los cambios
    grupo, filas, limite = f"test-cdc-{uuid.uuid4().hex}", [], time.monotonic() + 90
    while len(filas) < esperados and time.monotonic() < limite:
        consumir_cdc(tmp_path, BROKER, grupo=grupo, segundos=0.5, hasta_vaciar=True)
        ficheros = f"{(tmp_path / 'cdc_solicitudes').as_posix()}/*/*.parquet"
        if list(tmp_path.glob("cdc_solicitudes/*/*.parquet")):
            filas = duckdb.sql(
                f"SELECT DISTINCT sk_id_curr, lsn, op, estado FROM '{ficheros}' "
                f"WHERE sk_id_curr IN ({','.join(map(str, ids))})"
            ).fetchall()

    # Ningún cambio perdido: ni siquiera los inserts, que no se notan en el estado final
    assert len(filas) == esperados
    ultimo = {}
    for sk, _lsn, op, estado in sorted(filas, key=lambda f: f[1]):  # orden del WAL
        ultimo[sk] = None if op == "d" else estado
    with psycopg.connect(CORE) as pg:
        real = dict(
            pg.execute(
                "SELECT sk_id_curr, estado FROM core.solicitudes WHERE sk_id_curr = ANY(%s)", (ids,)
            ).fetchall()
        )
    assert {sk: e for sk, e in ultimo.items() if e is not None} == real
