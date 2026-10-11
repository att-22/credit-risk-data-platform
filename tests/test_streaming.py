"""Tests del streaming: contrato de eventos, micro-lotes y consumo contra Redpanda.

Los tests de integración necesitan un broker Kafka/Redpanda y se saltan si no
está definida la variable KAFKA_BOOTSTRAP_TEST (en la CI hay un job con Redpanda;
en local: `docker compose up -d redpanda` y KAFKA_BOOTSTRAP_TEST=localhost:19092).
"""

import json
import os
import uuid
from datetime import date

import duckdb
import pytest
from confluent_kafka import Consumer

from credit_risk import consumidor, eventos, productor
from credit_risk.consumidor import FalloSimulado, consumir, escribir_lote
from credit_risk.eventos import Evento, asegurar_topics, validar
from credit_risk.ingest import ESQUEMAS
from credit_risk.productor import eventos_del_dia, eventos_malos, publicar
from credit_risk.simulate_source import simular_origen

DIA = date(2026, 9, 1)
BROKER = os.getenv("KAFKA_BOOTSTRAP_TEST")
integracion = pytest.mark.skipif(not BROKER, reason="sin broker (KAFKA_BOOTSTRAP_TEST)")


def evento(sk_id="100002", **cambios):
    datos = dict.fromkeys(ESQUEMAS["application_test"])
    datos["SK_ID_CURR"] = sk_id
    base = {
        "evento_id": f"{DIA}/application_test/{sk_id}",
        "tabla": "application_test",
        "conjunto": "pruebas",
        "fecha_entrega": DIA.isoformat(),
        "publicado_en": "2026-10-11T18:00:00+00:00",
        "datos": datos,
    }
    return base | cambios


def leer_stream(bronze, deduplicar=True):
    """Filas del stream en bronze; con deduplicar, una por evento_id (la última publicada)."""
    ficheros = f"{bronze.as_posix()}/stream_*/*/*.parquet"
    consulta = f"SELECT * FROM read_parquet('{ficheros}', union_by_name=true)"
    if deduplicar:
        consulta += (
            " QUALIFY row_number() OVER (PARTITION BY _evento_id ORDER BY _publicado_en DESC) = 1"
        )
    return duckdb.sql(consulta).df()


# --- Contrato del evento (sin broker) -----------------------------------------------


def test_un_evento_correcto_es_valido():
    valido, motivo = validar(json.dumps(evento()).encode())
    assert motivo is None and valido.datos["SK_ID_CURR"] == "100002"


@pytest.mark.parametrize(
    "valor, contiene",
    [
        (b"{esto no es json", "JSON"),
        (json.dumps(evento(sk_id="abc")).encode(), "SK_ID_CURR"),
        (json.dumps(evento(tabla="otra")).encode(), "application"),
        (json.dumps(evento() | {"datos": {"SK_ID_CURR": "1"}}).encode(), "faltan"),
    ],
)
def test_los_eventos_invalidos_se_rechazan_con_motivo(valor, contiene):
    valido, motivo = validar(valor)
    assert valido is None and contiene in motivo


def test_los_eventos_malos_del_productor_son_invalidos():
    for malo in eventos_malos(4, DIA):
        assert validar(json.dumps(malo).encode())[0] is None


# --- Micro-lotes en bronze (sin broker) ----------------------------------------------


def test_escribe_un_fichero_por_particion_con_los_tipos_del_contrato(tmp_path):
    lote = [
        (0, 7, Evento(**evento("1"))),
        (0, 9, Evento(**evento("2"))),
        (2, 3, Evento(**evento("3"))),
    ]

    assert escribir_lote(lambda conjunto: tmp_path / conjunto, lote) == 3

    carpeta = tmp_path / "pruebas" / "stream_application_test" / f"fecha_entrega={DIA}"
    assert sorted(p.name for p in carpeta.iterdir()) == [
        "p0_o000000000007-000000000009.parquet",
        "p2_o000000000003-000000000003.parquet",
    ]
    describe = duckdb.sql(f"DESCRIBE SELECT * FROM '{carpeta.as_posix()}/*.parquet'").fetchall()
    tipos = {columna: tipo for columna, tipo, *_ in describe}
    assert tipos["SK_ID_CURR"] == "BIGINT" and tipos["_kafka_offset"] == "BIGINT"


def test_la_deduplicacion_deja_un_evento_por_id(tmp_path):
    # El mismo evento leído dos veces (otra partición/offset): solo cuenta una vez
    lote = [
        (0, 1, Evento(**evento("1"))),
        (1, 5, Evento(**evento("1"))),
        (1, 6, Evento(**evento("2"))),
    ]
    escribir_lote(lambda conjunto: tmp_path, lote)

    assert len(leer_stream(tmp_path, deduplicar=False)) == 3
    assert sorted(leer_stream(tmp_path)["SK_ID_CURR"]) == [1, 2]


# --- Integración con Redpanda ----------------------------------------------------------


# Contrato en miniatura de las tablas del fixture raw_dir (conftest.py)
MINI_ESQUEMAS = {
    "application_train": {"SK_ID_CURR": "BIGINT", "TARGET": "BIGINT"},
    "application_test": {"SK_ID_CURR": "BIGINT", "AMT_CREDIT": "DOUBLE"},
}


@pytest.fixture
def topics(monkeypatch):
    """Topics y grupo únicos por test: los tests no se pisan aunque compartan broker."""
    for modulo in (eventos, consumidor):
        monkeypatch.setattr(modulo, "ESQUEMAS", MINI_ESQUEMAS)
    sufijo = uuid.uuid4().hex[:8]
    for modulo in (eventos, productor, consumidor):
        monkeypatch.setattr(modulo, "TOPIC_SOLICITUDES", f"solicitudes-{sufijo}", raising=False)
        monkeypatch.setattr(modulo, "TOPIC_DLQ", f"solicitudes-{sufijo}.dlq", raising=False)
    asegurar_topics(BROKER)
    return f"grupo-{sufijo}"


@pytest.fixture
def eventos_banco(raw_dir, tmp_path):
    simular_origen(raw_dir, tmp_path / "origen" / "pruebas", DIA, 1)  # todo en un solo día
    return eventos_del_dia(tmp_path / "origen", "pruebas", DIA)


def consumir_todo(grupo, bronze, **kw):
    return consumir(lambda c: bronze, BROKER, grupo, segundos=0.5, hasta_vaciar=True, **kw)


@integracion
def test_publica_y_consume_con_dlq(topics, eventos_banco, tmp_path):
    publicar(eventos_banco + eventos_malos(2, DIA), BROKER, malformados=1)

    totales = consumir_todo(topics, tmp_path / "bronze")

    assert totales == {"escritos": len(eventos_banco), "dlq": 3}
    assert len(leer_stream(tmp_path / "bronze")) == len(eventos_banco)
    # La DLQ guarda el mensaje original y el motivo del rechazo en una cabecera
    lector = Consumer(
        {"bootstrap.servers": BROKER, "group.id": "lector-dlq", "auto.offset.reset": "earliest"}
    )
    lector.subscribe([consumidor.TOPIC_DLQ])
    mensajes = lector.consume(num_messages=10, timeout=10)
    lector.close()
    assert len(mensajes) == 3
    assert all(dict(m.headers())["motivo"] for m in mensajes)


@integracion
def test_una_caida_antes_del_commit_no_pierde_eventos(topics, eventos_banco, tmp_path):
    publicar(eventos_banco, BROKER)
    bronze = tmp_path / "bronze"

    with pytest.raises(FalloSimulado):  # escribe el lote y "muere" antes de confirmar
        consumir_todo(topics, bronze, fallar_tras_escribir=True)
    consumir_todo(topics, bronze)  # al volver, relee desde el último commit

    assert len(leer_stream(bronze)) == len(eventos_banco)  # nada perdido
    assert len(leer_stream(bronze, deduplicar=False)) >= len(eventos_banco)  # quizá repetido


@integracion
def test_republicar_un_dia_no_duplica_tras_deduplicar(topics, eventos_banco, tmp_path):
    publicar(eventos_banco, BROKER)
    publicar(eventos_banco, BROKER)  # p. ej. el productor se reintenta entero

    consumir_todo(topics, tmp_path / "bronze")

    assert len(leer_stream(tmp_path / "bronze", deduplicar=False)) == 2 * len(eventos_banco)
    assert len(leer_stream(tmp_path / "bronze")) == len(eventos_banco)
