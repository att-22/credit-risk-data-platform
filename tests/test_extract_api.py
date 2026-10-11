"""Tests de la extracción de la API del origen a la landing zone."""

import random
from datetime import date

import duckdb
import httpx
import pytest
from fastapi.testclient import TestClient

from credit_risk import extract_api
from credit_risk.api_origen import Almacen, app
from credit_risk.config import ORIGEN_API_KEY, TABLES
from credit_risk.extract_api import ExtraccionIncompleta, extraer_dia, pedir
from credit_risk.simulate_source import simular_origen

DIA = date(2026, 9, 1)


@pytest.fixture
def esperas(monkeypatch):
    """Sustituye time.sleep: los tests no esperan de verdad y se ven los reintentos."""
    registro = []
    monkeypatch.setattr(extract_api.time, "sleep", registro.append)
    return registro


@pytest.fixture
def banco(raw_dir, tmp_path):
    """Almacén del banco (conjunto 'pruebas') con 3 días de entregas."""
    raiz = tmp_path / "origen"
    simular_origen(raw_dir, raiz / "pruebas", DIA, 3)
    app.state.almacen = Almacen(raiz)
    app.state.tasa_fallos = 0.0
    return raiz / "pruebas"


def cliente_api(clave=ORIGEN_API_KEY):
    return TestClient(app, headers={"X-API-Key": clave})


def iguales(a, b):
    """¿Dos CSV comprimidos tienen exactamente las mismas filas (texto incluido)?"""
    leer = "SELECT * FROM read_csv('{}', all_varchar=true, hive_partitioning=false)"
    consulta = f"({leer.format(a.as_posix())} EXCEPT ALL {leer.format(b.as_posix())})"
    inversa = f"({leer.format(b.as_posix())} EXCEPT ALL {leer.format(a.as_posix())})"
    return duckdb.sql(f"SELECT count(*) FROM ({consulta} UNION ALL {inversa})").fetchone()[0] == 0


def test_la_entrega_extraida_es_igual_a_la_del_banco(banco, tmp_path, esperas):
    landing = tmp_path / "landing"
    with cliente_api() as cliente:
        filas = extraer_dia(cliente, "pruebas", DIA, landing, limite=1)  # fuerza la paginación

    for fichero in TABLES.values():
        assert iguales(
            landing / f"fecha={DIA}" / f"{fichero}.gz", banco / f"fecha={DIA}" / f"{fichero}.gz"
        )
    # El huérfano 99 siempre llega el primer día: la tabla no puede venir vacía
    assert filas["bureau_balance"] >= 1


def test_reintenta_los_fallos_transitorios(banco, tmp_path, esperas):
    app.state.tasa_fallos = 0.4
    app.state.azar = random.Random(1)
    landing = tmp_path / "landing"
    with cliente_api() as cliente:
        extraer_dia(cliente, "pruebas", DIA, landing, limite=1, espera_base=0.01)

    assert esperas  # hubo reintentos...
    fichero = TABLES["bureau_balance"] + ".gz"
    assert iguales(
        landing / f"fecha={DIA}" / fichero, banco / f"fecha={DIA}" / fichero
    )  # ...sin perder filas


def test_no_reintenta_los_errores_permanentes(banco, esperas):
    with cliente_api(clave="mala") as cliente, pytest.raises(httpx.HTTPStatusError) as error:
        pedir(cliente, f"/v1/pruebas/entregas/{DIA}/bureau", {})

    assert error.value.response.status_code == 401
    assert esperas == []  # una clave mala no se arregla reintentando


def test_se_rinde_tras_agotar_los_reintentos(banco, esperas):
    app.state.tasa_fallos = 1.0
    with cliente_api() as cliente, pytest.raises(httpx.HTTPStatusError):
        pedir(cliente, f"/v1/pruebas/entregas/{DIA}/bureau", {}, reintentos=3)

    assert len(esperas) == 3


def test_detecta_una_extraccion_incompleta(tmp_path, esperas):
    # API falsa que anuncia 3 filas pero solo entrega 2
    def responder(request):
        return httpx.Response(
            200, json={"columnas": ["A"], "filas": [["1"], ["2"]], "total": 3, "siguiente": None}
        )

    cliente = httpx.Client(transport=httpx.MockTransport(responder), base_url="http://falsa")
    with pytest.raises(ExtraccionIncompleta, match="recibidas 2"):
        extraer_dia(cliente, "pruebas", DIA, tmp_path / "landing")
    assert not (tmp_path / "landing" / f"fecha={DIA}").exists()  # nada a medias


def test_es_idempotente(banco, tmp_path, esperas):
    landing = tmp_path / "landing"
    with cliente_api() as cliente:
        extraer_dia(cliente, "pruebas", DIA, landing)
        extraer_dia(cliente, "pruebas", DIA, landing)

    assert [p.name for p in landing.iterdir()] == [f"fecha={DIA}"]  # sin carpetas .tmp
