"""Tests de la API del sistema origen simulado."""

import random
from datetime import date

import pytest
from fastapi.testclient import TestClient

from credit_risk.api_origen import Almacen, app
from credit_risk.config import ORIGEN_API_KEY
from credit_risk.simulate_source import simular_origen

CLAVE = {"X-API-Key": ORIGEN_API_KEY}
DIA = date(2026, 9, 1)
RUTA = f"/v1/pruebas/entregas/{DIA}"


@pytest.fixture
def cliente(raw_dir, tmp_path):
    """API sirviendo el almacén de un banco con 3 días de entregas (conjunto 'pruebas')."""
    simular_origen(raw_dir, tmp_path / "origen" / "pruebas", DIA, 3)
    app.state.almacen = Almacen(tmp_path / "origen")
    app.state.tasa_fallos = 0.0
    with TestClient(app) as cliente:  # cliente HTTP en memoria, sin levantar un servidor
        yield cliente


def test_sin_clave_responde_401(cliente):
    assert cliente.get(f"{RUTA}/bureau").status_code == 401
    assert cliente.get(f"{RUTA}/bureau", headers={"X-API-Key": "mala"}).status_code == 401


def test_lista_las_entregas(cliente):
    respuesta = cliente.get("/v1/pruebas/entregas", headers=CLAVE)
    assert respuesta.json() == ["2026-09-01", "2026-09-02", "2026-09-03"]


def test_tabla_o_entrega_inexistente_responde_404(cliente):
    assert cliente.get(f"{RUTA}/no_existe", headers=CLAVE).status_code == 404
    assert cliente.get("/v1/pruebas/entregas/2030-01-01/bureau", headers=CLAVE).status_code == 404


def test_pagina_con_cursor_hasta_el_final(cliente):
    # Con limite=1, cada página trae una fila y un cursor hasta la última
    filas, params = [], {"limite": 1}
    while True:
        pagina = cliente.get(f"{RUTA}/bureau_balance", headers=CLAVE, params=params).json()
        filas += pagina["filas"]
        if pagina["siguiente"] is None:
            break
        params = {"limite": 1, "cursor": pagina["siguiente"]}

    assert pagina["columnas"] == ["SK_ID_BUREAU", "MONTHS_BALANCE"]
    assert len(filas) == pagina["total"] > 1  # el huérfano del primer día incluido


def test_cursor_no_valido_responde_400(cliente):
    respuesta = cliente.get(f"{RUTA}/bureau", headers=CLAVE, params={"cursor": "basura"})
    assert respuesta.status_code == 400


def test_limite_fuera_de_rango_responde_422(cliente):
    # Validación automática de FastAPI a partir de la firma del endpoint
    assert cliente.get(f"{RUTA}/bureau", headers=CLAVE, params={"limite": 0}).status_code == 422


def test_simula_fallos_transitorios(cliente):
    app.state.tasa_fallos = 1.0
    app.state.azar = random.Random(0)
    codigos = {cliente.get(f"{RUTA}/bureau", headers=CLAVE).status_code for _ in range(20)}

    assert codigos == {429, 503}
    respuesta = cliente.get(f"{RUTA}/bureau", headers=CLAVE)
    assert respuesta.status_code == 503 or respuesta.headers["Retry-After"] == "1"
