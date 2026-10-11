"""Tests del simulador del sistema origen (landing zone por días)."""

from datetime import date

import duckdb
import pandas as pd
import pytest

from credit_risk.config import TABLES
from credit_risk.simulate_source import simular_origen

INICIO = date(2026, 9, 1)
DIAS = 5


@pytest.fixture
def raw_dir(tmp_path):
    """Las 8 tablas en miniatura: 4 clientes con historial y casos raros."""
    tablas = {
        "application_train": {"SK_ID_CURR": [1, 2, 3], "TARGET": [0, 1, 0]},
        "application_test": {"SK_ID_CURR": [4], "AMT_CREDIT": ["0.10"]},  # texto con cero final
        "bureau": {"SK_ID_CURR": [1, 1, 2], "SK_ID_BUREAU": [10, 11, 20]},
        # El crédito 99 no existe en bureau: es un huérfano, como los 43.041 reales
        "bureau_balance": {"SK_ID_BUREAU": [10, 10, 20, 99], "MONTHS_BALANCE": [0, -1, 0, 0]},
        "previous_application": {"SK_ID_PREV": [100, 101], "SK_ID_CURR": [1, 4]},
        "pos_cash_balance": {"SK_ID_PREV": [100], "SK_ID_CURR": [1]},
        "installments_payments": {"SK_ID_PREV": [100, 101], "SK_ID_CURR": [1, 4]},
        "credit_card_balance": {"SK_ID_PREV": [101], "SK_ID_CURR": [4]},
    }
    carpeta = tmp_path / "raw"
    carpeta.mkdir()
    for nombre, columnas in tablas.items():
        pd.DataFrame(columnas).to_csv(carpeta / TABLES[nombre], index=False)
    return carpeta


def leer(landing, nombre):
    """Filas de una tabla en todas las entregas, con la fecha de su carpeta.

    hive_partitioning: DuckDB convierte "fecha=2026-09-01/" de la ruta en una columna.
    """
    patron = (landing / "*" / f"{TABLES[nombre]}.gz").as_posix()
    return duckdb.sql(f"""
        SELECT * REPLACE (CAST(fecha AS VARCHAR) AS fecha)
        FROM read_csv('{patron}', all_varchar=true, union_by_name=true, hive_partitioning=true)
    """).df()


def test_no_se_pierde_ninguna_fila(raw_dir, tmp_path):
    filas = simular_origen(raw_dir, tmp_path / "landing", INICIO, DIAS)

    esperadas = {n: len(pd.read_csv(raw_dir / f)) for n, f in TABLES.items()}
    assert filas == esperadas


def test_cada_dia_llegan_todos_los_ficheros(raw_dir, tmp_path):
    landing = tmp_path / "landing"
    simular_origen(raw_dir, landing, INICIO, DIAS)

    dias = sorted(p.name for p in landing.iterdir())
    assert dias == [f"fecha=2026-09-0{d}" for d in range(1, DIAS + 1)]
    for dia in landing.iterdir():
        assert sorted(p.name for p in dia.iterdir()) == sorted(f + ".gz" for f in TABLES.values())


def test_el_historial_viaja_con_su_solicitud(raw_dir, tmp_path):
    landing = tmp_path / "landing"
    simular_origen(raw_dir, landing, INICIO, DIAS)

    solicitudes = pd.concat([leer(landing, "application_train"), leer(landing, "application_test")])
    fecha_cliente = solicitudes.set_index("SK_ID_CURR")["fecha"]
    for nombre in ["bureau", "previous_application", "installments_payments"]:
        historial = leer(landing, nombre)
        assert (historial["fecha"] == historial["SK_ID_CURR"].map(fecha_cliente)).all()


def test_los_huerfanos_llegan_el_primer_dia(raw_dir, tmp_path):
    landing = tmp_path / "landing"
    simular_origen(raw_dir, landing, INICIO, DIAS)

    balance = leer(landing, "bureau_balance")
    assert balance.loc[balance["SK_ID_BUREAU"] == "99", "fecha"].tolist() == ["2026-09-01"]


def test_el_reparto_es_determinista(raw_dir, tmp_path):
    simular_origen(raw_dir, tmp_path / "a", INICIO, DIAS)
    simular_origen(raw_dir, tmp_path / "b", INICIO, DIAS)

    a = leer(tmp_path / "a", "application_train").sort_values("SK_ID_CURR")
    b = leer(tmp_path / "b", "application_train").sort_values("SK_ID_CURR")
    assert a["fecha"].tolist() == b["fecha"].tolist()


def test_conserva_el_texto_original(raw_dir, tmp_path):
    # Se copia como texto: "0.10" no se convierte en 0.1
    landing = tmp_path / "landing"
    simular_origen(raw_dir, landing, INICIO, DIAS)

    assert leer(landing, "application_test")["AMT_CREDIT"].tolist() == ["0.10"]


def test_es_idempotente(raw_dir, tmp_path):
    landing = tmp_path / "landing"
    simular_origen(raw_dir, landing, INICIO, DIAS)
    filas = simular_origen(raw_dir, landing, INICIO, DIAS)

    assert filas["bureau_balance"] == 4
    assert sorted(p.name for p in tmp_path.iterdir()) == ["landing", "raw"]  # sin .tmp
