"""Tests de la ingesta a la capa bronze."""

import duckdb
import pandas as pd
import pytest

from credit_risk.ingest import csv_a_parquet


@pytest.fixture
def con():
    conexion = duckdb.connect()
    yield conexion  # el test usa la conexión...
    conexion.close()  # ...y al terminar se cierra


@pytest.fixture
def csv_origen(tmp_path):
    ruta = tmp_path / "raw" / "bureau.csv"
    ruta.parent.mkdir()
    pd.DataFrame({"SK_ID_CURR": [1, 1, 2], "AMT_CREDIT_SUM": [1000.0, 2000.0, 500.0]}).to_csv(
        ruta, index=False
    )
    return ruta


def test_convierte_todas_las_filas(con, csv_origen, tmp_path):
    destino = tmp_path / "bronze" / "bureau.parquet"

    filas = csv_a_parquet(con, csv_origen, destino)

    assert filas == 3
    assert destino.exists()


def test_conserva_columnas_y_anade_trazabilidad(con, csv_origen, tmp_path):
    destino = tmp_path / "bronze" / "bureau.parquet"

    csv_a_parquet(con, csv_origen, destino)
    columnas = [fila[0] for fila in con.execute(f"DESCRIBE '{destino.as_posix()}'").fetchall()]

    assert columnas == ["SK_ID_CURR", "AMT_CREDIT_SUM", "_ingested_at", "_source_file"]
    origen = con.execute(f"SELECT DISTINCT _source_file FROM '{destino.as_posix()}'").fetchall()
    assert origen == [("bureau.csv",)]


def test_es_idempotente(con, csv_origen, tmp_path):
    # Ejecutar dos veces no duplica filas ni deja ficheros temporales.
    destino = tmp_path / "bronze" / "bureau.parquet"

    csv_a_parquet(con, csv_origen, destino)
    filas = csv_a_parquet(con, csv_origen, destino)

    assert filas == 3
    assert [p.name for p in destino.parent.iterdir()] == ["bureau.parquet"]
