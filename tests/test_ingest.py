"""Tests de la ingesta incremental a la capa bronze."""

from datetime import date

import duckdb
import pandas as pd
import pytest

from credit_risk import ingest
from credit_risk.config import TABLES
from credit_risk.ingest import (
    CambioDeEsquema,
    EntregaIncompleta,
    csv_a_parquet,
    dias_ingeridos,
    entregas_en_landing,
    ingerir_dia,
)

ESQUEMA = {"SK_ID_CURR": "BIGINT", "AMT_CREDIT_SUM": "DOUBLE"}
DIA_1, DIA_2 = date(2026, 9, 1), date(2026, 9, 2)


@pytest.fixture
def con():
    conexion = duckdb.connect()
    yield conexion  # el test usa la conexión...
    conexion.close()  # ...y al terminar se cierra


@pytest.fixture
def csv_origen(tmp_path):
    ruta = tmp_path / "fecha=2026-09-01" / "bureau.csv.gz"
    ruta.parent.mkdir()
    pd.DataFrame({"SK_ID_CURR": [1, 1, 2], "AMT_CREDIT_SUM": [1000.0, 2000.0, 500.0]}).to_csv(
        ruta, index=False
    )
    return ruta


@pytest.fixture
def landing(tmp_path, monkeypatch):
    """Landing zone con dos entregas de las 8 tablas, todas con el mismo mini esquema."""
    monkeypatch.setattr(ingest, "ESQUEMAS", dict.fromkeys(TABLES, ESQUEMA))
    carpeta = tmp_path / "landing"
    for dia in (DIA_1, DIA_2):
        for fichero in TABLES.values():
            ruta = carpeta / f"fecha={dia}" / f"{fichero}.gz"
            ruta.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame({"SK_ID_CURR": [dia.day], "AMT_CREDIT_SUM": [1.5]}).to_csv(
                ruta, index=False
            )
    return carpeta


def test_convierte_todas_las_filas(con, csv_origen, tmp_path):
    destino = tmp_path / "bronze" / "bureau.parquet"

    filas = csv_a_parquet(con, csv_origen, destino, ESQUEMA)

    assert filas == 3
    assert destino.exists()


def test_aplica_el_esquema_y_anade_trazabilidad(con, csv_origen, tmp_path):
    destino = tmp_path / "bronze" / "bureau.parquet"

    csv_a_parquet(con, csv_origen, destino, ESQUEMA)
    tipos = con.execute(f"DESCRIBE '{destino.as_posix()}'").fetchall()

    assert [(t[0], t[1]) for t in tipos][:2] == [
        ("SK_ID_CURR", "BIGINT"),
        ("AMT_CREDIT_SUM", "DOUBLE"),
    ]
    assert [t[0] for t in tipos][2:] == ["_ingested_at", "_source_file"]
    origen = con.execute(f"SELECT DISTINCT _source_file FROM '{destino.as_posix()}'").fetchall()
    assert origen == [("fecha=2026-09-01/bureau.csv.gz",)]


def test_un_fichero_vacio_conserva_los_tipos(con, tmp_path):
    # Un día sin filas (solo cabecera) no puede deducir tipos: los pone el contrato
    vacio = tmp_path / "vacio.csv"
    vacio.write_text("SK_ID_CURR,AMT_CREDIT_SUM\n")
    destino = tmp_path / "vacio.parquet"

    assert csv_a_parquet(con, vacio, destino, ESQUEMA) == 0
    tipos = [t[1] for t in con.execute(f"DESCRIBE '{destino.as_posix()}'").fetchall()]
    assert tipos[:2] == ["BIGINT", "DOUBLE"]


def test_rechaza_un_cambio_de_esquema(con, tmp_path):
    cambiado = tmp_path / "cambiado.csv"
    cambiado.write_text("SK_ID_CURR,AMT_CREDIT_SUM,COLUMNA_NUEVA\n1,2.0,x\n")

    with pytest.raises(CambioDeEsquema, match="COLUMNA_NUEVA"):
        csv_a_parquet(con, cambiado, tmp_path / "x.parquet", ESQUEMA)


def test_es_idempotente(con, csv_origen, tmp_path):
    # Ejecutar dos veces no duplica filas ni deja ficheros temporales.
    destino = tmp_path / "bronze" / "bureau.parquet"

    csv_a_parquet(con, csv_origen, destino, ESQUEMA)
    filas = csv_a_parquet(con, csv_origen, destino, ESQUEMA)

    assert filas == 3
    assert [p.name for p in destino.parent.iterdir()] == ["bureau.parquet"]


def test_ingiere_un_dia_en_particiones_y_lo_registra(con, landing, tmp_path):
    bronze = tmp_path / "bronze"

    filas = ingerir_dia(con, landing, bronze, DIA_1)

    assert filas == dict.fromkeys(TABLES, 1)
    assert (bronze / "bureau" / "fecha_entrega=2026-09-01" / "data.parquet").exists()
    assert dias_ingeridos(con, bronze) == {DIA_1}


def test_solo_quedan_pendientes_las_entregas_nuevas(con, landing, tmp_path):
    bronze = tmp_path / "bronze"
    ingerir_dia(con, landing, bronze, DIA_1)

    pendientes = [d for d in entregas_en_landing(landing) if d not in dias_ingeridos(con, bronze)]

    assert pendientes == [DIA_2]


def test_reingerir_un_dia_no_duplica_el_control(con, landing, tmp_path):
    bronze = tmp_path / "bronze"
    ingerir_dia(con, landing, bronze, DIA_1)
    ingerir_dia(con, landing, bronze, DIA_2)
    ingerir_dia(con, landing, bronze, DIA_1)

    control = (bronze / "_ingestas.parquet").as_posix()
    por_dia = con.execute(
        f"SELECT fecha_entrega, count(*) FROM '{control}' GROUP BY ALL ORDER BY 1"
    ).fetchall()
    assert por_dia == [(DIA_1, len(TABLES)), (DIA_2, len(TABLES))]


def test_una_entrega_incompleta_no_se_ingiere(con, landing, tmp_path):
    (landing / f"fecha={DIA_2}" / "bureau.csv.gz").unlink()
    bronze = tmp_path / "bronze"

    with pytest.raises(EntregaIncompleta, match="bureau.csv"):
        ingerir_dia(con, landing, bronze, DIA_2)
    assert dias_ingeridos(con, bronze) == set()
