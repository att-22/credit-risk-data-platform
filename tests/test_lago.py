"""Tests del acceso al lago (lago.py), en disco y en S3.

Los mismos tests se ejecutan sobre una carpeta temporal y, si está definida la
variable LAGO_S3_TEST, sobre un bucket de S3 (en la CI, Moto creado con Terraform;
en local: ver infra/README.md y LAGO_S3_TEST=s3://credit-risk-local-lake-123456789012
con las variables AWS_* de Moto).
"""

import os
import uuid
from datetime import date

import pytest

from credit_risk import lago
from credit_risk.config import TABLES
from credit_risk.ingest import ESQUEMAS, dias_ingeridos, entregas_en_landing, ingerir_dia

BUCKET = os.getenv("LAGO_S3_TEST")


@pytest.fixture(params=["disco", "s3"])
def raiz(request, tmp_path):
    """Raíz de un lago vacío: una carpeta temporal o un prefijo único en el bucket."""
    if request.param == "disco":
        yield lago.ruta(tmp_path)
        return
    if not BUCKET:
        pytest.skip("sin bucket de S3 (LAGO_S3_TEST)")
    prefijo = lago.unir(BUCKET, "_tests", uuid.uuid4().hex)
    yield prefijo
    lago.borrar(prefijo)


def test_copiar_escribe_y_se_puede_leer(raiz):
    con = lago.conectar(raiz)
    destino = lago.unir(raiz, "zona", "a", "datos.parquet")
    lago.copiar(con, "SELECT range AS n FROM range(5)", destino, "FORMAT parquet")
    assert con.execute(f"SELECT sum(n) FROM '{destino}'").fetchone()[0] == 10
    assert lago.existe(destino)
    # En disco no queda el fichero temporal del renombrado atómico
    assert lago.ficheros(lago.unir(raiz, "zona", "a")) == {"datos.parquet"}


def test_listar_carpetas_y_ficheros(raiz):
    con = lago.conectar(raiz)
    for carpeta in ("fecha=2026-09-02", "fecha=2026-09-01", "otra"):
        destino = lago.unir(raiz, carpeta, "f.csv")
        lago.copiar(con, "SELECT 1 AS x", destino, "FORMAT csv")
    assert [lago.nombre(c) for c in lago.subcarpetas(raiz, "fecha=")] == [
        "fecha=2026-09-01",
        "fecha=2026-09-02",
    ]
    assert lago.ficheros(lago.unir(raiz, "otra")) == {"f.csv"}
    assert lago.subcarpetas(lago.unir(raiz, "no-existe")) == []


def test_borrar_quita_la_carpeta_entera(raiz):
    con = lago.conectar(raiz)
    lago.copiar(con, "SELECT 1 AS x", lago.unir(raiz, "z", "p=1", "f.parquet"), "FORMAT parquet")
    lago.borrar(lago.unir(raiz, "z"))
    assert not lago.existe(lago.unir(raiz, "z", "p=1", "f.parquet"))
    assert lago.subcarpetas(raiz) == []
    lago.borrar(lago.unir(raiz, "z"))  # borrar lo que no existe no falla


def test_subir_sustituye_el_destino(raiz, tmp_path):
    origen = tmp_path / "origen_local"
    (origen / "d").mkdir(parents=True)
    (origen / "d" / "nuevo.txt").write_text("hola")
    con = lago.conectar(raiz)
    destino = lago.unir(raiz, "subido")
    lago.copiar(con, "SELECT 1 AS x", lago.unir(destino, "d", "viejo.csv"), "FORMAT csv")
    lago.subir(origen, destino)
    assert lago.ficheros(lago.unir(destino, "d")) == {"nuevo.txt"}


def test_la_ingesta_funciona_igual(raiz):
    """Una entrega mínima en la landing del lago se ingiere en su bronze."""
    landing, bronze = lago.unir(raiz, "landing"), lago.unir(raiz, "bronze")
    con = lago.conectar(raiz)
    dia = date(2026, 9, 1)
    for tabla, fichero in TABLES.items():
        # Entrega sin filas, solo la cabecera: cumple el contrato si las columnas cuadran
        columnas = ", ".join(f'NULL::VARCHAR AS "{c}"' for c in ESQUEMAS[tabla])
        lago.copiar(
            con,
            f"SELECT {columnas} LIMIT 0",
            lago.unir(landing, f"fecha={dia}", f"{fichero}.gz"),
            "FORMAT csv, HEADER, COMPRESSION gzip",
        )
    assert entregas_en_landing(landing) == [dia]
    filas = ingerir_dia(con, landing, bronze, dia)
    assert set(filas) == set(TABLES) and sum(filas.values()) == 0
    assert dias_ingeridos(con, bronze) == {dia}
