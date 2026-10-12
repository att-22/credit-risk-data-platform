"""Tests de la puntuación por lotes (scoring.py).

Necesitan el grupo de dependencias `ml`; sin él se omiten (job "Modelo" de la CI).
"""

import duckdb
import numpy as np
import pytest

pytest.importorskip("lightgbm", reason="sin el grupo de dependencias ml")
pytest.importorskip("mlflow", reason="sin el grupo de dependencias ml")

from test_modelo import _mart_sintetico  # noqa: E402

from credit_risk import modelo  # noqa: E402
from credit_risk.scoring import decil, puntuar_pendientes  # noqa: E402


@pytest.fixture
def entorno(tmp_path):
    """Un mart sintético (3.000 con TARGET, 500 sin él) y un modelo publicado."""
    almacen, modelos = tmp_path / "w.duckdb", tmp_path / "modelos"
    _mart_sintetico(almacen)
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    modelo.ejecutar(almacen, modelos, uri, prueba_rapida=True)
    return almacen, modelos, (tmp_path / "scoring").as_posix()


def leer(scoring: str):
    return duckdb.sql(
        f"SELECT * FROM read_parquet('{scoring}/*/*.parquet', hive_partitioning = true)"
    ).df()


def test_el_decil_sale_de_los_cortes_del_entrenamiento():
    cortes = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]

    deciles = decil(np.array([0.0, 0.05, 0.1, 0.55, 0.95, 1.0]), cortes)

    assert deciles.tolist() == [1, 1, 2, 6, 10, 10]


def test_puntua_solo_las_solicitudes_sin_target(entorno):
    almacen, modelos, scoring = entorno

    resumen = puntuar_pendientes(almacen, modelos, scoring)

    puntuadas = leer(scoring)
    assert resumen["solicitudes"] == len(puntuadas) == 500
    assert puntuadas["SK_ID_CURR"].min() > 1_000_000  # las del conjunto sin TARGET
    assert puntuadas["SK_ID_CURR"].is_unique
    assert puntuadas["probabilidad_impago"].between(0, 1).all()
    assert puntuadas["decil_riesgo"].between(1, 10).all()
    assert sum(resumen["por_decil"]) == 500
    assert (puntuadas["modelo"].astype(str) == resumen["modelo"]).all()
    assert puntuadas["mart_ingerido_en"].notna().all()


def test_mas_riesgo_real_da_mas_puntuacion(entorno):
    """En el mart sintético el impago crece con DEUDA: la puntuación debe seguirla."""
    almacen, modelos, scoring = entorno
    puntuar_pendientes(almacen, modelos, scoring)

    with duckdb.connect(str(almacen), read_only=True) as con:
        deuda = con.execute(f"SELECT SK_ID_CURR, DEUDA FROM {modelo.MART}").df()
    juntas = leer(scoring).merge(deuda, on="SK_ID_CURR")

    assert juntas["probabilidad_impago"].corr(juntas["DEUDA"], method="spearman") > 0.8


def test_repetir_con_el_mismo_modelo_no_duplica(entorno):
    almacen, modelos, scoring = entorno
    puntuar_pendientes(almacen, modelos, scoring)
    puntuar_pendientes(almacen, modelos, scoring)

    assert len(leer(scoring)) == 500


def test_un_modelo_nuevo_conserva_las_puntuaciones_del_anterior(entorno, tmp_path):
    almacen, modelos, scoring = entorno
    primero = puntuar_pendientes(almacen, modelos, scoring)
    # Otro entrenamiento publica un modelo nuevo (su identificador es la fecha y hora)
    carpeta = modelos / modelo.VIGENTE
    metricas = carpeta / modelo.FICHERO_METRICAS
    metricas.write_text(
        metricas.read_text(encoding="utf-8").replace(primero["modelo"], "20990101T000000Z"),
        encoding="utf-8",
    )

    segundo = puntuar_pendientes(almacen, modelos, scoring)

    puntuadas = leer(scoring)
    assert segundo["modelo"] == "20990101T000000Z"
    assert puntuadas.groupby(puntuadas["modelo"].astype(str)).size().tolist() == [500, 500]


def test_sin_modelo_publicado_falla_con_un_mensaje_claro(tmp_path):
    _mart_sintetico(tmp_path / "w.duckdb")

    with pytest.raises(FileNotFoundError, match="No hay modelo vigente"):
        puntuar_pendientes(tmp_path / "w.duckdb", tmp_path / "vacio", tmp_path.as_posix())
