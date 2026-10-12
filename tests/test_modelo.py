"""Tests del entrenamiento del modelo (modelo.py).

Necesitan el grupo de dependencias `ml` (LightGBM, scikit-learn, MLflow): sin él
se omiten. En la CI los ejecuta el job "Modelo".
"""

import json

import duckdb
import numpy as np
import pandas as pd
import pytest

pytest.importorskip("lightgbm", reason="sin el grupo de dependencias ml")
pytest.importorskip("mlflow", reason="sin el grupo de dependencias ml")

from credit_risk import modelo  # noqa: E402
from credit_risk.modelo import (  # noqa: E402
    FICHERO_METRICAS,
    VIGENTE,
    ModeloInsuficiente,
    Preparador,
    ejecutar,
    repartir,
)


@pytest.fixture
def solicitudes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "CONJUNTO": ["train"] * 4,
            "SK_ID_CURR": [1, 2, 3, 4],
            "TARGET": [0, 1, 0, 1],
            "INGRESOS": [100.0, 200.0, np.nan, 400.0],
            "TIPO": ["Cash loans", "Revolving loans", None, "Cash loans"],
            "URGENTE": pd.array([True, False, None, True], dtype="boolean"),
            "CONSTANTE": [7, 7, 7, 7],
            "VACIA": [np.nan] * 4,
            "_INGESTED_AT": pd.Timestamp("2026-09-01", tz="UTC"),
        }
    )


def test_one_hot_con_columna_para_los_ausentes(solicitudes):
    matriz = Preparador().ajustar(solicitudes).transformar(solicitudes)

    assert matriz["TIPO_Cash_loans"].tolist() == [1, 0, 0, 1]
    assert matriz["TIPO_Revolving_loans"].tolist() == [0, 1, 0, 0]
    assert matriz["TIPO_nan"].tolist() == [0, 0, 1, 0]
    # Un booleano con nulos se trata como categoría: "no se sabe" es información
    assert matriz["URGENTE_True"].tolist() == [1, 0, 0, 1]
    assert matriz["URGENTE_nan"].tolist() == [0, 0, 1, 0]
    # Los nulos numéricos se conservan: LightGBM los maneja por sí mismo
    assert np.isnan(matriz["INGRESOS"].iloc[2])


def test_no_entran_identificadores_ni_columnas_sin_informacion(solicitudes):
    columnas = Preparador().ajustar(solicitudes).columnas

    assert not {"SK_ID_CURR", "TARGET", "CONJUNTO", "_INGESTED_AT"} & set(columnas)
    assert "CONSTANTE" not in columnas and "VACIA" not in columnas
    assert all(c.replace("_", "").isalnum() for c in columnas)  # nombres válidos para LightGBM


def test_datos_nuevos_dan_las_mismas_columnas(solicitudes):
    preparador = Preparador().ajustar(solicitudes)
    nuevos = pd.DataFrame(
        {
            "SK_ID_CURR": [9],
            "TIPO": ["Categoría nunca vista"],
            "INGRESOS": [50.0],
            "COLUMNA_NUEVA": [1],
        }
    )

    matriz = preparador.transformar(nuevos)

    assert list(matriz.columns) == preparador.columnas
    # Categoría desconocida: ninguna de sus columnas se activa, ni siquiera "sin dato"
    assert matriz[["TIPO_Cash_loans", "TIPO_Revolving_loans", "TIPO_nan"]].iloc[0].sum() == 0
    assert np.isnan(matriz["URGENTE_True"].iloc[0])  # la columna entera faltaba


def test_el_preparador_se_guarda_y_se_recupera(solicitudes, tmp_path):
    original = Preparador().ajustar(solicitudes)
    original.guardar(tmp_path / "p.json")

    recuperado = Preparador.cargar(tmp_path / "p.json")

    assert recuperado == original
    pd.testing.assert_frame_equal(
        recuperado.transformar(solicitudes), original.transformar(solicitudes)
    )


def _mart_sintetico(ruta, filas: int = 3000) -> None:
    """Un mart mínimo en el que el impago depende de dos variables."""
    rng = np.random.default_rng(0)
    deuda = rng.normal(size=filas)
    tipo = rng.choice(["A", "B"], size=filas)
    probabilidad = 1 / (1 + np.exp(-(2.5 * deuda + 1.5 * (tipo == "B") - 2.5)))
    datos = pd.DataFrame(  # noqa: F841 (DuckDB la lee por su nombre)
        {
            "CONJUNTO": "train",
            "SK_ID_CURR": rng.permutation(filas) + 100_000,
            "TARGET": (rng.random(filas) < probabilidad).astype("int64"),
            "DEUDA": deuda,
            "TIPO": tipo,
            "RUIDO": rng.normal(size=filas),
            "_INGESTED_AT": pd.Timestamp("2026-09-01", tz="UTC"),
        }
    )
    with duckdb.connect(str(ruta)) as con:
        con.execute(f"CREATE TABLE {modelo.MART} AS SELECT * FROM datos")
        # Solicitudes sin TARGET (las que se puntuarán): no deben entrar al entrenamiento
        con.execute(f"""
            INSERT INTO {modelo.MART}
            SELECT 'test', SK_ID_CURR + 1000000, NULL, DEUDA, TIPO, RUIDO, _INGESTED_AT
            FROM datos LIMIT 500
        """)


def test_el_reparto_es_estratificado_y_reproducible(tmp_path):
    _mart_sintetico(tmp_path / "w.duckdb")
    datos, version = modelo.cargar_mart(tmp_path / "w.duckdb")

    entrenamiento, prueba = repartir(datos)
    _, otra_vez = repartir(datos)

    assert version["mart_filas"] == 3500 and len(datos) == 3000  # sin las que no tienen TARGET
    assert datos["SK_ID_CURR"].is_monotonic_increasing
    assert len(prueba) == 600 and not set(prueba.SK_ID_CURR) & set(entrenamiento.SK_ID_CURR)
    assert prueba.SK_ID_CURR.tolist() == otra_vez.SK_ID_CURR.tolist()
    assert abs(prueba.TARGET.mean() - entrenamiento.TARGET.mean()) < 0.01


def test_entrena_registra_y_publica(tmp_path):
    _mart_sintetico(tmp_path / "w.duckdb")
    modelos, uri = tmp_path / "modelos", f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"

    resumen = ejecutar(tmp_path / "w.duckdb", modelos, uri, auc_minimo=0.75, prueba_rapida=True)

    assert resumen["auc"] > 0.80  # ha aprendido la relación que hay en los datos
    assert resumen["gini"] == pytest.approx(2 * resumen["auc"] - 1)
    assert resumen["filas_entrenamiento"] == 2400 and resumen["filas_prueba"] == 600
    guardadas = json.loads((modelos / VIGENTE / FICHERO_METRICAS).read_text(encoding="utf-8"))
    assert guardadas["modelo"] == resumen["modelo"]

    import mlflow

    mlflow.set_tracking_uri(uri)
    ejecucion = mlflow.search_runs(experiment_names=["scoring"]).iloc[0]
    assert ejecucion["metrics.auc"] == pytest.approx(resumen["auc"])
    assert ejecucion["tags.publicado"] == "True"
    assert ejecucion["tags.mart_filas"] == "3500"


def test_un_modelo_insuficiente_no_sustituye_al_vigente(tmp_path):
    _mart_sintetico(tmp_path / "w.duckdb")
    modelos, uri = tmp_path / "modelos", f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    bueno = ejecutar(tmp_path / "w.duckdb", modelos, uri, prueba_rapida=True)

    with pytest.raises(ModeloInsuficiente):
        ejecutar(tmp_path / "w.duckdb", modelos, uri, auc_minimo=0.999, prueba_rapida=True)

    vigente = json.loads((modelos / VIGENTE / FICHERO_METRICAS).read_text(encoding="utf-8"))
    assert vigente["modelo"] == bueno["modelo"]
