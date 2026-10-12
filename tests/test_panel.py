"""Tests del panel: sus consultas (panel_datos.py) y la aplicación (panel/app.py).

Las consultas solo necesitan DuckDB y se prueban siempre. La aplicación se arranca
sin navegador con la utilidad de pruebas de Streamlit (grupo de dependencias
`panel`); la versión con modelo necesita además el grupo `ml`.
"""

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest

from credit_risk import config, lago
from credit_risk import panel_datos as datos

APP = Path(__file__).parents[1] / "panel" / "app.py"
PESTANAS = ["Resumen", "Datos", "Cartera", "Modelo", "Puntuación y deriva"]


def crear_mart(almacen: Path, filas: int = 2000) -> None:
    """Un mart con las columnas que consulta el panel y un impago que depende de ellas."""
    rng = np.random.default_rng(3)
    contrato = rng.choice(["Cash loans", "Revolving loans"], size=filas, p=[0.9, 0.1])
    deuda = rng.normal(size=filas)
    # Los préstamos en efectivo impagan claramente más que los revolving
    probabilidad = 1 / (1 + np.exp(-(2.0 * deuda + 1.5 * (contrato == "Cash loans") - 3.5)))
    solicitudes = pd.DataFrame(  # noqa: F841 (DuckDB la lee por su nombre)
        {
            "CONJUNTO": "train",
            "SK_ID_CURR": np.arange(filas) + 100_000,
            "TARGET": (rng.random(filas) < probabilidad).astype("int64"),
            "NAME_CONTRACT_TYPE": contrato,
            "NAME_EDUCATION_TYPE": rng.choice(["Secondary", "Higher education"], size=filas),
            "NAME_INCOME_TYPE": rng.choice(["Working", "Pensioner"], size=filas),
            "NAME_FAMILY_STATUS": rng.choice(["Married", "Single"], size=filas),
            "NAME_HOUSING_TYPE": rng.choice(["House / apartment", "With parents"], size=filas),
            "CODE_GENDER": rng.choice(["F", "M"], size=filas),
            "DAYS_BIRTH": -rng.integers(21 * 365, 68 * 365, size=filas),
            "AMT_INCOME_TOTAL": rng.lognormal(12, 0.5, size=filas),
            "DEUDA": deuda,
            "_INGESTED_AT": pd.Timestamp("2026-09-30 08:00", tz="UTC"),
        }
    )
    with duckdb.connect(str(almacen)) as con:
        con.execute(f"CREATE TABLE {datos.MART} AS SELECT * FROM solicitudes")
        con.execute(f"""
            INSERT INTO {datos.MART}
            SELECT 'test', SK_ID_CURR + 1000000, NULL, * EXCLUDE (CONJUNTO, SK_ID_CURR, TARGET)
            FROM solicitudes LIMIT 300
        """)


@pytest.fixture
def lake(tmp_path, monkeypatch):
    """Un lago vacío al que apunta toda la configuración (como haría LAKE_DIR)."""
    raiz = tmp_path / "lake"
    raiz.mkdir()
    uri = raiz.as_posix()
    for nombre, valor in {
        "LAKE_DIR": raiz,
        "BRONZE_URI": f"{uri}/bronze",
        "SCORING_URI": f"{uri}/warehouse/scoring",
        "DERIVA_URI": f"{uri}/warehouse/deriva",
        "MODELS_DIR": tmp_path / "modelos",
        "MLFLOW_URI": f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}",
    }.items():
        monkeypatch.setattr(config, nombre, valor)
    return raiz


# --- Consultas ----------------------------------------------------------------------


def test_sin_mart_las_consultas_no_fallan(lake):
    almacen = lake / "warehouse.duckdb"

    assert datos.resumen_mart(almacen) is None
    assert datos.impago_por(almacen, "Tipo de contrato").empty
    assert datos.ingestas(config.BRONZE_URI).empty
    assert datos.modelo_vigente(config.MODELS_DIR) is None
    assert datos.entrenamientos(config.MLFLOW_URI).empty
    assert datos.puntuaciones_por_decil(config.SCORING_URI, "m").empty
    assert datos.puntuacion_de(config.SCORING_URI, "m", 1) is None
    assert datos.deriva(config.DERIVA_URI, "m").empty


def test_resumen_del_mart(lake):
    crear_mart(lake / "warehouse.duckdb")

    resumen = datos.resumen_mart(lake / "warehouse.duckdb")

    assert resumen["solicitudes"] == 2300
    assert resumen["con_resultado"] == 2000 and resumen["pendientes"] == 300
    assert 0 < resumen["tasa_impago"] < 1
    assert resumen["columnas"] == 13


def test_impago_por_categoria_cuadra_con_el_calculo_directo(lake):
    almacen = lake / "warehouse.duckdb"
    crear_mart(almacen)

    desglose = datos.impago_por(almacen, "Tipo de contrato").set_index("categoria")

    with duckdb.connect(str(almacen), read_only=True) as con:
        esperado = dict(
            con.execute(f"""
                SELECT NAME_CONTRACT_TYPE, avg(TARGET) FROM {datos.MART}
                WHERE TARGET IS NOT NULL GROUP BY 1
            """).fetchall()
        )
    assert desglose["solicitudes"].sum() == 2000  # solo las que tienen resultado
    tasa = desglose["tasa_impago"]
    assert tasa["Cash loans"] == pytest.approx(esperado["Cash loans"])
    assert tasa["Cash loans"] > tasa["Revolving loans"]


@pytest.mark.parametrize("dimension", list(datos.DIMENSIONES))
def test_todas_las_dimensiones_reparten_todas_las_solicitudes(lake, dimension):
    crear_mart(lake / "warehouse.duckdb")

    desglose = datos.impago_por(lake / "warehouse.duckdb", dimension)

    assert desglose["solicitudes"].sum() == 2000
    assert desglose["categoria"].is_unique and desglose["tasa_impago"].between(0, 1).all()


def test_los_tramos_numericos_salen_ordenados(lake):
    crear_mart(lake / "warehouse.duckdb")

    edades = datos.impago_por(lake / "warehouse.duckdb", "Edad")["categoria"].tolist()
    quintiles = datos.impago_por(lake / "warehouse.duckdb", "Ingresos (quintiles)")

    assert edades == sorted(edades) and edades[0].startswith("20 a 29")
    assert quintiles["categoria"].tolist() == ["Q1", "Q2", "Q3", "Q4", "Q5"]
    assert quintiles["solicitudes"].tolist() == [400] * 5


def escribir(tabla: pd.DataFrame, destino: str) -> None:
    con = lago.conectar(destino)
    con.register("tabla", tabla)
    lago.copiar(con, "SELECT * FROM tabla", destino, "FORMAT parquet")


def test_los_diez_deciles_aparecen_aunque_alguno_este_vacio(lake):
    puntuadas = pd.DataFrame(
        {
            "SK_ID_CURR": [1, 2, 3, 4],
            "probabilidad_impago": [0.1, 0.2, 0.9, 0.95],
            "decil_riesgo": np.array([1, 1, 10, 10], dtype="int8"),
            "puntuado_en": pd.Timestamp("2026-10-01", tz="UTC"),
        }
    )
    escribir(puntuadas, lago.unir(config.SCORING_URI, "modelo=m1", "data.parquet"))

    reparto = datos.puntuaciones_por_decil(config.SCORING_URI, "m1")

    assert reparto["decil_riesgo"].tolist() == list(range(1, 11))
    assert reparto["solicitudes"].tolist() == [2, 0, 0, 0, 0, 0, 0, 0, 0, 2]
    assert reparto["fraccion"].sum() == pytest.approx(1)
    assert datos.puntuacion_de(config.SCORING_URI, "m1", 3)["decil_riesgo"] == 10
    assert datos.puntuacion_de(config.SCORING_URI, "m1", 99) is None
    assert datos.puntuaciones_por_decil(config.SCORING_URI, "otro-modelo").empty


def test_modelo_vigente_lee_sus_metricas(lake):
    carpeta = config.MODELS_DIR / "vigente"
    carpeta.mkdir(parents=True)
    (carpeta / "metricas.json").write_text(json.dumps({"modelo": "m1", "auc": 0.79}))

    assert datos.modelo_vigente(config.MODELS_DIR) == {"modelo": "m1", "auc": 0.79}


# --- La aplicación ------------------------------------------------------------------


def abrir_panel():
    pytest.importorskip("streamlit", reason="sin el grupo de dependencias panel")
    from streamlit.testing.v1 import AppTest

    return AppTest.from_file(str(APP), default_timeout=60).run()


def test_el_panel_avisa_si_no_hay_mart(lake):
    panel = abrir_panel()

    assert not panel.exception
    assert "Todavía no hay mart" in panel.warning[0].value
    assert len(panel.tabs) == 0  # no pinta nada más


def test_el_panel_funciona_con_el_mart_y_sin_modelo(lake):
    crear_mart(lake / "warehouse.duckdb")

    panel = abrir_panel()

    assert not panel.exception
    assert [t.label for t in panel.tabs] == PESTANAS
    fichas = {m.label: m.value for m in panel.metric}
    assert fichas["Solicitudes en el mart"] == "2.300"
    assert fichas["Solicitudes por puntuar"] == "300"
    assert fichas["AUC del modelo vigente"] == "—"
    assert any("Aún no hay modelo publicado" in i.value for i in panel.info)


def test_el_panel_completo_con_modelo_puntuaciones_y_deriva(lake):
    pytest.importorskip("lightgbm", reason="sin el grupo de dependencias ml")
    pytest.importorskip("mlflow", reason="sin el grupo de dependencias ml")
    from credit_risk import deriva, modelo, scoring

    almacen = lake / "warehouse.duckdb"
    crear_mart(almacen)
    resumen = modelo.ejecutar(almacen, config.MODELS_DIR, config.MLFLOW_URI, prueba_rapida=True)
    scoring.puntuar_pendientes(almacen, config.MODELS_DIR, config.SCORING_URI)
    deriva.vigilar(almacen, config.MODELS_DIR, config.DERIVA_URI)

    panel = abrir_panel()

    assert not panel.exception
    fichas = {m.label: m.value for m in panel.metric}
    assert fichas["AUC del modelo vigente"] == f"{resumen['auc']:.4f}".replace(".", ",")
    assert fichas["Deriva de la puntuación (PSI)"] != "—"
    assert not any("Aún no hay modelo" in i.value for i in panel.info)
    # El historial de MLflow se lee sin tener MLflow en el panel
    historial = datos.entrenamientos(config.MLFLOW_URI)
    assert historial["modelo"].tolist() == [resumen["modelo"]]
    assert historial.loc[0, "auc"] == pytest.approx(resumen["auc"])

    # Con 300 solicitudes el PSI no es fiable: el panel lo avisa en vez de dar un estado
    assert any("el PSI no es fiable" in a.value for a in panel.warning)
    assert any("no fiable" in t.value for t in panel.caption)

    # Consultar una solicitud puntuada enseña su decil
    panel.number_input[0].set_value(1_100_000).run()
    assert not panel.exception
    assert "de 10" in {m.label: m.value for m in panel.metric}["Decil de riesgo"]
