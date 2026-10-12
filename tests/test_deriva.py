"""Tests de la monitorización de deriva (deriva.py).

Las funciones del PSI solo necesitan numpy y se prueban siempre; las que usan el
modelo necesitan el grupo de dependencias `ml` y, sin él, se omiten.
"""

import duckdb
import numpy as np
import pandas as pd
import pytest

from credit_risk import deriva
from credit_risk.deriva import (
    PUNTUACION,
    estado,
    hay_alarma,
    informe,
    psi,
    referencia,
    reparto,
)


def test_dos_repartos_iguales_tienen_psi_cero():
    iguales = np.full(10, 0.1)

    assert psi(iguales, iguales) == 0


def test_psi_de_un_caso_calculado_a_mano():
    esperado, observado = np.array([0.5, 0.5]), np.array([0.8, 0.2])

    # (0,8 - 0,5)·ln(0,8/0,5) + (0,2 - 0,5)·ln(0,2/0,5)
    assert psi(esperado, observado) == pytest.approx(0.3 * np.log(1.6) - 0.3 * np.log(0.4))


def test_un_tramo_vacio_no_da_infinito():
    assert np.isfinite(psi(np.array([0.5, 0.5]), np.array([1.0, 0.0])))


def test_los_umbrales_son_los_del_sector():
    assert [estado(v) for v in (0.0, 0.099, 0.10, 0.249, 0.25, 3.0)] == [
        "estable",
        "estable",
        "moderado",
        "moderado",
        "importante",
        "importante",
    ]


def test_con_pocas_solicitudes_no_se_da_la_alarma():
    assert hay_alarma(psi_puntuacion=0.40, solicitudes=5000, psi_maximo=0.25)
    assert not hay_alarma(psi_puntuacion=0.20, solicitudes=5000, psi_maximo=0.25)
    # 52 solicitudes en 10 deciles: un PSI alto ahí es ruido, no deriva
    assert not hay_alarma(psi_puntuacion=0.40, solicitudes=52, psi_maximo=0.25)


def test_los_ausentes_van_a_su_propio_tramo():
    fracciones = reparto(np.array([1.0, 2.0, 3.0, np.nan]), cortes=[1.5, 2.5])

    assert fracciones.tolist() == [0.25, 0.25, 0.25, 0.25]  # tres tramos y los ausentes


def test_la_referencia_guarda_las_variables_que_mas_pesan():
    rng = np.random.default_rng(1)
    matriz = pd.DataFrame(
        {
            "CONTINUA": rng.normal(size=1000),
            "INDICADOR": rng.integers(0, 2, size=1000).astype(float),
            "SIN_USO": rng.normal(size=1000),
        }
    )

    ref = referencia(matriz, importancia=np.array([5.0, 9.0, 0.0]))

    assert list(ref) == ["INDICADOR", "CONTINUA"]  # por importancia; la de peso 0, fuera
    assert len(ref["CONTINUA"]["cortes"]) == 9 and len(ref["CONTINUA"]["esperado"]) == 11
    assert len(ref["INDICADOR"]["cortes"]) <= 2  # un indicador 0/1 no tiene diez tramos
    assert sum(ref["CONTINUA"]["esperado"]) == pytest.approx(1)


def test_el_informe_distingue_lo_que_cambia_de_lo_que_no():
    rng = np.random.default_rng(2)
    entrenamiento = pd.DataFrame({"A": rng.normal(size=5000), "B": rng.normal(size=5000)})
    ref = referencia(entrenamiento, importancia=np.array([2.0, 1.0]))
    nuevos = pd.DataFrame({"A": rng.normal(size=5000) + 1.5, "B": rng.normal(size=5000)})
    cortes = np.arange(0.1, 1.0, 0.1).tolist()

    tabla = informe(ref, nuevos, probabilidad=rng.random(5000), cortes=cortes)

    por_variable = tabla.set_index("variable")
    assert tabla.loc[0, "variable"] == PUNTUACION  # la puntuación, siempre la primera
    assert por_variable.loc[PUNTUACION, "estado"] == "estable"  # uniforme: 10 % por decil
    assert por_variable.loc["A", "estado"] == "importante"
    assert por_variable.loc["B", "estado"] == "estable"
    assert tabla.loc[1, "variable"] == "A"  # las variables, de más a menos deriva


# --- Con el modelo -----------------------------------------------------------------


@pytest.fixture
def entorno(tmp_path):
    """Mart sintético (el impago depende de DEUDA y TIPO) y un modelo publicado."""
    pytest.importorskip("lightgbm", reason="sin el grupo de dependencias ml")
    pytest.importorskip("mlflow", reason="sin el grupo de dependencias ml")
    from test_modelo import _mart_sintetico

    from credit_risk import modelo

    almacen, modelos = tmp_path / "w.duckdb", tmp_path / "modelos"
    _mart_sintetico(almacen)
    uri = f"sqlite:///{(tmp_path / 'mlflow.db').as_posix()}"
    modelo.ejecutar(almacen, modelos, uri, prueba_rapida=True)
    return almacen, modelos, (tmp_path / "deriva").as_posix()


def alterar(almacen, cambio: str) -> None:
    """Modifica las solicitudes que se puntúan (las que no tienen TARGET)."""
    with duckdb.connect(str(almacen)) as con:
        con.execute(f"UPDATE mart_scoring__clientes SET {cambio} WHERE TARGET IS NULL")


def test_la_misma_poblacion_no_da_alarma(entorno):
    almacen, modelos, destino = entorno

    tabla, resumen = deriva.vigilar(almacen, modelos, destino)

    assert resumen["n"] == 500
    assert tabla.loc[0, "psi"] < deriva.PSI_MODERADO
    assert (tabla["estado"] != "importante").all()
    escrito = duckdb.sql(f"SELECT * FROM read_parquet('{destino}/*/*.parquet')").df()
    assert len(escrito) == len(tabla) and escrito["solicitudes"].eq(500).all()


def test_una_poblacion_con_mas_deuda_dispara_la_alarma(entorno):
    almacen, modelos, destino = entorno
    alterar(almacen, "DEUDA = DEUDA + 2")  # llegan clientes mucho más endeudados

    tabla, _ = deriva.vigilar(almacen, modelos, destino)

    por_variable = tabla.set_index("variable")
    assert por_variable.loc[PUNTUACION, "psi"] > deriva.PSI_IMPORTANTE
    assert tabla.loc[1, "variable"] == "DEUDA"  # el diagnóstico señala la causa
    assert por_variable.loc["RUIDO", "estado"] == "estable"  # lo que no cambió, estable


def test_una_variable_que_deja_de_llegar_se_detecta(entorno):
    """Un fallo en los datos (una columna que llega vacía) también es deriva."""
    almacen, modelos, destino = entorno
    alterar(almacen, "DEUDA = NULL")

    tabla, _ = deriva.vigilar(almacen, modelos, destino)

    assert tabla.set_index("variable").loc["DEUDA", "estado"] == "importante"


def test_un_modelo_sin_referencia_pide_reentrenar(entorno):
    almacen, modelos, destino = entorno
    (modelos / "vigente" / deriva.FICHERO_REFERENCIA).unlink()

    with pytest.raises(FileNotFoundError, match="vuelve a entrenar"):
        deriva.vigilar(almacen, modelos, destino)
