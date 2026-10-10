"""Tests de las funciones de limpieza e ingeniería de variables."""

import numpy as np
import pandas as pd
import pytest

from credit_risk.features import (
    CENTINELA_EMPLEO,
    agregar_por_cliente,
    anadir_ratios,
    limpiar_application,
    reducir_memoria,
)

# --------------------------------------------------------------------------
# limpiar_application
# --------------------------------------------------------------------------


def test_centinela_empleo_pasa_a_nulo(application):
    resultado = limpiar_application(application)

    assert pd.isna(resultado.loc[1, "DAYS_EMPLOYED"])
    assert CENTINELA_EMPLEO not in resultado["DAYS_EMPLOYED"].values


def test_valores_de_empleo_normales_no_cambian(application):
    resultado = limpiar_application(application)

    assert resultado.loc[0, "DAYS_EMPLOYED"] == -1000
    assert resultado.loc[3, "DAYS_EMPLOYED"] == -2000


def test_sexo_xna_pasa_a_nulo(application):
    resultado = limpiar_application(application)

    assert pd.isna(resultado.loc[2, "CODE_GENDER"])
    assert resultado.loc[0, "CODE_GENDER"] == "M"


def test_ingresos_extremos_se_recortan(application):
    resultado = limpiar_application(application)

    assert resultado["AMT_INCOME_TOTAL"].max() < 1e9
    assert resultado.loc[0, "AMT_INCOME_TOTAL"] == 100_000.0


def test_limpiar_no_modifica_el_dataframe_original(application):
    original = application.copy()

    limpiar_application(application)

    pd.testing.assert_frame_equal(application, original)


# --------------------------------------------------------------------------
# anadir_ratios
# --------------------------------------------------------------------------


def test_ratio_credito_ingresos(application):
    resultado = anadir_ratios(application)

    assert resultado.loc[0, "RATIO_CREDITO_INGRESOS"] == pytest.approx(5.0)


def test_division_por_cero_da_nulo_y_no_infinito(application):
    # El cliente 3 (fila 2) tiene ingresos 0.
    resultado = anadir_ratios(application)

    assert pd.isna(resultado.loc[2, "RATIO_CREDITO_INGRESOS"])
    columnas_numericas = resultado.select_dtypes("number")
    assert not np.isinf(columnas_numericas.to_numpy()).any()


def test_ext_source_media_ignora_nulos(application):
    # Fila 1: EXT_SOURCE_1 es nulo -> media de 0.6 y 0.8
    resultado = anadir_ratios(application)

    assert resultado.loc[1, "EXT_SOURCE_MEDIA"] == pytest.approx(0.7)
    assert resultado.loc[1, "EXT_SOURCE_MIN"] == pytest.approx(0.6)
    assert resultado.loc[1, "EXT_SOURCE_MAX"] == pytest.approx(0.8)


# --------------------------------------------------------------------------
# agregar_por_cliente
# --------------------------------------------------------------------------


def test_agregado_tiene_una_fila_por_cliente(bureau):
    resultado = agregar_por_cliente(bureau, clave="SK_ID_CURR", prefijo="bureau")

    assert sorted(resultado.index) == [1, 2]


def test_agregado_cuenta_registros(bureau):
    resultado = agregar_por_cliente(bureau, clave="SK_ID_CURR", prefijo="bureau")

    assert resultado.loc[1, "BUREAU_NUM_REGISTROS"] == 3
    assert resultado.loc[2, "BUREAU_NUM_REGISTROS"] == 1


def test_agregado_estadisticos_numericos(bureau):
    resultado = agregar_por_cliente(bureau, clave="SK_ID_CURR", prefijo="bureau")

    assert resultado.loc[1, "BUREAU_AMT_CREDIT_SUM_SUM"] == 6000.0
    assert resultado.loc[1, "BUREAU_AMT_CREDIT_SUM_MEAN"] == 2000.0
    assert resultado.loc[1, "BUREAU_AMT_CREDIT_SUM_MAX"] == 3000.0


def test_agregado_proporcion_de_categorias(bureau):
    # Cliente 1: 1 crédito activo de 3 -> proporción 1/3
    resultado = agregar_por_cliente(bureau, clave="SK_ID_CURR", prefijo="bureau")

    assert resultado.loc[1, "BUREAU_CREDIT_ACTIVE_ACTIVE_PROP"] == pytest.approx(1 / 3)
    assert resultado.loc[2, "BUREAU_CREDIT_ACTIVE_ACTIVE_PROP"] == pytest.approx(1.0)


def test_agregado_descarta_otros_identificadores(bureau):
    # SK_ID_BUREAU es un ID, no una variable: no tiene sentido sumarlo.
    resultado = agregar_por_cliente(bureau, clave="SK_ID_CURR", prefijo="bureau")

    assert not any("SK_ID_BUREAU" in col for col in resultado.columns)


# --------------------------------------------------------------------------
# reducir_memoria
# --------------------------------------------------------------------------


def test_reducir_memoria_pasa_floats_a_32_bits(application):
    resultado = reducir_memoria(application.copy())

    assert resultado["AMT_CREDIT"].dtype == "float32"
