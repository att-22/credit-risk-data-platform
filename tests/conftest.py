"""Fixtures compartidas por todos los tests.

Una *fixture* es un dato de prueba que pytest prepara y entrega a cada test que
lo pida por su nombre. Usamos DataFrames pequeños escritos a mano: así los
tests son rápidos, no dependen de los CSV reales y sabemos de antemano qué
resultado es el correcto.
"""
import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def application() -> pd.DataFrame:
    """Mini tabla `application` con los casos raros que detectó el EDA."""
    return pd.DataFrame(
        {
            "SK_ID_CURR": [1, 2, 3, 4],
            "DAYS_EMPLOYED": [-1000, 365243, -500, -2000],  # 365243 = centinela "sin empleo"
            "DAYS_BIRTH": [-10000, -20000, -15000, -12000],
            "CODE_GENDER": ["M", "F", "XNA", "F"],  # XNA = sexo no informado
            "AMT_INCOME_TOTAL": [100_000.0, 200_000.0, 0.0, 1e9],  # 0 y un valor extremo
            "AMT_CREDIT": [500_000.0, 400_000.0, 300_000.0, 600_000.0],
            "AMT_ANNUITY": [25_000.0, 20_000.0, 15_000.0, 30_000.0],
            "CNT_FAM_MEMBERS": [2.0, 1.0, 3.0, 4.0],
            "EXT_SOURCE_1": [0.2, np.nan, 0.5, 0.9],
            "EXT_SOURCE_2": [0.4, 0.6, np.nan, 0.7],
            "EXT_SOURCE_3": [0.6, 0.8, np.nan, 0.8],
        }
    )


@pytest.fixture
def bureau() -> pd.DataFrame:
    """Mini tabla `bureau`: varios créditos previos por cliente."""
    return pd.DataFrame(
        {
            "SK_ID_CURR": [1, 1, 1, 2],
            "SK_ID_BUREAU": [10, 11, 12, 20],
            "AMT_CREDIT_SUM": [1000.0, 2000.0, 3000.0, 500.0],
            "CREDIT_ACTIVE": ["Active", "Closed", "Closed", "Active"],
        }
    )
