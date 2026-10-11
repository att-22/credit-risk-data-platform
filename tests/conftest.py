"""Fixtures compartidas por todos los tests.

Una *fixture* es un dato de prueba que pytest prepara y entrega a cada test que
lo pida por su nombre. Usamos DataFrames pequeños escritos a mano: así los
tests son rápidos, no dependen de los CSV reales y sabemos de antemano qué
resultado es el correcto.
"""

import numpy as np
import pandas as pd
import pytest

from credit_risk.config import TABLES


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
