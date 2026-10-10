"""Funciones de limpieza e ingeniería de variables.

Reúne la lógica reutilizable que transforma las tablas originales en variables
listas para el modelado: limpieza de anomalías de la tabla principal, creación
de ratios financieros y agregación del historial crediticio por cliente.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from credit_risk.config import KEY_BUREAU, KEY_CURR, KEY_PREV

# Valor centinela que la tabla principal usa para "sin empleo" en DAYS_EMPLOYED
CENTINELA_EMPLEO = 365243
_IDS = {KEY_CURR, KEY_BUREAU, KEY_PREV}


def reducir_memoria(df: pd.DataFrame) -> pd.DataFrame:
    """Reduce el consumo de memoria ajustando los tipos numéricos a 32 bits."""
    for col in df.select_dtypes("float").columns:
        df[col] = df[col].astype("float32")
    for col in df.select_dtypes("integer").columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")
    return df


def limpiar_application(df: pd.DataFrame) -> pd.DataFrame:
    """Corrige las anomalías detectadas en el EDA sobre la tabla principal."""
    df = df.copy()
    # Valor imposible de antigüedad laboral -> ausente
    df["DAYS_EMPLOYED"] = df["DAYS_EMPLOYED"].replace(CENTINELA_EMPLEO, np.nan)
    # Sexo no informado -> ausente
    if "CODE_GENDER" in df:
        df["CODE_GENDER"] = df["CODE_GENDER"].replace("XNA", np.nan)
    # Ingresos con valores extremos poco verosímiles -> recorte al percentil 99,9
    if "AMT_INCOME_TOTAL" in df:
        tope = df["AMT_INCOME_TOTAL"].quantile(0.999)
        df["AMT_INCOME_TOTAL"] = df["AMT_INCOME_TOTAL"].clip(upper=tope)
    return df


def anadir_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Crea ratios financieros y resúmenes de las puntuaciones externas."""
    df = df.copy()
    df["RATIO_CREDITO_INGRESOS"] = df["AMT_CREDIT"] / df["AMT_INCOME_TOTAL"]
    df["RATIO_CUOTA_INGRESOS"] = df["AMT_ANNUITY"] / df["AMT_INCOME_TOTAL"]
    df["RATIO_CUOTA_CREDITO"] = df["AMT_ANNUITY"] / df["AMT_CREDIT"]
    df["INGRESO_POR_PERSONA"] = df["AMT_INCOME_TOTAL"] / df["CNT_FAM_MEMBERS"]
    df["PORC_VIDA_EMPLEADO"] = df["DAYS_EMPLOYED"] / df["DAYS_BIRTH"]

    ext = ["EXT_SOURCE_1", "EXT_SOURCE_2", "EXT_SOURCE_3"]
    df["EXT_SOURCE_MEDIA"] = df[ext].mean(axis=1)
    df["EXT_SOURCE_MIN"] = df[ext].min(axis=1)
    df["EXT_SOURCE_MAX"] = df[ext].max(axis=1)
    df["EXT_SOURCE_DESV"] = df[ext].std(axis=1)
    # Reemplazar posibles infinitos surgidos de divisiones por cero
    return df.replace([np.inf, -np.inf], np.nan)


def agregar_por_cliente(
    df: pd.DataFrame,
    clave: str,
    prefijo: str,
    cat_cols: list[str] | None = None,
    funciones=("mean", "max", "min", "sum"),
) -> pd.DataFrame:
    """Agrega una tabla de historial a nivel de la clave indicada.

    Calcula estadísticos de las variables numéricas y la proporción de cada
    categoría (one-hot + media) de las columnas categóricas seleccionadas, y
    añade el número de registros por grupo. Devuelve un DataFrame indexado por
    la clave de agrupación.
    """
    df = df.drop(columns=[c for c in _IDS if c in df.columns and c != clave], errors="ignore")
    num_cols = [c for c in df.select_dtypes("number").columns if c != clave]
    if cat_cols is None:
        cat_cols = [c for c in df.select_dtypes(exclude="number").columns if c != clave]

    partes = []
    if num_cols:
        g = df.groupby(clave)[num_cols].agg(list(funciones))
        g.columns = [f"{prefijo}_{col}_{fun}".upper() for col, fun in g.columns]
        partes.append(g)

    if cat_cols:
        dummies = pd.get_dummies(df[cat_cols], dummy_na=False).astype("int8")
        dummies[clave] = df[clave].values
        gd = dummies.groupby(clave).mean()
        gd.columns = [f"{prefijo}_{col}_PROP".upper() for col in gd.columns]
        partes.append(gd)

    agregado = pd.concat(partes, axis=1)
    agregado[f"{prefijo}_NUM_REGISTROS".upper()] = df.groupby(clave).size()
    return agregado
