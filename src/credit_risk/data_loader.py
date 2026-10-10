"""Carga de las tablas del dataset Home Credit Default Risk.

Funciones sencillas para leer las tablas desde la carpeta de datos sin tener
que recordar los nombres de fichero. Se apoya en la configuración de config.py.
"""
from __future__ import annotations

import pandas as pd

from credit_risk.config import RAW_DIR, TABLES


def load_table(name: str, **kwargs) -> pd.DataFrame:
    """Carga una tabla por su nombre lógico (ver config.TABLES)."""
    if name not in TABLES:
        raise KeyError(f"Tabla desconocida: {name!r}. Opciones: {list(TABLES)}")
    path = RAW_DIR / TABLES[name]
    if not path.exists():
        raise FileNotFoundError(
            f"No se encuentra {path}. ¿Está bien definida DATA_DIR en el archivo .env?"
        )
    return pd.read_csv(path, **kwargs)


def load_application_train(**kwargs) -> pd.DataFrame:
    """Atajo para la tabla principal de entrenamiento."""
    return load_table("application_train", **kwargs)
