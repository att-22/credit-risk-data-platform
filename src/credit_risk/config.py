"""Configuración central del proyecto: rutas y constantes.

Las rutas que dependen de cada máquina (dónde están los datos) NO se escriben en
el código: se leen de variables de entorno. Para desarrollo local se pueden
definir en un archivo `.env` en la raíz del repositorio (ver `.env.example`).
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Raíz del repositorio: src/credit_risk/config.py -> subir dos niveles.
ROOT = Path(__file__).resolve().parents[2]

# Carga las variables del archivo .env (si existe). No pisa las variables que
# ya estén definidas en el sistema, así en CI/Docker mandan las de fuera.
load_dotenv(ROOT / ".env")

# Datos de entrada: carpeta externa configurable (por defecto, ./data).
# Se tratan como SOLO LECTURA: este proyecto nunca escribe en ella.
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"

# Salidas generadas por este proyecto (siempre dentro del repositorio).
OUTPUTS_DIR = ROOT / "outputs"

# Semilla para reproducibilidad
RANDOM_STATE = 42

# Nombre de la variable objetivo y claves de unión
TARGET = "TARGET"
KEY_CURR = "SK_ID_CURR"
KEY_BUREAU = "SK_ID_BUREAU"
KEY_PREV = "SK_ID_PREV"

# Tablas del dataset (nombre lógico -> fichero)
TABLES = {
    "application_train": "application_train.csv",
    "application_test": "application_test.csv",
    "bureau": "bureau.csv",
    "bureau_balance": "bureau_balance.csv",
    "previous_application": "previous_application.csv",
    "pos_cash_balance": "POS_CASH_balance.csv",
    "installments_payments": "installments_payments.csv",
    "credit_card_balance": "credit_card_balance.csv",
}
