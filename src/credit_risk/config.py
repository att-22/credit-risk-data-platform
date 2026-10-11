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

# Lago de datos local: aquí se escriben las capas de la arquitectura medallion
# (bronze, silver, gold). Separado de DATA_DIR, que es solo de lectura.
LAKE_DIR = Path(os.getenv("LAKE_DIR", ROOT / "lake"))
BRONZE_DIR = LAKE_DIR / "bronze"

# Landing zone: donde el sistema origen (simulado) deja los ficheros de cada día,
# antes de que la ingesta los lleve a bronze. Ver simulate_source.py.
LANDING_DIR = Path(os.getenv("LANDING_DIR", LAKE_DIR / "landing"))

# Sistema origen simulado (Fase 3.5): el "banco" guarda sus entregas en
# ORIGEN_RAIZ/<conjunto>/fecha=AAAA-MM-DD/ y las sirve por una API REST
# (api_origen.py). Nuestra extracción (extract_api.py) la consulta en ORIGEN_API_URL.
ORIGEN_RAIZ = Path(os.getenv("ORIGEN_RAIZ", LAKE_DIR / "origen"))
ORIGEN_API_URL = os.getenv("ORIGEN_API_URL", "http://localhost:8000")
# Clave de la API. Por defecto una de desarrollo; en un despliegue real vendría
# de un gestor de secretos, nunca del código.
ORIGEN_API_KEY = os.getenv("ORIGEN_API_KEY", "clave-solo-para-desarrollo")

# Salidas generadas por este proyecto (por defecto, ./outputs del repositorio).
# También configurable: instalado como paquete (p. ej. en Docker), ROOT ya no
# apunta al repositorio y hay que indicar la carpeta desde fuera.
OUTPUTS_DIR = Path(os.getenv("OUTPUTS_DIR", ROOT / "outputs"))

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
