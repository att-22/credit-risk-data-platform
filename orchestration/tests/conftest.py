"""Configuración común de los tests de DAGs.

Airflow añade la carpeta dags/ al path de Python al leer los DAGs (por eso
pueden hacer `from comun import ...`). Aquí se hace lo mismo para que los tests
carguen los DAGs exactamente igual que el dag-processor.
"""

import sys

sys.path.insert(0, "/opt/airflow/dags")
