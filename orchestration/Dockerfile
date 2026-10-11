# ==========================================================================
# Imagen de Airflow con el proyecto credit-risk instalado
# ==========================================================================
# Parte de la imagen oficial de Airflow y añade nuestro proyecto (paquete
# Python + proyecto dbt) en un entorno virtual APARTE, /opt/credit-risk/.venv.
#
# ¿Por qué aparte? Airflow fija cientos de dependencias con versiones exactas.
# Si instaláramos dbt, pandas... en el mismo entorno, sus versiones chocarían.
# Así cada uno tiene las suyas y las tareas llaman a los ejecutables del
# proyecto por su ruta (/opt/credit-risk/.venv/bin/...).
#
# Se construye desde la raíz del repositorio (lo hace compose.yaml).

FROM apache/airflow:3.3.2-python3.13

# uv, la misma versión que en el Dockerfile principal
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /usr/local/bin/uv

USER root

# UV_PROJECT_ENVIRONMENT: dónde crear el entorno virtual del proyecto.
# UV_PYTHON + UV_PYTHON_DOWNLOADS=never: usar el Python 3.13 de la imagen.
ENV UV_PROJECT_ENVIRONMENT=/opt/credit-risk/.venv \
    UV_PYTHON=/usr/local/bin/python3 \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /opt/credit-risk

# 1) Dependencias (capa cacheada mientras no cambie uv.lock)
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

# 2) Nuestro paquete
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

# 3) Proyecto dbt con sus paquetes ya descargados (las tareas no necesitan internet)
COPY transform ./transform
RUN cd transform && /opt/credit-risk/.venv/bin/dbt deps

# 4) Muestra sintética para ejecuciones rápidas (parámetro "datos" del DAG)
COPY tests/fixtures/muestra ./muestra

# dbt escribe target/ y logs/ al ejecutarse; el lago es un volumen montado aquí.
RUN mkdir -p /opt/airflow/lake \
    && chown -R airflow:0 /opt/credit-risk /opt/airflow/lake

USER airflow
WORKDIR /opt/airflow
