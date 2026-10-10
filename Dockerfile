# ==========================================================================
# Imagen Docker de la plataforma de datos.
# Construcción en dos etapas (multi-stage):
#   1. builder: instala las dependencias con uv.
#   2. final:   copia solo el entorno ya instalado -> imagen más pequeña.
# ==========================================================================

# ---------- Etapa 1: builder ----------
FROM python:3.13-slim AS builder

# Copiamos el ejecutable de uv desde su imagen oficial (versión fijada).
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /usr/local/bin/uv

# UV_COMPILE_BYTECODE: precompila los .py -> arranque más rápido.
# UV_LINK_MODE=copy: copia los archivos en vez de enlazarlos (necesario en Docker).
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

# 1º solo los archivos de dependencias. Docker guarda cada paso en caché:
# mientras no cambien pyproject.toml ni uv.lock, este paso (el lento) no se repite.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

# 2º el código. Cambia a menudo, pero reinstalarlo es casi instantáneo.
COPY README.md ./
COPY src ./src
RUN uv sync --locked --no-dev --no-editable

# ---------- Etapa 2: imagen final ----------
FROM python:3.13-slim

# Usuario sin privilegios: no ejecutar como root es una buena práctica de seguridad.
RUN useradd --create-home appuser

WORKDIR /app

# Solo el entorno virtual con todo instalado (sin uv, sin código fuente suelto).
COPY --from=builder /app/.venv /app/.venv

# Carpeta de salidas, propiedad del usuario sin privilegios.
RUN mkdir /app/outputs && chown appuser:appuser /app/outputs

# PATH: los comandos del entorno (verify-data, python...) se encuentran directamente.
# DATA_DIR / OUTPUTS_DIR: rutas DENTRO del contenedor; los datos se montan al ejecutar.
ENV PATH="/app/.venv/bin:$PATH" \
    DATA_DIR=/data \
    OUTPUTS_DIR=/app/outputs

USER appuser

# Comando por defecto (se puede sustituir al ejecutar `docker run`).
CMD ["verify-data"]
