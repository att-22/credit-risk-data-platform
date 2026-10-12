"""Consultas del panel: lo que la plataforma produce, listo para enseñar.

El panel (panel/app.py) solo pinta; todo lo que lee del lago sale de aquí. Así las
consultas se prueban con pytest sin arrancar ninguna interfaz.

Cada función tolera que su parte todavía no exista (lago recién creado, modelo sin
entrenar...) devolviendo None o una tabla vacía: el panel enseña lo que haya.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from credit_risk import lago
from credit_risk.config import KEY_CURR, TARGET

MART = "mart_scoring__clientes"

# Dimensiones por las que se puede desglosar la tasa de impago: etiqueta -> SQL.
# Las numéricas se agrupan en tramos; el orden de las barras lo da `orden`.
DIMENSIONES: dict[str, dict[str, str]] = {
    "Tipo de contrato": {"sql": "NAME_CONTRACT_TYPE"},
    "Nivel de estudios": {"sql": "NAME_EDUCATION_TYPE"},
    "Fuente de ingresos": {"sql": "NAME_INCOME_TYPE"},
    "Situación familiar": {"sql": "NAME_FAMILY_STATUS"},
    "Vivienda": {"sql": "NAME_HOUSING_TYPE"},
    "Sexo": {"sql": "CODE_GENDER"},
    "Edad": {
        # DAYS_BIRTH son días hasta la solicitud, en negativo
        "sql": "CAST(floor(-DAYS_BIRTH / 365.25 / 10) * 10 AS INTEGER) || ' a ' || "
        "CAST(floor(-DAYS_BIRTH / 365.25 / 10) * 10 + 9 AS INTEGER) || ' años'",
        "orden": "min(-DAYS_BIRTH)",
    },
    "Ingresos (quintiles)": {
        "sql": "'Q' || ntile(5) OVER (ORDER BY AMT_INCOME_TOTAL)",
        "orden": "min(AMT_INCOME_TOTAL)",
    },
}


def _almacen(ruta: Path) -> duckdb.DuckDBPyConnection | None:
    """Conexión de solo lectura al almacén de dbt (None si no existe o no hay mart)."""
    if not ruta.exists():
        return None
    con = duckdb.connect(ruta.as_posix(), read_only=True)
    existe = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [MART]
    ).fetchone()[0]
    if not existe:
        con.close()
        return None
    return con


def resumen_mart(almacen: Path) -> dict | None:
    """Las cifras de cabecera del mart."""
    con = _almacen(almacen)
    if con is None:
        return None
    with con:
        fila = con.execute(f"""
            SELECT count(*), count({TARGET}), avg({TARGET}), max(_INGESTED_AT),
                   (SELECT count(*) FROM (DESCRIBE {MART}))
            FROM {MART}
        """).fetchone()
    return {
        "solicitudes": fila[0],
        "con_resultado": fila[1],
        "pendientes": fila[0] - fila[1],
        "tasa_impago": fila[2],
        "ingerido_hasta": fila[3],
        "columnas": fila[4],
    }


def impago_por(almacen: Path, dimension: str) -> pd.DataFrame:
    """Solicitudes y tasa de impago por cada valor de una dimensión."""
    definicion = DIMENSIONES[dimension]
    con = _almacen(almacen)
    if con is None:
        return pd.DataFrame(columns=["categoria", "solicitudes", "tasa_impago"])
    # Las categorías, de mayor a menor impago; los tramos numéricos, en su orden natural
    orden = definicion.get("orden", "avg(TARGET) DESC")
    with con:
        return con.execute(f"""
            WITH base AS (
                SELECT {definicion["sql"]} AS categoria, {TARGET}, DAYS_BIRTH, AMT_INCOME_TOTAL
                FROM {MART} WHERE {TARGET} IS NOT NULL
            )
            SELECT coalesce(categoria, '(sin dato)') AS categoria,
                   count(*) AS solicitudes,
                   avg({TARGET}) AS tasa_impago
            FROM base GROUP BY 1 ORDER BY {orden}
        """).df()


def ingestas(bronze_uri: str) -> pd.DataFrame:
    """Tabla de control de la ingesta: filas por día de entrega y tabla."""
    control = lago.unir(bronze_uri, "_ingestas.parquet")
    if not lago.existe(control):
        return pd.DataFrame(columns=["fecha_entrega", "tabla", "filas", "ingerido_en"])
    return (
        lago.conectar(control)
        .execute(f"SELECT * FROM '{control}' ORDER BY fecha_entrega, tabla")
        .df()
    )


def modelo_vigente(modelos_dir: Path) -> dict | None:
    """Métricas del modelo publicado (None si aún no hay ninguno)."""
    fichero = modelos_dir / "vigente" / "metricas.json"
    return json.loads(fichero.read_text(encoding="utf-8")) if fichero.exists() else None


def entrenamientos(mlflow_uri: str) -> pd.DataFrame:
    """Historial de entrenamientos registrado en MLflow, del más reciente al más antiguo.

    Lee la base de datos SQLite de MLflow directamente: el panel no necesita tener
    MLflow instalado para enseñar el historial.
    """
    columnas = ["modelo", "inicio", "auc", "gini", "ks", "segundos", "publicado", "commit"]
    ruta = Path(mlflow_uri.removeprefix("sqlite:///"))
    if not mlflow_uri.startswith("sqlite:///") or not ruta.exists():
        return pd.DataFrame(columns=columnas)
    with sqlite3.connect(f"file:{ruta.as_posix()}?mode=ro", uri=True) as con:
        return pd.read_sql_query(
            """
            SELECT r.name AS modelo,
                   datetime(r.start_time / 1000, 'unixepoch') AS inicio,
                   max(CASE WHEN m.key = 'auc' THEN m.value END) AS auc,
                   max(CASE WHEN m.key = 'gini' THEN m.value END) AS gini,
                   max(CASE WHEN m.key = 'ks' THEN m.value END) AS ks,
                   max(CASE WHEN m.key = 'segundos' THEN m.value END) AS segundos,
                   (SELECT value FROM tags t WHERE t.run_uuid = r.run_uuid
                     AND t.key = 'publicado') AS publicado,
                   (SELECT value FROM tags t WHERE t.run_uuid = r.run_uuid
                     AND t.key = 'commit') AS "commit"
            FROM runs r LEFT JOIN latest_metrics m ON m.run_uuid = r.run_uuid
            WHERE r.lifecycle_stage = 'active'
            GROUP BY r.run_uuid ORDER BY r.start_time DESC
            """,
            con,
        )


def _particion(uri: str, modelo: str) -> str | None:
    fichero = lago.unir(uri, f"modelo={modelo}", "data.parquet")
    return fichero if lago.existe(fichero) else None


def puntuaciones_por_decil(scoring_uri: str, modelo: str) -> pd.DataFrame:
    """Solicitudes puntuadas en cada decil de riesgo (los 10, aunque alguno esté vacío)."""
    fichero = _particion(scoring_uri, modelo)
    if fichero is None:
        return pd.DataFrame(columns=["decil_riesgo", "solicitudes", "fraccion"])
    return (
        lago.conectar(fichero)
        .execute(f"""
            SELECT d.decil_riesgo, count(p.decil_riesgo) AS solicitudes,
                   count(p.decil_riesgo) / greatest((SELECT count(*) FROM '{fichero}'), 1)
                       AS fraccion
            FROM (SELECT range AS decil_riesgo FROM range(1, 11)) d
            LEFT JOIN '{fichero}' p USING (decil_riesgo)
            GROUP BY 1 ORDER BY 1
        """)
        .df()
    )


def puntuacion_de(scoring_uri: str, modelo: str, cliente: int) -> dict | None:
    """La puntuación de una solicitud concreta (None si no está entre las puntuadas)."""
    fichero = _particion(scoring_uri, modelo)
    if fichero is None:
        return None
    filas = (
        lago.conectar(fichero)
        .execute(f"SELECT * FROM '{fichero}' WHERE {KEY_CURR} = ?", [cliente])
        .df()
    )
    return filas.iloc[0].to_dict() if len(filas) else None


def deriva(deriva_uri: str, modelo: str) -> pd.DataFrame:
    """El informe de deriva del modelo (la puntuación en la primera fila)."""
    fichero = _particion(deriva_uri, modelo)
    if fichero is None:
        return pd.DataFrame(columns=["variable", "psi", "estado", "solicitudes", "calculado_en"])
    return lago.conectar(fichero).execute(f"SELECT * FROM '{fichero}'").df()
