"""Comprueba que las agregaciones de Spark coinciden con los modelos de dbt.

Compara lake/spark/<tabla> (escrito por spark_jobs/agregados.py) con la tabla
equivalente de dbt en warehouse.duckdb, por la clave y columna a columna (con
tolerancia en las columnas de coma flotante: Spark y DuckDB suman en distinto
orden, y en coma flotante el orden cambia el último decimal).

Uso (con el lago construido por dbt y el trabajo de Spark ya ejecutado):
    uv run python scripts/comprobar_paridad_spark.py
"""

from __future__ import annotations

import sys

import duckdb

from credit_risk.config import LAKE_DIR

PARES = {  # tabla de Spark -> (modelo de dbt, clave)
    "bureau_balance_por_credito": ("int_bureau_balance__por_credito", "SK_ID_BUREAU"),
    "installments_por_cliente": ("int_installments_payments__por_cliente", "SK_ID_CURR"),
}
TOLERANCIA = 1e-9


def comparar(con: duckdb.DuckDBPyConnection, tabla_spark: str, modelo: str, clave: str) -> list:
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW s AS
        SELECT * FROM read_parquet('{(LAKE_DIR / "spark" / tabla_spark).as_posix()}/*.parquet')
    """)
    filas = con.execute(f"""
        SELECT (SELECT count(*) FROM s), (SELECT count(*) FROM {modelo}),
               (SELECT count(*) FROM s JOIN {modelo} d USING ({clave}))
    """).fetchone()
    problemas = [] if len(set(filas)) == 1 else [f"filas spark/dbt/comunes = {filas}"]

    columnas = [c for c, *_ in con.execute("DESCRIBE s").fetchall() if c != clave]
    for columna in columnas:
        c = f'"{columna}"'
        distintas = con.execute(f"""
            SELECT count(*) FROM s JOIN {modelo} d USING ({clave})
            WHERE (s.{c} IS NULL) <> (d.{c} IS NULL)
               OR abs(s.{c} - d.{c}) > {TOLERANCIA} * greatest(abs(s.{c}), abs(d.{c}), 1e-12)
        """).fetchone()[0]
        if distintas:
            problemas.append(f"{columna}: {distintas} filas distintas")
    print(f"{tabla_spark}: {filas[0]:,} filas, {len(columnas)} columnas comparadas")
    return problemas


def main() -> None:
    con = duckdb.connect((LAKE_DIR / "warehouse.duckdb").as_posix(), read_only=True)
    problemas = []
    for tabla_spark, (modelo, clave) in PARES.items():
        problemas += [f"{tabla_spark}: {p}" for p in comparar(con, tabla_spark, modelo, clave)]
    if problemas:
        sys.exit("DIFERENCIAS:\n  " + "\n  ".join(problemas))
    print("OK: Spark y dbt dan el mismo resultado.")


if __name__ == "__main__":
    main()
