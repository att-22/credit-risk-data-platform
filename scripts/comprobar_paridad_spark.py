"""Comprueba que las agregaciones de Spark (Parquet y Delta) coinciden con dbt.

Compara cada tabla de Spark con el modelo equivalente de dbt en warehouse.duckdb,
por la clave y columna a columna (con tolerancia en las columnas de coma
flotante: Spark y DuckDB suman en distinto orden, y en coma flotante el orden
cambia el último decimal).

- lake/spark/<tabla>: Parquet escrito por spark_jobs/agregados.py.
- lake/delta/<tabla>: tabla Delta construida de forma incremental con MERGE por
  spark_jobs/delta_lake.py (se lee con la librería deltalake, sin Spark).

Uso (con el lago construido por dbt y los trabajos de Spark ya ejecutados):
    uv run python scripts/comprobar_paridad_spark.py [--delta]
"""

from __future__ import annotations

import argparse
import sys

import duckdb

from credit_risk.config import LAKE_DIR

PARES = {  # tabla de Spark -> (modelo de dbt, clave)
    "bureau_balance_por_credito": ("int_bureau_balance__por_credito", "SK_ID_BUREAU"),
    "installments_por_cliente": ("int_installments_payments__por_cliente", "SK_ID_CURR"),
}
PARES_DELTA = {"bureau_balance_por_credito": PARES["bureau_balance_por_credito"]}
TOLERANCIA = 1e-9
NUMERICOS = ("DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "INTEGER", "HUGEINT")


def comparar(con: duckdb.DuckDBPyConnection, nombre: str, modelo: str, clave: str) -> list:
    """Compara la vista temporal `s` (Spark) con el modelo de dbt."""
    filas = con.execute(f"""
        SELECT (SELECT count(*) FROM s), (SELECT count(*) FROM {modelo}),
               (SELECT count(*) FROM s JOIN {modelo} d USING ({clave}))
    """).fetchone()
    problemas = [] if len(set(filas)) == 1 else [f"filas spark/dbt/comunes = {filas}"]

    columnas = [(c, t) for c, t, *_ in con.execute("DESCRIBE s").fetchall() if c != clave]
    for columna, tipo in columnas:
        c = f'"{columna}"'
        if tipo.startswith(NUMERICOS):
            distinto = f"""(s.{c} IS NULL) <> (d.{c} IS NULL)
                OR abs(s.{c} - d.{c}) > {TOLERANCIA} * greatest(abs(s.{c}), abs(d.{c}), 1e-12)"""
        else:
            distinto = f"s.{c} IS DISTINCT FROM d.{c}"
        n = con.execute(f"SELECT count(*) FROM s JOIN {modelo} d USING ({clave}) WHERE {distinto}")
        if distintas := n.fetchone()[0]:
            problemas.append(f"{columna}: {distintas} filas distintas")
    print(f"{nombre}: {filas[0]:,} filas, {len(columnas)} columnas comparadas")
    return problemas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--delta", action="store_true", help="comparar también las tablas Delta")
    args = parser.parse_args()

    con = duckdb.connect((LAKE_DIR / "warehouse.duckdb").as_posix(), read_only=True)
    problemas = []
    for tabla, (modelo, clave) in PARES.items():
        ruta = (LAKE_DIR / "spark" / tabla).as_posix()
        con.execute(f"CREATE OR REPLACE TEMP VIEW s AS SELECT * FROM '{ruta}/*.parquet'")
        problemas += [f"{tabla}: {p}" for p in comparar(con, tabla, modelo, clave)]

    if args.delta:
        from deltalake import DeltaTable  # solo hace falta con --delta (grupo spark)

        con.execute("DROP VIEW IF EXISTS s")  # la de las tablas Parquet
        for tabla, (modelo, clave) in PARES_DELTA.items():
            delta = DeltaTable((LAKE_DIR / "delta" / tabla).as_posix())
            con.register("s", delta.to_pyarrow_table())  # la última versión de la tabla
            nombre = f"delta/{tabla} (versión {delta.version()})"
            problemas += [f"{nombre}: {p}" for p in comparar(con, nombre, modelo, clave)]
            con.unregister("s")

    if problemas:
        sys.exit("DIFERENCIAS:\n  " + "\n  ".join(problemas))
    print("OK: Spark y dbt dan el mismo resultado.")


if __name__ == "__main__":
    main()
