"""Comprueba que la carga incremental da el mismo mart que una reconstrucción completa.

Es la prueba de fuego de los modelos incrementales: si la lógica de "qué hay
nuevo" tiene un fallo, el mart incremental acaba distinto del que se obtiene
recalculando todo desde cero.

Pasos (sobre el lago de LAKE_DIR, que se reconstruye entero):
  0. Primera ejecución sin datos visibles (corte anterior a cualquier ingesta):
     los modelos incrementales se crean vacíos y deben recuperarse después.
  1. Ingiere las entregas de la landing zone en tandas y, tras cada tanda,
     ejecuta dbt en modo incremental.
  2. Simula una entrega tardía: reingiere un día ya cargado y vuelve a ejecutar
     dbt (sus clientes se recalculan y sustituyen, sin duplicarse).
  3. Guarda una copia del mart incremental, reconstruye con --full-refresh y
     compara las dos versiones columna a columna.

Uso (antes: `uv run simulate-source` y, en transform/, `uv run dbt deps`):
    uv run python scripts/comprobar_paridad_incremental.py [--tandas 3]
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys

import duckdb

from credit_risk.config import BRONZE_DIR, LAKE_DIR, LANDING_DIR, ROOT
from credit_risk.ingest import entregas_en_landing

MART = "mart_scoring__clientes"
TOLERANCIA = 1e-9  # diferencia relativa admitida en columnas de coma flotante


def ejecutar(*comando: str, cwd=ROOT) -> None:
    print(f"$ {' '.join(comando)}", flush=True)
    subprocess.run(comando, cwd=cwd, check=True, stdout=subprocess.DEVNULL)


def dbt_build(*extra: str) -> None:
    # Solo el mart y lo que necesita; los tests de datos ya los ejecuta el job de dbt
    ejecutar(
        "dbt",
        "build",
        "--select",
        f"+{MART}",
        "--exclude",
        "test_type:data",
        *extra,
        cwd=ROOT / "transform",
    )


def comparar(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Diferencias entre mart_incremental (copia) y el mart reconstruido."""
    problemas = []
    filas = con.execute(f"""
        SELECT (SELECT count(*) FROM mart_incremental), (SELECT count(*) FROM {MART}),
               (SELECT count(*) FROM mart_incremental JOIN {MART} USING (SK_ID_CURR))
    """).fetchone()
    print(f"Filas: incremental={filas[0]:,}  reconstruido={filas[1]:,}  comunes={filas[2]:,}")
    if len(set(filas)) != 1:
        problemas.append(f"distinto número de filas: {filas}")

    columnas = con.execute(f"""
        SELECT column_name, data_type FROM duckdb_columns()
        WHERE table_name = '{MART}' AND column_name NOT IN ('SK_ID_CURR', '_INGESTED_AT')
        ORDER BY column_index
    """).fetchall()
    unir = f"FROM mart_incremental a JOIN {MART} b USING (SK_ID_CURR)"
    for columna, tipo in columnas:
        c = f'"{columna}"'
        if tipo in ("DOUBLE", "FLOAT") or tipo.startswith("DECIMAL"):
            distinto = f"""(a.{c} IS NULL) <> (b.{c} IS NULL)
                OR abs(a.{c} - b.{c}) > {TOLERANCIA} * greatest(abs(a.{c}), abs(b.{c}), 1e-12)"""
        else:
            distinto = f"a.{c} IS DISTINCT FROM b.{c}"
        n = con.execute(f"SELECT count(*) {unir} WHERE {distinto}").fetchone()[0]
        if n:
            problemas.append(f"{columna}: {n} filas distintas")
    print(f"Columnas comparadas: {len(columnas)}")
    return problemas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tandas", type=int, default=3)
    args = parser.parse_args()

    dias = entregas_en_landing(LANDING_DIR)
    if not dias:
        sys.exit(f"No hay entregas en {LANDING_DIR}: ejecuta antes `uv run simulate-source`.")

    # Lago limpio: bronze y warehouse se reconstruyen desde la landing zone
    shutil.rmtree(BRONZE_DIR, ignore_errors=True)
    (LAKE_DIR / "warehouse.duckdb").unlink(missing_ok=True)

    # 0) Primera ejecución que no ve ningún dato: crea las tablas incrementales
    #    vacías (su marca de agua es NULL y no debe bloquear las cargas siguientes)
    print("\n== Primera ejecución sin datos visibles")
    ejecutar("ingest-bronze", "--fecha", dias[0].isoformat())
    dbt_build("--vars", "{corte_ingesta: '1970-01-01 00:00:00+00'}")

    # 1) Carga incremental en tandas de días
    tamano = -(-len(dias) // args.tandas)  # división redondeando hacia arriba
    for i in range(0, len(dias), tamano):
        tanda = dias[i : i + tamano]
        print(f"\n== Tanda: {tanda[0]} ... {tanda[-1]} ({len(tanda)} días)")
        for dia in tanda:
            ejecutar("ingest-bronze", "--fecha", dia.isoformat())
        dbt_build()

    # 2) Entrega tardía: un día ya cargado vuelve a llegar. Se elige uno CON
    #    solicitudes (con la muestra hay días vacíos, y reingerir uno de ellos no
    #    pondría nada a prueba).
    control = (BRONZE_DIR / "_ingestas.parquet").as_posix()
    con_datos = [
        fila[0]
        for fila in duckdb.sql(f"""
            SELECT fecha_entrega FROM '{control}'
            WHERE tabla = 'application_train' AND filas > 0 ORDER BY 1
        """).fetchall()
    ]
    tardio = con_datos[len(con_datos) // 2]
    print(f"\n== Entrega tardía: se reingiere {tardio}")
    ejecutar("ingest-bronze", "--fecha", tardio.isoformat())
    dbt_build()

    # 3) Copia del mart incremental y reconstrucción completa
    almacen = (LAKE_DIR / "warehouse.duckdb").as_posix()
    with duckdb.connect(almacen) as con:
        con.execute(f"CREATE OR REPLACE TABLE mart_incremental AS SELECT * FROM {MART}")
    print("\n== Reconstrucción completa (--full-refresh)")
    dbt_build("--full-refresh")

    with duckdb.connect(almacen) as con:
        problemas = comparar(con)
        con.execute("DROP TABLE mart_incremental")

    if problemas:
        print("\nDIFERENCIAS ENTRE INCREMENTAL Y RECONSTRUCCIÓN:")
        for problema in problemas:
            print(f"  - {problema}")
        sys.exit(1)
    print("\nOK: el mart incremental es idéntico a la reconstrucción completa.")


if __name__ == "__main__":
    main()
