"""Ingesta a la capa bronze: CSV originales -> Parquet.

La capa bronze es una copia fiel de los datos de origen, sin limpiar ni
transformar, pero en un formato eficiente (Parquet) y con dos columnas de
trazabilidad: cuándo se ingirió cada fila y de qué fichero viene.

La ingesta es idempotente: ejecutarla varias veces produce el mismo resultado,
y nunca deja un fichero a medio escribir.

Uso:
    uv run ingest-bronze
"""

from __future__ import annotations

import time
from pathlib import Path

import duckdb

from credit_risk.config import BRONZE_DIR, RAW_DIR, TABLES


def csv_a_parquet(con: duckdb.DuckDBPyConnection, origen: Path, destino: Path) -> int:
    """Convierte un CSV en Parquet añadiendo columnas de trazabilidad.

    Escribe primero en un fichero temporal y luego lo renombra, para que el
    destino nunca quede a medias si el proceso se interrumpe. Devuelve el
    número de filas escritas.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_name(destino.name + ".tmp")

    con.execute(
        f"""
        COPY (
            SELECT
                *,
                current_timestamp AS _ingested_at,
                '{origen.name}'   AS _source_file
            FROM read_csv('{origen.as_posix()}')
        ) TO '{temporal.as_posix()}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    temporal.replace(destino)  # renombrado atómico: o está el fichero entero o no está

    return con.execute(f"SELECT count(*) FROM '{destino.as_posix()}'").fetchone()[0]


def main() -> None:
    print(f"Origen:  {RAW_DIR}")
    print(f"Destino: {BRONZE_DIR}\n")

    faltan = [f for f in TABLES.values() if not (RAW_DIR / f).exists()]
    if faltan:
        print(f"Faltan ficheros en el origen: {faltan}")
        raise SystemExit(1)

    con = duckdb.connect()  # base de datos en memoria: solo la usamos como motor
    inicio_total = time.perf_counter()

    for nombre, fichero in TABLES.items():
        inicio = time.perf_counter()
        destino = BRONZE_DIR / f"{nombre}.parquet"
        filas = csv_a_parquet(con, RAW_DIR / fichero, destino)
        mb = destino.stat().st_size / (1024 * 1024)
        segundos = time.perf_counter() - inicio
        print(f"[OK] {nombre:<22} {filas:>12,} filas  {mb:>7.1f} MB  {segundos:>5.1f} s")

    print(f"\nIngesta completada en {time.perf_counter() - inicio_total:.1f} s")


if __name__ == "__main__":
    main()
