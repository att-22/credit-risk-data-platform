"""Ingesta incremental a la capa bronze: landing zone -> Parquet particionado.

Cada entrega diaria de la landing zone (ver simulate_source.py) se convierte en
una partición de bronze por tabla:

    bronze/<tabla>/fecha_entrega=AAAA-MM-DD/data.parquet

Bronze sigue siendo una copia fiel del origen, con los tipos del contrato de
esquema (esquemas_bronze.json) y dos columnas de trazabilidad: cuándo se
ingirió cada fila y de qué fichero viene.

Incremental: una tabla de control (bronze/_ingestas.parquet) registra qué días
se han ingerido. Por defecto solo se procesan las entregas que aún no están en
ella. Se compara el CONJUNTO de días, no solo el último (un watermark de "fecha
máxima" se saltaría una entrega que llegara con retraso).

Idempotente: reingerir un día sobrescribe sus particiones y su fila de control.
Un día solo se marca como ingerido cuando sus 8 tablas están escritas.

La landing y bronze pueden estar en disco o en S3 (LAKE_URI=s3://...): ver lago.py.

Uso:
    uv run ingest-bronze                      # entregas pendientes
    uv run ingest-bronze --fecha 2026-09-03   # (re)ingerir un día concreto
    uv run ingest-bronze --todo               # reingerir todas las entregas
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date
from pathlib import Path

import duckdb

from credit_risk import lago
from credit_risk.config import BRONZE_URI, LANDING_URI, TABLES

ESQUEMAS: dict[str, dict[str, str]] = json.loads(
    Path(__file__).with_name("esquemas_bronze.json").read_text(encoding="utf-8")
)
PARTICION = "fecha_entrega"
CONTROL = "_ingestas.parquet"
# La ruta contiene "fecha=AAAA-MM-DD/": sin esto, DuckDB la convertiría en una
# columna más (particionado Hive) y el fichero no cumpliría el contrato.
LECTURA = "hive_partitioning=false"


class EntregaIncompleta(Exception):
    """Falta algún fichero en una entrega: el origen no ha cumplido el contrato."""


class CambioDeEsquema(Exception):
    """Las columnas de un fichero no coinciden con el contrato de esquema."""


def csv_a_parquet(
    con: duckdb.DuckDBPyConnection,
    origen: str | Path,
    destino: str | Path,
    esquema: dict[str, str],
) -> int:
    """Convierte un CSV en Parquet con los tipos del esquema y columnas de trazabilidad.

    El destino nunca queda a medias si el proceso se interrumpe (ver lago.copiar).
    Devuelve el número de filas escritas.
    """
    origen, destino = lago.ruta(origen), lago.ruta(destino)
    cabecera = [
        fila[0]
        for fila in con.execute(
            f"DESCRIBE SELECT * FROM read_csv('{origen}', {LECTURA}, all_varchar=true)"
        ).fetchall()
    ]
    if cabecera != list(esquema):
        nuevas = sorted(set(cabecera) - set(esquema))
        faltan = sorted(set(esquema) - set(cabecera))
        raise CambioDeEsquema(f"{origen}: columnas nuevas {nuevas}, faltan {faltan}")

    # columns=...: los tipos los dicta el contrato, no se deducen del fichero
    columnas = "{" + ", ".join(f"'{c}': '{t}'" for c, t in esquema.items()) + "}"
    fuente = "/".join(origen.split("/")[-2:])  # p. ej. fecha=2026-09-01/bureau.csv.gz
    lago.copiar(
        con,
        f"""
        SELECT
            *,
            current_timestamp AS _ingested_at,
            '{fuente}'        AS _source_file
        FROM read_csv('{origen}', {LECTURA}, header=true, columns={columnas})
        """,
        destino,
        "FORMAT parquet, COMPRESSION zstd",
    )
    return con.execute(f"SELECT count(*) FROM '{destino}'").fetchone()[0]


def entregas_en_landing(landing_dir: str | Path) -> list[date]:
    """Días que el origen ha dejado en la landing zone (carpetas fecha=AAAA-MM-DD)."""
    return [
        date.fromisoformat(lago.nombre(p).split("=")[1])
        for p in lago.subcarpetas(landing_dir, "fecha=")
    ]


def dias_ingeridos(con: duckdb.DuckDBPyConnection, bronze_dir: str | Path) -> set[date]:
    """Días ya registrados en la tabla de control."""
    control = lago.unir(bronze_dir, CONTROL)
    if not lago.existe(control):
        return set()
    filas = con.execute(f"SELECT DISTINCT {PARTICION} FROM '{control}'").fetchall()
    return {fila[0] for fila in filas}


def ingerir_dia(
    con: duckdb.DuckDBPyConnection, landing_dir: str | Path, bronze_dir: str | Path, dia: date
) -> dict[str, int]:
    """Ingiere las 8 tablas de una entrega y la registra en la tabla de control."""
    entrega = lago.unir(landing_dir, f"fecha={dia.isoformat()}")
    presentes = lago.ficheros(entrega)
    faltan = [f for f in TABLES.values() if f"{f}.gz" not in presentes]
    if faltan:
        raise EntregaIncompleta(f"{entrega}: faltan {faltan}")

    filas = {}
    for nombre, fichero in TABLES.items():
        destino = lago.unir(bronze_dir, nombre, f"{PARTICION}={dia.isoformat()}", "data.parquet")
        origen = lago.unir(entrega, f"{fichero}.gz")
        filas[nombre] = csv_a_parquet(con, origen, destino, ESQUEMAS[nombre])

    registrar(con, bronze_dir, dia, filas)
    return filas


def registrar(
    con: duckdb.DuckDBPyConnection, bronze_dir: str | Path, dia: date, filas: dict[str, int]
) -> None:
    """Sustituye las filas del día en la tabla de control (escritura atómica)."""
    control = lago.unir(bronze_dir, CONTROL)
    nuevas = " UNION ALL ".join(
        f"SELECT DATE '{dia.isoformat()}' AS {PARTICION}, '{tabla}' AS tabla, "
        f"{n}::BIGINT AS filas, current_timestamp AS ingerido_en"
        for tabla, n in filas.items()
    )
    anteriores = (
        f"SELECT * FROM '{control}' WHERE {PARTICION} <> DATE '{dia.isoformat()}'"
        if lago.existe(control)
        else f"SELECT * FROM ({nuevas}) WHERE false"
    )
    # Se materializa antes de escribir: la consulta lee el mismo fichero que sustituye
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _control AS
        SELECT * FROM ({anteriores} UNION ALL {nuevas}) ORDER BY {PARTICION}, tabla
    """)
    lago.copiar(con, "SELECT * FROM _control", control, "FORMAT parquet")
    con.execute("DROP TABLE _control")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    grupo = parser.add_mutually_exclusive_group()
    grupo.add_argument("--fecha", type=date.fromisoformat, help="(re)ingerir solo este día")
    grupo.add_argument("--todo", action="store_true", help="reingerir todas las entregas")
    args = parser.parse_args()

    print(f"Origen:  {LANDING_URI}")
    print(f"Destino: {BRONZE_URI}\n")
    # Base de datos en memoria: solo la usamos como motor (lee y escribe en el lago)
    con = lago.conectar(LANDING_URI, BRONZE_URI)

    disponibles = entregas_en_landing(LANDING_URI)
    if args.fecha:
        if args.fecha not in disponibles:
            print(f"No hay entrega del {args.fecha} en la landing zone.")
            raise SystemExit(1)
        pendientes = [args.fecha]
    elif args.todo:
        pendientes = disponibles
    else:
        ingeridos = dias_ingeridos(con, BRONZE_URI)
        pendientes = [d for d in disponibles if d not in ingeridos]
        print(f"Entregas: {len(disponibles)} en landing, {len(ingeridos)} ya ingeridas")

    if not pendientes:
        print("Nada que ingerir: bronze está al día.")
        return

    inicio_total = time.perf_counter()
    for dia in pendientes:
        inicio = time.perf_counter()
        filas = ingerir_dia(con, LANDING_URI, BRONZE_URI, dia)
        segundos = time.perf_counter() - inicio
        print(f"[OK] {dia}  {sum(filas.values()):>10,} filas  {segundos:>5.1f} s")

    print(f"\n{len(pendientes)} entregas ingeridas en {time.perf_counter() - inicio_total:.1f} s")


if __name__ == "__main__":
    main()
