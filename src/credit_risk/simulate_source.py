"""Simulador del sistema origen: reparte los CSV de Home Credit en entregas diarias.

El dataset de Kaggle es una foto fija, sin fechas reales. Para practicar la
carga incremental, este script simula que el sistema del banco deja cada día en
la *landing zone* un fichero por tabla con las solicitudes de ese día:

    landing/
    ├── fecha=2026-09-01/
    │   ├── application_train.csv.gz
    │   ├── bureau.csv.gz
    │   └── ...
    ├── fecha=2026-09-02/
    └── ...

Reglas:
- Cada solicitud (SK_ID_CURR) recibe una fecha sintética determinista: md5 del
  identificador módulo el número de días. Ejecutarlo dos veces da el mismo reparto.
- Toda la información de un cliente viaja con su solicitud: su historial de
  buró, solicitudes previas, pagos, tarjetas... llega el mismo día.
- Todos los días llegan los 8 ficheros (vacíos, solo con cabecera, si no hay filas).
- No se pierde ninguna fila: las que no se pueden asociar a ninguna solicitud
  (p. ej. los 43.041 créditos huérfanos de bureau_balance) van al primer día.
  La suma de filas de todas las entregas es igual a la de los CSV originales.
- Los ficheros conservan exactamente las columnas y el texto originales.

Uso:
    uv run simulate-source                       # 30 días desde 2026-09-01
    uv run simulate-source --inicio 2026-10-01 --dias 7
    uv run simulate-source --destino lake/origen/completo   # almacén del "banco" (API)
    uv run simulate-source --destino s3://bucket/landing    # landing en S3 (ver lago.py)
"""

from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

import duckdb

from credit_risk import lago
from credit_risk.config import KEY_BUREAU, KEY_CURR, LANDING_URI, RAW_DIR, TABLES

INICIO = date(2026, 9, 1)
DIAS = 30

SOLICITUDES = ["application_train", "application_test"]


def consulta_con_fecha(nombre: str, inicio: date) -> str:
    """SELECT de una tabla con su columna `fecha` de entrega.

    Las filas sin solicitud asociada reciben la fecha de inicio (primer día).
    """
    primer_dia = f"DATE '{inicio.isoformat()}'"
    if nombre == "bureau_balance":
        # bureau_balance no tiene SK_ID_CURR: hereda la fecha de su crédito en bureau
        return f"""
            SELECT t.*, coalesce(c.fecha, {primer_dia}) AS fecha
            FROM bureau_balance t
            LEFT JOIN (
                SELECT b.{KEY_BUREAU}, f.fecha
                FROM bureau b JOIN fechas f ON b.{KEY_CURR} = f.{KEY_CURR}
            ) c ON t.{KEY_BUREAU} = c.{KEY_BUREAU}
        """
    return f"""
        SELECT t.*, coalesce(f.fecha, {primer_dia}) AS fecha
        FROM {nombre} t
        LEFT JOIN fechas f ON t.{KEY_CURR} = f.{KEY_CURR}
    """


def simular_origen(
    raw_dir: Path, landing_dir: str | Path, inicio: date = INICIO, dias: int = DIAS
) -> dict[str, int]:
    """Genera la landing zone completa y devuelve las filas escritas por tabla.

    Se construye en una carpeta temporal y se sustituye al final, así la
    landing zone nunca queda a medias y repetir la ejecución da el mismo
    resultado (idempotente). Si el destino está en S3, se genera en local y se
    sube, como haría el sistema del banco.
    """
    if lago.es_s3(landing_dir):
        with tempfile.TemporaryDirectory() as carpeta:
            local = Path(carpeta) / "landing"
            filas = simular_origen(raw_dir, local, inicio, dias)
            lago.subir(local, lago.ruta(landing_dir))
        return filas

    landing_dir = Path(landing_dir)
    temporal = landing_dir.with_name(landing_dir.name + ".tmp")
    shutil.rmtree(temporal, ignore_errors=True)
    temporal.mkdir(parents=True)  # COPY de DuckDB no crea las carpetas padre
    con = duckdb.connect()

    # all_varchar: se copia el texto tal cual, sin reinterpretar números ni fechas
    for nombre, fichero in TABLES.items():
        ruta = (raw_dir / fichero).as_posix()
        con.execute(f"CREATE VIEW {nombre} AS SELECT * FROM read_csv('{ruta}', all_varchar=true)")

    # Fecha de cada solicitud: inicio + (md5 del identificador módulo días)
    union = " UNION ALL ".join(f"SELECT {KEY_CURR} FROM {t}" for t in SOLICITUDES)
    con.execute(f"""
        CREATE TABLE fechas AS
        SELECT {KEY_CURR},
               DATE '{inicio.isoformat()}' + CAST(md5_number({KEY_CURR}) % {dias} AS INTEGER)
                   AS fecha
        FROM ({union})
    """)

    filas = {}
    for nombre, fichero in TABLES.items():
        # 1) DuckDB escribe una carpeta por fecha: <temporal>/_<tabla>/fecha=.../data_0.csv.gz
        particiones = temporal / f"_{nombre}"
        con.execute(f"""
            COPY ({consulta_con_fecha(nombre, inicio)})
            TO '{particiones.as_posix()}'
            (FORMAT csv, HEADER, COMPRESSION gzip, PARTITION_BY (fecha),
             FILENAME_PATTERN 'data_{{i}}')
        """)
        # 2) Se reorganiza como entrega diaria: <temporal>/fecha=.../<fichero>.gz
        for parte in sorted(particiones.glob("fecha=*/data_*.csv.gz")):
            destino = temporal / parte.parent.name / f"{fichero}.gz"
            destino.parent.mkdir(parents=True, exist_ok=True)
            if destino.exists():
                raise RuntimeError(f"DuckDB escribió más de un fichero en {parte.parent}")
            parte.rename(destino)
        shutil.rmtree(particiones)
        # 3) Contrato de entrega: todos los días llegan todos los ficheros. Si una
        #    tabla no tiene filas ese día, el fichero va solo con la cabecera; así
        #    "hoy no hubo datos" no se confunde con "el fichero no ha llegado".
        for dia in range(dias):
            destino = temporal / f"fecha={inicio + timedelta(days=dia)}" / f"{fichero}.gz"
            if not destino.exists():
                destino.parent.mkdir(parents=True, exist_ok=True)
                con.execute(f"""
                    COPY (SELECT * FROM {nombre} LIMIT 0)
                    TO '{destino.as_posix()}' (FORMAT csv, HEADER, COMPRESSION gzip)
                """)
        filas[nombre] = con.execute(
            f"SELECT count(*) FROM read_csv('{(temporal / '*' / (fichero + '.gz')).as_posix()}')"
        ).fetchone()[0]

    # 4) Sustituir la landing zone anterior por la nueva. Antes se cierra DuckDB:
    #    en Windows no se puede renombrar una carpeta con ficheros abiertos dentro.
    con.close()
    shutil.rmtree(landing_dir, ignore_errors=True)
    temporal.rename(landing_dir)
    return filas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--inicio", type=date.fromisoformat, default=INICIO)
    parser.add_argument("--dias", type=int, default=DIAS)
    parser.add_argument(
        "--destino",
        default=LANDING_URI,
        help="carpeta de salida, local o s3:// (por defecto, la landing)",
    )
    args = parser.parse_args()

    print(f"Origen:  {RAW_DIR}")
    print(f"Destino: {args.destino}  ({args.dias} días desde {args.inicio})\n")
    inicio = time.perf_counter()
    filas = simular_origen(RAW_DIR, args.destino, args.inicio, args.dias)
    for nombre, n in filas.items():
        print(f"[OK] {nombre:<22} {n:>12,} filas")
    entregas = [lago.nombre(p) for p in lago.subcarpetas(args.destino, "fecha=")]
    print(f"\n{len(entregas)} entregas ({entregas[0]} ... {entregas[-1]})")
    print(f"Completado en {time.perf_counter() - inicio:.1f} s")


if __name__ == "__main__":
    main()
