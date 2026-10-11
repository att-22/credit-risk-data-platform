"""Genera el contrato de esquema de bronze a partir de los CSV completos.

La ingesta incremental lee cada día por separado. Si dejara que DuckDB dedujera
los tipos en cada fichero, un día una columna podría salir BIGINT y otro DOUBLE,
y los ficheros vacíos (solo cabecera) saldrían todo VARCHAR. Por eso los tipos
se fijan UNA vez, leyendo los CSV completos (sample_size=-1: todas las filas),
y se guardan en src/credit_risk/esquemas_bronze.json, que se versiona en Git.

Si el origen cambia de verdad (columna nueva, tipo distinto), se vuelve a
generar y el cambio queda revisado en un PR.

Uso (con DATA_DIR apuntando a los datos completos):
    uv run python scripts/generar_esquemas.py
"""

from __future__ import annotations

import json

import duckdb

from credit_risk.config import RAW_DIR, ROOT, TABLES

DESTINO = ROOT / "src" / "credit_risk" / "esquemas_bronze.json"


def main() -> None:
    con = duckdb.connect()
    esquemas = {}
    for nombre, fichero in TABLES.items():
        ruta = (RAW_DIR / fichero).as_posix()
        columnas = con.execute(
            f"DESCRIBE SELECT * FROM read_csv('{ruta}', sample_size=-1)"
        ).fetchall()
        esquemas[nombre] = {columna: tipo for columna, tipo, *_ in columnas}
        print(f"[OK] {nombre:<22} {len(columnas):>4} columnas")

    DESTINO.write_text(json.dumps(esquemas, indent=2) + "\n", encoding="utf-8")
    print(f"\nEsquema guardado en: {DESTINO}")


if __name__ == "__main__":
    main()
