"""Comprueba que el pipeline da exactamente lo mismo con el lago en S3 que en disco.

Ejecuta dos veces, desde cero, simulate-source -> ingest-bronze -> dbt build (hasta el mart),
cada una con su almacén de dbt en una carpeta temporal:
  - "disco": landing y bronze en una carpeta local,
  - "s3":    landing y bronze en el bucket (LAKE_URI), con el target s3 de dbt.
Después compara:
  1. bronze: los mismos ficheros (tabla/partición) y las mismas filas por tabla,
  2. el mart: mismas filas y mismos valores, columna a columna.

Uso (con Moto arrancado y `terraform apply` hecho; ver infra/README.md):
    uv run python scripts/comprobar_paridad_s3.py --lago s3://credit-risk-local-lake-123456789012
Las credenciales y el endpoint de S3 se toman del entorno (AWS_*); si no hay
AWS_ACCESS_KEY_ID, se usan las de mentira de Moto contra http://localhost:5000.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from pathlib import Path

from comprobar_paridad_incremental import MART, comparar

from credit_risk import lago
from credit_risk.config import ROOT

MOTO = {
    "AWS_ENDPOINT_URL": "http://localhost:5000",
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_REGION": "eu-west-1",
}


def pipeline(entorno: dict[str, str]) -> None:
    """simulate-source -> ingest-bronze -> dbt build del mart, con ese entorno."""
    pasos = [
        (["simulate-source"], ROOT),
        (["ingest-bronze"], ROOT),
        (
            ["dbt", "build", "--select", f"+{MART}", "--exclude", "test_type:data"],
            ROOT / "transform",
        ),
    ]
    for comando, carpeta in pasos:
        print(f"$ {' '.join(comando)}", flush=True)
        subprocess.run(comando, cwd=carpeta, env=entorno, check=True, stdout=subprocess.DEVNULL)


def contenido_bronze(con, bronze: str) -> dict[str, int]:
    """{tabla/partición: filas} de todos los Parquet de bronze (también los vacíos)."""
    filas = con.execute(rf"""
        WITH ficheros AS (
            SELECT replace(file, '\', '/') AS fichero
            FROM glob('{bronze}/*/*/*.parquet')
        ),
        conteos AS (
            SELECT replace(filename, '\', '/') AS fichero, count(*) AS filas
            FROM read_parquet('{bronze}/*/*/*.parquet', filename = true)
            GROUP BY 1
        )
        SELECT regexp_extract(fichero, '([^/]+/fecha_entrega=[^/]+)/data\.parquet$', 1),
               coalesce(filas, 0)
        FROM ficheros LEFT JOIN conteos USING (fichero)
    """).fetchall()
    return dict(filas)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lago", required=True, help="URI del lago en S3 (s3://bucket)")
    args = parser.parse_args()
    if not lago.es_s3(args.lago):
        sys.exit("--lago debe ser una URI s3://")

    base = os.environ | ({} if os.getenv("AWS_ACCESS_KEY_ID") else MOTO)
    for variable in ("LANDING_DIR", "LAKE_URI"):  # cada ejecución define su lago
        base.pop(variable, None)
    with tempfile.TemporaryDirectory() as carpeta:
        disco, en_s3 = Path(carpeta) / "disco", Path(carpeta) / "s3"
        en_s3.mkdir()

        print("== Lago en disco")
        pipeline(base | {"LAKE_DIR": str(disco), "DBT_TARGET": "dev"})
        print("\n== Lago en S3")
        os.environ.update({k: base[k] for k in MOTO if k in base})  # para lago.conectar
        # Desde cero, como el de disco: con bronze de una ejecución anterior, la ingesta
        # incremental no escribiría nada y se compararían datos viejos
        for zona in ("landing", "bronze"):
            lago.borrar(lago.unir(args.lago, zona))
        pipeline(base | {"LAKE_DIR": str(en_s3), "LAKE_URI": args.lago, "DBT_TARGET": "s3"})

        con = lago.conectar(args.lago)
        problemas = []
        b_disco = contenido_bronze(con, lago.unir(disco, "bronze"))
        b_s3 = contenido_bronze(con, lago.unir(args.lago, "bronze"))
        print(f"\nBronze: {len(b_disco)} ficheros en disco, {len(b_s3)} en S3")
        if b_disco != b_s3:
            distintos = sorted(set(b_disco.items()) ^ set(b_s3.items()))
            problemas.append(f"bronze distinto: {distintos[:5]}")

        con.execute(f"ATTACH '{(disco / 'warehouse.duckdb').as_posix()}' AS disco (READ_ONLY)")
        con.execute(f"ATTACH '{(en_s3 / 'warehouse.duckdb').as_posix()}' AS s3 (READ_ONLY)")
        problemas += comparar(con, f"disco.main.{MART}", f"s3.main.{MART}")
        con.close()

    if problemas:
        print("\nDIFERENCIAS ENTRE EL LAGO EN DISCO Y EN S3:")
        for problema in problemas:
            print(f"  - {problema}")
        sys.exit(1)
    print("\nOK: el lago en S3 da exactamente el mismo bronze y el mismo mart que en disco.")


if __name__ == "__main__":
    main()
