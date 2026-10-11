"""Extracción de la API del sistema origen a la landing zone.

Descarga la entrega de un día (las 8 tablas) página a página y la deja en
landing/fecha=AAAA-MM-DD/<fichero>.csv.gz, con el mismo contrato que las
entregas por fichero: a partir de ahí, ingest-bronze y dbt no cambian.

Buenas prácticas de extracción de APIs que aplica:
- Paginación por cursor hasta que la API devuelve `siguiente = null`.
- Reintentos con espera exponencial y aleatoria (backoff + jitter) ante fallos
  transitorios: 429 (respetando Retry-After), 5xx y errores de red. Los errores
  permanentes (401, 404...) no se reintentan: reintentar no los arregla.
- Verificación: las filas recibidas deben coincidir con el `total` que anuncia
  la API; si no, la entrega no se da por buena.
- Atómica e idempotente: el día se escribe en una carpeta temporal y solo al
  final sustituye a la anterior. Repetir la extracción da el mismo resultado.

Uso:
    uv run extract-api --fecha 2026-09-01 [--conjunto completo]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import random
import shutil
import time
from datetime import date
from pathlib import Path

import httpx

from credit_risk.config import LANDING_DIR, ORIGEN_API_KEY, ORIGEN_API_URL, TABLES

REINTENTABLES = {429, 500, 502, 503, 504}


class ExtraccionIncompleta(Exception):
    """La API ha devuelto menos (o más) filas de las que anunciaba."""


def pedir(
    cliente: httpx.Client, ruta: str, params: dict, reintentos: int = 6, espera_base: float = 0.5
) -> dict:
    """GET con reintentos ante fallos transitorios. Devuelve el JSON de la respuesta."""
    for intento in range(reintentos + 1):
        ultimo = intento == reintentos
        try:
            respuesta = cliente.get(ruta, params=params)
        except httpx.TransportError as error:  # red caída, timeout...
            if ultimo:
                raise
            motivo = f"error de red ({type(error).__name__})"
            espera = espera_base * 2**intento
        else:
            if respuesta.status_code not in REINTENTABLES or ultimo:
                respuesta.raise_for_status()  # 4xx permanentes: error inmediato
                return respuesta.json()
            motivo = f"HTTP {respuesta.status_code}"
            # Retry-After: el servidor dice cuánto esperar; si no, backoff exponencial
            espera = float(respuesta.headers.get("Retry-After", espera_base * 2**intento))
        # jitter: si muchos clientes fallan a la vez, que no reintenten todos a la vez
        espera *= random.uniform(0.5, 1.0)
        print(f"  [reintento {intento + 1}/{reintentos}] {motivo} en {ruta}: espero {espera:.1f} s")
        time.sleep(espera)
    raise AssertionError("inalcanzable")


def extraer_tabla(
    cliente: httpx.Client, conjunto: str, dia: date, tabla: str, destino: Path, limite: int, **kw
) -> int:
    """Descarga todas las páginas de una tabla y las escribe en un CSV comprimido."""
    ruta = f"/v1/{conjunto}/entregas/{dia.isoformat()}/{tabla}"
    params: dict = {"limite": limite}
    filas = 0
    with gzip.open(destino, "wt", encoding="utf-8", newline="") as fichero:
        escritor = csv.writer(fichero, lineterminator="\n")
        while True:
            pagina = pedir(cliente, ruta, params, **kw)
            if filas == 0:
                escritor.writerow(pagina["columnas"])
            # None (vacío en el origen) se escribe como campo vacío, igual que en el CSV original
            escritor.writerows([["" if v is None else v for v in fila] for fila in pagina["filas"]])
            filas += len(pagina["filas"])
            if pagina["siguiente"] is None:
                break
            params = {"limite": limite, "cursor": pagina["siguiente"]}
    if filas != pagina["total"]:
        raise ExtraccionIncompleta(
            f"{tabla} {dia}: recibidas {filas}, la API anuncia {pagina['total']}"
        )
    return filas


def extraer_dia(
    cliente: httpx.Client, conjunto: str, dia: date, landing_dir: Path, limite: int = 5_000, **kw
) -> dict[str, int]:
    """Extrae las 8 tablas de un día y sustituye su entrega en la landing zone."""
    destino = landing_dir / f"fecha={dia.isoformat()}"
    temporal = destino.with_name(destino.name + ".tmp")
    shutil.rmtree(temporal, ignore_errors=True)
    temporal.mkdir(parents=True)

    filas = {}
    for tabla, fichero in TABLES.items():
        filas[tabla] = extraer_tabla(
            cliente, conjunto, dia, tabla, temporal / f"{fichero}.gz", limite, **kw
        )

    shutil.rmtree(destino, ignore_errors=True)
    temporal.rename(destino)
    return filas


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--fecha", type=date.fromisoformat, required=True)
    parser.add_argument("--conjunto", default="completo", help="conjunto de datos de la API")
    parser.add_argument("--limite", type=int, default=5_000, help="filas por página")
    args = parser.parse_args()

    print(f"API:     {ORIGEN_API_URL}/v1/{args.conjunto}")
    print(f"Destino: {LANDING_DIR}\n")
    inicio = time.perf_counter()
    cabeceras = {"X-API-Key": ORIGEN_API_KEY}
    with httpx.Client(base_url=ORIGEN_API_URL, headers=cabeceras, timeout=60) as cliente:
        filas = extraer_dia(cliente, args.conjunto, args.fecha, LANDING_DIR, args.limite)
    for tabla, n in filas.items():
        print(f"[OK] {tabla:<22} {n:>10,} filas")
    print(f"\nEntrega del {args.fecha} extraída en {time.perf_counter() - inicio:.1f} s")


if __name__ == "__main__":
    main()
