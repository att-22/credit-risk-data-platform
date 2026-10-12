"""Puntuación por lotes: aplica el modelo vigente a las solicitudes sin resolver.

Las solicitudes del mart que todavía no tienen TARGET (no se sabe si pagarán) se
puntúan con el modelo publicado por `train-model` y el resultado se escribe en el
lago, una partición por modelo:

    warehouse/scoring/modelo=<id>/data.parquet

Por solicitud: la probabilidad que le da el modelo y su decil de riesgo (1 = el
10 % con menos riesgo, 10 = el 10 % con más), calculado con los cortes que el
entrenamiento guardó. Es un dato listo para consumir: quien decide sobre un
crédito solo necesita esta tabla.

Idempotente: volver a puntuar con el mismo modelo sobrescribe su partición; un
modelo nuevo escribe la suya y la anterior se conserva para poder compararlas.

Uso (necesita el grupo de dependencias `ml`, el mart y un modelo publicado):
    uv run --group ml score
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from credit_risk import lago
from credit_risk.config import KEY_CURR, LAKE_DIR, MODELS_DIR, SCORING_URI, TARGET
from credit_risk.modelo import MART, cargar_vigente, puntuar

DECILES = 10


def pendientes(almacen: Path) -> pd.DataFrame:
    """Solicitudes del mart sin TARGET: las que hay que puntuar."""
    with duckdb.connect(almacen.as_posix(), read_only=True) as con:
        return con.execute(f"SELECT * FROM {MART} WHERE {TARGET} IS NULL ORDER BY {KEY_CURR}").df()


def decil(probabilidad: np.ndarray, cortes: list[float]) -> np.ndarray:
    """Decil de riesgo (1-10) según los cortes de la prueba del entrenamiento."""
    return np.searchsorted(cortes, probabilidad, side="right") + 1


def puntuar_pendientes(
    almacen: Path, modelos_dir: Path = MODELS_DIR, scoring_uri: str = SCORING_URI
) -> dict:
    """Puntúa y escribe la partición del modelo vigente. Devuelve un resumen."""
    booster, preparador, metricas = cargar_vigente(modelos_dir)
    solicitudes = pendientes(almacen)
    probabilidad = puntuar(booster, preparador, solicitudes)

    resultado = pd.DataFrame(  # noqa: F841 (DuckDB la lee por su nombre)
        {
            KEY_CURR: solicitudes[KEY_CURR].to_numpy(),
            "probabilidad_impago": probabilidad,
            "decil_riesgo": decil(probabilidad, metricas["cortes_deciles"]).astype("int8"),
            "puntuado_en": datetime.now(UTC),
            # De qué datos sale cada puntuación: trazabilidad hasta la ingesta
            "mart_ingerido_en": solicitudes["_INGESTED_AT"].to_numpy(),
        }
    )
    destino = lago.unir(scoring_uri, f"modelo={metricas['modelo']}", "data.parquet")
    con = lago.conectar(scoring_uri)
    con.register("resultado", resultado)
    lago.copiar(con, "SELECT * FROM resultado", destino, "FORMAT parquet, COMPRESSION zstd")

    por_decil = np.bincount(resultado["decil_riesgo"], minlength=DECILES + 1)[1:]
    return {
        "modelo": metricas["modelo"],
        "solicitudes": len(resultado),
        "destino": destino,
        "probabilidad_media": float(probabilidad.mean()) if len(resultado) else None,
        "por_decil": por_decil.tolist(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.parse_args()

    almacen = LAKE_DIR / "warehouse.duckdb"
    if not almacen.exists():
        sys.exit(f"No existe {almacen}: construye antes el mart con dbt.")
    try:
        resumen = puntuar_pendientes(almacen)
    except FileNotFoundError as error:
        sys.exit(str(error))

    print(f"Modelo:  {resumen['modelo']}")
    print(f"Destino: {resumen['destino']}\n")
    print(f"{resumen['solicitudes']:,} solicitudes puntuadas")
    if resumen["solicitudes"]:
        print(f"Probabilidad media: {resumen['probabilidad_media']:.4f}")
        print("Por decil de riesgo (lo esperado es ~10 % en cada uno):")
        for numero, cuantas in enumerate(resumen["por_decil"], start=1):
            porcentaje = 100 * cuantas / resumen["solicitudes"]
            print(f"  {numero:>2}  {cuantas:>7,}  {porcentaje:5.1f} %")


if __name__ == "__main__":
    main()
