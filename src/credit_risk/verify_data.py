"""Verificación de integridad de los datos de entrada.

Comprueba que todas las tablas esperadas están presentes en la carpeta de
datos, mide sus dimensiones (filas x columnas) y guarda un inventario en
outputs/dimensiones.csv. También resume la distribución de la variable objetivo.

Uso:
    uv run verify-data
"""
from __future__ import annotations

import pandas as pd

from credit_risk.config import OUTPUTS_DIR, RAW_DIR, TABLES, TARGET


def main() -> None:
    print(f"Carpeta de datos: {RAW_DIR}\n")
    filas_inventario = []
    faltan = []

    for nombre, fichero in TABLES.items():
        path = RAW_DIR / fichero
        if not path.exists():
            faltan.append(fichero)
            print(f"[FALTA] {fichero}")
            continue
        # Número de columnas: leemos solo la cabecera.
        cols = pd.read_csv(path, nrows=0).shape[1]
        # Número de filas: contamos sin cargar todo en memoria.
        with open(path, "r", encoding="utf-8") as f:
            n_filas = sum(1 for _ in f) - 1  # -1 por la cabecera
        tam_mb = round(path.stat().st_size / (1024 * 1024), 1)
        filas_inventario.append(
            {"tabla": nombre, "fichero": fichero, "filas": n_filas,
             "columnas": cols, "tamaño_MB": tam_mb}
        )
        print(f"[OK] {nombre:<22} {n_filas:>10,} filas x {cols:>3} columnas  ({tam_mb} MB)")

    if faltan:
        print("\nFaltan ficheros. Revisa la variable DATA_DIR en el archivo .env.")
        raise SystemExit(1)

    # Las salidas van al repositorio, nunca a la carpeta de datos de entrada.
    OUTPUTS_DIR.mkdir(exist_ok=True)
    inventario = pd.DataFrame(filas_inventario)
    salida = OUTPUTS_DIR / "dimensiones.csv"
    inventario.to_csv(salida, index=False)
    print(f"\nInventario guardado en: {salida}")

    # Distribución de la variable objetivo.
    app = pd.read_csv(RAW_DIR / TABLES["application_train"], usecols=[TARGET])
    dist = app[TARGET].value_counts().sort_index()
    pct = app[TARGET].value_counts(normalize=True).sort_index() * 100
    print("\nDistribución de la variable objetivo (TARGET):")
    for clase in dist.index:
        print(f"  TARGET={clase}: {dist[clase]:>10,} ({pct[clase]:.2f} %)")
    tasa = pct.get(1, float('nan'))
    print(f"\nTasa de impago (TARGET=1): {tasa:.2f} %")


if __name__ == "__main__":
    main()
