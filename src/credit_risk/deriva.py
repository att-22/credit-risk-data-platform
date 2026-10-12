"""Monitorización de deriva: ¿se parece lo que se puntúa a lo que el modelo aprendió?

Un modelo se entrena con una población y se usa con otra. Si la segunda se aleja
de la primera (cambia el tipo de cliente, la economía, o un fallo en los datos),
sus puntuaciones dejan de ser fiables aunque nada "falle". Esto lo vigila.

Medida: el PSI (Population Stability Index), la habitual en riesgo de crédito.
Reparte los valores en tramos y compara qué fracción de casos se esperaba en cada
uno (e) con la que hay (o):

    PSI = suma de (o - e) * ln(o / e)

Regla del sector: < 0,10 estable · 0,10-0,25 cambio moderado · > 0,25 importante.

Se mide a dos niveles:
  - La PUNTUACIÓN del modelo: cada decil de riesgo debería tener el 10 % de las
    solicitudes (los cortes se fijaron al entrenar). Es la alarma.
  - Las VARIABLES que más pesan en el modelo, contra la referencia que guardó el
    entrenamiento (referencia.json). Es el diagnóstico: qué ha cambiado.

El informe se escribe en el lago (warehouse/deriva/modelo=<id>/data.parquet) y el
comando termina con error si el PSI de la puntuación supera el máximo. Con pocas
solicitudes el PSI es ruido (cada tramo tiene un puñado de casos): por debajo de
un mínimo se informa, pero no se da la alarma.

Uso (necesita el grupo de dependencias `ml`, el mart y un modelo publicado):
    uv run --group ml monitor-drift
    uv run --group ml monitor-drift --psi-maximo 0.10
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from credit_risk import lago
from credit_risk.config import DERIVA_URI, LAKE_DIR, MODELS_DIR

TRAMOS = 10
VARIABLES_VIGILADAS = 30  # las que más pesan en el modelo
PSI_MODERADO, PSI_IMPORTANTE = 0.10, 0.25
MINIMO_SOLICITUDES = 1000  # con menos, el PSI no es fiable
PUNTUACION = "(puntuación del modelo)"
FICHERO_REFERENCIA = "referencia.json"
# Suelo para las fracciones: un tramo vacío haría el logaritmo infinito
MINIMO = 1e-4


def reparto(valores: np.ndarray, cortes: list[float]) -> np.ndarray:
    """Fracción de valores en cada tramo; el último tramo son los ausentes."""
    valores = np.asarray(valores, dtype="float64")
    ausentes = np.isnan(valores)
    tramo = np.searchsorted(cortes, valores[~ausentes], side="right")
    cuenta = np.bincount(tramo, minlength=len(cortes) + 1).astype("float64")
    return np.append(cuenta, ausentes.sum()) / max(len(valores), 1)


def psi(esperado: np.ndarray, observado: np.ndarray) -> float:
    """Population Stability Index entre dos repartos por tramos."""
    e = np.clip(np.asarray(esperado, dtype="float64"), MINIMO, None)
    o = np.clip(np.asarray(observado, dtype="float64"), MINIMO, None)
    return float(np.sum((o - e) * np.log(o / e)))


def estado(valor: float) -> str:
    if valor < PSI_MODERADO:
        return "estable"
    return "moderado" if valor < PSI_IMPORTANTE else "importante"


def referencia(matriz: pd.DataFrame, importancia: np.ndarray) -> dict[str, dict]:
    """Cómo se reparten, en el entrenamiento, las variables que más pesan.

    Por variable: los cortes de sus deciles y la fracción de casos en cada tramo
    (el último, los ausentes). Ocupa poco y basta para calcular el PSI después sin
    volver a leer los datos de entrenamiento.
    """
    principales = np.argsort(importancia)[::-1][:VARIABLES_VIGILADAS]
    resultado = {}
    for indice in principales:
        if importancia[indice] <= 0:
            continue
        columna = matriz.columns[indice]
        valores = matriz[columna].to_numpy(dtype="float64")
        presentes = valores[~np.isnan(valores)]
        cuantiles = np.arange(1, TRAMOS) / TRAMOS
        # unique: una variable con pocos valores distintos (un indicador 0/1) da menos tramos
        cortes = np.unique(np.quantile(presentes, cuantiles)).tolist() if len(presentes) else []
        resultado[columna] = {
            "importancia": float(importancia[indice]),
            "cortes": cortes,
            "esperado": reparto(valores, cortes).tolist(),
        }
    return resultado


def informe(
    ref: dict[str, dict], matriz: pd.DataFrame, probabilidad: np.ndarray, cortes: list[float]
) -> pd.DataFrame:
    """PSI de la puntuación y de cada variable vigilada, de mayor a menor."""
    # Puntuación: sin ausentes, y por construcción un 10 % esperado en cada decil
    filas = [
        {
            "variable": PUNTUACION,
            "psi": psi(np.full(TRAMOS, 1 / TRAMOS), reparto(probabilidad, cortes)[:-1]),
            "importancia": None,
        }
    ]
    for variable, datos in ref.items():
        observado = reparto(matriz[variable].to_numpy(dtype="float64"), datos["cortes"])
        filas.append(
            {
                "variable": variable,
                "psi": psi(np.array(datos["esperado"]), observado),
                "importancia": datos["importancia"],
            }
        )
    tabla = pd.DataFrame(filas)
    tabla["estado"] = tabla["psi"].map(estado)
    # La puntuación primero; después, las variables de más a menos deriva
    variables = tabla.iloc[1:].sort_values("psi", ascending=False)
    return pd.concat([tabla.iloc[:1], variables], ignore_index=True)


def vigilar(
    almacen: Path, modelos_dir: Path = MODELS_DIR, deriva_uri: str = DERIVA_URI
) -> tuple[pd.DataFrame, dict]:
    """Calcula el informe para las solicitudes pendientes y lo escribe en el lago."""
    from credit_risk.modelo import VIGENTE, cargar_vigente, puntuar
    from credit_risk.scoring import pendientes

    booster, preparador, metricas = cargar_vigente(modelos_dir)
    fichero = modelos_dir / VIGENTE / FICHERO_REFERENCIA
    if not fichero.exists():
        raise FileNotFoundError(
            f"El modelo vigente no tiene {FICHERO_REFERENCIA}: vuelve a entrenar con `train-model`."
        )
    ref = json.loads(fichero.read_text(encoding="utf-8"))

    solicitudes = pendientes(almacen)
    tabla = informe(
        ref,
        preparador.transformar(solicitudes),
        puntuar(booster, preparador, solicitudes),
        metricas["cortes_deciles"],
    )
    tabla["solicitudes"] = len(solicitudes)
    tabla["calculado_en"] = datetime.now(UTC)

    destino = lago.unir(deriva_uri, f"modelo={metricas['modelo']}", "data.parquet")
    con = lago.conectar(deriva_uri)
    con.register("tabla", tabla)
    lago.copiar(con, "SELECT * FROM tabla", destino, "FORMAT parquet")
    return tabla, {"modelo": metricas["modelo"], "destino": destino, "n": len(solicitudes)}


def hay_alarma(psi_puntuacion: float, solicitudes: int, psi_maximo: float) -> bool:
    """Alarma si la puntuación ha derivado y hay casos suficientes para fiarse."""
    return solicitudes >= MINIMO_SOLICITUDES and psi_puntuacion > psi_maximo


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--psi-maximo",
        type=float,
        default=PSI_IMPORTANTE,
        help="PSI de la puntuación a partir del cual el comando termina con error",
    )
    args = parser.parse_args()

    almacen = LAKE_DIR / "warehouse.duckdb"
    if not almacen.exists():
        sys.exit(f"No existe {almacen}: construye antes el mart con dbt.")
    try:
        tabla, resumen = vigilar(almacen)
    except FileNotFoundError as error:
        sys.exit(str(error))

    print(f"Modelo:  {resumen['modelo']}")
    print(f"Informe: {resumen['destino']}")
    print(f"{resumen['n']:,} solicitudes comparadas con el entrenamiento\n")
    print(f"{'PSI':>7}  {'estado':<10}  variable")
    for fila in tabla.head(11).itertuples():
        print(f"{fila.psi:7.4f}  {fila.estado:<10}  {fila.variable}")
    variables = tabla.iloc[1:]
    for nivel in ("importante", "moderado"):
        print(f"Variables con cambio {nivel}: {(variables['estado'] == nivel).sum()}")

    psi_puntuacion = float(tabla.loc[0, "psi"])
    if hay_alarma(psi_puntuacion, resumen["n"], args.psi_maximo):
        sys.exit(
            f"\nDERIVA: el PSI de la puntuación ({psi_puntuacion:.4f}) supera el máximo "
            f"({args.psi_maximo}). La población ha cambiado: hay que revisar el modelo."
        )
    if resumen["n"] < MINIMO_SOLICITUDES:
        print(
            f"\nSin alarma: con {resumen['n']} solicitudes (menos de {MINIMO_SOLICITUDES}) "
            f"el PSI ({psi_puntuacion:.4f}) no es fiable."
        )
    else:
        print(f"\nOK: el PSI de la puntuación ({psi_puntuacion:.4f}) no supera {args.psi_maximo}.")


if __name__ == "__main__":
    main()
