"""Entrenamiento del modelo de scoring sobre el mart (Fase 6).

Cierra el círculo del proyecto: el mart que construye la plataforma alimenta el
mismo modelo que el TFM entrenó a partir de notebooks, en las mismas condiciones:

  - LightGBM con los hiperparámetros que el TFM ajustó con Optuna
    (hiperparametros_lgbm.json).
  - El mismo reparto: 80 % entrenamiento, 20 % prueba, estratificado y con la misma
    semilla. Como el CSV original está ordenado por SK_ID_CURR, ordenar el mart
    por esa columna da exactamente las mismas 61.503 solicitudes de prueba.
  - El número de árboles se decide por parada temprana y después se reentrena con
    todo el conjunto de entrenamiento.

La preparación que el TFM hacía en un notebook (one-hot, quitar columnas vacías o
constantes) vive aquí, en `Preparador`: se APRENDE solo con el entrenamiento y se
guarda junto al modelo, para aplicar exactamente la misma a los datos que se
puntúen después.

Cada ejecución se registra en MLflow (parámetros, métricas, ficheros y de qué
datos salió) y solo se publica como modelo vigente si supera un AUC mínimo.

Uso (necesita el grupo de dependencias `ml` y el mart construido con dbt):
    uv run --group ml train-model
    uv run --group ml train-model --auc-minimo 0.78
    uv run --group ml train-model --prueba      # rápido, para la muestra sintética
    uvx mlflow ui --backend-store-uri sqlite:///outputs/mlflow.db --port 5001
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from credit_risk import deriva
from credit_risk.config import (
    KEY_CURR,
    LAKE_DIR,
    MLFLOW_URI,
    MODELS_DIR,
    RANDOM_STATE,
    ROOT,
    TARGET,
)

MART = "mart_scoring__clientes"
# Columnas del mart que no son variables del modelo: identificadores y trazabilidad
NO_VARIABLES = {"CONJUNTO", KEY_CURR, TARGET, "_INGESTED_AT", "fecha_entrega"}
HIPERPARAMETROS: dict = json.loads(
    Path(__file__).with_name("hiperparametros_lgbm.json").read_text(encoding="utf-8")
)
# Para comprobar que todo funciona con la muestra sintética (cientos de filas)
HIPERPARAMETROS_PRUEBA = {"learning_rate": 0.1, "num_leaves": 8, "min_child_samples": 5}
AUC_TFM = 0.7900  # LightGBM ajustado del TFM, sobre el mismo conjunto de prueba
FRACCION_PRUEBA = 0.20
FICHERO_MODELO, FICHERO_PREPARADOR, FICHERO_METRICAS = (
    "modelo.txt",
    "preparador.json",
    "metricas.json",
)
VIGENTE = "vigente"


class ModeloInsuficiente(Exception):
    """El modelo entrenado no alcanza el AUC mínimo exigido: no se publica."""


@dataclass
class Preparador:
    """Convierte filas del mart en la matriz numérica que entiende el modelo.

    `ajustar` aprende de los datos de entrenamiento qué categorías existen y qué
    columnas no aportan nada; `transformar` aplica eso mismo a cualquier dato. Así,
    una categoría que aparezca más tarde no cambia el número de columnas (queda a
    cero en todas las de su variable) y no se filtra información de la prueba.
    """

    categorias: dict[str, list[str]] = field(default_factory=dict)
    columnas: list[str] = field(default_factory=list)

    def ajustar(self, datos: pd.DataFrame) -> Preparador:
        variables = datos.drop(columns=list(NO_VARIABLES & set(datos.columns)))
        self.categorias = {
            c: sorted(variables[c].dropna().astype(str).unique())
            for c in variables.columns
            if not pd.api.types.is_numeric_dtype(variables[c])
            or pd.api.types.is_bool_dtype(variables[c])
        }
        matriz = self._codificar(variables)
        # Fuera las columnas vacías o constantes: no pueden separar buenos de malos
        utiles = matriz.columns[matriz.nunique(dropna=True) > 1]
        self.columnas = list(utiles)
        return self

    def transformar(self, datos: pd.DataFrame) -> pd.DataFrame:
        matriz = self._codificar(datos)
        return matriz.reindex(columns=self.columnas).astype("float32")

    def _codificar(self, datos: pd.DataFrame) -> pd.DataFrame:
        """One-hot de las categóricas, con una columna más para "sin dato"."""
        numericas = [c for c in datos.columns if c not in self.categorias and c not in NO_VARIABLES]
        partes = [datos[numericas].astype("float64").set_axis(numericas, axis=1)]
        for columna, valores in self.categorias.items():
            if columna not in datos.columns:
                continue
            texto = datos[columna].astype("string")
            indicadores = {
                _nombre_valido(f"{columna}_{valor}"): (texto == valor).fillna(False)
                for valor in valores
            }
            indicadores[f"{columna}_nan"] = texto.isna()
            partes.append(pd.DataFrame(indicadores, index=datos.index).astype("int8"))
        matriz = pd.concat(partes, axis=1)
        return matriz.rename(columns=_nombre_valido)

    def guardar(self, ruta: Path) -> None:
        ruta.write_text(json.dumps(asdict(self), ensure_ascii=False, indent=1), encoding="utf-8")

    @classmethod
    def cargar(cls, ruta: Path) -> Preparador:
        return cls(**json.loads(ruta.read_text(encoding="utf-8")))


def _nombre_valido(nombre: str) -> str:
    """Solo letras, números y '_': LightGBM no admite otros caracteres en los nombres."""
    return re.sub(r"_+", "_", re.sub(r"[^0-9a-zA-Z_]+", "_", nombre)).strip("_")


def cargar_mart(almacen: Path) -> tuple[pd.DataFrame, dict]:
    """Solicitudes con TARGET, ordenadas por cliente, y de qué datos salen."""
    with duckdb.connect(almacen.as_posix(), read_only=True) as con:
        datos = con.execute(
            f"SELECT * FROM {MART} WHERE {TARGET} IS NOT NULL ORDER BY {KEY_CURR}"
        ).df()
        filas, hasta = con.execute(f"SELECT count(*), max(_INGESTED_AT) FROM {MART}").fetchone()
    # Versión de los datos: con esto se sabe con qué mart se entrenó cada modelo
    return datos, {"mart_filas": filas, "mart_ingerido_hasta": str(hasta)}


def repartir(datos: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Entrenamiento y prueba, estratificados por TARGET (el mismo reparto del TFM)."""
    from sklearn.model_selection import train_test_split

    return train_test_split(
        datos, test_size=FRACCION_PRUEBA, stratify=datos[TARGET], random_state=RANDOM_STATE
    )


def entrenar(
    entrenamiento: pd.DataFrame, hiperparametros: dict, arboles_max: int = 2000
) -> tuple[object, Preparador, int]:
    """Ajusta el preparador y el modelo. Devuelve (modelo, preparador, árboles)."""
    from lightgbm import LGBMClassifier, early_stopping, log_evaluation
    from sklearn.model_selection import train_test_split

    def nuevo(arboles: int) -> LGBMClassifier:
        # class_weight="balanced": solo el 8 % impaga; sin esto el modelo aprendería
        # a decir siempre "paga"
        return LGBMClassifier(
            n_estimators=arboles,
            class_weight="balanced",
            n_jobs=-1,
            random_state=RANDOM_STATE,
            verbosity=-1,
            **hiperparametros,
        )

    preparador = Preparador().ajustar(entrenamiento)
    X, y = preparador.transformar(entrenamiento), entrenamiento[TARGET].astype("int8")

    # 1) Cuántos árboles: se entrena con el 80 % y se para cuando el AUC del otro
    #    20 % lleva 50 árboles sin mejorar
    X_aj, X_val, y_aj, y_val = train_test_split(
        X, y, test_size=FRACCION_PRUEBA, stratify=y, random_state=RANDOM_STATE
    )
    provisional = nuevo(arboles_max)
    provisional.fit(
        X_aj,
        y_aj,
        eval_X=X_val,
        eval_y=y_val,
        eval_metric="auc",
        callbacks=[early_stopping(50, verbose=False), log_evaluation(0)],
    )
    arboles = provisional.best_iteration_ or arboles_max

    # 2) Modelo final: ese número de árboles, con todo el entrenamiento
    modelo = nuevo(arboles)
    modelo.fit(X, y)
    return modelo, preparador, arboles


def puntuar(booster, preparador: Preparador, datos: pd.DataFrame) -> np.ndarray:
    """Probabilidad de impago que el modelo asigna a cada fila.

    Sirve para ordenar las solicitudes por riesgo, no como probabilidad real: el
    modelo se entrena con las clases equilibradas (class_weight), así que la cifra
    está inflada respecto a la tasa de impago verdadera (~8 %).
    """
    return booster.predict(preparador.transformar(datos))


def evaluar(modelo, preparador: Preparador, prueba: pd.DataFrame) -> dict[str, float]:
    """AUC, Gini y KS sobre datos que el modelo no ha visto."""
    from sklearn.metrics import roc_auc_score, roc_curve

    y = prueba[TARGET].astype("int8")
    probabilidad = puntuar(modelo.booster_, preparador, prueba)
    auc = float(roc_auc_score(y, probabilidad))
    falsos, verdaderos, _ = roc_curve(y, probabilidad)
    return {
        "auc": auc,
        "gini": 2 * auc - 1,  # la misma medida, en la escala habitual en riesgo de crédito
        "ks": float(np.max(verdaderos - falsos)),  # separación máxima entre buenos y malos
    }


def guardar(
    modelo, preparador: Preparador, metricas: dict, referencia: dict, carpeta: Path
) -> None:
    carpeta.mkdir(parents=True, exist_ok=True)
    # Formato de texto de LightGBM: portable entre versiones y sin ejecutar código al
    # cargarlo, a diferencia de pickle/joblib
    modelo.booster_.save_model(str(carpeta / FICHERO_MODELO))
    preparador.guardar(carpeta / FICHERO_PREPARADOR)
    (carpeta / FICHERO_METRICAS).write_text(json.dumps(metricas, indent=1), encoding="utf-8")
    (carpeta / deriva.FICHERO_REFERENCIA).write_text(
        json.dumps(referencia, indent=1), encoding="utf-8"
    )


def publicar(carpeta: Path, modelos_dir: Path) -> Path:
    """Copia el modelo a modelos/vigente, que es el que usa la puntuación."""
    vigente, temporal = modelos_dir / VIGENTE, modelos_dir / f"{VIGENTE}.tmp"
    shutil.rmtree(temporal, ignore_errors=True)
    shutil.copytree(carpeta, temporal)
    shutil.rmtree(vigente, ignore_errors=True)
    temporal.rename(vigente)
    return vigente


def cargar_vigente(modelos_dir: Path = MODELS_DIR) -> tuple[object, Preparador, dict]:
    """El modelo publicado, su preparador y sus métricas (para puntuar con él)."""
    import lightgbm

    vigente = modelos_dir / VIGENTE
    if not (vigente / FICHERO_MODELO).exists():
        raise FileNotFoundError(
            f"No hay modelo vigente en {vigente}: entrena antes con `train-model`."
        )
    return (
        lightgbm.Booster(model_file=str(vigente / FICHERO_MODELO)),
        Preparador.cargar(vigente / FICHERO_PREPARADOR),
        json.loads((vigente / FICHERO_METRICAS).read_text(encoding="utf-8")),
    )


def _commit() -> str:
    """Commit del código con el que se entrena (vacío fuera de un repositorio)."""
    try:
        resultado = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True
        )
    except OSError:  # no hay git (p. ej. dentro de la imagen de Airflow)
        return ""
    return resultado.stdout.strip() if resultado.returncode == 0 else ""


def ejecutar(
    almacen: Path,
    modelos_dir: Path = MODELS_DIR,
    mlflow_uri: str = MLFLOW_URI,
    auc_minimo: float = 0.0,
    prueba_rapida: bool = False,
) -> dict:
    """Entrena, evalúa, registra en MLflow y publica el modelo si supera el mínimo."""
    import mlflow

    hiperparametros = HIPERPARAMETROS | (HIPERPARAMETROS_PRUEBA if prueba_rapida else {})
    arboles_max = 50 if prueba_rapida else 2000

    datos, version_datos = cargar_mart(almacen)
    entrenamiento, prueba = repartir(datos)
    print(f"Entrenamiento: {len(entrenamiento):,} solicitudes | prueba: {len(prueba):,}")

    inicio = time.perf_counter()
    modelo, preparador, arboles = entrenar(entrenamiento, hiperparametros, arboles_max)
    metricas = evaluar(modelo, preparador, prueba)
    segundos = time.perf_counter() - inicio
    print(f"Variables: {len(preparador.columnas)} | árboles: {arboles} | {segundos:.0f} s")
    print(
        f"AUC {metricas['auc']:.4f} | Gini {metricas['gini']:.4f} | KS {metricas['ks']:.4f}"
        f"   (TFM: AUC {AUC_TFM:.4f})"
    )

    identificador = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    carpeta = modelos_dir / identificador
    resumen = {
        **metricas,
        "arboles": arboles,
        "variables": len(preparador.columnas),
        "filas_entrenamiento": len(entrenamiento),
        "filas_prueba": len(prueba),
        "modelo": identificador,
        "commit": _commit(),
        **version_datos,
        # Puntuaciones que separan los deciles en la prueba (10 %, 20 %... 90 %). Con
        # ellas se asigna un decil de riesgo al puntuar, y son la referencia para
        # detectar si la población que se puntúa cambia respecto a la del entrenamiento
        "cortes_deciles": [
            float(q)
            for q in np.quantile(
                puntuar(modelo.booster_, preparador, prueba), np.arange(0.1, 1.0, 0.1)
            )
        ],
    }
    # Referencia para vigilar la deriva: cómo son, en el entrenamiento, las variables
    # que más pesan (por ganancia: cuánto reducen el error los cortes que hace el modelo)
    referencia = deriva.referencia(
        preparador.transformar(entrenamiento),
        modelo.booster_.feature_importance(importance_type="gain"),
    )
    guardar(modelo, preparador, resumen, referencia, carpeta)

    publicado = metricas["auc"] >= auc_minimo
    mlflow.set_tracking_uri(mlflow_uri)
    # Los ficheros de cada ejecución, junto a la base de datos de MLflow
    if mlflow.get_experiment_by_name("scoring") is None:
        mlflow.create_experiment("scoring", artifact_location=(modelos_dir / "mlflow").as_uri())
    mlflow.set_experiment("scoring")
    with mlflow.start_run(run_name=identificador):
        mlflow.log_params(hiperparametros | {"arboles": arboles, "arboles_max": arboles_max})
        mlflow.log_metrics(metricas | {"segundos": segundos, "auc_tfm": AUC_TFM})
        mlflow.set_tags(
            {
                "commit": resumen["commit"],
                "publicado": str(publicado),
                "prueba_rapida": str(prueba_rapida),
                **{k: str(v) for k, v in version_datos.items()},
            }
        )
        mlflow.log_artifacts(str(carpeta))

    if not publicado:
        raise ModeloInsuficiente(
            f"AUC {metricas['auc']:.4f} por debajo del mínimo {auc_minimo}: el modelo "
            f"queda en {carpeta} pero NO se publica; sigue vigente el anterior."
        )
    vigente = publicar(carpeta, modelos_dir)
    print(f"Modelo publicado en {vigente}")
    return resumen


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--auc-minimo",
        type=float,
        default=0.0,
        help="AUC de prueba por debajo del cual el modelo no se publica",
    )
    parser.add_argument(
        "--prueba",
        action="store_true",
        help="hiperparámetros pequeños y rápidos, para la muestra sintética",
    )
    args = parser.parse_args()

    almacen = LAKE_DIR / "warehouse.duckdb"
    if not almacen.exists():
        sys.exit(f"No existe {almacen}: construye antes el mart con dbt.")
    print(f"Mart:    {almacen}")
    print(f"Modelos: {MODELS_DIR}\n")
    try:
        ejecutar(almacen, auc_minimo=args.auc_minimo, prueba_rapida=args.prueba)
    except ModeloInsuficiente as error:
        sys.exit(str(error))


if __name__ == "__main__":
    main()
