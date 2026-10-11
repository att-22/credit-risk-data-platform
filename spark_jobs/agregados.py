"""Agregaciones de las tablas grandes con PySpark (equivalentes a los modelos de dbt).

Reimplementa en Spark dos modelos intermediate de dbt, los de las tablas más
grandes, para comparar el resultado (paridad) y el comportamiento:

    bronze/bureau_balance (27,3 M filas)        -> spark/bureau_balance_por_credito
    bronze/installments_payments (13,6 M filas) -> spark/installments_por_cliente

Conceptos de Spark que aparecen aquí:
- DataFrame y evaluación perezosa: las transformaciones (filter, groupBy...) solo
  construyen un plan; nada se ejecuta hasta una acción (write, count...).
- Particiones y shuffle: groupBy reparte las filas por clave entre particiones
  (un "shuffle", la operación cara de Spark). spark.sql.shuffle.partitions fija
  en cuántas; el valor por defecto (200) es para clústeres, no para un portátil.
- explain(): imprime el plan físico que Spark va a ejecutar.

Uso (dentro del contenedor de Spark, ver compose.yaml):
    spark-submit /opt/spark-jobs/agregados.py --lake /opt/lake
"""

from __future__ import annotations

import argparse
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

# Identificadores y metadatos: no se agregan (igual que en la macro de dbt)
EXCLUIDAS = {"SK_ID_CURR", "SK_ID_BUREAU", "SK_ID_PREV", "_INGESTED_AT", "FECHA_ENTREGA"}
FUNCIONES = {"MEAN": F.avg, "MAX": F.max, "MIN": F.min, "SUM": F.sum}


DELTA_VERSION = "4.4.1"


def sesion(nombre: str = "credit-risk", particiones: int = 16, delta: bool = False) -> SparkSession:
    constructor = (
        SparkSession.builder.appName(nombre)
        .master("local[*]")  # un "clúster" de una máquina: un ejecutor por núcleo
        .config("spark.sql.shuffle.partitions", particiones)
        .config("spark.sql.session.timeZone", "UTC")
    )
    if delta:
        # Delta Lake se engancha a Spark como extensión de SQL y como catálogo
        constructor = constructor.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        if not os.getenv("DELTA_JARS_INCLUIDOS"):  # fuera de la imagen (p. ej. la CI): se descargan
            constructor = constructor.config(
                "spark.jars.packages", f"io.delta:delta-spark_2.13:{DELTA_VERSION}"
            )
    return constructor.getOrCreate()


def bureau_balance_por_credito(balance: DataFrame, con_marca: bool = False) -> DataFrame:
    """Resumen del historial mensual de cada crédito (= int_bureau_balance__por_credito).

    con_marca añade _INGESTED_AT (la ingesta más reciente del crédito), la marca de
    agua que usa la carga incremental con MERGE de delta_lake.py.
    """
    impago = F.when(F.col("STATUS").isin("1", "2", "3", "4", "5"), 1.0).otherwise(0.0)
    agregados = [F.count(F.lit(1)).alias("BB_MESES"), F.avg(impago).alias("BB_PROP_IMPAGO")]
    if con_marca:
        agregados.append(F.max("_ingested_at").alias("_INGESTED_AT"))
    return balance.groupBy("SK_ID_BUREAU").agg(*agregados)


def con_comportamiento(pagos: DataFrame) -> DataFrame:
    """Variables de comportamiento de pago (= int_installments_payments__con_comportamiento)."""
    sin_pago = F.col("DAYS_ENTRY_PAYMENT").isNull()
    retraso = F.greatest(F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT"), F.lit(0))
    adelanto = F.greatest(F.col("DAYS_INSTALMENT") - F.col("DAYS_ENTRY_PAYMENT"), F.lit(0))
    return pagos.select(
        "*",
        (F.col("AMT_INSTALMENT") - F.col("AMT_PAYMENT")).alias("PAGO_DIF"),
        # nullif(x, 0): si la cuota es 0, el ratio queda NULL
        (F.col("AMT_PAYMENT") / F.nullif(F.col("AMT_INSTALMENT"), F.lit(0))).alias("PAGO_RATIO"),
        F.when(sin_pago, None).otherwise(retraso).alias("DIAS_RETRASO"),
        F.when(sin_pago, None).otherwise(adelanto).alias("DIAS_ADELANTO"),
    )


def agregar_por_cliente(df: DataFrame, clave: str, prefijo: str) -> DataFrame:
    """Media, máximo, mínimo y suma de cada columna numérica, y nº de registros.

    Versión Spark de la macro agregar_por_cliente de dbt: las columnas numéricas
    se descubren del esquema del DataFrame (en dbt, por introspección).
    """
    numericas = [
        c.name
        for c in df.schema.fields
        if isinstance(c.dataType, NumericType) and c.name.upper() not in EXCLUIDAS
    ]
    agregados = [F.count(F.lit(1)).alias(f"{prefijo}_NUM_REGISTROS")]
    for columna in numericas:
        for sufijo, funcion in FUNCIONES.items():
            agregados.append(funcion(columna).alias(f"{prefijo}_{columna.upper()}_{sufijo}"))
    return df.groupBy(clave).agg(*agregados)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--lake", default="/opt/lake", help="raíz del lago (bronze/ y spark/)")
    parser.add_argument("--explicar", action="store_true", help="imprimir los planes físicos")
    args = parser.parse_args()

    spark = sesion()
    bronze = f"{args.lake}/bronze"
    trabajos = {
        "bureau_balance_por_credito": bureau_balance_por_credito(
            spark.read.parquet(f"{bronze}/bureau_balance")
        ),
        "installments_por_cliente": agregar_por_cliente(
            con_comportamiento(spark.read.parquet(f"{bronze}/installments_payments")),
            "SK_ID_CURR",
            "INST",
        ),
    }
    for nombre, df in trabajos.items():
        if args.explicar:
            df.explain()
        inicio = time.perf_counter()
        # Hasta aquí nada se ha ejecutado: write es la acción que lanza el trabajo
        df.write.mode("overwrite").parquet(f"{args.lake}/spark/{nombre}")
        print(f"[OK] {nombre}: {time.perf_counter() - inicio:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
