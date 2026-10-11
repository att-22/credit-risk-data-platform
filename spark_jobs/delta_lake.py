"""Delta Lake: bronze y agregados como tablas Delta (ACID, MERGE, time travel).

Una tabla Delta son ficheros Parquet + un REGISTRO DE TRANSACCIONES (carpeta
_delta_log/ con un JSON por commit). Ese registro es lo que convierte una carpeta
de ficheros en una tabla con garantías de base de datos:

- ACID: cada escritura es un commit atómico; quien lee ve la versión anterior o
  la nueva, nunca una a medias, aunque haya escritores a la vez.
- Versiones y time travel: cada commit es una versión que se puede volver a leer
  (versionAsOf). Es el "corte de lectura" de la Fase 3, pero nativo.
- MERGE: upsert (actualizar si existe, insertar si no) en una sola operación;
  el equivalente al delete+insert de los modelos incrementales de dbt.
- Contrato de esquema: escribir columnas que la tabla no tiene es un error,
  salvo que se pida evolucionar el esquema (mergeSchema).

Subcomandos (dentro del contenedor de Spark):
    spark-submit /opt/spark-jobs/delta_lake.py cargar [--tandas 3]
    spark-submit /opt/spark-jobs/delta_lake.py historia
    spark-submit /opt/spark-jobs/delta_lake.py optimizar

`cargar` reproduce la llegada de los datos: copia bronze (Parquet) a una tabla
Delta día a día y, tras cada tanda de días, actualiza el agregado por crédito con
MERGE solo para los créditos con datos nuevos.
"""

from __future__ import annotations

import argparse
import time

from agregados import bureau_balance_por_credito, sesion
from delta.tables import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def rutas(lake: str) -> dict[str, str]:
    return {
        "bronze_parquet": f"{lake}/bronze/bureau_balance",
        "bronze_delta": f"{lake}/delta/bureau_balance",
        "agregado": f"{lake}/delta/bureau_balance_por_credito",
    }


def cargar_dia(spark: SparkSession, origen: str, destino: str, dia) -> None:
    """Copia un día de bronze a la tabla Delta, sustituyendo solo ese día.

    replaceWhere hace la escritura idempotente por día (como ingest-bronze --fecha):
    repetirla sustituye ese día, y el resto de la tabla no se toca.
    """
    (
        spark.read.parquet(origen)
        .where(F.col("fecha_entrega") == F.lit(dia))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"fecha_entrega = '{dia}'")
        .partitionBy("fecha_entrega")
        .save(destino)
    )


def actualizar_agregado(spark: SparkSession, bronze_delta: str, destino: str) -> str:
    """Agregado por crédito: completo la primera vez, con MERGE después."""
    balance = spark.read.format("delta").load(bronze_delta)
    if not DeltaTable.isDeltaTable(spark, destino):
        bureau_balance_por_credito(balance, con_marca=True).write.format("delta").save(destino)
        return "creado"

    # Marca de agua: la ingesta más reciente que ya contiene el agregado
    marca = spark.read.format("delta").load(destino).agg(F.max("_INGESTED_AT")).first()[0]
    afectados = balance.where(F.col("_ingested_at") > marca).select("SK_ID_BUREAU").distinct()
    # Los créditos afectados se recalculan con TODO su historial (como en dbt)
    nuevos = bureau_balance_por_credito(balance.join(afectados, "SK_ID_BUREAU"), con_marca=True)
    (
        DeltaTable.forPath(spark, destino)
        .alias("t")
        .merge(nuevos.alias("n"), "t.SK_ID_BUREAU = n.SK_ID_BUREAU")
        .whenMatchedUpdateAll()  # el crédito ya estaba: se actualiza
        .whenNotMatchedInsertAll()  # es nuevo: se inserta
        .execute()
    )
    return "MERGE"


def cargar(spark: SparkSession, lake: str, tandas: int) -> None:
    r = rutas(lake)
    dias = sorted(
        fila[0]
        for fila in spark.read.parquet(r["bronze_parquet"])
        .select("fecha_entrega")
        .distinct()
        .collect()
    )
    tamano = -(-len(dias) // tandas)
    for i in range(0, len(dias), tamano):
        inicio = time.perf_counter()
        tanda = dias[i : i + tamano]
        for dia in tanda:
            cargar_dia(spark, r["bronze_parquet"], r["bronze_delta"], dia)
        modo = actualizar_agregado(spark, r["bronze_delta"], r["agregado"])
        print(
            f"[tanda] {tanda[0]} .. {tanda[-1]}: {len(tanda)} días, agregado {modo} "
            f"({time.perf_counter() - inicio:.1f} s)"
        )


def historia(spark: SparkSession, lake: str) -> None:
    r = rutas(lake)
    tabla = DeltaTable.forPath(spark, r["agregado"])
    print("== Historial de commits de la tabla Delta del agregado")
    (
        tabla.history()
        .select(
            "version",
            "operation",
            F.col("operationMetrics.numOutputRows").alias("filas"),
            F.col("operationMetrics.numTargetRowsUpdated").alias("actualizadas"),
            F.col("operationMetrics.numTargetRowsInserted").alias("insertadas"),
        )
        .orderBy("version")
        .show(truncate=False)
    )
    print("== Time travel: la misma tabla en cada versión")
    for version in range(tabla.history().agg(F.max("version")).first()[0] + 1):
        filas = (
            spark.read.format("delta").option("versionAsOf", version).load(r["agregado"]).count()
        )
        print(f"  versión {version}: {filas:,} créditos")

    print("== Contrato de esquema: escribir una columna que la tabla no tiene")
    intruso = spark.createDataFrame(
        [(1, 1, 0.0, None, "x")],
        "SK_ID_BUREAU long, BB_MESES long, BB_PROP_IMPAGO double, "
        "_INGESTED_AT timestamp, COLUMNA_NUEVA string",
    )
    try:
        intruso.write.format("delta").mode("append").save(r["agregado"])
        print("  (se escribió: no debería)")
    except AnalysisException as error:
        print(f"  rechazado: {str(error).splitlines()[0][:150]}")


def optimizar(spark: SparkSession, lake: str) -> None:
    """Compacta ficheros pequeños (cada MERGE y cada día dejan ficheros nuevos)."""
    for nombre in ("bronze_delta", "agregado"):
        ruta = rutas(lake)[nombre]
        antes = spark.read.format("delta").load(ruta).inputFiles()
        metricas = DeltaTable.forPath(spark, ruta).optimize().executeCompaction().first()
        despues = spark.read.format("delta").load(ruta).inputFiles()
        print(
            f"[{nombre}] ficheros leídos: {len(antes)} -> {len(despues)} "
            f"(compactados {metricas.metrics.numFilesRemoved})"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("accion", choices=["cargar", "historia", "optimizar"])
    parser.add_argument("--lake", default="/opt/lake")
    parser.add_argument("--tandas", type=int, default=3)
    args = parser.parse_args()

    spark = sesion("delta-lake", delta=True)
    {
        "cargar": lambda: cargar(spark, args.lake, args.tandas),
        "historia": lambda: historia(spark, args.lake),
        "optimizar": lambda: optimizar(spark, args.lake),
    }[args.accion]()
    spark.stop()


if __name__ == "__main__":
    main()
