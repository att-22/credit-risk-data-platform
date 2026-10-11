# Databricks notebook source
# MAGIC %md
# MAGIC # Credit risk · Spark y Delta Lake en Databricks
# MAGIC
# MAGIC Notebook de la Fase 4.3. Ejecuta en **Databricks** (Free Edition) lo mismo que
# MAGIC `agregados.py` y `delta_lake.py` hacen en local:
# MAGIC
# MAGIC 1. Lee los CSV de un *volume* con los tipos del **contrato de esquema** del proyecto.
# MAGIC 2. Los guarda como tablas **Delta** gestionadas por **Unity Catalog** (bronze).
# MAGIC 3. Calcula las agregaciones con las **mismas funciones** que en local (`agregados.py`).
# MAGIC 4. Construye el agregado por crédito de forma **incremental con `MERGE`**, simula una
# MAGIC    entrega tardía y muestra el **historial**, el ***time travel*** y `OPTIMIZE`.
# MAGIC 5. **Se comprueba a sí mismo**: el agregado incremental debe ser idéntico al calculado
# MAGIC    de una vez.
# MAGIC
# MAGIC El mismo fichero funciona en local (contenedor de Spark), que es como se prueba en el
# MAGIC repositorio. Es un `.py` en formato de notebook de Databricks: cada
# MAGIC `# COMMAND ----------` separa una celda.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración
# MAGIC En Databricks, los dos *widgets* de arriba del notebook indican dónde están los CSV
# MAGIC (el *volume* donde los subiste) y en qué `catálogo.esquema` crear las tablas.

# COMMAND ----------

import json
import os
import sys
from pathlib import Path

from pyspark.sql import functions as F

EN_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

if EN_DATABRICKS:
    # En Databricks la sesión `spark`, `dbutils` y `display` ya existen.
    dbutils.widgets.text("datos", "/Volumes/workspace/credit_risk/muestra", "Carpeta de los CSV")  # noqa: F821
    dbutils.widgets.text("esquema", "workspace.credit_risk", "catálogo.esquema")  # noqa: F821
    DATOS = dbutils.widgets.get("datos")  # noqa: F821
    ESQUEMA = dbutils.widgets.get("esquema")  # noqa: F821
    REPO = Path.cwd().parent  # el notebook está en spark_jobs/ dentro de la Git folder
    sys.path.insert(0, str(Path.cwd()))
    mostrar = display  # noqa: F821
else:
    # En local: sesión de Spark con Delta y un catálogo local (ver agregados.sesion)
    REPO = Path(os.getenv("REPO_DIR", Path(__file__).resolve().parents[1]))
    DATOS = os.getenv("DATOS", str(REPO / "tests" / "fixtures" / "muestra" / "raw"))
    ESQUEMA = os.getenv("ESQUEMA", "credit_risk_demo")
    sys.path.insert(0, str(REPO / "spark_jobs"))
    from agregados import sesion

    spark = sesion("databricks-demo", particiones=4, delta=True)

    def mostrar(df):
        df.show(truncate=False)


from agregados import agregar_por_cliente, bureau_balance_por_credito, con_comportamiento  # noqa: E402
from delta.tables import DeltaTable  # noqa: E402

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ESQUEMA}")
print(f"Datos: {DATOS}\nTablas en: {ESQUEMA}\nEn Databricks: {EN_DATABRICKS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze: CSV → tablas Delta con el contrato de esquema
# MAGIC Los tipos no se deducen del fichero: salen de `src/credit_risk/esquemas_bronze.json`,
# MAGIC el mismo contrato que usa la ingesta en local.

# COMMAND ----------

CONTRATO = json.loads((REPO / "src" / "credit_risk" / "esquemas_bronze.json").read_text())
TIPOS_SPARK = {"BIGINT": "long", "DOUBLE": "double", "VARCHAR": "string", "BOOLEAN": "boolean"}
FICHEROS = {
    "bureau_balance": "bureau_balance.csv",
    "installments_payments": "installments_payments.csv",
}


def leer_csv(tabla: str):
    esquema = ", ".join(f"`{c}` {TIPOS_SPARK[t]}" for c, t in CONTRATO[tabla].items())
    return spark.read.option("header", True).schema(esquema).csv(f"{DATOS}/{FICHEROS[tabla]}")


for tabla in FICHEROS:
    leer_csv(tabla).write.format("delta").mode("overwrite").saveAsTable(f"{ESQUEMA}.bronze_{tabla}")
    print(f"{ESQUEMA}.bronze_{tabla}: {spark.table(f'{ESQUEMA}.bronze_{tabla}').count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Agregados con las funciones de `agregados.py`

# COMMAND ----------

pagos = spark.table(f"{ESQUEMA}.bronze_installments_payments")
cuotas = agregar_por_cliente(con_comportamiento(pagos), "SK_ID_CURR", "INST")
cuotas.write.format("delta").mode("overwrite").saveAsTable(f"{ESQUEMA}.installments_por_cliente")
print(f"installments_por_cliente: {cuotas.count():,} clientes, {len(cuotas.columns)} columnas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Agregado por crédito incremental con `MERGE`
# MAGIC - **Versión 0**: se crea con la mitad de los créditos (los pares).
# MAGIC - **Versión 1**: `MERGE` con todos → inserta los que faltaban.
# MAGIC - **Versión 2**: entrega tardía (un mes nuevo de 3 créditos) → `MERGE`
# MAGIC   **actualiza** solo esos.

# COMMAND ----------

AGREGADO = f"{ESQUEMA}.bureau_balance_por_credito"
BRONZE_BB = f"{ESQUEMA}.bronze_bureau_balance"
spark.sql(f"DROP TABLE IF EXISTS {AGREGADO}")


def merge(nuevos) -> None:
    (
        DeltaTable.forName(spark, AGREGADO)
        .alias("t")
        .merge(nuevos.alias("n"), "t.SK_ID_BUREAU = n.SK_ID_BUREAU")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


balance = spark.table(BRONZE_BB)
pares = balance.where(F.col("SK_ID_BUREAU") % 2 == 0)
bureau_balance_por_credito(pares).write.format("delta").saveAsTable(AGREGADO)  # versión 0
merge(bureau_balance_por_credito(balance))  # versión 1

# Entrega tardía: un mes más (con impago) para 3 créditos, y MERGE solo de esos créditos
tardios = [
    r.SK_ID_BUREAU
    for r in balance.select("SK_ID_BUREAU").distinct().orderBy("SK_ID_BUREAU").limit(3).collect()
]
mes_nuevo = spark.createDataFrame(
    [(c, 1, "5") for c in tardios], "SK_ID_BUREAU long, MONTHS_BALANCE long, STATUS string"
)
mes_nuevo.write.format("delta").mode("append").saveAsTable(BRONZE_BB)
afectados = spark.table(BRONZE_BB).where(F.col("SK_ID_BUREAU").isin(tardios))
merge(bureau_balance_por_credito(afectados))  # versión 2
print(f"Créditos con entrega tardía: {tardios}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Historial, *time travel* y `OPTIMIZE` (SQL nativo de Delta)

# COMMAND ----------

mostrar(
    spark.sql(f"DESCRIBE HISTORY {AGREGADO}")
    .select(
        "version",
        "operation",
        F.col("operationMetrics.numTargetRowsInserted").alias("insertadas"),
        F.col("operationMetrics.numTargetRowsUpdated").alias("actualizadas"),
    )
    .orderBy("version")
)
# Las versiones salen del historial: un MERGE sin cambios no crea versión nueva
ultima = spark.sql(f"DESCRIBE HISTORY {AGREGADO}").agg(F.max("version")).first()[0]
for version in range(ultima + 1):
    filas = spark.sql(f"SELECT count(*) FROM {AGREGADO} VERSION AS OF {version}").first()[0]
    print(f"versión {version}: {filas:,} créditos")
mostrar(
    spark.sql(f"OPTIMIZE {AGREGADO}").select("metrics.numFilesAdded", "metrics.numFilesRemoved")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Comprobación: el agregado incremental es idéntico al calculado de una vez

# COMMAND ----------

incremental = spark.table(AGREGADO)
de_una_vez = bureau_balance_por_credito(spark.table(BRONZE_BB))
diferencias = incremental.exceptAll(de_una_vez).count() + de_una_vez.exceptAll(incremental).count()
creditos = spark.table(BRONZE_BB).select("SK_ID_BUREAU").distinct().count()
assert incremental.count() == creditos, "faltan o sobran créditos"
assert diferencias == 0, f"{diferencias} filas distintas entre el MERGE y el cálculo completo"
actualizados = incremental.where(F.col("SK_ID_BUREAU").isin(tardios)).orderBy("SK_ID_BUREAU")
mostrar(actualizados)
print(f"OK: {creditos:,} créditos; el agregado incremental coincide con el cálculo completo.")
