"""Tests del trabajo de Delta Lake (spark_jobs/delta_lake.py) con datos mínimos.

La sesión de Spark (con Delta) y los saltos están en conftest.py. Fuera de la imagen de
Spark (p. ej. en la CI), la sesión descarga los jars de Delta de Maven.
"""

from datetime import date, datetime

import pytest
from requisitos_spark import saltar_sin_spark

saltar_sin_spark()

from delta.tables import DeltaTable  # noqa: E402
from delta_lake import actualizar_agregado, cargar_dia  # noqa: E402
from pyspark.errors import AnalysisException  # noqa: E402

ESQUEMA = (
    "SK_ID_BUREAU long, MONTHS_BALANCE long, STATUS string, "
    "_ingested_at timestamp, fecha_entrega date"
)
D1, D2 = date(2026, 9, 1), date(2026, 9, 2)
T1, T2, T3 = (datetime(2026, 10, 11, h) for h in (10, 11, 12))


@pytest.fixture
def bronze(spark, tmp_path):
    """Bronze en Parquet particionado por día: el crédito 10 el día 1 y el 20 el día 2."""
    filas = [(10, 0, "0", T1, D1), (10, -1, "1", T1, D1), (20, 0, "C", T2, D2)]
    ruta = str(tmp_path / "bronze_parquet")
    spark.createDataFrame(filas, ESQUEMA).write.partitionBy("fecha_entrega").parquet(ruta)
    return ruta


def leer(spark, ruta, **opciones):
    return spark.read.format("delta").options(**opciones).load(ruta)


def test_cargar_un_dia_es_idempotente(spark, bronze, tmp_path):
    destino = str(tmp_path / "bronze_delta")
    cargar_dia(spark, bronze, destino, D1)
    cargar_dia(spark, bronze, destino, D2)
    cargar_dia(spark, bronze, destino, D1)  # repetir un día lo sustituye, no lo duplica

    assert leer(spark, destino).count() == 3
    assert DeltaTable.forPath(spark, destino).history().count() == 3  # tres commits


def test_merge_inserta_lo_nuevo_y_actualiza_lo_reingerido(spark, bronze, tmp_path):
    origen, agregado = str(tmp_path / "bronze_delta"), str(tmp_path / "agregado")
    cargar_dia(spark, bronze, origen, D1)
    assert actualizar_agregado(spark, origen, agregado) == "creado"
    cargar_dia(spark, bronze, origen, D2)
    assert actualizar_agregado(spark, origen, agregado) == "MERGE"  # inserta el crédito 20

    # Entrega tardía: el crédito 10 vuelve a llegar, con un mes más
    (
        spark.createDataFrame([(10, -2, "5", T3, D1)], ESQUEMA)
        .write.format("delta")
        .mode("append")
        .save(origen)
    )
    actualizar_agregado(spark, origen, agregado)

    filas = {r.SK_ID_BUREAU: r for r in leer(spark, agregado).collect()}
    assert sorted(filas) == [10, 20]  # actualizado, no duplicado
    assert (filas[10].BB_MESES, filas[10].BB_PROP_IMPAGO) == (3, pytest.approx(2 / 3))
    metricas = DeltaTable.forPath(spark, agregado).history(1).first().operationMetrics
    assert (metricas["numTargetRowsUpdated"], metricas["numTargetRowsInserted"]) == ("1", "0")


def test_time_travel_lee_versiones_anteriores(spark, bronze, tmp_path):
    origen, agregado = str(tmp_path / "bronze_delta"), str(tmp_path / "agregado")
    cargar_dia(spark, bronze, origen, D1)
    actualizar_agregado(spark, origen, agregado)
    cargar_dia(spark, bronze, origen, D2)
    actualizar_agregado(spark, origen, agregado)

    assert leer(spark, agregado, versionAsOf=0).count() == 1
    assert leer(spark, agregado).count() == 2


def test_el_contrato_de_esquema_rechaza_columnas_nuevas(spark, bronze, tmp_path):
    destino = str(tmp_path / "bronze_delta")
    cargar_dia(spark, bronze, destino, D1)
    intruso = spark.createDataFrame(
        [(1, D1, "x")], "SK_ID_BUREAU long, fecha_entrega date, NUEVA string"
    )

    with pytest.raises(AnalysisException, match="DELTA_METADATA_MISMATCH"):
        intruso.write.format("delta").mode("append").save(destino)
