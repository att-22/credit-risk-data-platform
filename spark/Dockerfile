# ==========================================================================
# Imagen de Spark para los trabajos de spark_jobs/
# ==========================================================================
# La imagen oficial trae Spark 4.2 con Java 17 y Python: así no hace falta
# instalar Java en Windows. Los trabajos se montan como volumen (compose.yaml).

FROM spark:4.2.0-scala2.13-java17-python3-ubuntu

USER root
# pytest para ejecutar los tests de los trabajos dentro del contenedor
RUN pip install --no-cache-dir pytest==9.1.1
USER spark
WORKDIR /opt/spark/work-dir

# spark-submit añade PySpark al path de Python por su cuenta; `python3 -m pytest`
# no. Con esto, cualquier python3 del contenedor puede importar pyspark.
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip     PATH="/opt/spark/bin:${PATH}"
