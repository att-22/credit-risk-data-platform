# ==========================================================================
# Imagen de Spark para los trabajos de spark_jobs/
# ==========================================================================
# La imagen oficial trae Spark 4.2 con Java 17 y Python: así no hace falta
# instalar Java en Windows. Los trabajos se montan como volumen (compose.yaml).

FROM spark:4.2.0-scala2.13-java17-python3-ubuntu

ARG DELTA_VERSION=4.4.1

USER root
# pytest para los tests; delta-spark (--no-deps) solo aporta la API de Python
# (DeltaTable): PySpark ya viene con la imagen y no se instala dos veces.
RUN pip install --no-cache-dir pytest==9.1.1 && \
    pip install --no-cache-dir --no-deps delta-spark==${DELTA_VERSION}

# Delta Lake para Spark: los jars van DENTRO de la imagen (en el classpath de
# Spark), así los trabajos no descargan nada de internet al ejecutarse.
RUN cd /opt/spark/jars && \
    for jar in delta-spark_2.13 delta-storage; do \
      curl -fsSLO "https://repo1.maven.org/maven2/io/delta/${jar}/${DELTA_VERSION}/${jar}-${DELTA_VERSION}.jar"; \
    done

USER spark
WORKDIR /opt/spark/work-dir

# spark-submit añade PySpark al path de Python por su cuenta; `python3 -m pytest`
# no. Con esto, cualquier python3 del contenedor puede importar pyspark.
# DELTA_JARS_INCLUIDOS: la sesión de Spark no debe descargar Delta (ya está aquí).
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
    PATH="/opt/spark/bin:${PATH}" \
    DELTA_JARS_INCLUIDOS=1
