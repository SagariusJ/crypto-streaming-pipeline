# Imagen de Spark con los conectores de Kafka y S3 ya incluidos.
# Se descargan una sola vez al construir la imagen (no en cada spark-submit),
# asi los jobs arrancan rapido y funcionan sin internet.
FROM apache/spark:4.2.1-scala2.13-java21-python3-ubuntu

USER root

# Versiones alineadas con Spark 4.2.1 (spark-parent pom: hadoop 3.5.0).
ARG SPARK_KAFKA=org.apache.spark#spark-sql-kafka-0-10_2.13;4.2.1
ARG HADOOP_AWS=org.apache.hadoop#hadoop-aws;3.5.0

# Ivy viene incluido en Spark: resolvemos cada conector con sus dependencias
# transitivas y copiamos los jars a /opt/spark/jars. (hadoop-aws trae el AWS SDK
# v2 completo, ~700 MB: el primer build tarda unos minutos.)
RUN set -eux; \
    for dep in "$SPARK_KAFKA" "$HADOOP_AWS"; do \
      org="${dep%%#*}"; rest="${dep#*#}"; name="${rest%%;*}"; rev="${rest#*;}"; \
      java -cp "/opt/spark/jars/*" org.apache.ivy.Main \
        -dependency "$org" "$name" "$rev" -confs default \
        -retrieve "/tmp/jars/[artifact]-[revision](-[classifier]).[ext]" \
        -cache /tmp/ivy-cache > /dev/null; \
    done; \
    # No pisar los jars que Spark ya trae (mismas librerias, otra version).
    for jar in /tmp/jars/*.jar; do \
      base="$(basename "$jar" | sed -E 's/-[0-9][0-9.]*(-[a-z0-9.]+)?\.jar$//')"; \
      ls /opt/spark/jars/"$base"-[0-9]*.jar > /dev/null 2>&1 || cp "$jar" /opt/spark/jars/; \
    done; \
    rm -rf /tmp/jars /tmp/ivy-cache

USER spark
WORKDIR /opt/app
