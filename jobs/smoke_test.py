"""Prueba de humo del stack: Spark lee de Kafka y escribe/lee Parquet en S3.

No procesa datos reales: solo confirma que los conectores, la red de Docker y
las credenciales estan bien antes de construir el pipeline.

    docker compose --profile check run --rm spark-smoke-test
"""

import os
import sys

from pyspark.sql import SparkSession

bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
topic = os.environ["KAFKA_TOPIC"]
path = f"s3a://{os.environ['LAKE_BUCKET']}/_smoke_test"

spark = SparkSession.builder.appName("smoke-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Kafka: lectura batch del topic (puede estar vacio; lo importante es conectar)
kafka_rows = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic)
    .load()
    .count()
)
print(f"[OK] Kafka: topic '{topic}' accesible ({kafka_rows} mensajes)")

# 2) S3: escribir Parquet y leerlo de vuelta
spark.range(100).withColumnRenamed("id", "n").write.mode("overwrite").parquet(path)
read_back = spark.read.parquet(path).count()
print(f"[OK] S3: escritos y leidos {read_back} registros en {path}")

spark.stop()
sys.exit(0 if read_back == 100 else 1)
