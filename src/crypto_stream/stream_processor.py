"""Job de Spark Structured Streaming: Kafka -> data lake (Parquet en S3).

Lee el topic de trades UNA vez y escribe tres salidas, cada una como una query
de streaming independiente (con su propio checkpoint):

    Kafka "crypto.trades"
        |
        +--> parse_trades --+--> valid   --+--> trades/         1 fila por trade (sin duplicados)
                            |              +--> trades_1m/      agregados por minuto y par
                            +--> invalid ----> quarantine/      mensajes que no son un trade

Ejecutar (dentro del contenedor de Spark, ver docker-compose.yml):
    spark-submit --properties-file conf/spark-local.conf src/crypto_stream/stream_processor.py
"""

import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType

from crypto_stream.monitoring import start_monitoring

logger = logging.getLogger(__name__)

# Esquema del JSON que publica el producer. Declararlo (en vez de inferirlo) hace
# que un mensaje con otra forma no rompa el job: simplemente queda en null.
TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", LongType()),
        StructField("product_id", StringType()),
        StructField("price", StringType()),
        StructField("size", StringType()),
        StructField("side", StringType()),
        StructField("trade_time", StringType()),
        StructField("sequence", LongType()),
        StructField("ingested_at", StringType()),
    ]
)

# DECIMAL exacto (nunca float para dinero): hasta 12 digitos enteros y 8 decimales.
MONEY = DecimalType(20, 8)

# Cuanto esperamos trades atrasados antes de cerrar una ventana de 1 minuto.
WATERMARK_DELAY = "2 minutes"
WINDOW_DURATION = "1 minute"


def parse_trades(kafka_df: DataFrame) -> DataFrame:
    """Convierte los bytes de Kafka en columnas tipadas y marca si cada fila es valida."""
    parsed = kafka_df.select(
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"),
    )
    trades = parsed.select(
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.col("t.trade_id").alias("trade_id"),
        F.col("t.product_id").alias("product_id"),
        F.col("t.price").cast(MONEY).alias("price"),
        F.col("t.size").cast(MONEY).alias("size"),
        F.col("t.side").alias("side"),
        F.to_timestamp("t.trade_time").alias("trade_time"),
        F.col("t.sequence").alias("sequence"),
        F.to_timestamp("t.ingested_at").alias("ingested_at"),
    )
    return trades.withColumn("is_valid", _is_valid_trade())


def _is_valid_trade() -> Column:
    required = ["trade_id", "product_id", "price", "size", "side", "trade_time"]
    not_null = [F.col(c).isNotNull() for c in required]
    rules = [F.col("price") > 0, F.col("size") > 0, F.col("side").isin("buy", "sell")]
    condition = not_null[0]
    for rule in not_null[1:] + rules:
        condition = condition & rule
    # coalesce: si alguna regla da null (p. ej. price null), la fila es invalida.
    return F.coalesce(condition, F.lit(False))


def valid_trades(parsed: DataFrame) -> DataFrame:
    """Trades validos, sin duplicados, con columnas de particion para el lake."""
    return (
        parsed.where("is_valid")
        # El watermark limita cuanto tiempo Spark recuerda trade_ids para deduplicar.
        .withWatermark("trade_time", WATERMARK_DELAY)
        .dropDuplicatesWithinWatermark(["trade_id"])
        .withColumn("trade_date", F.to_date("trade_time"))
        .select(
            "trade_id",
            "product_id",
            "price",
            "size",
            "side",
            "trade_time",
            "sequence",
            "ingested_at",
            "kafka_partition",
            "kafka_offset",
            "trade_date",
        )
    )


def aggregate_per_minute(trades: DataFrame) -> DataFrame:
    """Metricas por par y ventana de 1 minuto (event time = trade_time).

    Con outputMode "append", cada ventana se escribe UNA sola vez: cuando el
    watermark la supera y ya no pueden llegar mas trades para ella.
    """
    return (
        trades.groupBy(F.window("trade_time", WINDOW_DURATION), "product_id")
        .agg(
            F.count("*").alias("trade_count"),
            F.sum("size").alias("volume"),
            F.sum(F.col("price") * F.col("size")).alias("notional"),
            F.min("price").alias("low_price"),
            F.max("price").alias("high_price"),
            F.min_by("price", "trade_time").alias("open_price"),
            F.max_by("price", "trade_time").alias("close_price"),
            F.sum(F.when(F.col("side") == "buy", F.col("size")).otherwise(0)).alias("buy_volume"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "product_id",
            "trade_count",
            "volume",
            # VWAP: precio promedio ponderado por volumen
            (F.col("notional") / F.col("volume")).cast(MONEY).alias("vwap"),
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "buy_volume",
            F.to_date(F.col("window.start")).alias("window_date"),
        )
    )


def quarantined(parsed: DataFrame) -> DataFrame:
    """Mensajes que no son un trade valido: se guardan crudos para revisarlos."""
    return parsed.where(~F.col("is_valid")).select(
        "raw_value",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.to_date("kafka_timestamp").alias("kafka_date"),
    )


def write_parquet(df: DataFrame, name: str, lake_uri: str, checkpoints: str, partitions: list):
    """Arranca una query que escribe Parquet particionado, en modo append."""
    return (
        df.writeStream.queryName(name)
        .format("parquet")
        .option("path", f"{lake_uri}/{name}")
        # El checkpoint guarda offsets de Kafka y estado: si el job se reinicia,
        # continua donde quedo sin perder ni duplicar archivos.
        .option("checkpointLocation", f"{checkpoints}/{name}")
        .partitionBy(*partitions)
        .outputMode("append")
        .trigger(processingTime=os.environ.get("TRIGGER_INTERVAL", "30 seconds"))
        .start()
    )


def main() -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    topic = os.environ["KAFKA_TOPIC"]
    lake_uri = os.environ.get("LAKE_URI", f"s3a://{os.environ['LAKE_BUCKET']}")
    checkpoints = os.environ.get("CHECKPOINT_DIR", "/opt/app/checkpoints")

    spark = SparkSession.builder.appName("crypto-stream-processor").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    start_monitoring(spark)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # solo aplica la primera vez (sin checkpoint)
        .option("maxOffsetsPerTrigger", 50_000)  # tope por micro-batch: protege la memoria
        .load()
    )
    parsed = parse_trades(kafka_df)
    trades = valid_trades(parsed)

    write_parquet(trades, "trades", lake_uri, checkpoints, ["trade_date", "product_id"])
    write_parquet(aggregate_per_minute(trades), "trades_1m", lake_uri, checkpoints, ["window_date"])
    write_parquet(quarantined(parsed), "quarantine", lake_uri, checkpoints, ["kafka_date"])

    logger.info("3 queries corriendo. Escribiendo en %s", lake_uri)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
