# Catalogo (Glue) + motor de consultas (Athena) sobre los Parquet del lake.
#
# Las tablas usan "partition projection": Athena calcula las particiones a partir
# de reglas (fechas, lista de pares) en vez de tener que registrarlas una por una.
# Asi los datos nuevos que escribe Spark son consultables al instante, sin crawlers
# ni "MSCK REPAIR TABLE".

resource "aws_glue_catalog_database" "lake" {
  name        = replace("${var.project}_lake", "-", "_")
  description = "Trades de cripto en tiempo real (Coinbase -> Kafka -> Spark)"
}

locals {
  lake_uri   = "s3://${aws_s3_bucket.lake.bucket}"
  date_range = "2026-01-01,NOW"
  parquet_serde = {
    input_format          = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format         = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
  }
}

resource "aws_glue_catalog_table" "trades" {
  name          = "trades"
  database_name = aws_glue_catalog_database.lake.name
  description   = "Un registro por trade, deduplicado por trade_id"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                 = "parquet"
    "projection.enabled"           = "true"
    "projection.trade_date.type"   = "date"
    "projection.trade_date.format" = "yyyy-MM-dd"
    "projection.trade_date.range"  = local.date_range
    "projection.product_id.type"   = "enum"
    "projection.product_id.values" = join(",", var.product_ids)
    "storage.location.template"    = "${local.lake_uri}/trades/trade_date=$${trade_date}/product_id=$${product_id}"
  }

  partition_keys {
    name = "trade_date"
    type = "date"
  }
  partition_keys {
    name = "product_id"
    type = "string"
  }

  storage_descriptor {
    location      = "${local.lake_uri}/trades/"
    input_format  = local.parquet_serde.input_format
    output_format = local.parquet_serde.output_format
    ser_de_info {
      serialization_library = local.parquet_serde.serialization_library
    }

    columns {
      name = "trade_id"
      type = "bigint"
    }
    columns {
      name = "price"
      type = "decimal(20,8)"
    }
    columns {
      name = "size"
      type = "decimal(20,8)"
    }
    columns {
      name = "side"
      type = "string"
    }
    columns {
      name = "trade_time"
      type = "timestamp"
    }
    columns {
      name = "sequence"
      type = "bigint"
    }
    columns {
      name = "ingested_at"
      type = "timestamp"
    }
    columns {
      name = "kafka_partition"
      type = "int"
    }
    columns {
      name = "kafka_offset"
      type = "bigint"
    }
  }
}

resource "aws_glue_catalog_table" "trades_1m" {
  name          = "trades_1m"
  database_name = aws_glue_catalog_database.lake.name
  description   = "OHLC, volumen y VWAP por par y minuto"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                  = "parquet"
    "projection.enabled"            = "true"
    "projection.window_date.type"   = "date"
    "projection.window_date.format" = "yyyy-MM-dd"
    "projection.window_date.range"  = local.date_range
    "storage.location.template"     = "${local.lake_uri}/trades_1m/window_date=$${window_date}"
  }

  partition_keys {
    name = "window_date"
    type = "date"
  }

  storage_descriptor {
    location      = "${local.lake_uri}/trades_1m/"
    input_format  = local.parquet_serde.input_format
    output_format = local.parquet_serde.output_format
    ser_de_info {
      serialization_library = local.parquet_serde.serialization_library
    }

    columns {
      name = "window_start"
      type = "timestamp"
    }
    columns {
      name = "window_end"
      type = "timestamp"
    }
    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "trade_count"
      type = "bigint"
    }
    columns {
      name = "volume"
      type = "decimal(30,8)"
    }
    columns {
      name = "vwap"
      type = "decimal(20,8)"
    }
    columns {
      name = "open_price"
      type = "decimal(20,8)"
    }
    columns {
      name = "high_price"
      type = "decimal(20,8)"
    }
    columns {
      name = "low_price"
      type = "decimal(20,8)"
    }
    columns {
      name = "close_price"
      type = "decimal(20,8)"
    }
    columns {
      name = "buy_volume"
      type = "decimal(30,8)"
    }
  }
}

resource "aws_glue_catalog_table" "quarantine" {
  name          = "quarantine"
  database_name = aws_glue_catalog_database.lake.name
  description   = "Mensajes de Kafka que no son un trade valido"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification                 = "parquet"
    "projection.enabled"           = "true"
    "projection.kafka_date.type"   = "date"
    "projection.kafka_date.format" = "yyyy-MM-dd"
    "projection.kafka_date.range"  = local.date_range
    "storage.location.template"    = "${local.lake_uri}/quarantine/kafka_date=$${kafka_date}"
  }

  partition_keys {
    name = "kafka_date"
    type = "date"
  }

  storage_descriptor {
    location      = "${local.lake_uri}/quarantine/"
    input_format  = local.parquet_serde.input_format
    output_format = local.parquet_serde.output_format
    ser_de_info {
      serialization_library = local.parquet_serde.serialization_library
    }

    columns {
      name = "raw_value"
      type = "string"
    }
    columns {
      name = "kafka_partition"
      type = "int"
    }
    columns {
      name = "kafka_offset"
      type = "bigint"
    }
    columns {
      name = "kafka_timestamp"
      type = "timestamp"
    }
  }
}

# Workgroup con limite de escaneo por consulta: una consulta mal escrita no puede
# costar mas de lo previsto. Los resultados van al mismo bucket (expiran en 7 dias).
resource "aws_athena_workgroup" "lake" {
  name          = var.project
  description   = "Consultas sobre el data lake de trades"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    bytes_scanned_cutoff_per_query     = var.athena_scan_limit_bytes
    publish_cloudwatch_metrics_enabled = true

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "${local.lake_uri}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
