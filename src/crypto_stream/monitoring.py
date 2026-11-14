"""Monitoreo del job de Spark: un StreamingQueryListener que expone metricas a Prometheus.

Spark llama al listener cada vez que una query termina un micro-batch. Con eso
actualizamos metricas que Prometheus lee en http://<contenedor>:9108/metrics.

Por que un listener: Spark Structured Streaming guarda sus offsets de Kafka en el
checkpoint, NO en un consumer group. Herramientas como kafka-consumer-groups.sh no
ven su lag: hay que medirlo desde Spark (metrica maxOffsetsBehindLatest).

Este modulo corre dentro de la imagen de Spark (Python 3.10).
"""

import json
import logging
import time
from datetime import datetime, timezone

from prometheus_client import Counter, Gauge, start_http_server
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

logger = logging.getLogger(__name__)

METRICS_PORT = 9108
LAG_WARNING_THRESHOLD = 5000  # mensajes de atraso que justifican un WARNING en el log

_Q = ["query"]  # cada metrica lleva la etiqueta query = trades | trades_1m | quarantine
INPUT_RATE = Gauge("streaming_input_rows_per_second", "Filas/s que llegan desde Kafka", _Q)
PROCESSED_RATE = Gauge("streaming_processed_rows_per_second", "Filas/s que Spark procesa", _Q)
BATCH_ROWS = Gauge("streaming_batch_input_rows", "Filas leidas en el ultimo micro-batch", _Q)
BATCH_DURATION = Gauge("streaming_batch_duration_seconds", "Duracion del ultimo micro-batch", _Q)
OFFSETS_BEHIND = Gauge("streaming_offsets_behind_latest", "Mensajes de Kafka sin procesar", _Q)
WATERMARK_DELAY = Gauge("streaming_watermark_delay_seconds", "Ahora menos el watermark", _Q)
STATE_ROWS = Gauge("streaming_state_rows", "Filas guardadas en el estado de la query", _Q)
LAST_PROGRESS = Gauge("streaming_last_progress_timestamp_seconds", "Ultimo reporte de la query", _Q)
QUERY_ACTIVE = Gauge("streaming_query_active", "1 si la query esta corriendo, 0 si se detuvo", _Q)
BATCHES = Counter("streaming_batches", "Micro-batches completados", _Q)


class MetricsListener(StreamingQueryListener):
    def __init__(self):
        super().__init__()
        self._names = {}  # id de query -> nombre (algunos eventos solo traen el id)

    def onQueryStarted(self, event):
        self._names[str(event.id)] = event.name
        QUERY_ACTIVE.labels(event.name).set(1)
        LAST_PROGRESS.labels(event.name).set(time.time())
        logger.info("Query iniciada: %s", event.name)

    def onQueryProgress(self, event):
        p = event.progress
        name = p.name
        offsets_behind = max_offsets_behind(p.sources)
        watermark_delay = watermark_delay_seconds(p.eventTime.get("watermark"))
        state_rows = sum(op.numRowsTotal for op in p.stateOperators)

        INPUT_RATE.labels(name).set(p.inputRowsPerSecond or 0)
        PROCESSED_RATE.labels(name).set(p.processedRowsPerSecond or 0)
        BATCH_ROWS.labels(name).set(p.numInputRows)
        BATCH_DURATION.labels(name).set(p.batchDuration / 1000)
        OFFSETS_BEHIND.labels(name).set(offsets_behind)
        STATE_ROWS.labels(name).set(state_rows)
        LAST_PROGRESS.labels(name).set(time.time())
        BATCHES.labels(name).inc()
        if watermark_delay is not None:
            WATERMARK_DELAY.labels(name).set(watermark_delay)

        # Una linea JSON por micro-batch: facil de buscar y de parsear en los logs.
        logger.info(
            "progress %s",
            json.dumps(
                {
                    "query": name,
                    "batch": p.batchId,
                    "rows": p.numInputRows,
                    "duration_s": round(p.batchDuration / 1000, 2),
                    "offsets_behind": offsets_behind,
                    "watermark_delay_s": watermark_delay,
                    "state_rows": state_rows,
                }
            ),
        )
        if offsets_behind > LAG_WARNING_THRESHOLD:
            logger.warning("%s va %d mensajes atrasado respecto a Kafka", name, offsets_behind)

    def onQueryIdle(self, event):
        # Sin datos nuevos la query no reporta progreso, pero sigue viva.
        name = self._names.get(str(event.id))
        if name:
            LAST_PROGRESS.labels(name).set(time.time())

    def onQueryTerminated(self, event):
        name = self._names.get(str(event.id), str(event.id))
        QUERY_ACTIVE.labels(name).set(0)
        if event.exception:
            logger.error("Query %s termino con error: %s", name, event.exception)
        else:
            logger.info("Query %s detenida", name)


def max_offsets_behind(sources) -> int:
    """Mayor atraso (en mensajes) entre las fuentes Kafka de la query."""
    behind = [int(s.metrics.get("maxOffsetsBehindLatest", 0)) for s in sources if s.metrics]
    return max(behind, default=0)


def watermark_delay_seconds(watermark: str | None) -> float | None:
    """Segundos entre ahora y el watermark. None si la query no usa watermark aun."""
    if not watermark:
        return None
    # Spark lo entrega como "2026-11-13T21:12:30.000Z"; Python 3.10 no acepta la "Z".
    moment = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
    if moment.year == 1970:  # watermark inicial: todavia no hay datos
        return None
    now = datetime.now(timezone.utc)  # noqa: UP017 (corre en Python 3.10, sin datetime.UTC)
    return round((now - moment).total_seconds(), 1)


def start_monitoring(spark: SparkSession, port: int = METRICS_PORT) -> None:
    """Expone /metrics y registra el listener. Llamar ANTES de arrancar las queries."""
    start_http_server(port)
    spark.streams.addListener(MetricsListener())
    logger.info("Metricas de streaming en :%d/metrics", port)
