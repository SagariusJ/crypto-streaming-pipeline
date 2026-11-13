"""Producer: lee trades en vivo del WebSocket de Coinbase y los publica en Kafka.

    Coinbase (canal "matches") --> validar --> Kafka "crypto.trades"     (key = product_id)
                                          +--> Kafka "crypto.trades.dlq" (si no es valido)

Ejecutar (con el stack de Docker arriba):
    uv run --env-file .env python -m crypto_stream.producer
"""

import asyncio
import json
import logging
import os
import signal
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import websockets
from confluent_kafka import KafkaError, Message, Producer

logger = logging.getLogger(__name__)

COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"
STATS_EVERY_SECONDS = 30

# Campos que debe traer un trade de Coinbase para que lo aceptemos.
REQUIRED_FIELDS = ("trade_id", "product_id", "price", "size", "side", "time", "sequence")


class InvalidTradeError(ValueError):
    """El mensaje no es un trade valido."""


def build_subscribe_message(product_ids: list[str]) -> str:
    """Mensaje de suscripcion al canal 'matches' (un mensaje por cada trade ejecutado)."""
    return json.dumps({"type": "subscribe", "product_ids": product_ids, "channels": ["matches"]})


def parse_trade(message: dict) -> dict:
    """Valida un mensaje 'match' de Coinbase y lo convierte en nuestro evento.

    Precio y cantidad se mantienen como TEXTO: convertirlos a float perderia
    precision (0.1 + 0.2 != 0.3). Spark los convierte a DECIMAL despues.
    """
    missing = [field for field in REQUIRED_FIELDS if message.get(field) in (None, "")]
    if missing:
        raise InvalidTradeError(f"faltan campos: {missing}")

    for field in ("price", "size"):
        try:
            value = Decimal(message[field])
        except InvalidOperation as exc:
            raise InvalidTradeError(f"{field} no es numerico: {message[field]!r}") from exc
        if value <= 0:
            raise InvalidTradeError(f"{field} debe ser positivo: {message[field]}")

    if message["side"] not in ("buy", "sell"):
        raise InvalidTradeError(f"side invalido: {message['side']!r}")

    return {
        "trade_id": int(message["trade_id"]),
        "product_id": message["product_id"],
        "price": message["price"],
        "size": message["size"],
        "side": message["side"],
        "trade_time": message["time"],  # momento del trade en Coinbase (event time)
        "sequence": int(message["sequence"]),
        "ingested_at": datetime.now(UTC).isoformat(),  # momento en que lo recibimos
    }


class TradeProducer:
    """Publica en Kafka y lleva contadores para el monitoreo."""

    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.dlq_topic = f"{topic}.dlq"
        self.stats = {"received": 0, "sent": 0, "invalid": 0, "delivery_errors": 0}
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                # acks=all + idempotencia: Kafka confirma solo cuando el mensaje esta
                # guardado, y los reintentos internos no generan duplicados.
                "acks": "all",
                "enable.idempotence": True,
                # Agrupar mensajes hasta 50 ms y comprimirlos: menos viajes de red.
                "linger.ms": 50,
                "compression.type": "lz4",
            }
        )

    def send_trade(self, trade: dict) -> None:
        # key = product_id: todos los trades de un par van a la misma particion,
        # asi Kafka garantiza su orden.
        self._produce(self.topic, key=trade["product_id"], value=trade)

    def send_invalid(self, raw: dict, reason: str) -> None:
        self.stats["invalid"] += 1
        logger.warning("Trade invalido enviado a %s: %s", self.dlq_topic, reason)
        self._produce(self.dlq_topic, key=None, value={"reason": reason, "raw": raw})

    def _produce(self, topic: str, key: str | None, value: dict) -> None:
        payload = json.dumps(value).encode()
        while True:
            try:
                self._producer.produce(topic, key=key, value=payload, on_delivery=self._on_delivery)
                break
            except BufferError:
                # Cola local llena (Kafka lento o caido): esperar a que se vacie un poco.
                logger.warning("Cola del producer llena, esperando...")
                self._producer.poll(1)
        # poll(0) atiende los callbacks de entrega pendientes sin bloquear.
        self._producer.poll(0)

    def _on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.stats["delivery_errors"] += 1
            logger.error("Fallo la entrega a %s: %s", msg.topic(), err)
        elif msg.topic() == self.topic:
            self.stats["sent"] += 1

    def flush(self, timeout: float = 10) -> int:
        """Espera a que se entreguen los mensajes pendientes. Devuelve cuantos quedaron."""
        return self._producer.flush(timeout)


async def stream_trades(producer: TradeProducer, product_ids: list[str]) -> None:
    """Escucha Coinbase para siempre, reconectando automaticamente si se corta."""
    # "async for ... in connect()" reconecta solo con backoff exponencial ante errores.
    async for websocket in websockets.connect(COINBASE_WS_URL, open_timeout=15, ping_interval=20):
        try:
            await websocket.send(build_subscribe_message(product_ids))
            logger.info("Conectado a Coinbase, suscrito a %s", product_ids)
            async for raw_message in websocket:
                message = json.loads(raw_message)
                handle_message(producer, message)
        except websockets.ConnectionClosed as exc:
            logger.warning("Conexion con Coinbase cerrada (%s). Reconectando...", exc)
            continue


def handle_message(producer: TradeProducer, message: dict) -> None:
    msg_type = message.get("type")
    if msg_type == "match":
        producer.stats["received"] += 1
        try:
            trade = parse_trade(message)
        except InvalidTradeError as exc:
            producer.send_invalid(message, str(exc))
            return
        producer.send_trade(trade)
    elif msg_type == "error":
        logger.error("Coinbase respondio con error: %s", message)
    # "subscriptions" y "last_match" (el ultimo trade previo a conectar) se ignoran:
    # last_match se repetiria en cada reconexion y generaria duplicados.


async def log_stats(producer: TradeProducer) -> None:
    started = time.monotonic()
    while True:
        await asyncio.sleep(STATS_EVERY_SECONDS)
        minutes = (time.monotonic() - started) / 60
        logger.info("Stats: %s | %.0f trades/min", producer.stats, producer.stats["sent"] / minutes)


async def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    topic = os.environ.get("KAFKA_TOPIC", "crypto.trades")
    product_ids = os.environ.get("PRODUCT_IDS", "BTC-USD,ETH-USD,SOL-USD").split(",")

    producer = TradeProducer(bootstrap, topic)
    logger.info("Publicando en %s (topic %s)", bootstrap, topic)

    # Ctrl+C o "docker stop" (SIGTERM): terminar ordenadamente.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows no lo soporta: ahi Ctrl+C lanza KeyboardInterrupt
            pass

    tasks = [
        asyncio.create_task(stream_trades(producer, product_ids)),
        asyncio.create_task(log_stats(producer)),
    ]
    try:
        await stop.wait()
    finally:
        for task in tasks:
            task.cancel()
        # Esperar a que las tareas terminen de cancelarse (cierra bien el WebSocket).
        await asyncio.gather(*tasks, return_exceptions=True)
        pending = producer.flush()
        logger.info("Producer detenido. Stats: %s | sin entregar: %d", producer.stats, pending)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
