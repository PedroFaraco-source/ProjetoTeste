from __future__ import annotations

import logging
import time
from collections.abc import Callable

try:
    import pika
except ModuleNotFoundError:
    pika = None

from app.core.config.settings import get_settings

logger = logging.getLogger(__name__)

MessageHandler = Callable[[object, object, object, bytes], None]


class RabbitConsumer:
    def __init__(self) -> None:
        self._settings = get_settings()

    def consume_forever(self, handler: MessageHandler) -> None:
        backoff_seconds = 1
        while True:
            try:
                self._consume_once(handler)
                backoff_seconds = 1
            except KeyboardInterrupt:
                raise
            except Exception:
                logger.error('Falha no consumidor RabbitMQ. Nova tentativa em %ss.', backoff_seconds)
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)

    def _consume_once(self, handler: MessageHandler) -> None:
        if pika is None:
            raise RuntimeError('Dependencia pika nao instalada.')
        if not self._settings.rabbitmq_url:
            raise RuntimeError('RABBITMQ_URL nao configurada.')

        params = pika.URLParameters(self._settings.rabbitmq_url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()

        channel.exchange_declare(
            exchange=self._settings.rabbitmq_exchange,
            exchange_type='topic',
            durable=True,
        )
        channel.queue_declare(queue=self._settings.rabbitmq_queue_analyze, durable=True)
        channel.queue_bind(
            queue=self._settings.rabbitmq_queue_analyze,
            exchange=self._settings.rabbitmq_exchange,
            routing_key=self._settings.rabbitmq_routing_key_analyze,
        )
        channel.basic_qos(prefetch_count=1)

        def _on_message(ch, method, properties, body):
            handler(ch, method, properties, body)

        channel.basic_consume(
            queue=self._settings.rabbitmq_queue_analyze,
            on_message_callback=_on_message,
            auto_ack=False,
        )

        try:
            channel.start_consuming()
        finally:
            if connection.is_open:
                connection.close()

    def publish_retry(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        body: bytes,
        correlation_id: str,
        retry_count: int,
    ) -> None:
        properties = pika.BasicProperties(
            content_type='application/json',
            delivery_mode=2,
            correlation_id=correlation_id,
            headers={'retry_count': retry_count},
        )
        channel.basic_publish(
            exchange=self._settings.rabbitmq_exchange,
            routing_key=self._settings.rabbitmq_routing_key_analyze,
            body=body,
            properties=properties,
        )
