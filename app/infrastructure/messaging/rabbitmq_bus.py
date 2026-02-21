from __future__ import annotations

import json
import logging
from time import perf_counter
from typing import Any

try:
    import pika
except ModuleNotFoundError:
    pika = None

from app.core.config.settings import get_settings
from app.infrastructure.monitoring.prometheus import (
    rabbit_publish_duration_seconds,
    rabbit_publish_failures_total,
    rabbit_publish_total,
)

logger = logging.getLogger(__name__)


class RabbitMQBus:
    def __init__(self) -> None:
        self._settings = get_settings()
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    def _ensure_channel(self):
        if pika is None:
            raise RuntimeError('Dependencia pika nao instalada.')
        if self._channel is not None and self._channel.is_open:
            return self._channel

        if not self._settings.rabbitmq_url:
            raise RuntimeError('RABBITMQ_URL nao configurada.')

        params = pika.URLParameters(self._settings.rabbitmq_url)
        params.socket_timeout = max(1, self._settings.rabbit_publish_timeout_seconds)
        params.blocked_connection_timeout = max(1, self._settings.rabbit_publish_timeout_seconds)

        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()
        self._channel.exchange_declare(
            exchange=self._settings.rabbitmq_exchange,
            exchange_type='topic',
            durable=True,
        )
        return self._channel

    def publish_event(
        self,
        event: dict[str, Any],
        routing_key: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> bool:
        if not self._settings.enable_rabbit:
            return False

        safe_correlation_id = str(event.get('correlationId', 'sem-correlation-id'))[:100]
        started_at = perf_counter()
        try:
            channel = self._ensure_channel()
            body = json.dumps(event, ensure_ascii=False).encode('utf-8')
            properties = pika.BasicProperties(
                content_type='application/json',
                delivery_mode=2,
                headers=headers or {},
                correlation_id=safe_correlation_id,
            )
            channel.basic_publish(
                exchange=self._settings.rabbitmq_exchange,
                routing_key=routing_key or self._settings.rabbitmq_routing_key_analyze,
                body=body,
                properties=properties,
                mandatory=False,
            )
            duration = max(perf_counter() - started_at, 0.0)
            rabbit_publish_total.labels(result='success').inc()
            rabbit_publish_duration_seconds.labels(result='success').observe(duration)
            return True
        except Exception:
            duration = max(perf_counter() - started_at, 0.0)
            rabbit_publish_total.labels(result='failure').inc()
            rabbit_publish_duration_seconds.labels(result='failure').observe(duration)
            rabbit_publish_failures_total.inc()
            logger.error('Falha ao publicar evento no RabbitMQ. correlation_id=%s', safe_correlation_id)
            self.close()
            return False

    def close(self) -> None:
        if self._channel is not None and self._channel.is_open:
            try:
                self._channel.close()
            except Exception:
                pass
        if self._connection is not None and self._connection.is_open:
            try:
                self._connection.close()
            except Exception:
                pass
        self._channel = None
        self._connection = None
