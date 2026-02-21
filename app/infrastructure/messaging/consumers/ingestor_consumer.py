from __future__ import annotations

import json
import logging
from datetime import datetime
from time import perf_counter
from typing import Any

try:
    import pika
except ModuleNotFoundError:
    pika = None

from sqlalchemy import inspect, text

from app.application.use_cases.persist_message_request import MessagePersistenceService
from app.core.config.settings import get_settings
from app.core.logging.setup import configure_logging
from app.domain.services.sentiment_service import to_rfc3339_z
from app.infrastructure.db.session import get_session_factory, init_db
from app.infrastructure.messaging.consumers.rabbit_consumer import RabbitConsumer
from app.infrastructure.monitoring.prometheus import (
    consumer_failures_total,
    consumer_messages_total,
    consumer_processing_duration_seconds,
    e2e_time_to_indexed_seconds,
    e2e_time_to_processed_seconds,
    ingest_failed_total,
    ingest_processed_total,
)
from app.infrastructure.search.elasticsearch_client import ElasticIndexWriter
from app.shared.utils.time import app_now, to_app_timezone

logger = logging.getLogger(__name__)

SUPPORTED_EVENTS = {'message_received', 'analyze_feed.completed'}


def _check_sql_ready() -> None:
    session_factory = get_session_factory()
    with session_factory() as session:
        session.execute(text('SELECT 1'))
        bind = session.get_bind()
        inspector = inspect(bind)
        tables = set(inspector.get_table_names())
        expected = {'messages', 'message_processing'}
        if not expected.issubset(tables):
            raise RuntimeError('Tabelas obrigatorias para processamento nao encontradas.')


def _parse_event(body: bytes) -> dict[str, Any]:
    event = json.loads(body.decode('utf-8'))
    if not isinstance(event, dict):
        raise ValueError('Evento invalido.')
    event_name = str(event.get('eventName', '')).strip()
    if event_name not in SUPPORTED_EVENTS:
        raise ValueError('Evento nao suportado.')
    payload = event.get('payload')
    if not isinstance(payload, dict):
        raise ValueError('Payload invalido.')
    return event


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        return app_now()
    normalized = value.strip().replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return app_now()
    if parsed.tzinfo is None:
        return to_app_timezone(parsed)
    return to_app_timezone(parsed)


def _read_retry_count(properties: pika.BasicProperties | None) -> int:
    if properties is None or properties.headers is None:
        return 0
    raw = properties.headers.get('retry_count', 0)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return 0


def _safe_correlation_id(value: Any) -> str:
    text_value = str(value or '').strip()
    return text_value[:64]


def _normalized_payload_from_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get('payload') if isinstance(event.get('payload'), dict) else {}
    event_name = str(event.get('eventName', '')).strip()

    if event_name == 'message_received':
        return {
            'sentiment_distribution': payload.get('sentiment_distribution') if isinstance(payload.get('sentiment_distribution'), dict) else {},
            'engagement_score': payload.get('engagement_score'),
            'trending_topics': payload.get('trending_topics') if isinstance(payload.get('trending_topics'), list) else [],
            'influence_ranking': payload.get('influence_ranking') if isinstance(payload.get('influence_ranking'), list) else [],
            'anomaly_detected': bool(payload.get('anomaly_detected', False)),
            'anomaly_type': payload.get('anomaly_type'),
            'flags': payload.get('flags') if isinstance(payload.get('flags'), dict) else {},
        }

    analysis = payload.get('analysis') if isinstance(payload.get('analysis'), dict) else {}
    return {
        'sentiment_distribution': analysis.get('sentiment_distribution') if isinstance(analysis.get('sentiment_distribution'), dict) else {},
        'engagement_score': analysis.get('engagement_score'),
        'trending_topics': analysis.get('trending_topics') if isinstance(analysis.get('trending_topics'), list) else [],
        'influence_ranking': analysis.get('influence_ranking') if isinstance(analysis.get('influence_ranking'), list) else [],
        'anomaly_detected': bool(analysis.get('anomaly_detected', False)),
        'anomaly_type': analysis.get('anomaly_type'),
        'flags': analysis.get('flags') if isinstance(analysis.get('flags'), dict) else (payload.get('flags') if isinstance(payload.get('flags'), dict) else {}),
    }


def _build_elastic_document(event: dict[str, Any], normalized_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    settings = get_settings()
    timestamp = _parse_timestamp(event.get('timestampUtc'))
    index_name = f"{settings.elasticsearch_index_prefix}-{timestamp.strftime('%Y.%m.%d')}"

    document = {
        'timestampUtc': to_rfc3339_z(timestamp),
        'eventName': event.get('eventName', 'message_received'),
        'correlationId': str(event.get('correlationId', 'sem-correlation-id'))[:64],
        'messageId': str(event.get('messageId', 'sem-message-id'))[:64],
        'analysis': normalized_payload,
        'flags': normalized_payload.get('flags', {}),
    }
    return index_name, document


def _observe_consumer_metrics(*, event_name: str, result: str, total_started: float) -> None:
    event_name_safe = event_name if event_name in SUPPORTED_EVENTS else 'unknown'
    consumer_messages_total.labels(event_name=event_name_safe, result=result).inc()
    consumer_processing_duration_seconds.labels(event_name=event_name_safe, result=result).observe(
        max(perf_counter() - total_started, 0.0)
    )


def _handle_message(
    writer: ElasticIndexWriter,
    channel: pika.adapters.blocking_connection.BlockingChannel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    total_started = perf_counter()
    settings = get_settings()
    retry_count = _read_retry_count(properties)
    fallback_correlation_id = _safe_correlation_id(getattr(properties, 'correlation_id', None))
    session_factory = get_session_factory()
    db_mark_processing_ms = 0.0
    db_persist_normalized_ms = 0.0
    elastic_ms = 0.0
    db_finalize_ms = 0.0
    db_failure_ms = 0.0
    parse_ms = 0.0
    queue_to_consumer_ms = 0.0
    event_name = 'unknown'

    parse_started = perf_counter()
    try:
        event = _parse_event(body)
        event_name = str(event.get('eventName', '')).strip() or 'unknown'
    except Exception as exc:
        parse_ms = (perf_counter() - parse_started) * 1000.0
        ingest_failed_total.inc()
        consumer_failures_total.labels(stage='parse').inc()
        _observe_consumer_metrics(event_name='unknown', result='failed', total_started=total_started)

        if fallback_correlation_id:
            db_failure_started = perf_counter()
            with session_factory() as session:
                service = MessagePersistenceService(session)
                service.mark_processing_failed(
                    correlation_id=fallback_correlation_id,
                    failure_stage='parse',
                    failed_reason=f'Falha ao interpretar evento da fila: {str(exc)[:900]}',
                )
            db_failure_ms = (perf_counter() - db_failure_started) * 1000.0
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.error(
            'Mensagem invalida descartada pelo worker. ms_parse=%.2f ms_db_falha=%.2f ms_total=%.2f',
            parse_ms,
            db_failure_ms,
            (perf_counter() - total_started) * 1000.0,
        )
        return
    parse_ms = (perf_counter() - parse_started) * 1000.0

    correlation_id = _safe_correlation_id(event.get('correlationId'))
    if not correlation_id:
        ingest_failed_total.inc()
        consumer_failures_total.labels(stage='validacao').inc()
        _observe_consumer_metrics(event_name=event_name, result='failed', total_started=total_started)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.error('Mensagem sem correlation_id descartada pelo worker.')
        return

    message_id = str(event.get('messageId', '')).strip()
    db_mark_processing_started = perf_counter()
    with session_factory() as session:
        service = MessagePersistenceService(session)
        persisted_message_id = service.mark_processing(correlation_id=correlation_id)
    db_mark_processing_ms = (perf_counter() - db_mark_processing_started) * 1000.0
    if not message_id:
        message_id = persisted_message_id or ''

    if not message_id:
        ingest_failed_total.inc()
        consumer_failures_total.labels(stage='db_lookup').inc()
        _observe_consumer_metrics(event_name=event_name, result='failed', total_started=total_started)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.error('Mensagem recebida sem registro previo no banco. correlation_id=%s', correlation_id)
        return

    event_timestamp = _parse_timestamp(event.get('timestampUtc'))
    queue_to_consumer_ms = max((app_now() - event_timestamp).total_seconds() * 1000.0, 0.0)
    normalized_payload = _normalized_payload_from_event(event)

    try:
        db_persist_started = perf_counter()
        with session_factory() as session:
            service = MessagePersistenceService(session)
            service.persist_normalized_outputs(message_id=message_id, payload=normalized_payload)
        db_persist_normalized_ms = (perf_counter() - db_persist_started) * 1000.0

        elastic_name: str | None = settings.elasticsearch_index_prefix
        elastic_index_name: str | None = None
        if settings.bypass_elastic_for_tests:
            logger.info('Modo de teste sem Elastic ativo no worker. correlation_id=%s', correlation_id)
        else:
            index_name, document = _build_elastic_document(event, normalized_payload)
            elastic_started = perf_counter()
            writer.write(
                index_name=index_name,
                document=document,
                alias_name=settings.elasticsearch_index_prefix,
            )
            elastic_ms = (perf_counter() - elastic_started) * 1000.0
            elastic_index_name = index_name
            e2e_time_to_indexed_seconds.labels(event_name=event_name).observe(
                max((app_now() - event_timestamp).total_seconds(), 0.0)
            )

        db_finalize_started = perf_counter()
        with session_factory() as session:
            service = MessagePersistenceService(session)
            service.mark_processed(
                correlation_id=correlation_id,
                elastic_name=elastic_name,
                elastic_index_name=elastic_index_name,
            )
        db_finalize_ms = (perf_counter() - db_finalize_started) * 1000.0
        e2e_time_to_processed_seconds.labels(event_name=event_name).observe(
            max((app_now() - event_timestamp).total_seconds(), 0.0)
        )

        logger.info(
            (
                'Processamento concluido no worker. correlation_id=%s '
                'ms_fila_ate_consumer=%.2f ms_parse=%.2f ms_db_marcar_processando=%.2f '
                'ms_db_normalizacao=%.2f ms_elastic=%.2f ms_db_finalizacao=%.2f ms_total=%.2f'
            ),
            correlation_id,
            queue_to_consumer_ms,
            parse_ms,
            db_mark_processing_ms,
            db_persist_normalized_ms,
            elastic_ms,
            db_finalize_ms,
            (perf_counter() - total_started) * 1000.0,
        )
        _observe_consumer_metrics(event_name=event_name, result='success', total_started=total_started)
    except Exception as exc:
        ingest_failed_total.inc()
        consumer_failures_total.labels(stage='consumer').inc()

        db_failure_started = perf_counter()
        with session_factory() as session:
            service = MessagePersistenceService(session)
            service.mark_processing_failed(
                correlation_id=correlation_id,
                failure_stage='consumer',
                failed_reason=f'Falha no processamento da mensagem: {str(exc)[:900]}',
            )
        db_failure_ms = (perf_counter() - db_failure_started) * 1000.0

        _observe_consumer_metrics(event_name=event_name, result='failed', total_started=total_started)

        if retry_count > settings.worker_retry_limit:
            logger.error(
                (
                    'Mensagem descartada apos retries no worker. correlation_id=%s '
                    'retry=%s ms_fila_ate_consumer=%.2f ms_parse=%.2f ms_db_marcar_processando=%.2f ms_db_normalizacao=%.2f '
                    'ms_elastic=%.2f ms_db_falha=%.2f ms_total=%.2f'
                ),
                correlation_id,
                retry_count,
                queue_to_consumer_ms,
                parse_ms,
                db_mark_processing_ms,
                db_persist_normalized_ms,
                elastic_ms,
                db_failure_ms,
                (perf_counter() - total_started) * 1000.0,
            )
        else:
            logger.error(
                (
                    'Falha no processamento do worker. correlation_id=%s retry=%s '
                    'ms_fila_ate_consumer=%.2f ms_parse=%.2f ms_db_marcar_processando=%.2f ms_db_normalizacao=%.2f '
                    'ms_elastic=%.2f ms_db_falha=%.2f ms_total=%.2f'
                ),
                correlation_id,
                retry_count,
                queue_to_consumer_ms,
                parse_ms,
                db_mark_processing_ms,
                db_persist_normalized_ms,
                elastic_ms,
                db_failure_ms,
                (perf_counter() - total_started) * 1000.0,
            )

    ingest_processed_total.inc()
    channel.basic_ack(delivery_tag=method.delivery_tag)


def run_worker() -> None:
    configure_logging()
    if pika is None:
        raise RuntimeError('Dependencia pika nao instalada.')
    settings = get_settings()
    init_db()
    _check_sql_ready()

    consumer = RabbitConsumer()
    writer = ElasticIndexWriter(settings.elasticsearch_url, timeout_seconds=settings.elastic_timeout_seconds)

    logger.info('Worker iniciado. fila=%s', settings.rabbitmq_queue_analyze)

    def _wrapped_handler(channel, method, properties, body):
        _handle_message(
            writer=writer,
            channel=channel,
            method=method,
            properties=properties,
            body=body,
        )

    consumer.consume_forever(_wrapped_handler)


def main() -> None:
    try:
        run_worker()
    except KeyboardInterrupt:
        logger.info('Worker finalizado manualmente.')
    except Exception:
        logger.error('Falha fatal no worker.')


if __name__ == '__main__':
    main()
