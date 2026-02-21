from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any

from app.application.use_cases.persist_message_request import MessagePersistenceService
from app.core.config.settings import get_settings
from app.core.logging.setup import configure_logging
from app.domain.services.sentiment_service import to_rfc3339_z
from app.infrastructure.db.repositories.message_repository import MessageRepository
from app.infrastructure.db.session import get_session_factory, init_db
from app.infrastructure.messaging.rabbitmq_bus import RabbitMQBus
from app.infrastructure.monitoring.prometheus import (
    elastic_audit_log_failures_total,
    elastic_bulk_documents_total,
    elastic_bulk_duration_seconds,
    elastic_bulk_errors_total,
    elastic_bulk_requests_total,
    elastic_log_failures_total,
)
from app.infrastructure.search.elasticsearch_client import ElasticIndexWriter
from app.shared.utils.time import app_now

logger = logging.getLogger(__name__)

HTTP_AUDIT_EVENT_TYPE = 'http_audit_log'


def _compute_backoff_seconds(attempts: int) -> int:
    if attempts <= 1:
        return 1
    if attempts == 2:
        return 5
    if attempts == 3:
        return 15
    return 60


def _build_event_envelope(*, message_id: str, correlation_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    now_utc = app_now()
    return {
        'eventName': event_type,
        'timestampUtc': to_rfc3339_z(now_utc),
        'correlationId': correlation_id,
        'messageId': message_id,
        'payload': payload,
    }


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, str):
        normalized = value.strip().replace('Z', '+00:00')
        try:
            parsed = datetime.fromisoformat(normalized)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)


def _resolve_audit_index_name(index_prefix: str, payload: dict[str, Any]) -> str:
    timestamp_value = payload.get('@timestamp')
    parsed = _parse_timestamp(timestamp_value)
    return f"{index_prefix}-{parsed.strftime('%Y.%m.%d')}"


def _extract_bulk_failures(errors: list[dict[str, Any]]) -> dict[str, str]:
    failures: dict[str, str] = {}
    for item in errors:
        action = item.get('index') if isinstance(item, dict) else None
        if not isinstance(action, dict):
            continue
        event_id = str(action.get('_id', '')).strip()
        error_obj = action.get('error')
        reason = 'Falha ao indexar log de auditoria no Elasticsearch.'
        if isinstance(error_obj, dict):
            details = str(error_obj.get('reason') or error_obj.get('type') or '')
            if details:
                reason = f'Falha ao indexar log de auditoria no Elasticsearch: {details[:500]}'
        if event_id:
            failures[event_id] = reason
    return failures


def _ensure_audit_template(writer: ElasticIndexWriter, settings) -> None:
    mappings = {
        'dynamic': True,
        'properties': {
            '@timestamp': {'type': 'date'},
            'event': {'type': 'keyword'},
            'service': {'type': 'keyword'},
            'correlation_id': {'type': 'keyword'},
            'method': {'type': 'keyword'},
            'path': {'type': 'keyword'},
            'status_code': {'type': 'integer'},
            'duration_ms': {'type': 'float'},
            'items_count': {'type': 'integer'},
            'error': {
                'properties': {
                    'type': {'type': 'keyword'},
                    'message': {'type': 'text'},
                    'stage': {'type': 'keyword'},
                }
            },
            'request_sample': {'type': 'flattened'},
            'response_sample': {'type': 'flattened'},
        },
    }
    settings_body = {'number_of_shards': 1, 'number_of_replicas': 1}
    writer.ensure_index_template(
        template_name=settings.elastic_audit_template_name,
        index_patterns=[f'{settings.elastic_audit_index_prefix}-*'],
        mappings=mappings,
        settings=settings_body,
    )


def _publish_audit_events(writer: ElasticIndexWriter, settings, events: list[Any]) -> tuple[set[str], dict[str, str], float]:
    operations: list[dict[str, Any]] = []
    for event in events:
        payload = event.payload if isinstance(event.payload, dict) else {}
        operations.append(
            {
                '_op_type': 'index',
                '_index': _resolve_audit_index_name(settings.elastic_audit_index_prefix, payload),
                '_id': str(event.id),
                '_source': payload,
            }
        )

    if not operations:
        return set(), {}, 0.0

    started_at = perf_counter()
    try:
        success_count, errors = writer.bulk_write(operations)
        duration_seconds = max(perf_counter() - started_at, 0.0)
        failures = _extract_bulk_failures(errors)
        failed_ids = set(failures.keys())

        elastic_bulk_duration_seconds.labels(operation='audit_logs').observe(duration_seconds)
        elastic_bulk_requests_total.labels(operation='audit_logs', result='success').inc()
        elastic_bulk_documents_total.labels(operation='audit_logs', result='success').inc(success_count)

        if failed_ids:
            fail_count = len(failed_ids)
            elastic_bulk_requests_total.labels(operation='audit_logs', result='partial_error').inc()
            elastic_bulk_documents_total.labels(operation='audit_logs', result='error').inc(fail_count)
            elastic_bulk_errors_total.labels(operation='audit_logs').inc(fail_count)
            elastic_audit_log_failures_total.inc(fail_count)
            elastic_log_failures_total.inc(fail_count)

        succeeded_ids = {str(event.id) for event in events if str(event.id) not in failed_ids}
        return succeeded_ids, failures, duration_seconds * 1000.0
    except Exception:
        duration_seconds = max(perf_counter() - started_at, 0.0)
        fail_count = len(operations)
        elastic_bulk_duration_seconds.labels(operation='audit_logs').observe(duration_seconds)
        elastic_bulk_requests_total.labels(operation='audit_logs', result='failure').inc()
        elastic_bulk_documents_total.labels(operation='audit_logs', result='error').inc(fail_count)
        elastic_bulk_errors_total.labels(operation='audit_logs').inc(fail_count)
        elastic_audit_log_failures_total.inc(fail_count)
        elastic_log_failures_total.inc(fail_count)
        logger.error('Falha ao enviar lote de auditoria HTTP para o Elasticsearch.')
        failure_reason = 'Falha ao enviar lote de auditoria HTTP para o Elasticsearch.'
        failures = {str(event.id): failure_reason for event in events}
        return set(), failures, duration_seconds * 1000.0


def _chunked_events(events: list[Any], chunk_size: int) -> list[list[Any]]:
    size = max(1, chunk_size)
    return [events[i : i + size] for i in range(0, len(events), size)]


def run_worker() -> None:
    configure_logging()
    settings = get_settings()
    init_db()

    session_factory = get_session_factory()
    rabbit_bus = RabbitMQBus()
    elastic_writer = ElasticIndexWriter(settings.elasticsearch_url, timeout_seconds=settings.elastic_timeout_seconds)

    queue_messaging = (
        f'exchange={settings.rabbitmq_exchange};'
        f'routing_key={settings.rabbitmq_routing_key_analyze};'
        f'queue={settings.rabbitmq_queue_analyze}'
    )

    logger.info('Publicador de outbox iniciado. worker_id=%s', settings.outbox_worker_id)
    if settings.bypass_rabbit_for_tests:
        logger.info('Modo de teste sem RabbitMQ ativo no publicador de outbox.')

    if not settings.bypass_elastic_for_tests:
        try:
            _ensure_audit_template(elastic_writer, settings)
        except Exception:
            logger.error('Falha ao configurar template de auditoria no Elasticsearch.')

    try:
        while True:
            loop_started = perf_counter()
            now_utc = app_now()
            lock_cutoff = now_utc - timedelta(seconds=max(1, settings.outbox_lock_timeout_seconds))
            only_audit = settings.bypass_rabbit_for_tests

            claim_started = perf_counter()
            with session_factory() as session:
                repository = MessageRepository(session)
                events = repository.claim_outbox_events(
                    now_utc=now_utc,
                    lock_cutoff_utc=lock_cutoff,
                    worker_id=settings.outbox_worker_id,
                    limit=max(1, settings.outbox_batch_size),
                    event_types=[HTTP_AUDIT_EVENT_TYPE] if only_audit else None,
                )
                session.commit()
            claim_db_ms = (perf_counter() - claim_started) * 1000.0

            if not events:
                time.sleep(max(0.05, settings.outbox_poll_interval_ms / 1000.0))
                continue

            audit_events = [event for event in events if str(event.event_type) == HTTP_AUDIT_EVENT_TYPE]
            rabbit_events = [event for event in events if str(event.event_type) != HTTP_AUDIT_EVENT_TYPE]

            publish_queue_ms_total = 0.0
            audit_bulk_ms_total = 0.0
            update_db_ms_total = 0.0
            success_count = 0
            failed_count = 0
            batch_ids: set[str] = set()

            if audit_events:
                if settings.bypass_elastic_for_tests:
                    with session_factory() as session:
                        repository = MessageRepository(session)
                        processed_at = app_now()
                        for event in audit_events:
                            repository.mark_outbox_published(event_id=event.id, now_utc=processed_at)
                        session.commit()
                    success_count += len(audit_events)
                    logger.info('Modo de teste sem Elastic ativo no publicador de outbox para auditoria HTTP.')
                else:
                    succeeded_ids: set[str] = set()
                    failed_by_id: dict[str, str] = {}
                    for chunk in _chunked_events(audit_events, settings.elastic_audit_bulk_size):
                        chunk_succeeded, chunk_failed, chunk_ms = _publish_audit_events(elastic_writer, settings, chunk)
                        succeeded_ids.update(chunk_succeeded)
                        failed_by_id.update(chunk_failed)
                        audit_bulk_ms_total += chunk_ms
                    with session_factory() as session:
                        repository = MessageRepository(session)
                        processed_at = app_now()
                        for event in audit_events:
                            event_id = str(event.id)
                            if event_id in succeeded_ids:
                                repository.mark_outbox_published(event_id=event.id, now_utc=processed_at)
                                success_count += 1
                            else:
                                backoff = _compute_backoff_seconds(int(event.attempts or 1))
                                repository.mark_outbox_failed(
                                    event_id=event.id,
                                    now_utc=processed_at,
                                    available_at_utc=processed_at + timedelta(seconds=backoff),
                                    last_error=failed_by_id.get(event_id, 'Falha ao indexar log de auditoria no Elasticsearch.'),
                                )
                                failed_count += 1
                        session.commit()
                    logger.info(
                        'Lote de auditoria HTTP finalizado. eventos=%s sucesso=%s falhas=%s ms_bulk_elastic=%.2f',
                        len(audit_events),
                        len(succeeded_ids),
                        len(failed_by_id),
                        audit_bulk_ms_total,
                    )

            for event in rabbit_events:
                payload = event.payload if isinstance(event.payload, dict) else {}
                batch_id = str(payload.get('batch_id', '')).strip()
                if batch_id:
                    batch_ids.add(batch_id)

                envelope = _build_event_envelope(
                    message_id=event.message_id,
                    correlation_id=event.correlation_id,
                    event_type=event.event_type,
                    payload=payload,
                )

                publish_started = perf_counter()
                try:
                    published = False if settings.bypass_rabbit_for_tests else bool(rabbit_bus.publish_event(envelope))
                except Exception:
                    published = False
                publish_queue_ms = (perf_counter() - publish_started) * 1000.0
                publish_queue_ms_total += publish_queue_ms

                processed_at = app_now()
                update_started = perf_counter()
                with session_factory() as session:
                    repository = MessageRepository(session)
                    service = MessagePersistenceService(session)

                    if published:
                        repository.mark_outbox_published(event_id=event.id, now_utc=processed_at)
                        service.mark_queued(message_id=event.message_id, queue_messaging=queue_messaging)
                        success_count += 1
                        logger.info(
                            (
                                'Evento do outbox publicado. correlation_id=%s batch_id=%s '
                                'tentativa=%s ms_publicacao_fila=%.2f'
                            ),
                            event.correlation_id,
                            batch_id or 'sem-batch-id',
                            int(event.attempts or 1),
                            publish_queue_ms,
                        )
                    else:
                        backoff = _compute_backoff_seconds(int(event.attempts or 1))
                        repository.mark_outbox_failed(
                            event_id=event.id,
                            now_utc=processed_at,
                            available_at_utc=processed_at + timedelta(seconds=backoff),
                            last_error='Falha ao publicar evento no RabbitMQ.',
                        )
                        failed_count += 1
                        logger.error(
                            (
                                'Falha ao publicar evento do outbox. correlation_id=%s batch_id=%s '
                                'tentativa=%s ms_publicacao_fila=%.2f'
                            ),
                            event.correlation_id,
                            batch_id or 'sem-batch-id',
                            int(event.attempts or 1),
                            publish_queue_ms,
                        )
                    session.commit()
                update_db_ms = (perf_counter() - update_started) * 1000.0
                update_db_ms_total += update_db_ms
                logger.info(
                    'Persistencia do outbox concluida. correlation_id=%s ms_db=%.2f',
                    event.correlation_id,
                    update_db_ms,
                )

            loop_total_ms = (perf_counter() - loop_started) * 1000.0
            logger.info(
                (
                    'Lote do outbox finalizado. eventos=%s sucesso=%s falhas=%s '
                    'batches=%s ms_claim_db=%.2f ms_publicacao_fila_total=%.2f ms_bulk_auditoria_total=%.2f '
                    'ms_persistencia_db_total=%.2f ms_total_lote=%.2f'
                ),
                len(events),
                success_count,
                failed_count,
                ','.join(sorted(batch_ids)) if batch_ids else 'sem-batch-id',
                claim_db_ms,
                publish_queue_ms_total,
                audit_bulk_ms_total,
                update_db_ms_total,
                loop_total_ms,
            )
    finally:
        rabbit_bus.close()


def main() -> None:
    try:
        run_worker()
    except KeyboardInterrupt:
        logger.info('Publicador de outbox finalizado manualmente.')
    except Exception:
        logger.error('Falha fatal no publicador de outbox.')


if __name__ == '__main__':
    main()
