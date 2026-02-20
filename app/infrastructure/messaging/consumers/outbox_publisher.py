from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from time import perf_counter
from typing import Any

from app.core.config.settings import get_settings
from app.core.logging.setup import configure_logging
from app.domain.services.sentiment_service import to_rfc3339_z
from app.application.use_cases.persist_message_request import MessagePersistenceService
from app.infrastructure.db.repositories.message_repository import MessageRepository
from app.infrastructure.db.session import get_session_factory, init_db
from app.infrastructure.messaging.rabbitmq_bus import RabbitMQBus
from app.shared.utils.time import app_now

logger = logging.getLogger(__name__)


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


def run_worker() -> None:
    configure_logging()
    settings = get_settings()
    init_db()

    session_factory = get_session_factory()
    rabbit_bus = RabbitMQBus()

    queue_messaging = (
        f'exchange={settings.rabbitmq_exchange};'
        f'routing_key={settings.rabbitmq_routing_key_analyze};'
        f'queue={settings.rabbitmq_queue_analyze}'
    )

    logger.info('Publicador de outbox iniciado. worker_id=%s', settings.outbox_worker_id)
    if settings.bypass_rabbit_for_tests:
        logger.info('Modo de teste sem RabbitMQ ativo no publicador de outbox.')

    try:
        while True:
            if settings.bypass_rabbit_for_tests:
                time.sleep(max(0.1, settings.outbox_poll_interval_ms / 1000.0))
                continue

            loop_started = perf_counter()
            now_utc = app_now()
            lock_cutoff = now_utc - timedelta(seconds=max(1, settings.outbox_lock_timeout_seconds))

            claim_started = perf_counter()
            with session_factory() as session:
                repository = MessageRepository(session)
                events = repository.claim_outbox_events(
                    now_utc=now_utc,
                    lock_cutoff_utc=lock_cutoff,
                    worker_id=settings.outbox_worker_id,
                    limit=max(1, settings.outbox_batch_size),
                )
                session.commit()
            claim_db_ms = (perf_counter() - claim_started) * 1000.0

            if not events:
                time.sleep(max(0.05, settings.outbox_poll_interval_ms / 1000.0))
                continue

            publish_queue_ms_total = 0.0
            update_db_ms_total = 0.0
            success_count = 0
            failed_count = 0
            batch_ids: set[str] = set()

            for event in events:
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
                    published = bool(rabbit_bus.publish_event(envelope))
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
                    'batches=%s ms_claim_db=%.2f ms_publicacao_fila_total=%.2f '
                    'ms_persistencia_db_total=%.2f ms_total_lote=%.2f'
                ),
                len(events),
                success_count,
                failed_count,
                ','.join(sorted(batch_ids)) if batch_ids else 'sem-batch-id',
                claim_db_ms,
                publish_queue_ms_total,
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
