from __future__ import annotations

import logging
import uuid
from time import perf_counter
from typing import Any

from fastapi import APIRouter, Body, Depends, status
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.api.v1.dependencies.request_context import get_correlation_id
from app.api.v1.dependencies.request_context import get_publisher
from app.application.dtos.analysis import AnalyzeFeedResponse
from app.application.dtos.analysis import validate_analyze_payload
from app.application.dtos.batch import BatchIngestResponse, validate_batch_payload
from app.application.use_cases.ingest_batch_fastpath import BatchIngestFastpathUseCase
from app.application.use_cases.persist_message_request import MessagePersistenceService
from app.core.config.settings import get_settings
from app.domain.services.sentiment_service import analyze_messages, to_rfc3339_z
from app.infrastructure.db.session import get_db
from app.shared.utils.time import app_now

logger = logging.getLogger(__name__)

router = APIRouter(tags=['analysis'])


@router.post('/analyze-feed', response_model=AnalyzeFeedResponse | BatchIngestResponse)
async def analyze_feed(
    payload: dict[str, Any] = Body(...),
    correlation_id: str = Depends(get_correlation_id),
    publisher=Depends(get_publisher),
    db: Session = Depends(get_db),
):
    settings = get_settings()

    if isinstance(payload, dict) and 'items' in payload:
        validation_started = perf_counter()
        validated_batch = validate_batch_payload(payload)
        validation_ms = (perf_counter() - validation_started) * 1000.0

        if settings.bypass_persistence_for_tests:
            batch_id = str(uuid.uuid4())
            logger.info(
                (
                    'Modo de teste sem persistencia ativo no lote. '
                    'batch_id=%s itens=%s ms_validacao=%.2f'
                ),
                batch_id,
                len(validated_batch.items),
                validation_ms,
            )
            return JSONResponse(
                status_code=status.HTTP_202_ACCEPTED,
                content={
                    'batch_id': batch_id,
                    'accepted': len(validated_batch.items),
                },
            )

        ingest_started = perf_counter()
        result = BatchIngestFastpathUseCase(db).execute(items=validated_batch.items)
        route_total_ms = (perf_counter() - ingest_started) * 1000.0
        logger.info(
            (
                'Ingestao em lote concluida. '
                'batch_id=%s itens=%s '
                'ms_validacao=%.2f ms_preparo=%.2f ms_query_existentes=%.2f ms_dedupe=%.2f '
                'ms_resolve_users=%.2f ms_build_rows=%.2f ms_insert_messages=%.2f '
                'ms_insert_processing=%.2f ms_insert_outbox=%.2f ms_flush=%.2f ms_commit=%.2f '
                'ms_total_use_case=%.2f ms_total_rota=%.2f'
            ),
            result.batch_id,
            len(validated_batch.items),
            validation_ms,
            result.timings_ms.get('prepare_items', 0.0),
            result.timings_ms.get('query_existing_messages', 0.0),
            result.timings_ms.get('dedupe_batch', 0.0),
            result.timings_ms.get('resolve_users', 0.0),
            result.timings_ms.get('build_rows', 0.0),
            result.timings_ms.get('insert_messages', 0.0),
            result.timings_ms.get('insert_processing', 0.0),
            result.timings_ms.get('insert_outbox', 0.0),
            result.timings_ms.get('flush', 0.0),
            result.timings_ms.get('commit', 0.0),
            result.timings_ms.get('total', 0.0),
            route_total_ms,
        )
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                'batch_id': result.batch_id,
                'accepted': result.accepted,
            },
        )

    validated = validate_analyze_payload(payload)
    normalized_messages = [
        {
            'user_id': message.user_id,
            'content': message.content,
            'timestamp': message.timestamp,
            'hashtags': message.hashtags,
            'reactions': message.reactions,
            'shares': message.shares,
            'views': message.views,
        }
        for message in validated.messages
    ]

    analysis = analyze_messages(
        messages=normalized_messages,
        time_window_minutes=validated.time_window_minutes,
    )

    if settings.bypass_persistence_for_tests:
        logger.info(
            'Modo de teste sem persistencia ativo no analyze-feed. mensagens=%s',
            len(normalized_messages),
        )
        return {'analysis': analysis}

    persistence_service = MessagePersistenceService(db)
    persist_result = persistence_service.save_message_request(
        normalized_messages=normalized_messages,
        analysis=analysis,
        correlation_id=correlation_id,
    )

    if persist_result.created_new:
        now_utc = app_now()
        event_envelope = {
            'eventName': 'analyze_feed.completed',
            'timestampUtc': to_rfc3339_z(now_utc),
            'correlationId': correlation_id,
            'messageId': persist_result.message_id,
            'payload': {
                'messages_count': len(normalized_messages),
                'time_window_minutes': validated.time_window_minutes,
                'analysis': analysis,
                'flags': analysis.get('flags', {}),
                'user_ids': sorted({item['user_id'] for item in normalized_messages}),
            },
        }

        queue_messaging = (
            f'exchange={settings.rabbitmq_exchange};'
            f'routing_key={settings.rabbitmq_routing_key_analyze};'
            f'queue={settings.rabbitmq_queue_analyze}'
        )

        if settings.bypass_rabbit_for_tests:
            published = False
            logger.info('Modo de teste sem RabbitMQ ativo no analyze-feed. correlation_id=%s', correlation_id)
        else:
            try:
                published = bool(publisher.publish_event(event_envelope))
            except Exception:
                published = False

        if published:
            persistence_service.mark_queued(
                message_id=persist_result.message_id,
                queue_messaging=queue_messaging,
            )
        else:
            if not settings.bypass_rabbit_for_tests:
                logger.error('Falha ao publicar evento no RabbitMQ. correlation_id=%s', correlation_id)
                persistence_service.mark_publish_failed(
                    message_id=persist_result.message_id,
                    failed_reason='Falha ao publicar evento no RabbitMQ.',
                )
    else:
        logger.info('Mensagem ja registrada para correlation_id=%s', correlation_id)

    return {'analysis': analysis}
