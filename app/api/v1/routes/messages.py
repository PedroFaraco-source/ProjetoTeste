from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.application.dtos.analysis import MessagesPageResponse, parse_rfc3339_z
from app.domain.services.sentiment_service import to_rfc3339_z
from app.infrastructure.db.repositories.message_repository import MessageRepository
from app.infrastructure.db.session import get_db

router = APIRouter(tags=['messages'])


@router.get('/messages', response_model=MessagesPageResponse)
def list_messages(
    user_id: str | None = Query(default=None),
    from_utc: str | None = Query(default=None),
    to_utc: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=200),
    db: Session = Depends(get_db),
):
    from_dt = parse_rfc3339_z(from_utc, code='INVALID_FROM_UTC') if from_utc is not None else None
    to_dt = parse_rfc3339_z(to_utc, code='INVALID_TO_UTC') if to_utc is not None else None

    repository = MessageRepository(db)
    total, rows = repository.list_messages(
        user_key=user_id,
        from_dt=from_dt,
        to_dt=to_dt,
        page=page,
        page_size=page_size,
    )

    items: list[dict[str, Any]] = []
    for row in rows:
        related = repository.load_related_data(row.id)
        sentiment = related['sentiment']
        flags = related['flags']
        anomaly = related['anomaly']
        processing = related['processing']
        influence_items = related['influence_items']
        topics = related['topics']

        analysis = {
            'sentiment_distribution': {
                'positive': float(sentiment.positive) if sentiment is not None else 0.0,
                'negative': float(sentiment.negative) if sentiment is not None else 0.0,
                'neutral': float(sentiment.neutral) if sentiment is not None else 0.0,
            },
            'engagement_score': float(row.engagement_score) if row.engagement_score is not None else 0.0,
            'trending_topics': topics,
            'influence_ranking': [
                {
                    'user_id': item.external_user_key,
                    'followers': item.followers,
                    'engagement_rate': float(item.engagement_rate),
                    'influence_score': float(item.influence_score),
                }
                for item in influence_items
            ],
            'anomaly_detected': bool(anomaly.anomaly_detected) if anomaly is not None else False,
            'anomaly_type': anomaly.anomaly_type if anomaly is not None else None,
            'flags': {
                'mbras_employee': bool(flags.mbras_employee) if flags is not None else False,
                'special_pattern': bool(flags.special_pattern) if flags is not None else False,
                'candidate_awareness': bool(flags.candidate_awareness) if flags is not None else False,
            },
        }

        items.append(
            {
                'id': row.id,
                'created_at_utc': to_rfc3339_z(row.created_at_utc),
                'correlation_id': row.correlation_id,
                'user_id': row.user_id,
                'user_external_key': row.user.external_user_key if row.user is not None else None,
                'engagement_score': float(row.engagement_score) if row.engagement_score is not None else None,
                'analysis': analysis,
                'processing_success': processing.processing_success if processing is not None else None,
                'processing_status': processing.processing_status if processing is not None else None,
                'failure_stage': processing.failure_stage if processing is not None else None,
                'failure_reason': processing.failed_reason if processing is not None else None,
                'queue_messaging': processing.queue_messaging if processing is not None else None,
                'elastic_name': processing.elastic_name if processing is not None else None,
                'elastic_index_name': processing.elastic_index_name if processing is not None else None,
                'processed_at_utc': (
                    to_rfc3339_z(processing.updated_at_utc) if processing is not None and processing.updated_at_utc is not None else None
                ),
            }
        )

    return {
        'page': page,
        'page_size': page_size,
        'total': total,
        'items': items,
    }
