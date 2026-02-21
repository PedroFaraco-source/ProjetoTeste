from __future__ import annotations

import logging
import uuid
from datetime import datetime
from threading import Lock
from typing import Any

from app.infrastructure.db.repositories.message_repository import MessageRepository
from app.infrastructure.db.session import get_session_factory
from app.shared.utils.time import app_now

logger = logging.getLogger(__name__)

_AUDIT_ANCHOR_CORRELATION_ID = 'audit-log-anchor-correlation-id'
_AUDIT_ANCHOR_USER_KEY = 'audit_log_system_user'
_AUDIT_ANCHOR_MESSAGE_ID: str | None = None
_AUDIT_ANCHOR_LOCK = Lock()


def _get_or_create_anchor_message_id(repository: MessageRepository) -> str:
    global _AUDIT_ANCHOR_MESSAGE_ID

    if _AUDIT_ANCHOR_MESSAGE_ID:
        return _AUDIT_ANCHOR_MESSAGE_ID

    with _AUDIT_ANCHOR_LOCK:
        if _AUDIT_ANCHOR_MESSAGE_ID:
            return _AUDIT_ANCHOR_MESSAGE_ID

        existing = repository.get_message_by_correlation_id(_AUDIT_ANCHOR_CORRELATION_ID)
        if existing is not None:
            _AUDIT_ANCHOR_MESSAGE_ID = existing.id
            return _AUDIT_ANCHOR_MESSAGE_ID

        user = repository.get_user_by_external_key(_AUDIT_ANCHOR_USER_KEY)
        if user is None:
            user = repository.create_user(user_id=None, external_user_key=_AUDIT_ANCHOR_USER_KEY)

        message = repository.create_message(
            user_id=user.id,
            correlation_id=_AUDIT_ANCHOR_CORRELATION_ID,
            request_raw=None,
            engagement_score=None,
            ranking=None,
            influence_ranking_score=None,
        )
        _AUDIT_ANCHOR_MESSAGE_ID = message.id
        return _AUDIT_ANCHOR_MESSAGE_ID


def persist_http_audit_outbox_event(*, correlation_id: str, payload: dict[str, Any]) -> bool:
    safe_correlation_id = str(correlation_id or '').strip()[:64]
    now_utc: datetime = app_now()
    session_factory = get_session_factory()

    try:
        with session_factory() as session:
            repository = MessageRepository(session)
            anchor_message_id = _get_or_create_anchor_message_id(repository)
            repository.bulk_insert_outbox_events(
                [
                    {
                        'id': str(uuid.uuid4()),
                        'message_id': anchor_message_id,
                        'correlation_id': safe_correlation_id or 'sem-correlation-id',
                        'event_type': 'http_audit_log',
                        'payload': payload,
                        'status': 'pending',
                        'attempts': 0,
                        'last_error': None,
                        'available_at_utc': now_utc,
                        'locked_at_utc': None,
                        'locked_by': None,
                        'created_at_utc': now_utc,
                        'updated_at_utc': now_utc,
                    }
                ]
            )
            session.commit()
            return True
    except Exception:
        logger.error('Falha ao persistir evento de auditoria HTTP no outbox. correlation_id=%s', safe_correlation_id or 'sem-correlation-id')
        return False
