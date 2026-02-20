from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from app.infrastructure.db.repositories.message_repository import MessageRepository

PROCESSING_STATUS_RECEIVED = 'received'
PROCESSING_STATUS_QUEUED = 'queued'
PROCESSING_STATUS_PROCESSING = 'processing'
PROCESSING_STATUS_PROCESSED = 'processed'
PROCESSING_STATUS_FAILED = 'failed'


@dataclass
class PersistResult:
    message_id: str
    created_new: bool


class MessagePersistenceService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.repository = MessageRepository(db)

    def save_message_request(
        self,
        *,
        normalized_messages: list[dict[str, Any]],
        analysis: dict[str, Any],
        correlation_id: str,
    ) -> PersistResult:
        existing = self.repository.get_message_by_correlation_id(correlation_id)
        if existing is not None:
            return PersistResult(message_id=existing.id, created_new=False)

        owner_user_key = self._extract_owner_user_key(normalized_messages)
        user = self._find_or_create_user(owner_user_key)

        sentiment = analysis.get('sentiment_distribution') if isinstance(analysis.get('sentiment_distribution'), dict) else {}
        flags = analysis.get('flags') if isinstance(analysis.get('flags'), dict) else {}
        influence_ranking = analysis.get('influence_ranking') if isinstance(analysis.get('influence_ranking'), list) else []
        trending_topics = analysis.get('trending_topics') if isinstance(analysis.get('trending_topics'), list) else []

        influence_scores = [
            float(item.get('influence_score', 0.0))
            for item in influence_ranking
            if isinstance(item, dict)
        ]
        max_influence_score = max(influence_scores) if influence_scores else None

        message = self.repository.create_message(
            user_id=user.id,
            correlation_id=correlation_id,
            request_raw=None,
            engagement_score=self._to_float_or_none(analysis.get('engagement_score')),
            ranking=None,
            influence_ranking_score=max_influence_score,
        )

        self.repository.create_sentiment(
            message_id=message.id,
            positive=self._to_float_or_zero(sentiment.get('positive')),
            negative=self._to_float_or_zero(sentiment.get('negative')),
            neutral=self._to_float_or_zero(sentiment.get('neutral')),
        )
        self.repository.create_flags(
            message_id=message.id,
            mbras_employee=bool(flags.get('mbras_employee', False)),
            special_pattern=bool(flags.get('special_pattern', False)),
            candidate_awareness=bool(flags.get('candidate_awareness', False)),
        )
        self.repository.create_anomaly(
            message_id=message.id,
            anomaly_detected=bool(analysis.get('anomaly_detected', False)),
            anomaly_type=self._to_short_text_or_none(analysis.get('anomaly_type')),
        )
        self.repository.create_processing(
            message_id=message.id,
            queue_messaging=None,
            processing_success=None,
            processing_status=PROCESSING_STATUS_RECEIVED,
            failure_stage=None,
            failed_reason=None,
            elastic_name=None,
            elastic_index_name=None,
        )

        for topic in sorted({str(item).strip() for item in trending_topics if str(item).strip()}):
            topic_entity = self.repository.get_or_create_topic(topic)
            self.repository.add_message_topic(message_id=message.id, topic_id=topic_entity.id)

        for item in influence_ranking:
            if not isinstance(item, dict):
                continue
            external_user_key = str(item.get('user_id', '')).strip()
            if not external_user_key:
                continue
            self.repository.add_influence_item(
                message_id=message.id,
                external_user_key=external_user_key,
                followers=int(item.get('followers', 0)),
                engagement_rate=float(item.get('engagement_rate', 0.0)),
                influence_score=float(item.get('influence_score', 0.0)),
            )

        self.db.commit()
        return PersistResult(message_id=message.id, created_new=True)

    def mark_queued(self, *, message_id: str, queue_messaging: str) -> None:
        self.repository.update_processing(
            message_id=message_id,
            processing_success=None,
            processing_status=PROCESSING_STATUS_QUEUED,
            queue_messaging=queue_messaging,
            failure_stage=None,
            failed_reason=None,
        )
        self.db.commit()

    def mark_publish_failed(self, *, message_id: str, failed_reason: str) -> None:
        self.repository.update_processing(
            message_id=message_id,
            processing_success=False,
            processing_status=PROCESSING_STATUS_FAILED,
            failure_stage='rabbit',
            failed_reason=failed_reason[:1000],
        )
        self.db.commit()

    def mark_processing(self, *, correlation_id: str) -> str | None:
        message = self.repository.get_message_by_correlation_id(correlation_id)
        if message is None:
            return None
        self.repository.update_processing(
            message_id=message.id,
            processing_success=None,
            processing_status=PROCESSING_STATUS_PROCESSING,
            failure_stage=None,
            failed_reason=None,
        )
        self.db.commit()
        return message.id

    def mark_processed(self, *, correlation_id: str, elastic_name: str | None, elastic_index_name: str | None) -> bool:
        message = self.repository.get_message_by_correlation_id(correlation_id)
        if message is None:
            return False
        self.repository.update_processing(
            message_id=message.id,
            processing_success=True,
            processing_status=PROCESSING_STATUS_PROCESSED,
            failure_stage=None,
            failed_reason=None,
            elastic_name=elastic_name,
            elastic_index_name=elastic_index_name,
        )
        self.db.commit()
        return True

    def mark_processing_failed(self, *, correlation_id: str, failure_stage: str, failed_reason: str) -> bool:
        message = self.repository.get_message_by_correlation_id(correlation_id)
        if message is None:
            return False
        self.repository.update_processing(
            message_id=message.id,
            processing_success=False,
            processing_status=PROCESSING_STATUS_FAILED,
            failure_stage=failure_stage[:32],
            failed_reason=failed_reason[:1000],
        )
        self.db.commit()
        return True

    def persist_normalized_outputs(self, *, message_id: str, payload: dict[str, Any]) -> bool:
        message = self.repository.get_message_by_id(message_id)
        if message is None:
            return False

        sentiment = payload.get('sentiment_distribution') if isinstance(payload.get('sentiment_distribution'), dict) else {}
        flags = payload.get('flags') if isinstance(payload.get('flags'), dict) else {}
        anomaly_detected = bool(payload.get('anomaly_detected', False))
        anomaly_type = self._to_short_text_or_none(payload.get('anomaly_type'))
        engagement_score = self._to_float_or_none(payload.get('engagement_score'))
        topics = payload.get('trending_topics') if isinstance(payload.get('trending_topics'), list) else []
        influence_ranking = payload.get('influence_ranking') if isinstance(payload.get('influence_ranking'), list) else []

        self.repository.update_message_engagement(message_id=message_id, engagement_score=engagement_score)
        self.repository.upsert_sentiment(
            message_id=message_id,
            positive=self._to_float_or_zero(sentiment.get('positive')),
            negative=self._to_float_or_zero(sentiment.get('negative')),
            neutral=self._to_float_or_zero(sentiment.get('neutral')),
        )
        self.repository.upsert_flags(
            message_id=message_id,
            mbras_employee=bool(flags.get('mbras_employee', False)),
            special_pattern=bool(flags.get('special_pattern', False)),
            candidate_awareness=bool(flags.get('candidate_awareness', False)),
        )
        self.repository.upsert_anomaly(
            message_id=message_id,
            anomaly_detected=anomaly_detected,
            anomaly_type=anomaly_type,
        )

        normalized_influence_items: list[dict[str, Any]] = []
        for item in influence_ranking:
            if not isinstance(item, dict):
                continue
            external_user_key = str(item.get('user_id', '')).strip()
            if not external_user_key:
                continue
            normalized_influence_items.append(
                {
                    'external_user_key': external_user_key,
                    'followers': int(item.get('followers', 0)),
                    'engagement_rate': float(item.get('engagement_rate', 0.0)),
                    'influence_score': float(item.get('influence_score', 0.0)),
                }
            )

        self.repository.replace_influence_items(message_id=message_id, items=normalized_influence_items)
        self.repository.replace_topics(
            message_id=message_id,
            topic_names=[str(item).strip() for item in topics if str(item).strip()],
        )
        self.db.commit()
        return True

    def _extract_owner_user_key(self, normalized_messages: list[dict[str, Any]]) -> str:
        if not normalized_messages:
            return 'user_sem_identificador'
        value = str(normalized_messages[0].get('user_id', '')).strip()
        return value or 'user_sem_identificador'

    def _find_or_create_user(self, external_or_uuid: str):
        candidate = external_or_uuid.strip()
        parsed_uuid: uuid.UUID | None = None
        try:
            parsed_uuid = uuid.UUID(candidate)
        except Exception:
            parsed_uuid = None

        if parsed_uuid is not None:
            user_by_id = self.repository.get_user_by_id(str(parsed_uuid))
            if user_by_id is not None:
                return user_by_id
            user_by_external = self.repository.get_user_by_external_key(candidate)
            if user_by_external is not None:
                return user_by_external
            return self.repository.create_user(user_id=str(parsed_uuid), external_user_key=candidate)

        user_by_external = self.repository.get_user_by_external_key(candidate)
        if user_by_external is not None:
            return user_by_external
        return self.repository.create_user(user_id=None, external_user_key=candidate)

    @staticmethod
    def _to_float_or_zero(value: Any) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0

    @staticmethod
    def _to_float_or_none(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _to_short_text_or_none(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text[:256] if text else None

    @staticmethod
    def to_safe_json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False)
