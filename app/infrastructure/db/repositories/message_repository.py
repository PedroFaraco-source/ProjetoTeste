from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import delete, insert, or_, select
from sqlalchemy.orm import Session

from app.infrastructure.db.models import (
    InfluenceRankingItem,
    Message,
    MessageAnomaly,
    MessageFlags,
    MessageProcessing,
    MessageSentiment,
    MessageTopic,
    OutboxEvent,
    Topic,
    User,
)
from app.shared.utils.time import app_now


class MessageRepository:
    _UNSET = object()

    def __init__(self, db: Session) -> None:
        self.db = db

    def get_message_by_correlation_id(self, correlation_id: str) -> Message | None:
        stmt = select(Message).where(Message.correlation_id == correlation_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_messages_by_correlation_ids(self, correlation_ids: list[str]) -> list[Message]:
        if not correlation_ids:
            return []
        stmt = select(Message).where(Message.correlation_id.in_(correlation_ids))
        return self.db.execute(stmt).scalars().all()

    def get_message_by_id(self, message_id: str) -> Message | None:
        return self.db.get(Message, message_id)

    def get_user_by_id(self, user_id: str) -> User | None:
        return self.db.get(User, user_id)

    def get_user_by_external_key(self, external_user_key: str) -> User | None:
        stmt = select(User).where(User.external_user_key == external_user_key)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_users_by_ids(self, user_ids: list[str]) -> list[User]:
        if not user_ids:
            return []
        stmt = select(User).where(User.id.in_(user_ids))
        return self.db.execute(stmt).scalars().all()

    def get_users_by_external_keys(self, external_user_keys: list[str]) -> list[User]:
        if not external_user_keys:
            return []
        stmt = select(User).where(User.external_user_key.in_(external_user_keys))
        return self.db.execute(stmt).scalars().all()

    def create_user(self, *, user_id: str | None, external_user_key: str | None) -> User:
        user = User(id=user_id, external_user_key=external_user_key)
        self.db.add(user)
        self.db.flush()
        return user

    def bulk_insert_users(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            self._insert_ignore_conflicts(User, rows)

    def create_message(
        self,
        *,
        user_id: str,
        correlation_id: str,
        request_raw: str | None,
        engagement_score: float | None,
        ranking: float | None,
        influence_ranking_score: float | None,
    ) -> Message:
        message = Message(
            user_id=user_id,
            correlation_id=correlation_id,
            request_raw=request_raw,
            engagement_score=engagement_score,
            ranking=ranking,
            influence_ranking_score=influence_ranking_score,
        )
        self.db.add(message)
        self.db.flush()
        return message

    def bulk_insert_messages(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            self.db.execute(insert(Message), rows)

    def create_sentiment(self, *, message_id: str, positive: float, negative: float, neutral: float) -> MessageSentiment:
        sentiment = MessageSentiment(
            message_id=message_id,
            positive=positive,
            negative=negative,
            neutral=neutral,
        )
        self.db.add(sentiment)
        return sentiment

    def create_flags(
        self,
        *,
        message_id: str,
        mbras_employee: bool,
        special_pattern: bool,
        candidate_awareness: bool,
    ) -> MessageFlags:
        flags = MessageFlags(
            message_id=message_id,
            mbras_employee=mbras_employee,
            special_pattern=special_pattern,
            candidate_awareness=candidate_awareness,
        )
        self.db.add(flags)
        return flags

    def create_anomaly(self, *, message_id: str, anomaly_detected: bool, anomaly_type: str | None) -> MessageAnomaly:
        anomaly = MessageAnomaly(
            message_id=message_id,
            anomaly_detected=anomaly_detected,
            anomaly_type=anomaly_type,
        )
        self.db.add(anomaly)
        return anomaly

    def create_processing(
        self,
        *,
        message_id: str,
        queue_messaging: str | None,
        processing_success: bool | None,
        processing_status: str,
        failure_stage: str | None,
        failed_reason: str | None,
        elastic_name: str | None,
        elastic_index_name: str | None,
    ) -> MessageProcessing:
        processing = MessageProcessing(
            message_id=message_id,
            queue_messaging=queue_messaging,
            processing_success=processing_success,
            processing_status=processing_status,
            failure_stage=failure_stage,
            failed_reason=failed_reason,
            elastic_name=elastic_name,
            elastic_index_name=elastic_index_name,
        )
        self.db.add(processing)
        return processing

    def bulk_insert_message_processing(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            self.db.execute(insert(MessageProcessing), rows)

    def update_processing(
        self,
        *,
        message_id: str,
        processing_success: bool | None | object = _UNSET,
        processing_status: str | object = _UNSET,
        queue_messaging: str | None | object = _UNSET,
        failure_stage: str | None | object = _UNSET,
        failed_reason: str | None | object = _UNSET,
        elastic_name: str | None | object = _UNSET,
        elastic_index_name: str | None | object = _UNSET,
        force_update_timestamp: bool = True,
    ) -> MessageProcessing | None:
        stmt = select(MessageProcessing).where(MessageProcessing.message_id == message_id)
        processing = self.db.execute(stmt).scalar_one_or_none()
        if processing is None:
            return None

        if processing_success is not self._UNSET:
            processing.processing_success = processing_success
        if processing_status is not self._UNSET:
            processing.processing_status = processing_status
        if queue_messaging is not self._UNSET:
            processing.queue_messaging = queue_messaging
        if failure_stage is not self._UNSET:
            processing.failure_stage = failure_stage
        if failed_reason is not self._UNSET:
            processing.failed_reason = failed_reason
        if elastic_name is not self._UNSET:
            processing.elastic_name = elastic_name
        if elastic_index_name is not self._UNSET:
            processing.elastic_index_name = elastic_index_name
        if force_update_timestamp:
            processing.updated_at_utc = app_now()
        return processing

    def update_message_engagement(self, *, message_id: str, engagement_score: float | None) -> None:
        message = self.get_message_by_id(message_id)
        if message is not None:
            message.engagement_score = engagement_score

    def get_or_create_topic(self, name: str) -> Topic:
        stmt = select(Topic).where(Topic.name == name)
        topic = self.db.execute(stmt).scalar_one_or_none()
        if topic is not None:
            return topic
        topic = Topic(name=name)
        self.db.add(topic)
        self.db.flush()
        return topic

    def get_topics_by_names(self, names: list[str]) -> list[Topic]:
        if not names:
            return []
        stmt = select(Topic).where(Topic.name.in_(names))
        return self.db.execute(stmt).scalars().all()

    def add_message_topic(self, *, message_id: str, topic_id: str) -> MessageTopic:
        stmt = select(MessageTopic).where(MessageTopic.message_id == message_id, MessageTopic.topic_id == topic_id)
        existing = self.db.execute(stmt).scalar_one_or_none()
        if existing is not None:
            return existing
        message_topic = MessageTopic(message_id=message_id, topic_id=topic_id)
        self.db.add(message_topic)
        return message_topic

    def replace_topics(self, *, message_id: str, topic_names: list[str]) -> None:
        self.db.execute(delete(MessageTopic).where(MessageTopic.message_id == message_id))
        cleaned_names = sorted({str(name).strip() for name in topic_names if str(name).strip()})
        if not cleaned_names:
            return

        existing_topics = self.get_topics_by_names(cleaned_names)
        by_name = {item.name: item for item in existing_topics}
        missing = [name for name in cleaned_names if name not in by_name]
        if missing:
            self._insert_ignore_conflicts(Topic, [{'id': str(uuid.uuid4()), 'name': name} for name in missing], ['name'])
            existing_topics = self.get_topics_by_names(cleaned_names)
            by_name = {item.name: item for item in existing_topics}

        rows = [
            {
                'message_id': message_id,
                'topic_id': by_name[name].id,
            }
            for name in cleaned_names
            if name in by_name
        ]
        if rows:
            self._insert_ignore_conflicts(MessageTopic, rows, ['message_id', 'topic_id'])

    def add_influence_item(
        self,
        *,
        message_id: str,
        external_user_key: str,
        followers: int,
        engagement_rate: float,
        influence_score: float,
    ) -> InfluenceRankingItem:
        item = InfluenceRankingItem(
            message_id=message_id,
            external_user_key=external_user_key,
            followers=followers,
            engagement_rate=engagement_rate,
            influence_score=influence_score,
        )
        self.db.add(item)
        return item

    def replace_influence_items(self, *, message_id: str, items: list[dict[str, Any]]) -> None:
        self.db.execute(delete(InfluenceRankingItem).where(InfluenceRankingItem.message_id == message_id))
        if not items:
            return
        rows = [
            {
                'id': str(uuid.uuid4()),
                'message_id': message_id,
                'external_user_key': item['external_user_key'],
                'followers': int(item['followers']),
                'engagement_rate': float(item['engagement_rate']),
                'influence_score': float(item['influence_score']),
            }
            for item in items
        ]
        self.db.execute(insert(InfluenceRankingItem), rows)

    def upsert_sentiment(self, *, message_id: str, positive: float, negative: float, neutral: float) -> None:
        current = self.db.execute(select(MessageSentiment).where(MessageSentiment.message_id == message_id)).scalar_one_or_none()
        if current is None:
            self.create_sentiment(message_id=message_id, positive=positive, negative=negative, neutral=neutral)
            return
        current.positive = positive
        current.negative = negative
        current.neutral = neutral

    def upsert_flags(
        self,
        *,
        message_id: str,
        mbras_employee: bool,
        special_pattern: bool,
        candidate_awareness: bool,
    ) -> None:
        current = self.db.execute(select(MessageFlags).where(MessageFlags.message_id == message_id)).scalar_one_or_none()
        if current is None:
            self.create_flags(
                message_id=message_id,
                mbras_employee=mbras_employee,
                special_pattern=special_pattern,
                candidate_awareness=candidate_awareness,
            )
            return
        current.mbras_employee = mbras_employee
        current.special_pattern = special_pattern
        current.candidate_awareness = candidate_awareness

    def upsert_anomaly(self, *, message_id: str, anomaly_detected: bool, anomaly_type: str | None) -> None:
        current = self.db.execute(select(MessageAnomaly).where(MessageAnomaly.message_id == message_id)).scalar_one_or_none()
        if current is None:
            self.create_anomaly(message_id=message_id, anomaly_detected=anomaly_detected, anomaly_type=anomaly_type)
            return
        current.anomaly_detected = anomaly_detected
        current.anomaly_type = anomaly_type

    def bulk_insert_outbox_events(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            self.db.execute(insert(OutboxEvent), rows)

    def claim_outbox_events(
        self,
        *,
        now_utc: datetime,
        lock_cutoff_utc: datetime,
        worker_id: str,
        limit: int,
        event_types: list[str] | None = None,
    ) -> list[OutboxEvent]:
        stmt = (
            select(OutboxEvent)
            .where(
                OutboxEvent.status.in_(['pending', 'failed']),
                OutboxEvent.available_at_utc <= now_utc,
                or_(OutboxEvent.locked_at_utc.is_(None), OutboxEvent.locked_at_utc < lock_cutoff_utc),
            )
            .order_by(OutboxEvent.created_at_utc.asc())
            .limit(limit)
        )
        if event_types:
            stmt = stmt.where(OutboxEvent.event_type.in_(event_types))
        try:
            stmt = stmt.with_for_update(skip_locked=True)
        except Exception:
            pass

        rows = self.db.execute(stmt).scalars().all()
        for item in rows:
            item.locked_at_utc = now_utc
            item.locked_by = worker_id
            item.attempts = int(item.attempts or 0) + 1
            item.updated_at_utc = now_utc
        return rows

    def mark_outbox_published(self, *, event_id: str, now_utc: datetime) -> None:
        event = self.db.get(OutboxEvent, event_id)
        if event is None:
            return
        event.status = 'published'
        event.last_error = None
        event.locked_at_utc = None
        event.locked_by = None
        event.updated_at_utc = now_utc

    def mark_outbox_failed(self, *, event_id: str, now_utc: datetime, available_at_utc: datetime, last_error: str) -> None:
        event = self.db.get(OutboxEvent, event_id)
        if event is None:
            return
        event.status = 'failed'
        event.last_error = (last_error or '')[:1000]
        event.available_at_utc = available_at_utc
        event.locked_at_utc = None
        event.locked_by = None
        event.updated_at_utc = now_utc

    def list_messages(
        self,
        *,
        user_key: str | None,
        from_dt: datetime | None,
        to_dt: datetime | None,
        page: int,
        page_size: int,
    ) -> tuple[int, list[Message]]:
        query = self.db.query(Message).join(User, User.id == Message.user_id)
        if user_key:
            query = query.filter(User.external_user_key.ilike(f'%{user_key.strip()}%'))
        if from_dt is not None:
            query = query.filter(Message.created_at_utc >= from_dt)
        if to_dt is not None:
            query = query.filter(Message.created_at_utc <= to_dt)

        total = query.count()
        rows = (
            query.order_by(Message.created_at_utc.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
            .all()
        )
        return total, rows

    def load_related_data(self, message_id: str) -> dict[str, Any]:
        sentiment = self.db.execute(select(MessageSentiment).where(MessageSentiment.message_id == message_id)).scalar_one_or_none()
        flags = self.db.execute(select(MessageFlags).where(MessageFlags.message_id == message_id)).scalar_one_or_none()
        anomaly = self.db.execute(select(MessageAnomaly).where(MessageAnomaly.message_id == message_id)).scalar_one_or_none()
        processing = self.db.execute(select(MessageProcessing).where(MessageProcessing.message_id == message_id)).scalar_one_or_none()
        influence_items = self.db.execute(
            select(InfluenceRankingItem).where(InfluenceRankingItem.message_id == message_id)
        ).scalars().all()
        topic_rows = (
            self.db.query(Topic.name)
            .join(MessageTopic, MessageTopic.topic_id == Topic.id)
            .filter(MessageTopic.message_id == message_id)
            .order_by(Topic.name.asc())
            .all()
        )
        return {
            'sentiment': sentiment,
            'flags': flags,
            'anomaly': anomaly,
            'processing': processing,
            'influence_items': influence_items,
            'topics': [name for (name,) in topic_rows],
        }

    def _insert_ignore_conflicts(
        self,
        model: Any,
        rows: list[dict[str, Any]],
        conflict_columns: list[str] | None = None,
    ) -> None:
        if not rows:
            return

        filtered_rows = rows
        if conflict_columns:
            filtered_rows = self._filter_existing_rows(
                model=model,
                rows=rows,
                conflict_columns=conflict_columns,
            )
        if not filtered_rows:
            return
        self.db.execute(insert(model), filtered_rows)

    def _filter_existing_rows(
        self,
        *,
        model: Any,
        rows: list[dict[str, Any]],
        conflict_columns: list[str],
    ) -> list[dict[str, Any]]:
        if not conflict_columns:
            return rows

        if len(conflict_columns) == 1:
            column_name = conflict_columns[0]
            values = []
            seen_values: set[Any] = set()
            for row in rows:
                value = row.get(column_name)
                if value in seen_values:
                    continue
                seen_values.add(value)
                values.append(value)

            column = getattr(model, column_name)
            existing_values = {
                value
                for value in self.db.execute(select(column).where(column.in_(values))).scalars().all()
            }
            return [row for row in rows if row.get(column_name) not in existing_values]

        unique_rows: list[dict[str, Any]] = []
        seen_keys: set[tuple[Any, ...]] = set()
        for row in rows:
            key = tuple(row.get(column_name) for column_name in conflict_columns)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            unique_rows.append(row)
        return unique_rows
