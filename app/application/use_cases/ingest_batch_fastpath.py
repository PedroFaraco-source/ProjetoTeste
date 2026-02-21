from __future__ import annotations

import uuid
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from sqlalchemy.orm import Session

from app.infrastructure.db.repositories.message_repository import MessageRepository
from app.infrastructure.monitoring.prometheus import db_fast_path_duration_seconds
from app.infrastructure.monitoring.prometheus import tempo_db_ms
from app.shared.utils.time import app_now


@dataclass
class BatchIngestResult:
    batch_id: str
    accepted: int
    timings_ms: dict[str, float]


class BatchIngestFastpathUseCase:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.repository = MessageRepository(db)

    def execute(self, *, items: list[dict[str, Any]]) -> BatchIngestResult:
        total_started = perf_counter()
        stage_started = total_started
        now_utc = app_now()
        batch_id = str(uuid.uuid4())
        timings_ms: dict[str, float] = {}

        prepared: list[dict[str, Any]] = []
        for idx, item in enumerate(items):
            raw_correlation = str(item.get('correlation_id', '')).strip()
            correlation_id = raw_correlation or str(uuid.uuid4())
            prepared.append({'index': idx, 'item': item, 'correlation_id': correlation_id})
        timings_ms['prepare_items'] = (perf_counter() - stage_started) * 1000.0

        stage_started = perf_counter()
        all_correlation_ids = [entry['correlation_id'] for entry in prepared]
        existing_messages = self.repository.get_messages_by_correlation_ids(all_correlation_ids)
        existing_by_correlation = {item.correlation_id: item for item in existing_messages}
        timings_ms['query_existing_messages'] = (perf_counter() - stage_started) * 1000.0

        stage_started = perf_counter()
        to_create: list[dict[str, Any]] = []
        scheduled_correlation_ids: set[str] = set()
        for entry in prepared:
            correlation_id = entry['correlation_id']
            if correlation_id in existing_by_correlation or correlation_id in scheduled_correlation_ids:
                continue
            to_create.append(entry)
            scheduled_correlation_ids.add(correlation_id)
        timings_ms['dedupe_batch'] = (perf_counter() - stage_started) * 1000.0

        if to_create:
            stage_started = perf_counter()
            user_map = self._resolve_users_for_batch(to_create)
            timings_ms['resolve_users'] = (perf_counter() - stage_started) * 1000.0

            stage_started = perf_counter()
            message_rows: list[dict[str, Any]] = []
            processing_rows: list[dict[str, Any]] = []
            outbox_rows: list[dict[str, Any]] = []

            for entry in to_create:
                item = entry['item']
                correlation_id = entry['correlation_id']
                user_id_raw = str(item.get('user_id', '')).strip()
                user_pk = user_map[user_id_raw]
                message_id = str(uuid.uuid4())
                engagement_score = self._to_float_or_none(item.get('engagement_score'))

                message_rows.append(
                    {
                        'id': message_id,
                        'user_id': user_pk,
                        'correlation_id': correlation_id,
                        'engagement_score': engagement_score,
                        'request_raw': None,
                        'ranking': None,
                        'influence_ranking_score': None,
                        'created_at_utc': now_utc,
                    }
                )
                processing_rows.append(
                    {
                        'id': str(uuid.uuid4()),
                        'message_id': message_id,
                        'queue_messaging': None,
                        'processing_success': None,
                        'processing_status': 'received',
                        'failure_stage': None,
                        'failed_reason': None,
                        'elastic_name': None,
                        'elastic_index_name': None,
                        'updated_at_utc': now_utc,
                    }
                )
                outbox_rows.append(
                    {
                        'id': str(uuid.uuid4()),
                        'message_id': message_id,
                        'correlation_id': correlation_id,
                        'event_type': 'message_received',
                        'payload': self._build_event_payload(item, batch_id=batch_id),
                        'status': 'pending',
                        'attempts': 0,
                        'last_error': None,
                        'available_at_utc': now_utc,
                        'locked_at_utc': None,
                        'locked_by': None,
                        'created_at_utc': now_utc,
                        'updated_at_utc': now_utc,
                    }
                )
            timings_ms['build_rows'] = (perf_counter() - stage_started) * 1000.0

            stage_started = perf_counter()
            self.repository.bulk_insert_messages(message_rows)
            timings_ms['insert_messages'] = (perf_counter() - stage_started) * 1000.0

            stage_started = perf_counter()
            self.repository.bulk_insert_message_processing(processing_rows)
            timings_ms['insert_processing'] = (perf_counter() - stage_started) * 1000.0

            stage_started = perf_counter()
            self.repository.bulk_insert_outbox_events(outbox_rows)
            timings_ms['insert_outbox'] = (perf_counter() - stage_started) * 1000.0

            stage_started = perf_counter()
            self.db.flush()
            timings_ms['flush'] = (perf_counter() - stage_started) * 1000.0

        stage_started = perf_counter()
        self.db.commit()
        timings_ms['commit'] = (perf_counter() - stage_started) * 1000.0
        timings_ms['total'] = (perf_counter() - total_started) * 1000.0

        db_time_ms = (
            timings_ms.get('query_existing_messages', 0.0)
            + timings_ms.get('resolve_users', 0.0)
            + timings_ms.get('insert_messages', 0.0)
            + timings_ms.get('insert_processing', 0.0)
            + timings_ms.get('insert_outbox', 0.0)
            + timings_ms.get('flush', 0.0)
            + timings_ms.get('commit', 0.0)
        )
        db_fast_path_duration_seconds.labels(operation='fast_path').observe(max(db_time_ms / 1000.0, 0.0))
        tempo_db_ms.labels(operation='ingest_batch_fastpath').observe(max(db_time_ms, 0.0))
        return BatchIngestResult(batch_id=batch_id, accepted=len(prepared), timings_ms=timings_ms)

    def _resolve_users_for_batch(self, entries: list[dict[str, Any]]) -> dict[str, str]:
        user_values = [str(entry['item'].get('user_id', '')).strip() for entry in entries]

        uuid_values = sorted({value for value in user_values if self._is_uuid(value)})
        external_values = sorted({value for value in user_values if not self._is_uuid(value)})

        users_by_id = {item.id: item for item in self.repository.get_users_by_ids(uuid_values)}
        users_by_external = {item.external_user_key: item for item in self.repository.get_users_by_external_keys(external_values)}

        missing_rows: list[dict[str, Any]] = []
        for value in uuid_values:
            if value not in users_by_id:
                missing_rows.append({'id': value, 'external_user_key': value})
        for value in external_values:
            if value not in users_by_external:
                missing_rows.append({'id': str(uuid.uuid4()), 'external_user_key': value})

        if missing_rows:
            self.repository.bulk_insert_users(missing_rows)
            self.db.flush()
            users_by_id = {item.id: item for item in self.repository.get_users_by_ids(uuid_values)}
            users_by_external = {item.external_user_key: item for item in self.repository.get_users_by_external_keys(external_values)}

        resolved: dict[str, str] = {}
        for value in user_values:
            if self._is_uuid(value):
                resolved[value] = value
            else:
                user = users_by_external.get(value)
                if user is not None:
                    resolved[value] = user.id
        return resolved

    @staticmethod
    def _is_uuid(value: str) -> bool:
        try:
            uuid.UUID(value)
            return True
        except Exception:
            return False

    @staticmethod
    def _to_float_or_none(value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _build_event_payload(item: dict[str, Any], *, batch_id: str) -> dict[str, Any]:
        allowed_keys = {
            'user_id',
            'sentiment_distribution',
            'engagement_score',
            'trending_topics',
            'influence_ranking',
            'anomaly_detected',
            'anomaly_type',
            'flags',
        }
        payload = {key: item.get(key) for key in allowed_keys if key in item}
        payload['batch_id'] = batch_id
        return payload
