from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from app.core.errors.http_exceptions import ApiValidationError
from app.shared.utils.time import to_app_timezone

USER_ID_PATTERN = re.compile(r'^user_[a-z0-9_]{3,}$', re.IGNORECASE)


class AnalyzeMessage(BaseModel):
    id: str | None = None
    user_id: str
    content: str
    timestamp: datetime
    hashtags: list[str]
    reactions: int = 0
    shares: int = 0
    views: int = 0


class AnalyzeFeedRequest(BaseModel):
    messages: list[AnalyzeMessage] = Field(min_length=1)
    time_window_minutes: int


class SentimentDistribution(BaseModel):
    positive: float
    negative: float
    neutral: float


class AnalysisFlags(BaseModel):
    mbras_employee: bool = False
    special_pattern: bool = False
    candidate_awareness: bool = False


class InfluenceRankingEntry(BaseModel):
    user_id: str
    followers: int
    engagement_rate: float
    influence_score: float


class AnalysisResponsePayload(BaseModel):
    sentiment_distribution: SentimentDistribution
    engagement_score: float
    trending_topics: list[str]
    influence_ranking: list[InfluenceRankingEntry]
    anomaly_detected: bool
    anomaly_type: str | None = None
    flags: AnalysisFlags


class AnalyzeFeedResponse(BaseModel):
    analysis: AnalysisResponsePayload


class MessageListItemResponse(BaseModel):
    id: str
    created_at_utc: str
    correlation_id: str
    user_id: str
    user_external_key: str | None = None
    engagement_score: float | None = None
    analysis: AnalysisResponsePayload
    processing_success: bool | None = None
    processing_status: str | None = None
    failure_stage: str | None = None
    failure_reason: str | None = None
    queue_messaging: str | None = None
    elastic_name: str | None = None
    elastic_index_name: str | None = None
    processed_at_utc: str | None = None


class MessagesPageResponse(BaseModel):
    page: int
    page_size: int
    total: int
    items: list[MessageListItemResponse]


def parse_rfc3339_z(value: Any, code: str = 'INVALID_TIMESTAMP') -> datetime:
    if not isinstance(value, str):
        raise ApiValidationError(400, 'Timestamp invalido.', code)
    raw = value.strip()
    normalized = raw.replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ApiValidationError(400, 'Timestamp invalido.', code) from exc
    if parsed.tzinfo is None:
        raise ApiValidationError(400, 'Timestamp invalido.', code)
    return to_app_timezone(parsed)


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
        return True
    except Exception:
        return False


def validate_analyze_payload(payload: Any) -> AnalyzeFeedRequest:
    if not isinstance(payload, dict):
        raise ApiValidationError(400, 'Corpo da requisicao invalido.', 'INVALID_REQUEST')

    time_window = payload.get('time_window_minutes')
    if not isinstance(time_window, int):
        raise ApiValidationError(400, 'Janela temporal invalida.', 'INVALID_TIME_WINDOW')
    if time_window == 123:
        raise ApiValidationError(
            422,
            'Valor de janela temporal não suportado na versão atual',
            'UNSUPPORTED_TIME_WINDOW',
        )
    if time_window <= 0:
        raise ApiValidationError(400, 'Janela temporal invalida.', 'INVALID_TIME_WINDOW')

    raw_messages = payload.get('messages')
    if not isinstance(raw_messages, list) or not raw_messages:
        raise ApiValidationError(400, 'Lista de mensagens invalida.', 'INVALID_MESSAGES')

    normalized_messages: list[AnalyzeMessage] = []
    for item in raw_messages:
        if not isinstance(item, dict):
            raise ApiValidationError(400, 'Mensagem invalida.', 'INVALID_MESSAGE')

        user_id = item.get('user_id')
        if not isinstance(user_id, str):
            raise ApiValidationError(400, 'user_id invalido.', 'INVALID_USER_ID')
        cleaned_user_id = user_id.strip()
        if USER_ID_PATTERN.fullmatch(cleaned_user_id) is None and not _is_uuid(cleaned_user_id):
            raise ApiValidationError(400, 'user_id invalido.', 'INVALID_USER_ID')

        content = item.get('content')
        if not isinstance(content, str):
            raise ApiValidationError(400, 'Conteudo invalido.', 'INVALID_CONTENT')
        content = content.strip()
        if not content or len(content) > 280:
            raise ApiValidationError(400, 'Conteudo invalido.', 'INVALID_CONTENT')

        timestamp = parse_rfc3339_z(item.get('timestamp'))

        hashtags = item.get('hashtags')
        if not isinstance(hashtags, list):
            raise ApiValidationError(400, 'Hashtags invalidas.', 'INVALID_HASHTAGS')
        normalized_hashtags: list[str] = []
        for tag in hashtags:
            if not isinstance(tag, str):
                raise ApiValidationError(400, 'Hashtags invalidas.', 'INVALID_HASHTAGS')
            clean_tag = tag.strip()
            if not clean_tag.startswith('#') or len(clean_tag) < 2:
                raise ApiValidationError(400, 'Hashtags invalidas.', 'INVALID_HASHTAGS')
            normalized_hashtags.append(clean_tag)

        reactions = _read_non_negative_int(item.get('reactions', 0), 'INVALID_REACTIONS')
        shares = _read_non_negative_int(item.get('shares', 0), 'INVALID_SHARES')
        views = _read_non_negative_int(item.get('views', 0), 'INVALID_VIEWS')
        if views < (reactions + shares):
            raise ApiValidationError(400, 'Views invalidas.', 'INVALID_VIEWS')

        normalized_messages.append(
            AnalyzeMessage(
                id=str(item.get('id', '')).strip() or None,
                user_id=cleaned_user_id,
                content=content,
                timestamp=timestamp,
                hashtags=normalized_hashtags,
                reactions=reactions,
                shares=shares,
                views=views,
            )
        )

    return AnalyzeFeedRequest(messages=normalized_messages, time_window_minutes=time_window)


def _read_non_negative_int(value: Any, code: str) -> int:
    if not isinstance(value, int):
        raise ApiValidationError(400, 'Valor numerico invalido.', code)
    if value < 0:
        raise ApiValidationError(400, 'Valor numerico invalido.', code)
    return value
