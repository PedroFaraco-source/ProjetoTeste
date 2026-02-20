from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.core.errors.http_exceptions import ApiValidationError


class BatchIngestResponse(BaseModel):
    batch_id: str
    accepted: int


class BatchIngestRequest(BaseModel):
    items: list[dict[str, Any]] = Field(min_length=1, max_length=1000)


def validate_batch_payload(payload: Any) -> BatchIngestRequest:
    if not isinstance(payload, dict):
        raise ApiValidationError(400, 'Corpo da requisicao invalido.', 'INVALID_REQUEST')

    items = payload.get('items')
    if not isinstance(items, list) or not items:
        raise ApiValidationError(400, 'Lista de itens invalida.', 'INVALID_ITEMS')
    if len(items) > 1000:
        raise ApiValidationError(400, 'Lote excede o limite de 1000 itens.', 'BATCH_LIMIT_EXCEEDED')

    normalized: list[dict[str, Any]] = []
    for idx, item in enumerate(items):
        normalized.append(validate_batch_item(item=item, item_index=idx))

    return BatchIngestRequest(items=normalized)


def validate_batch_item(*, item: Any, item_index: int) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise ApiValidationError(400, f'Item {item_index} invalido.', 'INVALID_ITEM')

    sentiment = item.get('sentiment_distribution')
    if not isinstance(sentiment, dict):
        raise ApiValidationError(400, f'Item {item_index} sem sentiment_distribution valido.', 'INVALID_SENTIMENT_DISTRIBUTION')

    for key in ('positive', 'negative', 'neutral'):
        value = sentiment.get(key)
        if not isinstance(value, (int, float)):
            raise ApiValidationError(400, f'Item {item_index} com campo {key} invalido.', 'INVALID_SENTIMENT_DISTRIBUTION')

    flags = item.get('flags')
    if flags is not None:
        if not isinstance(flags, dict):
            raise ApiValidationError(400, f'Item {item_index} com flags invalidas.', 'INVALID_FLAGS')
        for key in ('mbras_employee', 'special_pattern', 'candidate_awareness'):
            if key in flags and not isinstance(flags.get(key), bool):
                raise ApiValidationError(400, f'Item {item_index} com flag {key} invalida.', 'INVALID_FLAGS')

    influence_ranking = item.get('influence_ranking')
    if influence_ranking is not None and not isinstance(influence_ranking, list):
        raise ApiValidationError(400, f'Item {item_index} com influence_ranking invalido.', 'INVALID_INFLUENCE_RANKING')

    user_id = item.get('user_id')
    if user_id is None or not str(user_id).strip():
        raise ApiValidationError(400, f'Item {item_index} sem user_id.', 'INVALID_USER_ID')

    cleaned = dict(item)
    cleaned['user_id'] = str(user_id).strip()
    return cleaned
