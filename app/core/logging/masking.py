from __future__ import annotations

import re
from typing import Any

MAX_STRING_LENGTH = 256
MASKED_VALUE = '[MASCARADO]'

_SENSITIVE_KEY_PARTS = (
    'password',
    'token',
    'authorization',
    'x-api-key',
    'api_key',
    'cpf',
    'cnpj',
    'email',
    'secret',
    'otp',
    'hash',
    'salt',
    'connection_string',
    'refresh_token',
)

_SENSITIVE_REGEX = re.compile(
    r'(?i)(password|token|authorization|x-api-key|api_key|cpf|cnpj|email|secret|otp|hash|salt|connection_string|refresh_token)\s*[:=]\s*([^\s,;]+)'
)

_EMAIL_REGEX = re.compile(r'(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b')
_BEARER_REGEX = re.compile(r'(?i)Bearer\s+[A-Za-z0-9._\-+/=]+')


def truncate_text(value: str, max_length: int = MAX_STRING_LENGTH) -> str:
    text = str(value)
    if len(text) <= max_length:
        return text
    return text[: max_length - 3] + '...'


def is_sensitive_key(key: str) -> bool:
    normalized = str(key or '').strip().lower()
    return any(part in normalized for part in _SENSITIVE_KEY_PARTS)


def sanitize_error_text(value: Any) -> str:
    text = truncate_text(str(value or ''))
    text = _BEARER_REGEX.sub('Bearer [MASCARADO]', text)
    text = _EMAIL_REGEX.sub('[MASCARADO]', text)
    text = _SENSITIVE_REGEX.sub(lambda m: f'{m.group(1)}={MASKED_VALUE}', text)
    return truncate_text(text)


def mask_headers(headers: dict[str, Any] | None) -> dict[str, Any]:
    if not headers:
        return {}
    masked: dict[str, Any] = {}
    for key, value in headers.items():
        key_text = str(key)
        if key_text.lower() == 'authorization' or is_sensitive_key(key_text):
            masked[key_text] = MASKED_VALUE
            continue
        masked[key_text] = mask_for_log(value)
    return masked


def mask_for_log(value: Any, parent_key: str | None = None) -> Any:
    if parent_key and is_sensitive_key(parent_key):
        return MASKED_VALUE

    if isinstance(value, dict):
        masked_dict: dict[str, Any] = {}
        for key, item_value in value.items():
            key_text = str(key)
            if is_sensitive_key(key_text):
                masked_dict[key_text] = MASKED_VALUE
            else:
                masked_dict[key_text] = mask_for_log(item_value, parent_key=key_text)
        return masked_dict

    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        sample = mask_for_log(sequence[0]) if sequence else None
        return {
            'items_count': len(sequence),
            'first_item_sample': sample,
        }

    if isinstance(value, str):
        return sanitize_error_text(value)

    if isinstance(value, (int, float, bool)) or value is None:
        return value

    return truncate_text(str(value))


def extract_items_count(payload: Any) -> int:
    if isinstance(payload, dict):
        for candidate_key in ('items', 'messages'):
            candidate = payload.get(candidate_key)
            if isinstance(candidate, list):
                return len(candidate)
        return 0
    if isinstance(payload, list):
        return len(payload)
    return 0


def compact_payload_for_audit(payload: Any) -> Any:
    if isinstance(payload, dict):
        compacted: dict[str, Any] = {}
        for key, value in payload.items():
            key_text = str(key)
            if key_text in {'items', 'messages'} and isinstance(value, list):
                compacted[f'{key_text}_count'] = len(value)
                compacted['first_item_sample'] = mask_for_log(value[0]) if value else None
                continue
            compacted[key_text] = mask_for_log(value, parent_key=key_text)
        return compacted
    if isinstance(payload, list):
        return {
            'items_count': len(payload),
            'first_item_sample': mask_for_log(payload[0]) if payload else None,
        }
    return mask_for_log(payload)
