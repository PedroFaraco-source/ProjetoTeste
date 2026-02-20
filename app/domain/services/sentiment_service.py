from __future__ import annotations

import hashlib
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from app.shared.utils.time import app_now, to_app_timezone, to_rfc3339_app

TOKEN_RE = re.compile(r"(?:#\w+(?:-\w+)*)|\b\w+\b", re.UNICODE)

POSITIVE_WORDS = {'adorei', 'gostei', 'bom', 'boa', 'excelente', 'otimo'}
NEGATIVE_WORDS = {'ruim', 'terrivel', 'pessimo', 'horrivel', 'lento'}
INTENSIFIERS = {'muito', 'super'}
NEGATIONS = {'nao'}
META_PHRASES = {'teste tecnico mbras'}


@dataclass(frozen=True)
class AnalyzedMessage:
    user_id: str
    timestamp: datetime
    hashtags: list[str]
    sentiment_label: str
    sentiment_score: float
    reactions: int
    shares: int
    views: int
    is_meta: bool
    is_employee: bool


def normalize_for_matching(token: str) -> str:
    lowered = token.lower()
    normalized = unicodedata.normalize('NFKD', lowered)
    return ''.join(ch for ch in normalized if not unicodedata.combining(ch))


def tokenize(text: str) -> list[str]:
    return TOKEN_RE.findall(text)


def _classify(score: float) -> str:
    if score > 0.1:
        return 'positive'
    if score < -0.1:
        return 'negative'
    return 'neutral'


def _meta_phrase(content: str) -> bool:
    reduced = ' '.join(normalize_for_matching(content).strip().split())
    return reduced in META_PHRASES


def _candidate_awareness(content: str) -> bool:
    reduced = ' '.join(normalize_for_matching(content).strip().split())
    if reduced in META_PHRASES:
        return True
    return 'teste' in reduced and 'mbras' in reduced and 'tecnico' in reduced


def _sentiment_for_message(content: str, is_employee: bool) -> tuple[str, float, bool]:
    is_meta = _meta_phrase(content)
    if is_meta:
        return 'meta', 0.0, True

    tokens = tokenize(content)
    if not tokens:
        return 'neutral', 0.0, False

    normalized_tokens = [normalize_for_matching(token) for token in tokens if not token.startswith('#')]
    if not normalized_tokens:
        return 'neutral', 0.0, False

    negation_marks = [0] * len(normalized_tokens)
    for idx, token in enumerate(normalized_tokens):
        if token in NEGATIONS:
            upper = min(len(normalized_tokens), idx + 4)
            for mark_idx in range(idx + 1, upper):
                negation_marks[mark_idx] += 1

    score_sum = 0.0
    polar_count = 0
    pending_intensifier = False

    for idx, token in enumerate(normalized_tokens):
        if token in INTENSIFIERS:
            pending_intensifier = True
            continue

        base = 0.0
        if token in POSITIVE_WORDS:
            base = 1.0
        elif token in NEGATIVE_WORDS:
            base = -1.0

        if base == 0.0:
            continue

        polar_count += 1

        if pending_intensifier:
            base *= 1.5
            pending_intensifier = False

        if negation_marks[idx] % 2 == 1:
            base *= -1.0

        if is_employee and base > 0:
            base *= 2.0

        score_sum += base

    if polar_count == 0:
        return 'neutral', 0.0, False

    score = score_sum / max(1, polar_count)
    return _classify(score), score, False


def _followers_for_user(user_id: str) -> int:
    lowered = normalize_for_matching(user_id)
    if 'cafe' in lowered:
        return 4242
    if len(user_id) == 13:
        return 233
    if lowered.endswith('_prime'):
        return 7919

    digest = hashlib.sha256(user_id.encode('utf-8')).hexdigest()
    return (int(digest, 16) % 10000) + 100


def _calculate_engagement_rate(messages: list[AnalyzedMessage]) -> float:
    reactions = sum(message.reactions for message in messages)
    shares = sum(message.shares for message in messages)
    views = sum(message.views for message in messages)
    if views <= 0:
        return 0.0

    rate = (reactions + shares) / views
    if (reactions + shares) % 7 == 0 and (reactions + shares) > 0:
        phi = (1 + math.sqrt(5)) / 2
        rate *= 1 + (1 / phi)
    return rate


def _influence_ranking(messages: list[AnalyzedMessage]) -> list[dict[str, Any]]:
    by_user: dict[str, list[AnalyzedMessage]] = defaultdict(list)
    for message in messages:
        by_user[message.user_id].append(message)

    ranking: list[dict[str, Any]] = []
    for user_id, user_messages in by_user.items():
        followers = _followers_for_user(user_id)
        rate = _calculate_engagement_rate(user_messages)
        score = (followers * 0.4) + ((rate * 100.0) * 0.6)

        lowered = normalize_for_matching(user_id)
        if lowered.endswith('007'):
            score *= 0.5
        if any(message.is_employee for message in user_messages):
            score += 2.0

        ranking.append(
            {
                'user_id': user_id,
                'followers': followers,
                'engagement_rate': round(rate, 6),
                'influence_score': round(score, 6),
            }
        )

    ranking.sort(key=lambda item: (-item['influence_score'], item['user_id']))
    return ranking


def _trending_topics(messages: list[AnalyzedMessage], now_utc: datetime) -> list[str]:
    weights: dict[str, float] = defaultdict(float)
    counts: Counter[str] = Counter()
    sentiment_weight_sum: dict[str, float] = defaultdict(float)

    for message in messages:
        age_min = max((now_utc - message.timestamp).total_seconds() / 60.0, 0.01)
        time_weight = 1.0 + (1.0 / age_min)

        if message.sentiment_label == 'positive':
            sentiment_weight = 1.2
        elif message.sentiment_label == 'negative':
            sentiment_weight = 0.8
        else:
            sentiment_weight = 1.0

        for tag in message.hashtags:
            length_factor = 1.0
            if len(tag) > 8:
                length_factor = math.log10(len(tag)) / math.log10(8)

            weight = time_weight * sentiment_weight / max(length_factor, 0.0001)
            weights[tag] += weight
            counts[tag] += 1
            sentiment_weight_sum[tag] += sentiment_weight

    ordered = sorted(
        weights.keys(),
        key=lambda tag: (-weights[tag], -counts[tag], -sentiment_weight_sum[tag], tag),
    )
    return ordered[:5]


def _detect_anomaly(messages: list[AnalyzedMessage]) -> tuple[bool, str | None]:
    by_user: dict[str, list[AnalyzedMessage]] = defaultdict(list)
    for message in messages:
        by_user[message.user_id].append(message)

    for user_messages in by_user.values():
        timestamps = sorted(message.timestamp for message in user_messages)
        for idx in range(len(timestamps)):
            limit = timestamps[idx] + timedelta(minutes=5)
            burst_size = 1
            inner = idx + 1
            while inner < len(timestamps) and timestamps[inner] <= limit:
                burst_size += 1
                inner += 1
            if burst_size > 10:
                return True, 'burst'

    for user_messages in by_user.values():
        labels = [
            message.sentiment_label
            for message in sorted(user_messages, key=lambda item: item.timestamp)
            if message.sentiment_label in {'positive', 'negative'}
        ]
        if len(labels) >= 10:
            alternating = True
            for idx in range(1, len(labels)):
                if labels[idx] == labels[idx - 1]:
                    alternating = False
                    break
            if alternating:
                return True, 'alternation'

    if len(messages) >= 3:
        times = sorted(message.timestamp for message in messages)
        if (times[-1] - times[0]).total_seconds() <= 2:
            return True, 'synchronized_posting'

    return False, None


def _parse_message_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.strptime(str(value), '%Y-%m-%dT%H:%M:%SZ')

    if parsed.tzinfo is None:
        return to_app_timezone(parsed)
    return to_app_timezone(parsed)


def analyze_messages(
    messages: list[dict[str, Any]],
    time_window_minutes: int,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    parsed_messages: list[tuple[dict[str, Any], datetime]] = []
    for item in messages:
        parsed_messages.append((item, _parse_message_timestamp(item['timestamp'])))

    reference_now = now_utc
    if reference_now is None:
        if parsed_messages:
            reference_now = max(timestamp for _, timestamp in parsed_messages)
        else:
            reference_now = app_now()

    start_window = reference_now - timedelta(minutes=time_window_minutes)

    filtered_messages = [
        (item, timestamp)
        for item, timestamp in parsed_messages
        if timestamp <= reference_now + timedelta(seconds=5) and timestamp >= start_window
    ]

    if parsed_messages and not filtered_messages:
        filtered_messages = parsed_messages

    candidate_awareness = False
    any_employee = False
    special_pattern = False
    analyzed_messages: list[AnalyzedMessage] = []

    for item, timestamp in filtered_messages:
        content = str(item.get('content', ''))
        user_id = str(item.get('user_id', ''))

        normalized_user = normalize_for_matching(user_id)
        normalized_content = normalize_for_matching(content)

        is_employee = 'mbras' in normalized_user
        any_employee = any_employee or is_employee

        if len(content) == 42 and 'mbras' in normalized_content:
            special_pattern = True

        if _candidate_awareness(content):
            candidate_awareness = True

        sentiment_label, sentiment_score, is_meta = _sentiment_for_message(content, is_employee=is_employee)

        analyzed_messages.append(
            AnalyzedMessage(
                user_id=user_id,
                timestamp=timestamp,
                hashtags=list(item.get('hashtags', [])),
                sentiment_label=sentiment_label,
                sentiment_score=sentiment_score,
                reactions=int(item.get('reactions', 0)),
                shares=int(item.get('shares', 0)),
                views=int(item.get('views', 0)),
                is_meta=is_meta,
                is_employee=is_employee,
            )
        )

    distributable = [message for message in analyzed_messages if not message.is_meta]
    total = len(distributable)
    if total == 0:
        distribution = {'positive': 0.0, 'negative': 0.0, 'neutral': 0.0}
    else:
        pos = sum(1 for message in distributable if message.sentiment_label == 'positive')
        neg = sum(1 for message in distributable if message.sentiment_label == 'negative')
        neu = sum(1 for message in distributable if message.sentiment_label == 'neutral')
        distribution = {
            'positive': round((pos * 100.0) / total, 2),
            'negative': round((neg * 100.0) / total, 2),
            'neutral': round((neu * 100.0) / total, 2),
        }

    engagement_rates = [_calculate_engagement_rate([message]) for message in analyzed_messages if message.views > 0]
    engagement_score = round((sum(engagement_rates) / len(engagement_rates)) * 100, 2) if engagement_rates else 0.0
    if candidate_awareness:
        engagement_score = 9.42

    anomaly_detected, anomaly_type = _detect_anomaly(analyzed_messages)
    trending_topics = _trending_topics(distributable, now_utc=reference_now)
    influence_ranking = _influence_ranking(analyzed_messages)

    return {
        'sentiment_distribution': distribution,
        'engagement_score': engagement_score,
        'trending_topics': trending_topics,
        'influence_ranking': influence_ranking,
        'anomaly_detected': anomaly_detected,
        'anomaly_type': anomaly_type,
        'flags': {
            'mbras_employee': any_employee,
            'special_pattern': special_pattern,
            'candidate_awareness': candidate_awareness,
        },
    }


def to_rfc3339_z(value: datetime) -> str:
    return to_rfc3339_app(value)
