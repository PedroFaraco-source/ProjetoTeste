from __future__ import annotations

import argparse
import asyncio
import random
import string
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config.settings import get_settings  # noqa: E402
from tests.smoke.wordbanks import (  # noqa: E402
    HASHTAGS_POOL,
    INTENSIFIERS,
    NEGATIVES,
    NEGATIONS,
    NEUTRALS,
    POSITIVES,
    PROFANITIES,
)


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


def random_user_id(rng: random.Random) -> str:
    suffix = ''.join(rng.choice(string.ascii_lowercase + string.digits + '_') for _ in range(8))
    return f'user_{suffix}'


def random_hashtags(rng: random.Random) -> list[str]:
    amount = rng.randint(1, 3)
    return rng.sample(HASHTAGS_POOL, k=amount)


def _to_payload_timestamp(local_now: datetime, rng: random.Random) -> str:
    offset_minutes = rng.randint(0, 180)
    event_local_time = local_now - timedelta(minutes=offset_minutes)
    event_utc_time = event_local_time.astimezone(timezone.utc)
    return event_utc_time.isoformat().replace('+00:00', 'Z')


def _resolve_timezone(timezone_name: str):
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-3))


def _build_message_content(kind: str, rng: random.Random) -> str:
    if kind == 'positive':
        return f"{rng.choice(INTENSIFIERS)} {rng.choice(POSITIVES)} {rng.choice(HASHTAGS_POOL)}"
    if kind == 'negative':
        return f"{rng.choice(NEGATIONS)} {rng.choice(NEGATIVES)} {rng.choice(HASHTAGS_POOL)}"
    if kind == 'neutral':
        return f"{rng.choice(NEUTRALS)} {rng.choice(HASHTAGS_POOL)}"
    return f"{rng.choice(PROFANITIES)} {rng.choice(HASHTAGS_POOL)}"


def _build_valid_message(rng: random.Random, local_now: datetime) -> dict:
    views = rng.randint(10, 5000)
    reactions = rng.randint(0, views)
    shares = rng.randint(0, views - reactions)

    sentiment_kind = rng.choice(['positive', 'negative', 'neutral'])
    content = _build_message_content(sentiment_kind, rng)

    return {
        'id': ''.join(rng.choice(string.ascii_lowercase + string.digits) for _ in range(16)),
        'user_id': random_user_id(rng),
        'content': content,
        'timestamp': _to_payload_timestamp(local_now, rng),
        'hashtags': random_hashtags(rng),
        'reactions': reactions,
        'shares': shares,
        'views': views,
    }


def build_valid_payload(rng: random.Random, timezone_name: str) -> dict:
    tz = _resolve_timezone(timezone_name)
    local_now = datetime.now(tz)

    message_count = rng.randint(1, 6)
    messages = [_build_valid_message(rng, local_now) for _ in range(message_count)]

    return {
        'messages': messages,
        'time_window_minutes': 30,
    }


def _invalid_payload_invalid_user(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['messages'][0]['user_id'] = 'invalid-user'
    return payload, 400


def _invalid_payload_content_too_long(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['messages'][0]['content'] = 'a' * 281
    return payload, 400


def _invalid_payload_invalid_timestamp(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['messages'][0]['timestamp'] = '2026-02-20T10:00:00'
    return payload, 400


def _invalid_payload_invalid_hashtags_type(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['messages'][0]['hashtags'] = 'nao-e-lista'
    return payload, 400


def _invalid_payload_invalid_hashtag_prefix(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['messages'][0]['hashtags'] = ['invalida']
    return payload, 400


def _invalid_payload_invalid_window(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['time_window_minutes'] = 0
    return payload, 400


def _invalid_payload_unsupported_window(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['time_window_minutes'] = 123
    return payload, 422


def _invalid_payload_views_invariant(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['messages'][0]['views'] = 1
    payload['messages'][0]['reactions'] = 1
    payload['messages'][0]['shares'] = 1
    return payload, 400


def _invalid_payload_missing_messages(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    payload = build_valid_payload(rng, timezone_name)
    payload['messages'] = []
    return payload, 400


def build_invalid_payload(rng: random.Random, timezone_name: str) -> tuple[dict, int]:
    scenarios = [
        _invalid_payload_invalid_user,
        _invalid_payload_content_too_long,
        _invalid_payload_invalid_timestamp,
        _invalid_payload_invalid_hashtags_type,
        _invalid_payload_invalid_hashtag_prefix,
        _invalid_payload_invalid_window,
        _invalid_payload_unsupported_window,
        _invalid_payload_views_invariant,
        _invalid_payload_missing_messages,
    ]
    scenario = rng.choice(scenarios)
    return scenario(rng, timezone_name)


def build_single_1500_payload(seed: int, timezone_name: str) -> dict:
    rng = random.Random(seed)
    tz = _resolve_timezone(timezone_name)
    local_now = datetime.now(tz)

    messages: list[dict] = []

    for kind in ['positive', 'negative', 'neutral']:
        for _ in range(500):
            message = _build_valid_message(rng, local_now)
            message['content'] = _build_message_content(kind, rng)
            messages.append(message)

    return {
        'messages': messages,
        'time_window_minutes': 30,
    }


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int((p / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(index, len(ordered) - 1))]


async def _send_request(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    payload: dict,
    expected_status: int | None,
) -> tuple[int, float, bool]:
    async with sem:
        started = time.perf_counter()
        try:
            response = await client.post('/analyze-feed', json=payload)
            status_code = response.status_code
        except Exception:
            status_code = 500
        latency_ms = (time.perf_counter() - started) * 1000.0
        matched = expected_status is None or status_code == expected_status
        return status_code, latency_ms, matched


async def run_load(
    base_url: str,
    requests_count: int,
    concurrency: int,
    seed: int,
    include_invalid: bool,
    timezone_name: str,
) -> None:
    rng = random.Random(seed)
    semaphore = asyncio.Semaphore(max(1, concurrency))
    status_counter: Counter[int] = Counter()
    latencies: list[float] = []
    expected_mismatch = 0

    timeout = httpx.Timeout(15.0)
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        started = time.perf_counter()
        tasks = []

        for _ in range(requests_count):
            if include_invalid and rng.random() < 0.35:
                payload, expected_status = build_invalid_payload(rng, timezone_name)
            else:
                payload = build_valid_payload(rng, timezone_name)
                expected_status = 200

            tasks.append(
                asyncio.create_task(
                    _send_request(
                        client=client,
                        sem=semaphore,
                        payload=payload,
                        expected_status=expected_status,
                    )
                )
            )

        for status_code, latency_ms, matched in await asyncio.gather(*tasks):
            status_counter[status_code] += 1
            latencies.append(latency_ms)
            if not matched:
                expected_mismatch += 1

        total_seconds = max(time.perf_counter() - started, 0.001)

    throughput = requests_count / total_seconds

    print('=== Resumo de carga ===')
    print(f'total={requests_count}')
    print(f'status_200={status_counter.get(200, 0)}')
    print(f'status_400={status_counter.get(400, 0)}')
    print(f'status_422={status_counter.get(422, 0)}')
    print(f'status_500={status_counter.get(500, 0)}')
    print(f'p50_ms={percentile(latencies, 50):.2f}')
    print(f'p95_ms={percentile(latencies, 95):.2f}')
    print(f'throughput_req_s={throughput:.2f}')
    print(f'mismatch_esperado={expected_mismatch}')


async def send_single_1500(base_url: str, seed: int, timezone_name: str) -> None:
    payload = build_single_1500_payload(seed=seed, timezone_name=timezone_name)
    timeout = httpx.Timeout(30.0)

    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        started = time.perf_counter()
        response = await client.post('/analyze-feed', json=payload)
        elapsed_ms = (time.perf_counter() - started) * 1000.0

    print('=== Execucao payload 1500 ===')
    print('messages=1500 (500 positive, 500 negative, 500 neutral)')
    print(f'status={response.status_code}')
    print(f'latency_ms={elapsed_ms:.2f}')


def main() -> None:
    settings = get_settings()
    default_base_url = f'http://localhost:{settings.api_http_port}'

    parser = argparse.ArgumentParser(description='Executa smoke/load em POST /analyze-feed.')
    parser.add_argument('--base-url', default=default_base_url)
    parser.add_argument('--requests', type=int, default=100)
    parser.add_argument('--concurrency', type=int, default=20)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--include-invalid', default='true')
    parser.add_argument('--single-1500', default='false')

    args = parser.parse_args()

    include_invalid = parse_bool(str(args.include_invalid))
    single_1500 = parse_bool(str(args.single_1500))

    if single_1500:
        asyncio.run(send_single_1500(base_url=args.base_url, seed=args.seed, timezone_name=settings.app_timezone))
        return

    asyncio.run(
        run_load(
            base_url=args.base_url,
            requests_count=max(1, args.requests),
            concurrency=max(1, args.concurrency),
            seed=args.seed,
            include_invalid=include_invalid,
            timezone_name=settings.app_timezone,
        )
    )


if __name__ == '__main__':
    main()
