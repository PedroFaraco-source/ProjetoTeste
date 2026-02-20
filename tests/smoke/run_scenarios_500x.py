from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import random
import string
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.smoke.wordbanks import HASHTAGS_POOL, NEGATIVES, NEUTRALS, POSITIVES  # noqa: E402

REQUIRED_ANALYSIS_KEYS = {
    'sentiment_distribution',
    'engagement_score',
    'trending_topics',
    'influence_ranking',
    'anomaly_detected',
    'anomaly_type',
    'flags',
}


@dataclass
class ScenarioResult:
    scenario: str
    status_code: int
    latency_ms: float
    passed: bool
    detail: str


@dataclass(frozen=True)
class ScenarioTemplate:
    name: str
    expected_status: int
    kind: str  # valid | invalid
    build_payload: Callable[[random.Random, datetime], dict[str, Any]]
    validate_response: Callable[[int, dict[str, Any]], tuple[bool, str]]


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {'1', 'true', 'yes', 'on'}


def _resolve_sao_paulo_timezone():
    try:
        return ZoneInfo('America/Sao_Paulo')
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-3))


def _rng_for(seed: int, scenario_name: str, repetition: int) -> random.Random:
    digest = hashlib.sha256(f'{seed}:{scenario_name}:{repetition}'.encode('utf-8')).hexdigest()
    return random.Random(int(digest[:16], 16))


def _random_id(rng: random.Random, length: int = 16) -> str:
    return ''.join(rng.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def _random_user_id(rng: random.Random) -> str:
    suffix = ''.join(rng.choice(string.ascii_lowercase + string.digits + '_') for _ in range(8))
    return f'user_{suffix}'


def _random_timestamp_z(rng: random.Random, base_now_sp: datetime) -> str:
    offset_seconds = rng.randint(0, 30 * 60)
    event_sp = base_now_sp - timedelta(seconds=offset_seconds)
    return event_sp.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _random_hashtags(rng: random.Random, min_count: int = 1, max_count: int = 3) -> list[str]:
    count = rng.randint(min_count, max_count)
    return rng.sample(HASHTAGS_POOL, k=count)


def _valid_engagement(rng: random.Random) -> tuple[int, int, int]:
    views = rng.randint(10, 5000)
    reactions = rng.randint(0, views)
    shares = rng.randint(0, views - reactions)
    return reactions, shares, views


def _build_message(
    rng: random.Random,
    base_now_sp: datetime,
    *,
    content: str,
    user_id: str | None = None,
    hashtags: list[str] | None = None,
    reactions: int | None = None,
    shares: int | None = None,
    views: int | None = None,
) -> dict[str, Any]:
    default_reactions, default_shares, default_views = _valid_engagement(rng)
    return {
        'id': _random_id(rng),
        'content': content,
        'timestamp': _random_timestamp_z(rng, base_now_sp),
        'user_id': user_id or _random_user_id(rng),
        'hashtags': hashtags or _random_hashtags(rng),
        'reactions': default_reactions if reactions is None else reactions,
        'shares': default_shares if shares is None else shares,
        'views': default_views if views is None else views,
    }


def _make_special_pattern_content() -> str:
    content = 'mbras ' + ('á' * 36)
    if len(content) != 42:
        raise RuntimeError('Falha ao montar conteudo especial de 42 caracteres.')
    return content


def _validate_422_exact(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    expected = {
        'error': 'Valor de janela temporal não suportado na versão atual',
        'code': 'UNSUPPORTED_TIME_WINDOW',
    }
    if status_code != 422:
        return False, f'status_inesperado={status_code}'
    if body != expected:
        return False, 'payload_422_diferente_do_esperado'
    return True, 'ok'


def _validate_400(status_code: int, _: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 400:
        return False, f'status_inesperado={status_code}'
    return True, 'ok'


def _validate_basic_positive(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    analysis = body.get('analysis')
    if not isinstance(analysis, dict):
        return False, 'analysis_ausente'
    if not REQUIRED_ANALYSIS_KEYS.issubset(set(analysis.keys())):
        return False, 'analysis_sem_chaves_obrigatorias'
    distribution = analysis.get('sentiment_distribution', {})
    if distribution.get('positive') != 100.0:
        return False, 'positivo_nao_100'
    trending = analysis.get('trending_topics', [])
    if '#produto' not in trending:
        return False, 'hashtag_produto_ausente'
    return True, 'ok'


def _validate_meta_flags(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    analysis = body.get('analysis', {})
    flags = analysis.get('flags', {})
    distribution = analysis.get('sentiment_distribution', {})

    if flags.get('mbras_employee') is not True:
        return False, 'flag_mbras_employee_invalida'
    if flags.get('candidate_awareness') is not True:
        return False, 'flag_candidate_awareness_invalida'
    if analysis.get('engagement_score') != 9.42:
        return False, 'engagement_score_invalido'
    if distribution != {'positive': 0.0, 'negative': 0.0, 'neutral': 0.0}:
        return False, 'distribuicao_meta_invalida'
    return True, 'ok'


def _validate_neutral_100(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    distribution = body.get('analysis', {}).get('sentiment_distribution', {})
    if distribution.get('neutral') != 100.0:
        return False, 'neutral_nao_100'
    return True, 'ok'


def _validate_positive_100(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    distribution = body.get('analysis', {}).get('sentiment_distribution', {})
    if distribution.get('positive') != 100.0:
        return False, 'positive_nao_100'
    return True, 'ok'


def _validate_mbras_flag(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    flags = body.get('analysis', {}).get('flags', {})
    if flags.get('mbras_employee') is not True:
        return False, 'flag_mbras_employee_invalida'
    return True, 'ok'


def _validate_special_pattern(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    flags = body.get('analysis', {}).get('flags', {})
    if flags.get('special_pattern') is not True:
        return False, 'flag_special_pattern_invalida'
    return True, 'ok'


def _validate_influence_user(target_user: str) -> Callable[[int, dict[str, Any]], tuple[bool, str]]:
    def _validator(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
        if status_code != 200:
            return False, f'status_inesperado={status_code}'
        ranking = body.get('analysis', {}).get('influence_ranking', [])
        users = [item.get('user_id') for item in ranking if isinstance(item, dict)]
        if target_user not in users:
            return False, f'usuario_ausente_no_ranking={target_user}'
        return True, 'ok'

    return _validator


def _validate_cross_trending(status_code: int, body: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    trending = body.get('analysis', {}).get('trending_topics', [])
    if '#positivo' in trending and '#negativo' in trending:
        if trending.index('#positivo') > trending.index('#negativo'):
            return False, 'ordem_trending_invalida'
    return True, 'ok'


def _validate_only_200(status_code: int, _: dict[str, Any]) -> tuple[bool, str]:
    if status_code != 200:
        return False, f'status_inesperado={status_code}'
    return True, 'ok'


def _build_templates(include_invalid: bool) -> list[ScenarioTemplate]:
    valid_templates: list[ScenarioTemplate] = [
        ScenarioTemplate(
            name='mandatory_basic_positive',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='adorei produto #produto',
                        hashtags=['#produto'],
                        reactions=2,
                        shares=1,
                        views=10,
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_basic_positive,
        ),
        ScenarioTemplate(
            name='mandatory_window_123',
            expected_status=422,
            kind='invalid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='adorei #produto',
                        hashtags=['#produto'],
                    )
                ],
                'time_window_minutes': 123,
            },
            validate_response=_validate_422_exact,
        ),
        ScenarioTemplate(
            name='mandatory_flags_meta',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='teste técnico mbras',
                        user_id='user_mbras_meta001',
                        hashtags=['#mbras'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_meta_flags,
        ),
        ScenarioTemplate(
            name='mandatory_intensifier_orphan',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='muito',
                        hashtags=['#neutro'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_neutral_100,
        ),
        ScenarioTemplate(
            name='mandatory_double_negation',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='não não gostei',
                        hashtags=['#positivo'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_positive_100,
        ),
        ScenarioTemplate(
            name='mandatory_case_insensitive_mbras',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom atendimento',
                        user_id='user_MBRAS_007',
                        hashtags=['#mbras'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_mbras_flag,
        ),
        ScenarioTemplate(
            name='edge_special_pattern',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content=_make_special_pattern_content(),
                        hashtags=['#mbras'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_special_pattern,
        ),
        ScenarioTemplate(
            name='edge_unicode_trap_user_cafe',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom #produto',
                        user_id='user_café',
                        hashtags=['#produto'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_influence_user('user_café'),
        ),
        ScenarioTemplate(
            name='edge_13chars_trap',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom #produto',
                        user_id='user_13chars',
                        hashtags=['#produto'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_influence_user('user_13chars'),
        ),
        ScenarioTemplate(
            name='edge_prime_trap',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom #produto',
                        user_id='user_math_prime',
                        hashtags=['#produto'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_influence_user('user_math_prime'),
        ),
        ScenarioTemplate(
            name='edge_golden_ratio_trap',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom #produto',
                        hashtags=['#produto'],
                        reactions=4,
                        shares=3,
                        views=20,
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_only_200,
        ),
        ScenarioTemplate(
            name='edge_cross_trending_validation',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content=f'{rng.choice(POSITIVES)} #positivo',
                        hashtags=['#positivo'],
                        reactions=2,
                        shares=1,
                        views=20,
                    ),
                    _build_message(
                        rng,
                        now_sp,
                        content=f'{rng.choice(NEGATIVES)} #negativo',
                        hashtags=['#negativo'],
                        reactions=1,
                        shares=0,
                        views=20,
                    ),
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_cross_trending,
        ),
        ScenarioTemplate(
            name='edge_long_hashtag_decay',
            expected_status=200,
            kind='valid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content=f'{rng.choice(NEUTRALS)} #short #verylonghashtag',
                        hashtags=['#short', '#verylonghashtag'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_only_200,
        ),
    ]

    if not include_invalid:
        return valid_templates

    invalid_templates: list[ScenarioTemplate] = [
        ScenarioTemplate(
            name='invalid_views_invariant',
            expected_status=400,
            kind='invalid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom #produto',
                        hashtags=['#produto'],
                        reactions=5,
                        shares=5,
                        views=5,
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_400,
        ),
        ScenarioTemplate(
            name='invalid_timestamp_missing_z',
            expected_status=400,
            kind='invalid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    {
                        **_build_message(rng, now_sp, content='bom #produto', hashtags=['#produto']),
                        'timestamp': _random_timestamp_z(rng, now_sp).replace('Z', ''),
                    }
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_400,
        ),
        ScenarioTemplate(
            name='invalid_hashtag_without_prefix',
            expected_status=400,
            kind='invalid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom produto',
                        hashtags=['invalida'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_400,
        ),
        ScenarioTemplate(
            name='invalid_content_too_long',
            expected_status=400,
            kind='invalid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='x' * 281,
                        hashtags=['#produto'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_400,
        ),
        ScenarioTemplate(
            name='invalid_user_regex',
            expected_status=400,
            kind='invalid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom #produto',
                        user_id='usuario-invalido',
                        hashtags=['#produto'],
                    )
                ],
                'time_window_minutes': 30,
            },
            validate_response=_validate_400,
        ),
        ScenarioTemplate(
            name='invalid_window_le_zero',
            expected_status=400,
            kind='invalid',
            build_payload=lambda rng, now_sp: {
                'messages': [
                    _build_message(
                        rng,
                        now_sp,
                        content='bom #produto',
                        hashtags=['#produto'],
                    )
                ],
                'time_window_minutes': 0,
            },
            validate_response=_validate_400,
        ),
    ]
    return valid_templates + invalid_templates


async def _execute_case(
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    scenario: ScenarioTemplate,
    payload: dict[str, Any],
) -> ScenarioResult:
    async with semaphore:
        started = time.perf_counter()
        try:
            response = await client.post('/analyze-feed', json=payload)
            status_code = response.status_code
            try:
                body = response.json()
            except Exception:
                body = {}
        except Exception:
            status_code = 599
            body = {}
        latency_ms = (time.perf_counter() - started) * 1000.0

    passed, detail = scenario.validate_response(status_code, body if isinstance(body, dict) else {})
    return ScenarioResult(
        scenario=scenario.name,
        status_code=status_code,
        latency_ms=latency_ms,
        passed=passed,
        detail=detail,
    )


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = int((p / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(index, len(ordered) - 1))]


def _json_safe(status_counter: dict[int, int]) -> dict[str, int]:
    return {str(key): value for key, value in status_counter.items()}


async def run_scenarios(
    *,
    base_url: str,
    seed: int,
    repeat: int,
    concurrency: int,
    timeout_seconds: int,
    include_invalid: bool,
    json_report: str | None,
) -> None:
    timezone_sp = _resolve_sao_paulo_timezone()
    base_now_sp = datetime.now(timezone_sp)
    templates = _build_templates(include_invalid=include_invalid)

    all_cases: list[tuple[ScenarioTemplate, dict[str, Any]]] = []
    for template in templates:
        for iteration in range(repeat):
            rng = _rng_for(seed, template.name, iteration)
            payload = template.build_payload(rng, base_now_sp)
            all_cases.append((template, payload))

    semaphore = asyncio.Semaphore(max(1, concurrency))
    timeout = httpx.Timeout(timeout_seconds)

    scenario_pass: dict[str, int] = {template.name: 0 for template in templates}
    scenario_fail: dict[str, int] = {template.name: 0 for template in templates}
    status_counter: dict[int, int] = {}
    latencies: list[float] = []

    started_at = time.perf_counter()
    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:
        tasks = [
            asyncio.create_task(_execute_case(client, semaphore, scenario, payload))
            for scenario, payload in all_cases
        ]
        results = await asyncio.gather(*tasks)
    elapsed_seconds = max(time.perf_counter() - started_at, 0.001)

    for result in results:
        status_counter[result.status_code] = status_counter.get(result.status_code, 0) + 1
        latencies.append(result.latency_ms)
        if result.passed:
            scenario_pass[result.scenario] += 1
        else:
            scenario_fail[result.scenario] += 1

    total_requests = len(results)
    status_5xx = sum(count for status, count in status_counter.items() if status >= 500)
    throughput = total_requests / elapsed_seconds

    print('=== Resumo de cenarios ===')
    print(f'total_requisicoes={total_requests}')
    print(f'status_200={status_counter.get(200, 0)}')
    print(f'status_400={status_counter.get(400, 0)}')
    print(f'status_422={status_counter.get(422, 0)}')
    print(f'status_5xx={status_5xx}')
    print(f'p50_ms={_percentile(latencies, 50):.2f}')
    print(f'p95_ms={_percentile(latencies, 95):.2f}')
    print(f'throughput_req_s={throughput:.2f}')
    print('--- Cenarios ---')
    for scenario_name in sorted(scenario_pass.keys()):
        print(
            f'{scenario_name}: pass={scenario_pass[scenario_name]} fail={scenario_fail[scenario_name]}'
        )

    if json_report:
        report_payload = {
            'base_url': base_url,
            'seed': seed,
            'repeat': repeat,
            'concurrency': concurrency,
            'timeout_seconds': timeout_seconds,
            'include_invalid': include_invalid,
            'total_requests': total_requests,
            'elapsed_seconds': elapsed_seconds,
            'throughput_req_s': throughput,
            'latency': {
                'p50_ms': _percentile(latencies, 50),
                'p95_ms': _percentile(latencies, 95),
            },
            'status_counts': _json_safe(status_counter),
            'scenario_pass': scenario_pass,
            'scenario_fail': scenario_fail,
        }
        Path(json_report).write_text(
            json.dumps(report_payload, ensure_ascii=False, indent=2),
            encoding='utf-8',
        )
        print(f'relatorio_json_salvo={json_report}')


def main() -> None:
    parser = argparse.ArgumentParser(description='Executa stress de cenarios em /analyze-feed.')
    parser.add_argument('--base-url', default='http://localhost:8000')
    parser.add_argument('--seed', type=int, default=123)
    parser.add_argument('--repeat', type=int, default=500)
    parser.add_argument('--concurrency', type=int, default=20)
    parser.add_argument('--timeout-seconds', type=int, default=10)
    parser.add_argument('--include-invalid', default='true')
    parser.add_argument('--json-report', default='')
    args = parser.parse_args()

    asyncio.run(
        run_scenarios(
            base_url=args.base_url,
            seed=args.seed,
            repeat=max(1, args.repeat),
            concurrency=max(1, args.concurrency),
            timeout_seconds=max(1, args.timeout_seconds),
            include_invalid=parse_bool(str(args.include_invalid)),
            json_report=args.json_report or None,
        )
    )


if __name__ == '__main__':
    main()

