from __future__ import annotations

import argparse
import asyncio
import json
import random
import string
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.smoke.wordbanks import HASHTAGS_POOL, NEGATIVES, NEUTRALS, POSITIVES  # noqa: E402


def parse_bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = int((p / 100.0) * (len(ordered) - 1))
    return ordered[max(0, min(idx, len(ordered) - 1))]


def resolve_timezone() -> Any:
    try:
        return ZoneInfo("America/Sao_Paulo")
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-3))


def sanitize_base_url(base_url: str) -> str:
    parsed = urlsplit(base_url)
    host = parsed.hostname or "indefinido"
    port = f":{parsed.port}" if parsed.port else ""
    scheme = parsed.scheme or "http"
    return f"{scheme}://{host}{port}"


def short_text(value: str, limit: int = 2048) -> str:
    return value[:limit]


def random_user_id(rng: random.Random) -> str:
    suffix = "".join(rng.choice(string.ascii_lowercase + string.digits + "_") for _ in range(8))
    return f"user_{suffix}"


def random_id(rng: random.Random, length: int = 16) -> str:
    return "".join(rng.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def random_timestamp_z(rng: random.Random, base_now_sp: datetime) -> str:
    delta_seconds = rng.randint(0, 30 * 60)
    ts_local = base_now_sp - timedelta(seconds=delta_seconds)
    return ts_local.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def valid_engagement(rng: random.Random) -> tuple[int, int, int]:
    views = rng.randint(10, 5000)
    reactions = rng.randint(0, views)
    shares = rng.randint(0, views - reactions)
    return reactions, shares, views


def valid_message(
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
    reactions_default, shares_default, views_default = valid_engagement(rng)
    return {
        "id": random_id(rng),
        "content": content,
        "timestamp": random_timestamp_z(rng, base_now_sp),
        "user_id": user_id or random_user_id(rng),
        "hashtags": hashtags or rng.sample(HASHTAGS_POOL, k=rng.randint(1, 3)),
        "reactions": reactions_default if reactions is None else reactions,
        "shares": shares_default if shares is None else shares,
        "views": views_default if views is None else views,
    }


def special_pattern_content() -> str:
    content = "mbras " + ("á" * 36)
    if len(content) != 42:
        raise RuntimeError("Falha ao gerar conteudo especial de 42 caracteres.")
    return content


def build_payload_for_scenario(
    scenario_type: str,
    rng: random.Random,
    base_now_sp: datetime,
) -> tuple[dict[str, Any], int]:
    if scenario_type == "valid_basic":
        payload = {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="adorei produto #produto",
                    hashtags=["#produto"],
                    reactions=2,
                    shares=1,
                    views=10,
                )
            ],
            "time_window_minutes": 30,
        }
        return payload, 200

    if scenario_type == "window_422":
        payload = {
            "messages": [valid_message(rng, base_now_sp, content="adorei #produto", hashtags=["#produto"])],
            "time_window_minutes": 123,
        }
        return payload, 422

    if scenario_type == "edge_muito":
        return {
            "messages": [valid_message(rng, base_now_sp, content="muito", hashtags=["#neutro"])],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_double_negation":
        return {
            "messages": [valid_message(rng, base_now_sp, content="não não gostei", hashtags=["#positivo"])],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_meta_mbras":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="teste técnico mbras",
                    user_id="user_MBRAS_meta01",
                    hashtags=["#mbras"],
                )
            ],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_special_pattern":
        return {
            "messages": [valid_message(rng, base_now_sp, content=special_pattern_content(), hashtags=["#mbras"])],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_unicode_cafe":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="bom #produto",
                    user_id="user_café",
                    hashtags=["#produto"],
                )
            ],
            "time_window_minutes": 30,
        }, 400

    if scenario_type == "edge_13chars":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="bom #produto",
                    user_id="user_12345678",
                    hashtags=["#produto"],
                )
            ],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_prime":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="bom #produto",
                    user_id="user_math_prime",
                    hashtags=["#produto"],
                )
            ],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_golden_ratio":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="bom #produto",
                    hashtags=["#produto"],
                    reactions=4,
                    shares=3,
                    views=20,
                )
            ],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_cross_trending":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content=f"{rng.choice(POSITIVES)} #positivo",
                    hashtags=["#positivo"],
                    reactions=2,
                    shares=1,
                    views=20,
                ),
                valid_message(
                    rng,
                    base_now_sp,
                    content=f"{rng.choice(NEGATIVES)} #negativo",
                    hashtags=["#negativo"],
                    reactions=1,
                    shares=0,
                    views=20,
                ),
            ],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "edge_long_hashtag_decay":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content=f"{rng.choice(NEUTRALS)} #short #verylonghashtag",
                    hashtags=["#short", "#verylonghashtag"],
                )
            ],
            "time_window_minutes": 30,
        }, 200

    if scenario_type == "invalid_views":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="bom #produto",
                    hashtags=["#produto"],
                    reactions=4,
                    shares=4,
                    views=7,
                )
            ],
            "time_window_minutes": 30,
        }, 400

    if scenario_type == "invalid_timestamp":
        message = valid_message(rng, base_now_sp, content="bom #produto", hashtags=["#produto"])
        message["timestamp"] = message["timestamp"].replace("Z", "")
        return {"messages": [message], "time_window_minutes": 30}, 400

    if scenario_type == "invalid_hashtag":
        return {
            "messages": [valid_message(rng, base_now_sp, content="bom produto", hashtags=["invalida"])],
            "time_window_minutes": 30,
        }, 400

    if scenario_type == "invalid_content_len":
        return {
            "messages": [valid_message(rng, base_now_sp, content="x" * 281, hashtags=["#produto"])],
            "time_window_minutes": 30,
        }, 400

    if scenario_type == "invalid_user_regex":
        return {
            "messages": [
                valid_message(
                    rng,
                    base_now_sp,
                    content="bom #produto",
                    user_id="usuario-invalido",
                    hashtags=["#produto"],
                )
            ],
            "time_window_minutes": 30,
        }, 400

    if scenario_type == "invalid_time_window":
        return {
            "messages": [valid_message(rng, base_now_sp, content="bom #produto", hashtags=["#produto"])],
            "time_window_minutes": 0,
        }, 400

    raise RuntimeError(f"Cenario nao suportado: {scenario_type}")


def is_ok_for_scenario(
    scenario_type: str,
    expected_status: int,
    status_code: int,
    response_json: dict[str, Any],
) -> tuple[bool, str]:
    if status_code != expected_status:
        return False, f"status_inesperado={status_code}"

    if scenario_type == "window_422":
        expected = {
            "error": "Valor de janela temporal não suportado na versão atual",
            "code": "UNSUPPORTED_TIME_WINDOW",
        }
        if response_json != expected:
            return False, "payload_422_diferente_do_esperado"

    if scenario_type == "edge_cross_trending":
        trending = response_json.get("analysis", {}).get("trending_topics", [])
        if "#positivo" in trending and "#negativo" in trending:
            if trending.index("#positivo") > trending.index("#negativo"):
                return False, "ordem_trending_invalida"

    return True, "ok"


def choose_scenario_types(batch_size: int, include_invalid: bool, rng: random.Random) -> list[str]:
    valid_types = ["valid_basic"]
    edge_types = [
        "edge_muito",
        "edge_double_negation",
        "edge_meta_mbras",
        "edge_special_pattern",
        "edge_unicode_cafe",
        "edge_13chars",
        "edge_prime",
        "edge_golden_ratio",
        "edge_cross_trending",
        "edge_long_hashtag_decay",
    ]
    invalid_types = [
        "invalid_views",
        "invalid_timestamp",
        "invalid_hashtag",
        "invalid_content_len",
        "invalid_user_regex",
        "invalid_time_window",
    ]

    plan: list[str] = []
    plan.extend(valid_types)
    plan.extend(edge_types)
    plan.append("window_422")
    if include_invalid:
        plan.extend(invalid_types)

    weights: dict[str, int] = {
        "valid_basic": 40,
        "window_422": 8,
        "edge_muito": 6,
        "edge_double_negation": 6,
        "edge_meta_mbras": 6,
        "edge_special_pattern": 6,
        "edge_unicode_cafe": 4,
        "edge_13chars": 6,
        "edge_prime": 6,
        "edge_golden_ratio": 6,
        "edge_cross_trending": 6,
        "edge_long_hashtag_decay": 6,
        "invalid_views": 10,
        "invalid_timestamp": 10,
        "invalid_hashtag": 10,
        "invalid_content_len": 10,
        "invalid_user_regex": 10,
        "invalid_time_window": 10,
    }

    available = valid_types + edge_types + ["window_422"]
    if include_invalid:
        available += invalid_types

    while len(plan) < batch_size:
        total_weight = sum(weights[name] for name in available)
        pick = rng.randint(1, total_weight)
        running = 0
        for name in available:
            running += weights[name]
            if pick <= running:
                plan.append(name)
                break

    if len(plan) > batch_size:
        plan = plan[:batch_size]

    rng.shuffle(plan)
    return plan


async def send_one(
    *,
    client: httpx.AsyncClient,
    global_sem: asyncio.Semaphore,
    batch_sem: asyncio.Semaphore | None,
    run_id: str,
    batch_index: int,
    request_index: int,
    scenario_type: str,
    payload: dict[str, Any],
    expected_status: int,
) -> dict[str, Any]:
    sent_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    started = time.perf_counter()

    if batch_sem is not None:
        async with global_sem:
            async with batch_sem:
                return await _do_request(
                    client=client,
                    run_id=run_id,
                    batch_index=batch_index,
                    request_index=request_index,
                    scenario_type=scenario_type,
                    payload=payload,
                    expected_status=expected_status,
                    sent_at=sent_at,
                    started=started,
                )

    async with global_sem:
        return await _do_request(
            client=client,
            run_id=run_id,
            batch_index=batch_index,
            request_index=request_index,
            scenario_type=scenario_type,
            payload=payload,
            expected_status=expected_status,
            sent_at=sent_at,
            started=started,
        )


async def _do_request(
    *,
    client: httpx.AsyncClient,
    run_id: str,
    batch_index: int,
    request_index: int,
    scenario_type: str,
    payload: dict[str, Any],
    expected_status: int,
    sent_at: str,
    started: float,
) -> dict[str, Any]:
    try:
        response = await client.post("/analyze-feed", json=payload)
        status_code = response.status_code
        try:
            response_json = response.json()
            response_text = None
        except Exception:
            response_json = None
            response_text = short_text(response.text)

        ok, reason = is_ok_for_scenario(
            scenario_type,
            expected_status,
            status_code,
            response_json if isinstance(response_json, dict) else {},
        )
        error = None
    except Exception as exc:
        status_code = 0
        response_json = None
        response_text = None
        ok = False
        reason = "erro_de_requisicao"
        error = f"Falha na requisicao: {str(exc)[:300]}"

    latency_ms = (time.perf_counter() - started) * 1000.0

    return {
        "run_id": run_id,
        "batch_index": batch_index,
        "request_index": request_index,
        "scenario_type": scenario_type,
        "sent_at_utc": sent_at,
        "latency_ms": round(latency_ms, 3),
        "request": payload,
        "response_status": status_code,
        "response_json": response_json,
        "response_text": response_text,
        "error": error,
        "ok": ok,
        "reason": reason,
    }


async def run_batch(
    *,
    run_id: str,
    batch_index: int,
    batch_size: int,
    include_invalid: bool,
    base_now_sp: datetime,
    global_sem: asyncio.Semaphore,
    batch_sem_limit: int | None,
    client: httpx.AsyncClient,
    run_dir: Path,
    batch_digits: int,
    seed: int,
) -> dict[str, Any]:
    batch_seed = seed + batch_index
    rng = random.Random(batch_seed)
    scenario_types = choose_scenario_types(batch_size=batch_size, include_invalid=include_invalid, rng=rng)

    batch_sem = asyncio.Semaphore(batch_sem_limit) if batch_sem_limit is not None else None

    requests_to_send: list[tuple[int, str, dict[str, Any], int]] = []
    for request_index, scenario_type in enumerate(scenario_types):
        request_rng = random.Random(batch_seed * 10_000 + request_index)
        payload, expected_status = build_payload_for_scenario(scenario_type, request_rng, base_now_sp)
        requests_to_send.append((request_index, scenario_type, payload, expected_status))

    tasks = [
        asyncio.create_task(
            send_one(
                client=client,
                global_sem=global_sem,
                batch_sem=batch_sem,
                run_id=run_id,
                batch_index=batch_index,
                request_index=request_index,
                scenario_type=scenario_type,
                payload=payload,
                expected_status=expected_status,
            )
        )
        for request_index, scenario_type, payload, expected_status in requests_to_send
    ]

    started = time.perf_counter()
    results = await asyncio.gather(*tasks)
    elapsed = max(time.perf_counter() - started, 0.001)

    batch_file = run_dir / f"batch_{batch_index:0{batch_digits}d}.jsonl"
    status_counts: dict[str, int] = {"200": 0, "400": 0, "422": 0, "5xx": 0, "errors": 0}
    passed = 0
    failed = 0
    latencies: list[float] = []
    failures: list[dict[str, Any]] = []

    with batch_file.open("w", encoding="utf-8") as fp:
        for line_number, item in enumerate(results, start=1):
            status = int(item["response_status"])
            if status == 200:
                status_counts["200"] += 1
            elif status == 400:
                status_counts["400"] += 1
            elif status == 422:
                status_counts["422"] += 1
            elif status >= 500:
                status_counts["5xx"] += 1
            else:
                status_counts["errors"] += 1

            latencies.append(float(item["latency_ms"]))

            if bool(item["ok"]):
                passed += 1
            else:
                failed += 1
                failures.append(
                    {
                        "batch_index": batch_index,
                        "batch_file": batch_file.name,
                        "line_number": line_number,
                        "scenario_type": item["scenario_type"],
                        "response_status": item["response_status"],
                        "reason": item.get("reason"),
                        "error": item.get("error"),
                    }
                )

            fp.write(json.dumps(item, ensure_ascii=False) + "\n")

    print(
        f"[Batch {batch_index:0{batch_digits}d}] concluido: pass={passed} fail={failed} "
        f"status(200={status_counts['200']},400={status_counts['400']},422={status_counts['422']},"
        f"5xx={status_counts['5xx']},errors={status_counts['errors']})"
    )

    return {
        "batch_index": batch_index,
        "batch_file": batch_file.name,
        "batch_seed": batch_seed,
        "total": len(results),
        "passed": passed,
        "failed": failed,
        "status_counts": status_counts,
        "latency_p50_ms": percentile(latencies, 50),
        "latency_p95_ms": percentile(latencies, 95),
        "latencies_ms": latencies,
        "elapsed_seconds": elapsed,
        "throughput_req_s": len(results) / elapsed,
        "failures": failures,
    }


async def run_batched_load(
    *,
    base_url: str,
    batches: int,
    batch_size: int,
    parallel_batches: int,
    seed: int,
    include_invalid: bool,
    timeout_seconds: int,
    concurrency_total: int,
    concurrency_per_batch: int | None,
    output_dir: Path,
    json_report: Path | None,
) -> None:
    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    base_now_sp = datetime.now(resolve_timezone())

    effective_total = concurrency_total
    if concurrency_per_batch is not None:
        effective_total = min(concurrency_total, concurrency_per_batch * parallel_batches)

    sanitized_base_url = sanitize_base_url(base_url)
    print("--------------------------Inicio da execucao---------------------------")
    print(
        f"base_url={sanitized_base_url} batches={batches} batch_size={batch_size} "
        f"parallel_batches={parallel_batches} concurrency_total_efetiva={effective_total}"
    )

    global_sem = asyncio.Semaphore(max(1, effective_total))
    batch_gate = asyncio.Semaphore(max(1, parallel_batches))
    batch_digits = max(2, len(str(max(0, batches - 1))))

    timeout = httpx.Timeout(max(1, timeout_seconds))

    async with httpx.AsyncClient(base_url=base_url, timeout=timeout) as client:

        async def run_guarded_batch(batch_idx: int) -> dict[str, Any]:
            async with batch_gate:
                return await run_batch(
                    run_id=run_id,
                    batch_index=batch_idx,
                    batch_size=batch_size,
                    include_invalid=include_invalid,
                    base_now_sp=base_now_sp,
                    global_sem=global_sem,
                    batch_sem_limit=concurrency_per_batch,
                    client=client,
                    run_dir=run_dir,
                    batch_digits=batch_digits,
                    seed=seed,
                )

        started = time.perf_counter()
        batch_results = await asyncio.gather(
            *[asyncio.create_task(run_guarded_batch(batch_idx)) for batch_idx in range(batches)]
        )
        elapsed = max(time.perf_counter() - started, 0.001)

    batch_results = sorted(batch_results, key=lambda item: int(item["batch_index"]))

    total_sent = sum(item["total"] for item in batch_results)
    total_passed = sum(item["passed"] for item in batch_results)
    total_failed = sum(item["failed"] for item in batch_results)

    overall_status = {"200": 0, "400": 0, "422": 0, "5xx": 0, "errors": 0}
    all_latencies: list[float] = []
    first_failures: list[dict[str, Any]] = []

    for item in batch_results:
        for key in overall_status:
            overall_status[key] += int(item["status_counts"][key])
        all_latencies.extend(float(value) for value in item.pop("latencies_ms", []))
        if len(first_failures) < 20:
            remaining = 20 - len(first_failures)
            first_failures.extend(item["failures"][:remaining])

    summary = {
        "run_id": run_id,
        "configuration": {
            "base_url": sanitized_base_url,
            "batches": batches,
            "batch_size": batch_size,
            "parallel_batches": parallel_batches,
            "seed": seed,
            "include_invalid": include_invalid,
            "timeout_seconds": timeout_seconds,
            "concurrency_total": concurrency_total,
            "concurrency_per_batch": concurrency_per_batch,
            "effective_global_concurrency": effective_total,
            "output_dir": str(run_dir),
        },
        "totals_overall": {
            "sent": total_sent,
            "passed": total_passed,
            "failed": total_failed,
            "status_counts": overall_status,
            "latency_p50_ms": percentile(all_latencies, 50),
            "latency_p95_ms": percentile(all_latencies, 95),
            "throughput_req_s": total_sent / elapsed,
            "elapsed_seconds": elapsed,
        },
        "totals_per_batch": batch_results,
        "first_failures": first_failures,
    }

    summary_path = run_dir / "run_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if json_report is not None:
        json_report.parent.mkdir(parents=True, exist_ok=True)
        json_report.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("------------------Resumo final------------------")
    print(f"total_enviado={total_sent}")
    print(f"status_200={overall_status['200']}")
    print(f"status_400={overall_status['400']}")
    print(f"status_422={overall_status['422']}")
    print(f"status_5xx={overall_status['5xx']}")
    print(f"erros_requisicao={overall_status['errors']}")
    print(f"latencia_p50_ms={summary['totals_overall']['latency_p50_ms']:.2f}")
    print(f"latencia_p95_ms={summary['totals_overall']['latency_p95_ms']:.2f}")
    print(f"throughput_req_s={summary['totals_overall']['throughput_req_s']:.2f}")
    print(f"falhas_totais={total_failed}")
    print(f"logs_em={run_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa carga em lotes paralelos com auditoria por batch.")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--batches", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--parallel-batches", type=int, default=10)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--include-invalid", default="true")
    parser.add_argument("--timeout-seconds", type=int, default=10)
    parser.add_argument("--concurrency-total", type=int, default=200)
    parser.add_argument("--concurrency-per-batch", type=int, default=None)
    parser.add_argument("--output-dir", default="tests/smoke/output")
    parser.add_argument("--json-report", default="")

    args = parser.parse_args()

    asyncio.run(
        run_batched_load(
            base_url=args.base_url,
            batches=max(1, args.batches),
            batch_size=max(1, args.batch_size),
            parallel_batches=max(1, args.parallel_batches),
            seed=args.seed,
            include_invalid=parse_bool(str(args.include_invalid)),
            timeout_seconds=max(1, args.timeout_seconds),
            concurrency_total=max(1, args.concurrency_total),
            concurrency_per_batch=(
                max(1, args.concurrency_per_batch) if args.concurrency_per_batch is not None else None
            ),
            output_dir=Path(args.output_dir),
            json_report=Path(args.json_report) if args.json_report else None,
        )
    )


if __name__ == "__main__":
    main()
