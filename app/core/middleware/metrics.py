from __future__ import annotations

import json
import logging
import time
import traceback
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request
from starlette.concurrency import iterate_in_threadpool
from starlette.responses import Response

from app.core.config.settings import get_settings
from app.core.logging.masking import extract_items_count, mask_for_log, sanitize_error_text, truncate_text
from app.infrastructure.monitoring.prometheus import (
    analyze_feed_failed_total,
    analyze_feed_requests_total,
    analyze_requests_total,
    bounded_exception_type,
    http_ack_duration_seconds,
    http_inflight_requests,
    http_request_duration_seconds,
    http_requests_status_class_total,
    http_requests_total,
    status_class_from_code,
)

logger = logging.getLogger(__name__)


def _resolve_route_path(request: Request) -> str:
    route = request.scope.get('route')
    if route is not None and hasattr(route, 'path'):
        return str(route.path)
    return request.url.path


async def _read_request_body(request: Request, *, max_bytes: int) -> tuple[bytes, bool]:
    content_length = request.headers.get('content-length')
    if content_length:
        try:
            if int(content_length) > max_bytes:
                return b'', True
        except ValueError:
            pass

    body = await request.body()

    async def _receive() -> dict[str, Any]:
        return {'type': 'http.request', 'body': body, 'more_body': False}

    request._receive = _receive

    if len(body) > max_bytes:
        return b'', True
    return body, False


def _json_loads_if_possible(raw_bytes: bytes) -> Any:
    if not raw_bytes:
        return None
    try:
        return json.loads(raw_bytes.decode('utf-8'))
    except Exception:
        return truncate_text(raw_bytes.decode('utf-8', errors='ignore'))


def _response_json_payload(content_type: str, response_body: bytes) -> Any:
    if 'application/json' not in content_type:
        return None
    return _json_loads_if_possible(response_body)


async def _read_response_body(response: Response) -> bytes:
    if hasattr(response, 'body') and isinstance(response.body, (bytes, bytearray)) and response.body:
        return bytes(response.body)
    body = b''
    body_iterator = getattr(response, 'body_iterator', None)
    if body_iterator is None:
        return body
    async for chunk in body_iterator:
        if isinstance(chunk, str):
            body += chunk.encode('utf-8')
        elif isinstance(chunk, (bytes, bytearray)):
            body += bytes(chunk)
    return body


def _replace_response_body(response: Response, body: bytes) -> None:
    response.body = body
    response.body_iterator = iterate_in_threadpool([body])
    response.headers['content-length'] = str(len(body))


def _compact_stacktrace(exc: Exception) -> str:
    lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    merged = ''.join(lines)
    return truncate_text(merged, max_length=800)


def register_metrics_middleware(app: FastAPI) -> None:
    @app.middleware('http')
    async def _metrics_middleware(request: Request, call_next):
        settings = get_settings()
        started_at = time.perf_counter()
        status_code = 500
        error_info: dict[str, Any] | None = None
        response_payload_for_log: Any = None
        response: Response | None = None

        correlation_id = str(getattr(request.state, 'correlation_id', '')).strip()
        if not correlation_id:
            correlation_id = request.headers.get('X-Correlation-Id') or str(uuid.uuid4())
            request.state.correlation_id = correlation_id

        request_payload_for_log: Any = None
        items_count = 0
        request_body_truncated = False

        http_inflight_requests.inc()

        try:
            request_body, request_body_truncated = await _read_request_body(
                request,
                max_bytes=max(1024, settings.http_log_body_max_bytes),
            )
            parsed_request_body = _json_loads_if_possible(request_body)
            if request_body_truncated:
                request_payload_for_log = {'aviso': 'corpo_truncado_por_tamanho'}
            else:
                request_payload_for_log = parsed_request_body
                items_count = extract_items_count(parsed_request_body)

            response = await call_next(request)
            status_code = int(response.status_code)

            response.headers['X-Correlation-Id'] = correlation_id
            duration_ms = int(max(time.perf_counter() - started_at, 0.0) * 1000)
            response.headers['X-Request-Duration-Ms'] = str(duration_ms)

            response_body = await _read_response_body(response)
            content_type = str(response.headers.get('content-type', '')).lower()
            response_payload = _response_json_payload(content_type, response_body)
            if isinstance(response_payload, dict) and 'correlation_id' not in response_payload:
                response_payload['correlation_id'] = correlation_id
                response_body = json.dumps(response_payload, ensure_ascii=False).encode('utf-8')
            _replace_response_body(response, response_body)
            if isinstance(response_payload, dict):
                response_payload_for_log = response_payload
            else:
                response_payload_for_log = response_payload

            if status_code >= 400:
                state_error = getattr(request.state, 'observability_error', None)
                if isinstance(state_error, dict):
                    error_info = state_error
                elif isinstance(response_payload_for_log, dict):
                    error_info = {
                        'error_type': 'HTTPError',
                        'error_message': sanitize_error_text(response_payload_for_log.get('error', 'Erro HTTP sem detalhe.')),
                    }

            return response
        except Exception as exc:
            status_code = 500
            error_type = bounded_exception_type(exc.__class__.__name__)
            error_info = {
                'error_type': error_type,
                'error_message': sanitize_error_text(str(exc) or 'Falha interna sem detalhes.'),
            }
            if settings.http_log_include_stacktrace:
                error_info['stacktrace'] = _compact_stacktrace(exc)
            request.state.observability_error = error_info
            raise
        finally:
            metric_path = _resolve_route_path(request)
            raw_path = request.url.path
            duration_seconds = max(time.perf_counter() - started_at, 0.0)
            status_class = status_class_from_code(status_code)

            http_requests_total.labels(method=request.method, path=metric_path, status=str(status_code)).inc()
            http_requests_status_class_total.labels(
                method=request.method,
                path=metric_path,
                status_class=status_class,
            ).inc()
            http_request_duration_seconds.labels(method=request.method, path=metric_path).observe(duration_seconds)
            http_ack_duration_seconds.labels(method=request.method, path=metric_path).observe(duration_seconds)
            http_inflight_requests.dec()

            if request.method == 'POST' and metric_path == '/analyze-feed':
                analyze_requests_total.inc()
                analyze_feed_requests_total.inc()
                if status_code >= 400:
                    analyze_feed_failed_total.inc()

            event_doc: dict[str, Any] = {
                '@timestamp': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'event': 'http_request',
                'correlation_id': correlation_id,
                'method': request.method,
                'path': raw_path,
                'status_code': status_code,
                'duration_ms': round(duration_seconds * 1000.0, 3),
                'items_count': items_count,
                'request_sample': mask_for_log(request_payload_for_log),
                'response_sample': mask_for_log(response_payload_for_log),
            }

            client_host = request.client.host if request.client is not None else None
            if client_host:
                event_doc['client_ip'] = truncate_text(client_host, max_length=128)

            user_agent = request.headers.get('user-agent')
            if user_agent:
                event_doc['user_agent'] = truncate_text(user_agent, max_length=256)

            if error_info:
                event_doc['error_type'] = bounded_exception_type(str(error_info.get('error_type', 'UnknownError')))
                event_doc['error_message'] = sanitize_error_text(error_info.get('error_message'))
                if settings.http_log_include_stacktrace and error_info.get('stacktrace'):
                    event_doc['stacktrace'] = sanitize_error_text(error_info.get('stacktrace'))

            http_log_writer = getattr(request.app.state, 'http_log_writer', None)
            if http_log_writer is not None:
                try:
                    http_log_writer.enqueue(event_doc)
                except Exception:
                    logger.error('Falha ao enfileirar log HTTP para envio ao Elastic.')
