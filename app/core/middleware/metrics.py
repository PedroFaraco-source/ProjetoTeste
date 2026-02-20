from __future__ import annotations

import time

from fastapi import FastAPI, Request

from app.infrastructure.monitoring.prometheus import (
    analyze_feed_failed_total,
    analyze_feed_requests_total,
    analyze_requests_total,
    http_request_duration_seconds,
    http_requests_total,
)


def register_metrics_middleware(app: FastAPI) -> None:
    @app.middleware('http')
    async def _metrics_middleware(request: Request, call_next):
        started_at = time.perf_counter()
        status_code = 500

        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        finally:
            path = request.url.path
            duration_seconds = max(time.perf_counter() - started_at, 0.0)
            http_requests_total.labels(method=request.method, path=path, status=str(status_code)).inc()
            http_request_duration_seconds.labels(method=request.method, path=path).observe(duration_seconds)

            if request.method == 'POST' and path == '/analyze-feed':
                analyze_requests_total.inc()
                analyze_feed_requests_total.inc()
                if status_code >= 400:
                    analyze_feed_failed_total.inc()
