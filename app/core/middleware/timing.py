from __future__ import annotations

import time

from fastapi import FastAPI, Request


def register_timing_middleware(app: FastAPI) -> None:
    @app.middleware('http')
    async def _timing_middleware(request: Request, call_next):
        started_at = time.perf_counter()
        response = await call_next(request)
        duration_seconds = max(time.perf_counter() - started_at, 0.0)
        response.headers['X-Request-Duration-Ms'] = str(int(duration_seconds * 1000))
        return response
