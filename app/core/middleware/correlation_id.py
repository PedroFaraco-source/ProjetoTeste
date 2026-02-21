from __future__ import annotations

import uuid

from fastapi import FastAPI, Request


def register_correlation_id_middleware(app: FastAPI) -> None:
    @app.middleware('http')
    async def _correlation_id_middleware(request: Request, call_next):
        incoming_value = str(request.headers.get('X-Correlation-Id', '')).strip()
        correlation_id = incoming_value[:128] if incoming_value else str(uuid.uuid4())
        request.state.correlation_id = correlation_id
        response = await call_next(request)
        response.headers['X-Correlation-Id'] = correlation_id
        return response
