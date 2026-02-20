from __future__ import annotations

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.infrastructure.monitoring.healthchecks import build_readiness_payload

router = APIRouter(tags=['system'])


@router.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}


@router.get('/ready')
def ready():
    ready_state, payload = build_readiness_payload()
    if ready_state:
        return payload
    return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=payload)
