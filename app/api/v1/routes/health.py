from __future__ import annotations

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.core.config.settings import get_settings
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


@router.get('/debug/force-500')
def force_500():
    settings = get_settings()
    if settings.app_env not in {'local', 'test', 'dev'}:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={'error': 'Rota indisponivel neste ambiente.', 'code': 'NOT_FOUND'},
        )
    raise RuntimeError('Erro controlado para validar observabilidade.')
