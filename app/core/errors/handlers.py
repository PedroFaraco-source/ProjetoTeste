from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.errors.http_exceptions import ApiValidationError

logger = logging.getLogger(__name__)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiValidationError)
    async def _api_validation_handler(request: Request, exc: ApiValidationError):
        return JSONResponse(status_code=exc.status_code, content={'error': exc.error, 'code': exc.code})

    @app.exception_handler(Exception)
    async def _unexpected_error_handler(request: Request, exc: Exception):
        logger.error('Falha interna no processamento da requisicao.')
        return JSONResponse(
            status_code=500,
            content={'error': 'Falha interna no processamento da requisicao.', 'code': 'INTERNAL_ERROR'},
        )
