from __future__ import annotations

import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.core.errors.http_exceptions import ApiValidationError
from app.core.logging.masking import sanitize_error_text, truncate_text
from app.infrastructure.monitoring.prometheus import bounded_exception_type, http_exception_total, status_class_from_code

logger = logging.getLogger(__name__)


def _stacktrace_compact(exc: Exception) -> str:
    lines = traceback.format_exception(type(exc), exc, exc.__traceback__)
    return truncate_text(''.join(lines), max_length=800)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiValidationError)
    async def _api_validation_handler(request: Request, exc: ApiValidationError):
        exception_type = bounded_exception_type('ApiValidationError')
        http_exception_total.labels(exception_type=exception_type, status_class=status_class_from_code(exc.status_code)).inc()
        correlation_id = str(getattr(request.state, 'correlation_id', '')).strip()
        request.state.observability_error = {
            'error_type': exception_type,
            'error_message': sanitize_error_text(exc.error),
        }
        content = {'error': exc.error, 'code': exc.code}
        if correlation_id:
            content['correlation_id'] = correlation_id
        return JSONResponse(status_code=exc.status_code, content=content)

    @app.exception_handler(Exception)
    async def _unexpected_error_handler(request: Request, exc: Exception):
        exception_type = bounded_exception_type(exc.__class__.__name__)
        http_exception_total.labels(exception_type=exception_type, status_class='5xx').inc()

        safe_error_message = sanitize_error_text(str(exc) or 'Falha interna sem detalhes.')
        safe_stacktrace = _stacktrace_compact(exc)
        request.state.observability_error = {
            'error_type': exception_type,
            'error_message': safe_error_message,
            'stacktrace': safe_stacktrace,
        }

        correlation_id = str(getattr(request.state, 'correlation_id', '')).strip() or 'sem-correlation-id'
        logger.error(
            'Falha interna no processamento da requisicao. correlation_id=%s tipo_erro=%s motivo=%s',
            correlation_id,
            exception_type,
            safe_error_message,
        )
        return JSONResponse(
            status_code=500,
            content={
                'error': 'Falha interna no processamento da requisicao.',
                'code': 'INTERNAL_ERROR',
                'correlation_id': correlation_id,
            },
        )
