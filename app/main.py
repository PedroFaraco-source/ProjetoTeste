from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.v1.router import api_v1_router
from app.core.config.settings import get_settings
from app.core.errors.handlers import register_exception_handlers
from app.core.middleware.correlation_id import register_correlation_id_middleware
from app.core.middleware.metrics import register_metrics_middleware
from app.core.middleware.timing import register_timing_middleware
from app.core.logging.setup import configure_logging
from app.infrastructure.db.session import init_db, shutdown_db
from app.infrastructure.messaging.rabbitmq_bus import RabbitMQBus
from app.infrastructure.monitoring.elastic_http_logger import AsyncElasticHttpLogger


@asynccontextmanager
async def _lifespan(app: FastAPI):
    configure_logging()
    settings = get_settings()
    init_db()
    app.state.rabbit_bus = RabbitMQBus()

    elastic_url = '' if settings.bypass_elastic_for_tests else settings.elasticsearch_url
    app.state.http_log_writer = AsyncElasticHttpLogger(
        elasticsearch_url=elastic_url,
        index_prefix=settings.elastic_logs_index_prefix,
        template_name=settings.elastic_logs_template_name,
        timeout_seconds=settings.elastic_timeout_seconds,
        queue_size=settings.elastic_http_log_queue_size,
        batch_size=settings.elastic_http_log_batch_size,
        flush_interval_ms=settings.elastic_http_log_flush_interval_ms,
    )
    await app.state.http_log_writer.start()

    try:
        yield
    finally:
        http_log_writer = getattr(app.state, 'http_log_writer', None)
        if http_log_writer is not None:
            await http_log_writer.stop()

        rabbit_bus = getattr(app.state, 'rabbit_bus', None)
        if rabbit_bus is not None:
            rabbit_bus.close()
        shutdown_db()


def create_app() -> FastAPI:
    app = FastAPI(
        title='ProjetoMBras',
        version='1.0.0',
        docs_url='/docs',
        redoc_url=None,
        openapi_url='/openapi.json',
        lifespan=_lifespan,
    )

    register_correlation_id_middleware(app)
    register_timing_middleware(app)
    register_metrics_middleware(app)
    register_exception_handlers(app)
    app.include_router(api_v1_router)
    return app


app = create_app()
