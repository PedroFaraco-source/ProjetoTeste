from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from time import perf_counter

from fastapi import FastAPI

from app.api.v1.router import api_v1_router
from app.core.errors.handlers import register_exception_handlers
from app.core.middleware.correlation_id import register_correlation_id_middleware
from app.core.middleware.metrics import register_metrics_middleware
from app.core.middleware.timing import register_timing_middleware
from app.core.logging.setup import configure_logging
from app.infrastructure.db.session import init_db, shutdown_db
from app.infrastructure.messaging.rabbitmq_bus import RabbitMQBus
from app.infrastructure.monitoring.prometheus import (
    elastic_retention_deleted_total,
    elastic_retention_duration_seconds,
    elastic_retention_runs_total,
)
from tools.elastic_retention import RetentionConfig, run_retention_once

logger = logging.getLogger(__name__)


async def _elastic_retention_worker(config: RetentionConfig, stop_event: asyncio.Event) -> None:
    interval_seconds = max(60, config.interval_minutes * 60)
    logger.info(
        'Rotina de retencao do Elasticsearch iniciada. indice=%s dias=%s intervalo_minutos=%s',
        config.index,
        config.days,
        config.interval_minutes,
    )

    while not stop_event.is_set():
        started_at = perf_counter()
        result_label = 'success'
        deleted_count = 0

        try:
            result = await run_retention_once(config)
            if result.success:
                deleted_count = result.deleted
            else:
                result_label = 'failed'
        except Exception:
            result_label = 'failed'
            logger.warning('Falha inesperada no worker de retencao do Elasticsearch.')

        duration_seconds = max(perf_counter() - started_at, 0.0)
        elastic_retention_duration_seconds.observe(duration_seconds)
        elastic_retention_runs_total.labels(result=result_label).inc()
        if deleted_count > 0:
            elastic_retention_deleted_total.inc(deleted_count)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except asyncio.TimeoutError:
            continue

    logger.info('Rotina de retencao do Elasticsearch finalizada.')


@asynccontextmanager
async def _lifespan(app: FastAPI):
    configure_logging()
    init_db()
    app.state.rabbit_bus = RabbitMQBus()

    retention_config = RetentionConfig.from_env()
    app.state.elastic_retention_stop_event = None
    app.state.elastic_retention_task = None
    if retention_config.enabled:
        app.state.elastic_retention_stop_event = asyncio.Event()
        app.state.elastic_retention_task = asyncio.create_task(
            _elastic_retention_worker(retention_config, app.state.elastic_retention_stop_event),
            name='elastic-retention-worker',
        )
    else:
        logger.info('Rotina de retencao do Elasticsearch desativada por configuracao.')

    try:
        yield
    finally:
        retention_stop_event = getattr(app.state, 'elastic_retention_stop_event', None)
        if retention_stop_event is not None:
            retention_stop_event.set()

        retention_task = getattr(app.state, 'elastic_retention_task', None)
        if retention_task is not None:
            try:
                await asyncio.wait_for(retention_task, timeout=10)
            except asyncio.TimeoutError:
                retention_task.cancel()
                logger.warning('Encerramento do worker de retencao do Elasticsearch excedeu o tempo limite.')
            except Exception:
                logger.warning('Falha ao encerrar worker de retencao do Elasticsearch.')

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
