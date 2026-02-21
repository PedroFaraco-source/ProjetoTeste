from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from app.infrastructure.monitoring.prometheus import (
    elastic_bulk_documents_total,
    elastic_bulk_duration_seconds,
    elastic_bulk_errors_total,
    elastic_bulk_requests_total,
    elastic_log_failures_total,
)
from app.infrastructure.search.elasticsearch_client import ElasticIndexWriter

logger = logging.getLogger(__name__)


class AsyncElasticHttpLogger:
    def __init__(
        self,
        *,
        elasticsearch_url: str,
        index_prefix: str,
        template_name: str,
        timeout_seconds: int,
        queue_size: int,
        batch_size: int,
        flush_interval_ms: int,
    ) -> None:
        self._writer = ElasticIndexWriter(elasticsearch_url, timeout_seconds=timeout_seconds)
        self._index_prefix = index_prefix
        self._template_name = template_name
        self._batch_size = max(1, batch_size)
        self._flush_interval_seconds = max(0.1, flush_interval_ms / 1000.0)
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=max(10, queue_size))
        self._stop_event = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None
        self._enabled = bool(elasticsearch_url.strip())

    async def start(self) -> None:
        if not self._enabled:
            logger.info('Logger HTTP para Elastic desativado por configuracao.')
            return
        try:
            await asyncio.to_thread(self._ensure_template)
        except Exception:
            elastic_log_failures_total.inc()
            logger.error('Falha ao configurar template de logs HTTP no Elasticsearch.')
        self._worker_task = asyncio.create_task(self._run(), name='elastic-http-log-writer')
        logger.info('Logger HTTP para Elastic iniciado. fila_max=%s lote=%s', self._queue.maxsize, self._batch_size)

    async def stop(self) -> None:
        if self._worker_task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._worker_task, timeout=10)
        except asyncio.TimeoutError:
            elastic_log_failures_total.inc()
            logger.error('Encerramento do logger HTTP no Elastic excedeu o tempo limite.')
        finally:
            self._worker_task = None

    def enqueue(self, document: dict[str, Any]) -> None:
        if not self._enabled:
            return
        try:
            self._queue.put_nowait(document)
        except asyncio.QueueFull:
            elastic_log_failures_total.inc()
            logger.error('Fila de logs HTTP para Elastic esta cheia. evento descartado.')

    def _ensure_template(self) -> None:
        mappings = {
            'dynamic': True,
            'properties': {
                '@timestamp': {'type': 'date'},
                'event': {'type': 'keyword'},
                'correlation_id': {'type': 'keyword'},
                'method': {'type': 'keyword'},
                'path': {'type': 'keyword'},
                'status_code': {'type': 'integer'},
                'duration_ms': {'type': 'float'},
                'items_count': {'type': 'integer'},
                'client_ip': {'type': 'keyword'},
                'user_agent': {'type': 'keyword', 'ignore_above': 512},
                'error_type': {'type': 'keyword'},
                'error_message': {'type': 'text'},
                'stacktrace': {'type': 'text'},
                'request_sample': {'type': 'flattened'},
                'response_sample': {'type': 'flattened'},
            },
        }
        settings = {
            'number_of_shards': 1,
            'number_of_replicas': 1,
        }
        self._writer.ensure_index_template(
            template_name=self._template_name,
            index_patterns=[f'{self._index_prefix}-*'],
            mappings=mappings,
            settings=settings,
        )

    def _resolve_index_name(self, timestamp_value: Any) -> str:
        if isinstance(timestamp_value, str):
            normalized = timestamp_value.replace('Z', '+00:00')
            try:
                parsed = datetime.fromisoformat(normalized)
                return f"{self._index_prefix}-{parsed.astimezone(timezone.utc).strftime('%Y.%m.%d')}"
            except Exception:
                pass
        now_utc = datetime.now(timezone.utc)
        return f"{self._index_prefix}-{now_utc.strftime('%Y.%m.%d')}"

    async def _run(self) -> None:
        batch: list[dict[str, Any]] = []
        while True:
            if self._stop_event.is_set() and self._queue.empty() and not batch:
                break
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=self._flush_interval_seconds)
                batch.append(item)
                if len(batch) >= self._batch_size:
                    await self._flush(batch)
                    batch.clear()
                self._queue.task_done()
            except asyncio.TimeoutError:
                if batch:
                    await self._flush(batch)
                    batch.clear()
            except Exception:
                elastic_log_failures_total.inc()
                logger.error('Falha inesperada no loop de escrita de logs HTTP para Elastic.')

        if batch:
            await self._flush(batch)

    async def _flush(self, batch: list[dict[str, Any]]) -> None:
        if not batch:
            return

        operations: list[dict[str, Any]] = []
        for doc in batch:
            timestamp_value = doc.get('@timestamp')
            operations.append(
                {
                    '_index': self._resolve_index_name(timestamp_value),
                    '_source': doc,
                }
            )

        started_at = perf_counter()
        try:
            success_count, errors = await asyncio.to_thread(self._writer.bulk_write, operations)
            duration_seconds = max(perf_counter() - started_at, 0.0)
            elastic_bulk_duration_seconds.labels(operation='http_request_logs').observe(duration_seconds)
            elastic_bulk_requests_total.labels(operation='http_request_logs', result='success').inc()
            elastic_bulk_documents_total.labels(operation='http_request_logs', result='success').inc(success_count)

            error_count = len(errors)
            if error_count > 0:
                elastic_log_failures_total.inc(error_count)
                elastic_bulk_errors_total.labels(operation='http_request_logs').inc(error_count)
                elastic_bulk_requests_total.labels(operation='http_request_logs', result='partial_error').inc()
                elastic_bulk_documents_total.labels(operation='http_request_logs', result='error').inc(error_count)
                logger.error(
                    'Falha parcial no bulk de logs HTTP para Elastic. sucesso=%s erros=%s',
                    success_count,
                    error_count,
                )
        except Exception:
            duration_seconds = max(perf_counter() - started_at, 0.0)
            elastic_bulk_duration_seconds.labels(operation='http_request_logs').observe(duration_seconds)
            elastic_log_failures_total.inc(len(batch))
            elastic_bulk_errors_total.labels(operation='http_request_logs').inc(len(batch))
            elastic_bulk_requests_total.labels(operation='http_request_logs', result='failure').inc()
            elastic_bulk_documents_total.labels(operation='http_request_logs', result='error').inc(len(batch))
            logger.error('Falha ao enviar lote de logs HTTP para Elastic.')
