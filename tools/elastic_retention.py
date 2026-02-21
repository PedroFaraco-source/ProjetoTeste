from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from time import monotonic
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import httpx

logger = logging.getLogger(__name__)

CONNECT_TIMEOUT_SECONDS = 2.0
READ_TIMEOUT_SECONDS = 20.0
TASK_POLL_INTERVAL_SECONDS = 1.0
TASK_WAIT_TIMEOUT_SECONDS = 180
DEFAULT_ELASTIC_URL = 'http://localhost:5000'
DEFAULT_RETENTION_INDEX = 'index-projetoMb'
DEFAULT_RETENTION_FIELD = '@timestamp'
DEFAULT_RETENTION_FALLBACK_FIELD = 'created_at'


def _to_bool(raw_value: str | None, default: bool) -> bool:
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if not normalized:
        return default
    return normalized in {'1', 'true', 'yes', 'on'}


def _to_int(raw_value: str | None, default: int, minimum: int) -> int:
    if raw_value is None:
        return default
    normalized = raw_value.strip()
    if not normalized:
        return default
    try:
        parsed = int(normalized)
    except ValueError:
        return default
    return max(parsed, minimum)


def _sanitize_url_for_logs(url: str) -> str:
    if not url:
        return 'indefinido'
    parsed = urlsplit(url)
    netloc = parsed.hostname or ''
    if parsed.port:
        netloc = f'{netloc}:{parsed.port}'
    return urlunsplit((parsed.scheme, netloc, parsed.path, '', '')) or 'indefinido'


@dataclass(frozen=True, slots=True)
class RetentionConfig:
    elastic_url: str = DEFAULT_ELASTIC_URL
    elastic_user: str | None = None
    elastic_password: str | None = None
    verify_ssl: bool = False
    enabled: bool = False
    index: str = DEFAULT_RETENTION_INDEX
    days: int = 15
    field: str = DEFAULT_RETENTION_FIELD
    interval_minutes: int = 360
    task_wait_timeout_seconds: int = TASK_WAIT_TIMEOUT_SECONDS

    @classmethod
    def from_env(cls) -> RetentionConfig:
        elastic_user = (os.getenv('ELASTIC_USER') or '').strip() or None
        elastic_password = os.getenv('ELASTIC_PASSWORD')
        if elastic_password is not None:
            elastic_password = elastic_password.strip() or None

        return cls(
            elastic_url=(os.getenv('ELASTIC_URL') or DEFAULT_ELASTIC_URL).strip() or DEFAULT_ELASTIC_URL,
            elastic_user=elastic_user,
            elastic_password=elastic_password,
            verify_ssl=_to_bool(os.getenv('ELASTIC_VERIFY_SSL'), False),
            enabled=_to_bool(os.getenv('ELASTIC_RETENTION_ENABLED'), False),
            index=(os.getenv('ELASTIC_RETENTION_INDEX') or DEFAULT_RETENTION_INDEX).strip() or DEFAULT_RETENTION_INDEX,
            days=_to_int(os.getenv('ELASTIC_RETENTION_DAYS'), default=15, minimum=1),
            field=(os.getenv('ELASTIC_RETENTION_FIELD') or DEFAULT_RETENTION_FIELD).strip() or DEFAULT_RETENTION_FIELD,
            interval_minutes=_to_int(os.getenv('ELASTIC_RETENTION_INTERVAL_MINUTES'), default=360, minimum=1),
            task_wait_timeout_seconds=_to_int(
                os.getenv('ELASTIC_RETENTION_TASK_MAX_WAIT_SECONDS'),
                default=TASK_WAIT_TIMEOUT_SECONDS,
                minimum=10,
            ),
        )


@dataclass(frozen=True, slots=True)
class RetentionRunResult:
    success: bool
    index: str
    field: str
    days: int
    total: int = 0
    deleted: int = 0
    batches: int = 0
    version_conflicts: int = 0
    failures_count: int = 0


class ElasticRetentionRunner:
    def __init__(self, config: RetentionConfig) -> None:
        self._config = config

    async def run(self) -> RetentionRunResult:
        timeout = httpx.Timeout(timeout=READ_TIMEOUT_SECONDS, connect=CONNECT_TIMEOUT_SECONDS)
        auth: tuple[str, str] | None = None
        if self._config.elastic_user:
            auth = (self._config.elastic_user, self._config.elastic_password or '')

        async with httpx.AsyncClient(
            base_url=self._config.elastic_url.rstrip('/'),
            timeout=timeout,
            verify=self._config.verify_ssl,
            auth=auth,
        ) as client:
            await self._ensure_cluster_reachable(client)
            index_exists = await self._index_exists(client, self._config.index)
            if not index_exists:
                logger.warning('Retencao ignorada: indice "%s" nao encontrado.', self._config.index)
                return RetentionRunResult(
                    success=False,
                    index=self._config.index,
                    field=self._config.field,
                    days=self._config.days,
                )

            target_field = self._config.field or DEFAULT_RETENTION_FIELD
            should_retry_with_fallback = False

            if target_field == DEFAULT_RETENTION_FIELD:
                has_primary_field = await self._has_field_mapping(client, self._config.index, DEFAULT_RETENTION_FIELD)
                should_retry_with_fallback = not has_primary_field

            primary_result = await self._delete_old_documents(client, self._config.index, target_field, self._config.days)

            if should_retry_with_fallback:
                logger.warning(
                    'Campo "%s" ausente no mapeamento do indice "%s". Tentando fallback "%s".',
                    DEFAULT_RETENTION_FIELD,
                    self._config.index,
                    DEFAULT_RETENTION_FALLBACK_FIELD,
                )
                return await self._delete_old_documents(
                    client,
                    self._config.index,
                    DEFAULT_RETENTION_FALLBACK_FIELD,
                    self._config.days,
                )

            return primary_result

    async def _ensure_cluster_reachable(self, client: httpx.AsyncClient) -> None:
        response = await client.get('/')
        if response.status_code >= 400:
            raise RuntimeError(
                f'Elasticsearch indisponivel para retencao (status HTTP {response.status_code}).'
            )

    async def _index_exists(self, client: httpx.AsyncClient, index_name: str) -> bool:
        encoded_index = quote(index_name, safe='')
        response = await client.head(f'/{encoded_index}')
        if response.status_code == 404:
            return False
        if response.status_code >= 400:
            raise RuntimeError(f'Falha ao verificar indice "{index_name}" (status HTTP {response.status_code}).')
        return True

    async def _has_field_mapping(self, client: httpx.AsyncClient, index_name: str, field_name: str) -> bool:
        encoded_index = quote(index_name, safe='')
        response = await client.get(f'/{encoded_index}/_mapping')
        if response.status_code >= 400:
            raise RuntimeError(
                f'Falha ao ler o mapeamento do indice "{index_name}" (status HTTP {response.status_code}).'
            )
        payload = response.json()
        for index_payload in payload.values():
            mappings = index_payload.get('mappings', {})
            if self._mapping_contains_field(mappings, field_name):
                return True
        return False

    def _mapping_contains_field(self, node: dict[str, Any], field_name: str) -> bool:
        properties = node.get('properties')
        if isinstance(properties, dict):
            if field_name in properties:
                return True
            for value in properties.values():
                if isinstance(value, dict) and self._mapping_contains_field(value, field_name):
                    return True
        fields = node.get('fields')
        if isinstance(fields, dict) and field_name in fields:
            return True
        return False

    async def _delete_old_documents(
        self,
        client: httpx.AsyncClient,
        index_name: str,
        field_name: str,
        retention_days: int,
    ) -> RetentionRunResult:
        encoded_index = quote(index_name, safe='')
        payload = {
            'query': {
                'range': {
                    field_name: {
                        'lt': f'now-{retention_days}d/d',
                    }
                }
            }
        }
        params = {
            'conflicts': 'proceed',
            'slices': 'auto',
            'wait_for_completion': 'false',
        }

        response = await client.post(f'/{encoded_index}/_delete_by_query', params=params, json=payload)
        if response.status_code >= 400:
            raise RuntimeError(
                (
                    f'Falha ao executar _delete_by_query no indice "{index_name}" '
                    f'com campo "{field_name}" (status HTTP {response.status_code}).'
                )
            )

        body = response.json()
        task_id = str(body.get('task') or '').strip()
        if task_id:
            task_body = await self._wait_for_task(client, task_id)
            response_stats = task_body.get('response', {}) if isinstance(task_body, dict) else {}
        else:
            response_stats = body.get('response', body)

        failures = response_stats.get('failures') or []
        return RetentionRunResult(
            success=True,
            index=index_name,
            field=field_name,
            days=retention_days,
            total=int(response_stats.get('total', 0)),
            deleted=int(response_stats.get('deleted', 0)),
            batches=int(response_stats.get('batches', 0)),
            version_conflicts=int(response_stats.get('version_conflicts', 0)),
            failures_count=len(failures),
        )

    async def _wait_for_task(self, client: httpx.AsyncClient, task_id: str) -> dict[str, Any]:
        deadline = monotonic() + float(self._config.task_wait_timeout_seconds)
        encoded_task_id = quote(task_id, safe=':')
        while True:
            if monotonic() >= deadline:
                raise RuntimeError(
                    f'Tempo limite excedido ao aguardar tarefa de retencao "{task_id}".'
                )

            response = await client.get(f'/_tasks/{encoded_task_id}')
            if response.status_code >= 400:
                raise RuntimeError(
                    (
                        f'Falha ao consultar tarefa de retencao "{task_id}" '
                        f'(status HTTP {response.status_code}).'
                    )
                )

            payload = response.json()
            if payload.get('completed') is True:
                return payload

            await asyncio.sleep(TASK_POLL_INTERVAL_SECONDS)


async def run_retention_once(config: RetentionConfig | None = None) -> RetentionRunResult:
    effective_config = config or RetentionConfig.from_env()
    runner = ElasticRetentionRunner(effective_config)
    try:
        result = await runner.run()
    except Exception:
        logger.warning(
            'Falha na retencao de documentos do Elasticsearch. url=%s indice=%s',
            _sanitize_url_for_logs(effective_config.elastic_url),
            effective_config.index,
        )
        return RetentionRunResult(
            success=False,
            index=effective_config.index,
            field=effective_config.field,
            days=effective_config.days,
        )

    if result.success:
        logger.info(
            (
                'Retencao concluida no Elasticsearch. indice=%s campo=%s dias=%s '
                'total=%s removidos=%s lotes=%s conflitos_versao=%s falhas=%s'
            ),
            result.index,
            result.field,
            result.days,
            result.total,
            result.deleted,
            result.batches,
            result.version_conflicts,
            result.failures_count,
        )
    else:
        logger.warning(
            'Retencao nao executada. indice=%s campo=%s dias=%s',
            result.index,
            result.field,
            result.days,
        )

    return result
