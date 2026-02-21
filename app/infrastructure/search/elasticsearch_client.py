from __future__ import annotations

from typing import Any

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


class ElasticIndexWriter:
    def __init__(self, elasticsearch_url: str, timeout_seconds: int = 2) -> None:
        self._url = elasticsearch_url
        self._client: Elasticsearch | None = None
        self._timeout_seconds = timeout_seconds

    def _client_or_none(self) -> Elasticsearch | None:
        if not self._url:
            return None
        if self._client is None:
            self._client = Elasticsearch(
                self._url,
                request_timeout=max(1, self._timeout_seconds),
                retry_on_timeout=False,
                max_retries=0,
            )
        return self._client

    def _ensure_alias(self, alias_name: str, index_name: str) -> None:
        client = self._client_or_none()
        if client is None:
            raise RuntimeError('ELASTICSEARCH_URL nao configurada.')
        client.indices.put_alias(index=index_name, name=alias_name)

    def ensure_index_template(
        self,
        *,
        template_name: str,
        index_patterns: list[str],
        mappings: dict[str, Any],
        settings: dict[str, Any] | None = None,
    ) -> None:
        client = self._client_or_none()
        if client is None:
            raise RuntimeError('ELASTICSEARCH_URL nao configurada.')
        body: dict[str, Any] = {
            'index_patterns': index_patterns,
            'template': {
                'mappings': mappings,
            },
        }
        if settings:
            body['template']['settings'] = settings
        client.indices.put_index_template(name=template_name, **body)

    def write(
        self,
        index_name: str,
        document: dict[str, Any],
        alias_name: str | None = None,
    ) -> None:
        client = self._client_or_none()
        if client is None:
            raise RuntimeError('ELASTICSEARCH_URL nao configurada.')
        client.index(index=index_name, document=document)
        if alias_name:
            self._ensure_alias(alias_name=alias_name, index_name=index_name)

    def bulk_write(
        self,
        documents: list[dict[str, Any]],
    ) -> tuple[int, list[dict[str, Any]]]:
        client = self._client_or_none()
        if client is None:
            raise RuntimeError('ELASTICSEARCH_URL nao configurada.')
        if not documents:
            return 0, []
        success_count, errors = bulk(
            client,
            documents,
            stats_only=False,
            raise_on_error=False,
            raise_on_exception=False,
            request_timeout=max(1, self._timeout_seconds),
        )
        return int(success_count), list(errors or [])

    def elastic_write(
        self,
        index_name: str,
        document: dict[str, Any],
        alias_name: str | None = None,
    ) -> None:
        self.write(index_name=index_name, document=document, alias_name=alias_name)

    def ping(self) -> bool:
        client = self._client_or_none()
        if client is None:
            return False
        return bool(client.ping())


class ElasticIndexSearcher:
    def __init__(self, elasticsearch_url: str, timeout_seconds: int = 2) -> None:
        self._writer = ElasticIndexWriter(elasticsearch_url, timeout_seconds)

    def search(self, index_name: str, query: dict[str, Any], size: int = 50) -> list[dict[str, Any]]:
        client = self._writer._client_or_none()
        if client is None:
            return []
        response = client.search(index=index_name, query=query, size=size)
        hits = response.get('hits', {}).get('hits', [])
        return [item.get('_source', {}) for item in hits]
