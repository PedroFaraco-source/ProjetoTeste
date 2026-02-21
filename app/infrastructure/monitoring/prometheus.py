from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

http_requests_total = Counter(
    'http_requests_total',
    'Total de requisicoes HTTP por metodo, path e status.',
    ['method', 'path', 'status'],
)

http_requests_status_class_total = Counter(
    'http_requests_status_class_total',
    'Total de requisicoes HTTP por metodo, path e classe de status.',
    ['method', 'path', 'status_class'],
)

http_inflight_requests = Gauge(
    'http_inflight_requests',
    'Quantidade de requisicoes HTTP em andamento.',
)

http_request_duration_seconds = Histogram(
    'http_request_duration_seconds',
    'Duracao das requisicoes HTTP em segundos.',
    ['method', 'path'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

http_ack_duration_seconds = Histogram(
    'http_ack_duration_seconds',
    'Duracao ate o ACK da resposta HTTP em segundos.',
    ['method', 'path'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

http_exception_total = Counter(
    'http_exception_total',
    'Total de excecoes HTTP por tipo e classe de status.',
    ['exception_type', 'status_class'],
)

tempo_db_ms = Histogram(
    'tempo_db_ms',
    'Tempo de caminho rapido no banco em milissegundos.',
    ['operation'],
    buckets=(1, 2, 5, 10, 25, 50, 100, 200, 500, 1000, 2000, 5000),
)

rabbit_publish_failures_total = Counter(
    'rabbit_publish_failures_total',
    'Total de falhas ao publicar eventos no RabbitMQ.',
)

rabbit_publish_total = Counter(
    'rabbit_publish_total',
    'Total de publicacoes no RabbitMQ por resultado.',
    ['result'],
)

rabbit_publish_duration_seconds = Histogram(
    'rabbit_publish_duration_seconds',
    'Duracao da publicacao no RabbitMQ em segundos.',
    ['result'],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

ingest_processed_total = Counter(
    'ingest_processed_total',
    'Total de mensagens processadas com persistencia em banco.',
)

ingest_failed_total = Counter(
    'ingest_failed_total',
    'Total de falhas de ingestao.',
)

consumer_messages_total = Counter(
    'consumer_messages_total',
    'Total de mensagens processadas pelo consumer por resultado.',
    ['event_name', 'result'],
)

consumer_failures_total = Counter(
    'consumer_failures_total',
    'Total de falhas do consumer por etapa.',
    ['stage'],
)

consumer_processing_duration_seconds = Histogram(
    'consumer_processing_duration_seconds',
    'Duracao de processamento do consumer em segundos.',
    ['event_name', 'result'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)

elastic_bulk_requests_total = Counter(
    'elastic_bulk_requests_total',
    'Total de operacoes bulk no Elasticsearch.',
    ['operation', 'result'],
)

elastic_bulk_documents_total = Counter(
    'elastic_bulk_documents_total',
    'Total de documentos enviados em bulk para Elasticsearch.',
    ['operation', 'result'],
)

elastic_bulk_duration_seconds = Histogram(
    'elastic_bulk_duration_seconds',
    'Duracao de operacoes bulk no Elasticsearch em segundos.',
    ['operation'],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)

elastic_bulk_errors_total = Counter(
    'elastic_bulk_errors_total',
    'Total de erros em operacoes bulk no Elasticsearch.',
    ['operation'],
)

elastic_log_failures_total = Counter(
    'elastic_log_failures_total',
    'Total de falhas ao registrar logs HTTP no Elasticsearch.',
)

e2e_time_to_processed_seconds = Histogram(
    'e2e_time_to_processed_seconds',
    'Tempo fim a fim ate processado no consumer.',
    ['event_name'],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

e2e_time_to_indexed_seconds = Histogram(
    'e2e_time_to_indexed_seconds',
    'Tempo fim a fim ate indexado no Elasticsearch.',
    ['event_name'],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120),
)

analyze_requests_total = Counter(
    'analyze_requests_total',
    'Total de requisicoes POST /analyze-feed.',
)

analyze_feed_requests_total = Counter(
    'analyze_feed_requests_total',
    'Total de requisicoes POST /analyze-feed (compatibilidade).',
)

analyze_feed_failed_total = Counter(
    'analyze_feed_failed_total',
    'Total de erros em POST /analyze-feed (compatibilidade).',
)

_ALLOWED_EXCEPTION_TYPES = {
    'ApiValidationError',
    'RequestValidationError',
    'HTTPException',
    'RuntimeError',
    'ValueError',
    'TypeError',
    'ConnectionError',
    'TimeoutError',
    'OperationalError',
    'IntegrityError',
    'AssertionError',
    'UnknownError',
}


def status_class_from_code(status_code: int) -> str:
    if 200 <= status_code < 300:
        return '2xx'
    if 400 <= status_code < 500:
        return '4xx'
    if 500 <= status_code < 600:
        return '5xx'
    return 'other'


def bounded_exception_type(value: str | None) -> str:
    candidate = str(value or '').strip()[:64]
    if not candidate:
        return 'UnknownError'
    if candidate in _ALLOWED_EXCEPTION_TYPES:
        return candidate
    return 'OtherError'
