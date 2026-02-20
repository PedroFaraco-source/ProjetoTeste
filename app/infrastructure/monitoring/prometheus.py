from prometheus_client import Counter, Histogram

http_requests_total = Counter(
    'http_requests_total',
    'Total de requisicoes HTTP por metodo, path e status.',
    ['method', 'path', 'status'],
)

http_request_duration_seconds = Histogram(
    'http_request_duration_seconds',
    'Duracao das requisicoes HTTP em segundos.',
    ['method', 'path'],
)

rabbit_publish_failures_total = Counter(
    'rabbit_publish_failures_total',
    'Total de falhas ao publicar eventos no RabbitMQ.',
)

ingest_processed_total = Counter(
    'ingest_processed_total',
    'Total de mensagens processadas com persistencia em banco.',
)

ingest_failed_total = Counter(
    'ingest_failed_total',
    'Total de falhas de ingestao.',
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
