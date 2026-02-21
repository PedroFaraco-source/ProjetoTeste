from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote_plus
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, ConfigDict

ENV_PATH = Path('.env')


def _load_dotenv() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip().lstrip('\ufeff')
        value = value.strip()
        if key and key not in os.environ:
            os.environ[key] = value


def _to_bool(value: str, default: bool) -> bool:
    if value is None or str(value).strip() == '':
        return default
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def _to_int(value: str, default: int) -> int:
    if value is None or str(value).strip() == '':
        return default
    return int(str(value).strip())


def _running_in_container() -> bool:
    return Path('/.dockerenv').exists()


def _resolve_sqlserver_host(host: str) -> str:
    normalized = (host or '').strip()
    if normalized.lower() in {'localhost', '127.0.0.1'} and _running_in_container():
        return 'host.docker.internal'
    return normalized or 'localhost'


def _resolve_service_url(url: str) -> str:
    normalized = (url or '').strip()
    if not normalized:
        return normalized
    if not _running_in_container():
        return normalized

    parsed = urlsplit(normalized)
    host = parsed.hostname or ''
    if host.lower() not in {'localhost', '127.0.0.1'}:
        return normalized

    userinfo = ''
    if parsed.username:
        userinfo = parsed.username
        if parsed.password is not None:
            userinfo += f':{parsed.password}'
        userinfo += '@'

    port = f':{parsed.port}' if parsed.port is not None else ''
    netloc = f'{userinfo}host.docker.internal{port}'
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


class Settings(BaseModel):
    model_config = ConfigDict(extra='ignore')

    app_env: str = 'local'
    api_http_port: int = 8000
    app_timezone: str = 'America/Sao_Paulo'
    tz: str = 'America/Sao_Paulo'

    rabbitmq_url: str = 'amqp://admin:admin123@localhost:5672/'
    rabbitmq_exchange: str = 'api.events'
    rabbitmq_routing_key_analyze: str = 'mbras.analyze'
    rabbitmq_queue_analyze: str = 'mbras.analyze.queue'

    elasticsearch_url: str = 'http://elasticsearch:9200'
    kibana_url: str = 'http://kibana:5601'
    elasticsearch_index_prefix: str = 'projetombras-api-events'
    elastic_logs_index_prefix: str = 'projetombras-logs'
    elastic_logs_template_name: str = 'projetombras-logs-template'

    sqlserver_host: str = 'localhost'
    sqlserver_port: int = 1434
    sqlserver_database: str = 'ProjetoMBras'
    sqlserver_user: str = 'sa'
    sqlserver_password: str = ''
    sqlserver_odbc_driver: str = 'ODBC Driver 18 for SQL Server'
    database_url: str | None = None

    prometheus_metrics_enabled: bool = True
    enable_rabbit: bool = True
    bypass_persistence_for_tests: bool = False
    bypass_rabbit_for_tests: bool = False
    bypass_elastic_for_tests: bool = False

    db_pool_size: int = 20
    db_max_overflow: int = 30
    db_pool_timeout: int = 30
    db_pool_recycle: int = 1800
    worker_retry_limit: int = 5
    outbox_poll_interval_ms: int = 300
    outbox_lock_timeout_seconds: int = 30
    outbox_batch_size: int = 200
    outbox_worker_id: str = 'outbox-worker-local'

    rabbit_publish_timeout_seconds: int = 2
    elastic_timeout_seconds: int = 2

    elastic_http_log_queue_size: int = 5000
    elastic_http_log_batch_size: int = 100
    elastic_http_log_flush_interval_ms: int = 1000
    http_log_include_stacktrace: bool = False
    http_log_body_max_bytes: int = 65536

    @property
    def sqlalchemy_url(self) -> str:
        if self.database_url:
            return self.database_url
        if self.sqlserver_host and self.sqlserver_database and self.sqlserver_user and self.sqlserver_password:
            user = quote_plus(self.sqlserver_user)
            password = quote_plus(self.sqlserver_password)
            database = quote_plus(self.sqlserver_database)
            driver = quote_plus(self.sqlserver_odbc_driver)
            return (
                f'mssql+pyodbc://{user}:{password}@{self.sqlserver_host}:{self.sqlserver_port}/{database}'
                f'?driver={driver}&Encrypt=no&TrustServerCertificate=yes'
            )

        return 'sqlite:///./projetombras.db'


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    _load_dotenv()
    sqlserver_host_raw = os.getenv('SQLSERVER_HOST', 'localhost').strip()

    return Settings(
        app_env=os.getenv('APP_ENV', 'local').strip().lower() or 'local',
        api_http_port=_to_int(os.getenv('API_HTTP_PORT'), 8000),
        app_timezone=os.getenv('APP_TIMEZONE', 'America/Sao_Paulo').strip() or 'America/Sao_Paulo',
        tz=os.getenv('TZ', 'America/Sao_Paulo').strip() or 'America/Sao_Paulo',
        rabbitmq_url=_resolve_service_url(os.getenv('RABBITMQ_URL', 'amqp://admin:admin123@localhost:5672/').strip()),
        rabbitmq_exchange=os.getenv('RABBITMQ_EXCHANGE', 'api.events').strip(),
        rabbitmq_routing_key_analyze=os.getenv('RABBITMQ_ROUTING_KEY_ANALYZE', 'mbras.analyze').strip(),
        rabbitmq_queue_analyze=os.getenv('RABBITMQ_QUEUE_ANALYZE', 'mbras.analyze.queue').strip(),
        elasticsearch_url=_resolve_service_url(os.getenv('ELASTICSEARCH_URL', 'http://elasticsearch:9200').strip()),
        kibana_url=os.getenv('KIBANA_URL', 'http://kibana:5601').strip(),
        elasticsearch_index_prefix=os.getenv('ELASTICSEARCH_INDEX_PREFIX', 'projetombras-api-events').strip(),
        elastic_logs_index_prefix=os.getenv('ELASTIC_LOGS_INDEX_PREFIX', 'projetombras-logs').strip() or 'projetombras-logs',
        elastic_logs_template_name=(
            os.getenv('ELASTIC_LOGS_TEMPLATE_NAME', 'projetombras-logs-template').strip() or 'projetombras-logs-template'
        ),
        sqlserver_host=_resolve_sqlserver_host(sqlserver_host_raw),
        sqlserver_port=_to_int(os.getenv('SQLSERVER_PORT'), 1434),
        sqlserver_database=os.getenv('SQLSERVER_DATABASE', 'ProjetoMBras').strip(),
        sqlserver_user=os.getenv('SQLSERVER_USER', 'sa').strip(),
        sqlserver_password=os.getenv('SQLSERVER_PASSWORD', '').strip(),
        sqlserver_odbc_driver=os.getenv('SQLSERVER_ODBC_DRIVER', 'ODBC Driver 18 for SQL Server').strip()
        or 'ODBC Driver 18 for SQL Server',
        database_url=os.getenv('DATABASE_URL', '').strip() or None,
        prometheus_metrics_enabled=_to_bool(os.getenv('PROMETHEUS_METRICS_ENABLED'), True),
        enable_rabbit=_to_bool(os.getenv('ENABLE_RABBIT'), True),
        bypass_persistence_for_tests=_to_bool(os.getenv('BYPASS_PERSISTENCE_FOR_TESTS'), False),
        bypass_rabbit_for_tests=_to_bool(os.getenv('BYPASS_RABBIT_FOR_TESTS'), False),
        bypass_elastic_for_tests=_to_bool(os.getenv('BYPASS_ELASTIC_FOR_TESTS'), False),
        db_pool_size=_to_int(os.getenv('DB_POOL_SIZE'), 20),
        db_max_overflow=_to_int(os.getenv('DB_MAX_OVERFLOW'), 30),
        db_pool_timeout=_to_int(os.getenv('DB_POOL_TIMEOUT'), 30),
        db_pool_recycle=_to_int(os.getenv('DB_POOL_RECYCLE'), 1800),
        worker_retry_limit=_to_int(os.getenv('WORKER_RETRY_LIMIT'), 5),
        outbox_poll_interval_ms=_to_int(os.getenv('OUTBOX_POLL_INTERVAL_MS'), 300),
        outbox_lock_timeout_seconds=_to_int(os.getenv('OUTBOX_LOCK_TIMEOUT_SECONDS'), 30),
        outbox_batch_size=_to_int(os.getenv('OUTBOX_BATCH_SIZE'), 200),
        outbox_worker_id=(os.getenv('OUTBOX_WORKER_ID', 'outbox-worker-local').strip() or 'outbox-worker-local'),
        rabbit_publish_timeout_seconds=_to_int(os.getenv('RABBIT_PUBLISH_TIMEOUT_SECONDS'), 2),
        elastic_timeout_seconds=_to_int(os.getenv('ELASTIC_TIMEOUT_SECONDS'), 2),
        elastic_http_log_queue_size=_to_int(os.getenv('ELASTIC_HTTP_LOG_QUEUE_SIZE'), 5000),
        elastic_http_log_batch_size=_to_int(os.getenv('ELASTIC_HTTP_LOG_BATCH_SIZE'), 100),
        elastic_http_log_flush_interval_ms=_to_int(os.getenv('ELASTIC_HTTP_LOG_FLUSH_INTERVAL_MS'), 1000),
        http_log_include_stacktrace=_to_bool(os.getenv('HTTP_LOG_INCLUDE_STACKTRACE'), False),
        http_log_body_max_bytes=_to_int(os.getenv('HTTP_LOG_BODY_MAX_BYTES'), 65536),
    )


def reload_settings() -> Settings:
    get_settings.cache_clear()
    return get_settings()
