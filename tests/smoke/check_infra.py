from __future__ import annotations

import sys
from pathlib import Path

import httpx
from sqlalchemy import create_engine, text

try:
    import pika
except ModuleNotFoundError:  # pragma: no cover - depende do ambiente
    pika = None

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.config.settings import get_settings  # noqa: E402


def check_rabbit(settings) -> tuple[bool, str]:
    if pika is None:
        return False, 'Dependencia pika nao instalada.'
    if not settings.rabbitmq_url:
        return False, 'RABBITMQ_URL nao configurada.'
    try:
        connection = pika.BlockingConnection(pika.URLParameters(settings.rabbitmq_url))
        connection.close()
        return True, 'RabbitMQ OK'
    except Exception:
        return False, 'Falha ao conectar no RabbitMQ.'


def check_elastic(settings) -> tuple[bool, str]:
    if not settings.elasticsearch_url:
        return False, 'ELASTICSEARCH_URL nao configurada.'
    url = settings.elasticsearch_url.rstrip('/') + '/'
    try:
        response = httpx.get(url, timeout=5)
        if response.status_code < 500:
            return True, f'Elasticsearch OK (status={response.status_code})'
        return False, f'Elasticsearch respondeu erro (status={response.status_code})'
    except Exception:
        return False, 'Falha ao conectar no Elasticsearch.'


def check_sql(settings) -> tuple[bool, str]:
    try:
        engine = create_engine(settings.sqlalchemy_url)
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        engine.dispose()
        return True, 'SQL OK'
    except Exception:
        return False, 'Falha ao conectar no SQL.'


def main() -> None:
    settings = get_settings()
    checks = [
        ('RabbitMQ', check_rabbit(settings)),
        ('Elasticsearch', check_elastic(settings)),
        ('SQL', check_sql(settings)),
    ]

    print('=== Check de infraestrutura ===')
    for name, (ok, message) in checks:
        status = 'OK' if ok else 'FALHA'
        print(f'[{status}] {name}: {message}')


if __name__ == '__main__':
    main()
