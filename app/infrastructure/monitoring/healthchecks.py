from __future__ import annotations

from typing import Any

from sqlalchemy import text

from app.core.config.settings import get_settings
from app.infrastructure.db.session import get_session_factory

try:
    import pika
except ModuleNotFoundError:
    pika = None


def check_database_ready() -> tuple[bool, str]:
    try:
        session_factory = get_session_factory()
        with session_factory() as session:
            session.execute(text('SELECT 1'))
        return True, 'ok'
    except Exception:
        return False, 'falha_db'


def check_rabbit_ready() -> tuple[bool, str]:
    settings = get_settings()
    if not settings.enable_rabbit:
        return True, 'desabilitado'
    if pika is None:
        return False, 'pika_nao_instalado'
    try:
        params = pika.URLParameters(settings.rabbitmq_url)
        params.socket_timeout = 2
        params.blocked_connection_timeout = 2
        connection = pika.BlockingConnection(params)
        if connection.is_open:
            connection.close()
        return True, 'ok'
    except Exception:
        return False, 'falha_rabbit'


def build_readiness_payload() -> tuple[bool, dict[str, Any]]:
    db_ok, db_message = check_database_ready()
    rabbit_ok, rabbit_message = check_rabbit_ready()
    ready = db_ok and rabbit_ok
    return ready, {
        'status': 'ready' if ready else 'not_ready',
        'checks': {
            'database': {'ok': db_ok, 'detail': db_message},
            'rabbitmq': {'ok': rabbit_ok, 'detail': rabbit_message},
        },
    }
