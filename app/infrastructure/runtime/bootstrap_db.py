from __future__ import annotations

import logging
import time
from typing import Callable, TypeVar

import pyodbc

from app.core.config.settings import get_settings

logger = logging.getLogger(__name__)
MAX_RETRIES = 10
RETRY_SLEEP_SECONDS = 1

T = TypeVar('T')


def _mask_host(host: str) -> str:
    if not host:
        return 'indefinido'
    if len(host) <= 4:
        return '*' * len(host)
    return f'{host[:2]}***{host[-2:]}'


def _build_master_connection_string() -> str:
    settings = get_settings()
    driver = settings.sqlserver_odbc_driver or 'ODBC Driver 18 for SQL Server'
    return (
        f'DRIVER={{{driver}}};'
        f'SERVER={settings.sqlserver_host},{settings.sqlserver_port};'
        'DATABASE=master;'
        f'UID={settings.sqlserver_user};'
        f'PWD={settings.sqlserver_password};'
        'TrustServerCertificate=yes;'
    )


def _safe_identifier(name: str) -> str:
    return name.replace(']', ']]')


def _safe_literal(name: str) -> str:
    return name.replace("'", "''")


def _with_retry(operation_name: str, fn: Callable[[], T]) -> T:
    last_error: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as exc:
            last_error = exc
            if attempt >= MAX_RETRIES:
                logger.error('Falha ao criar banco de dados.')
                raise
            logger.warning(
                'Falha temporaria ao acessar SQL Server. operacao=%s tentativa=%s/%s',
                operation_name,
                attempt,
                MAX_RETRIES,
            )
            time.sleep(RETRY_SLEEP_SECONDS)

    if last_error is not None:
        raise last_error
    raise RuntimeError('Falha inesperada durante retry.')


def ensure_database_exists() -> None:
    settings = get_settings()
    db_name = settings.sqlserver_database
    connection_string = _build_master_connection_string()

    logger.info(
        'Iniciando bootstrap do banco. ambiente=%s host=%s porta=%s banco=%s',
        settings.app_env,
        _mask_host(settings.sqlserver_host),
        settings.sqlserver_port,
        settings.sqlserver_database,
    )

    def _check_exists() -> bool:
        with pyodbc.connect(connection_string, autocommit=True, timeout=5) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT DB_ID(?)', db_name)
            return cursor.fetchone()[0] is not None

    existed_before = _with_retry('verificacao do banco', _check_exists)

    def _ensure() -> None:
        with pyodbc.connect(connection_string, autocommit=True, timeout=5) as connection:
            cursor = connection.cursor()
            statement = f"IF DB_ID(N'{_safe_literal(db_name)}') IS NULL CREATE DATABASE [{_safe_identifier(db_name)}]"
            cursor.execute(statement)
            cursor.execute('SELECT DB_ID(?)', db_name)
            created_id = cursor.fetchone()[0]
            if created_id is None:
                raise RuntimeError('Banco nao foi criado.')

    _with_retry('criacao/verificacao do banco', _ensure)

    if existed_before:
        logger.info('Banco de dados já existe. Prosseguindo.')
    else:
        logger.info('Banco de dados criado com sucesso.')


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    ensure_database_exists()


if __name__ == '__main__':
    main()
