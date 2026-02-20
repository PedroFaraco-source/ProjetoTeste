from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config

from app.core.config.settings import get_settings
from app.infrastructure.runtime.bootstrap_db import ensure_database_exists

logger = logging.getLogger(__name__)
MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / 'db' / 'migrations'


def run_migrations() -> None:
    settings = get_settings()
    alembic_ini = MIGRATIONS_DIR / 'alembic.ini'

    if not alembic_ini.exists():
        raise RuntimeError('Arquivo alembic.ini nao encontrado.')

    config = Config(str(alembic_ini))
    config.set_main_option('script_location', str(MIGRATIONS_DIR / 'alembic'))

    os.environ['PROJECTMBRAS_DATABASE_URL'] = settings.sqlalchemy_url
    try:
        command.upgrade(config, 'head')
    finally:
        os.environ.pop('PROJECTMBRAS_DATABASE_URL', None)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    try:
        logger.info('Iniciando fluxo de migracao.')
        ensure_database_exists()
        run_migrations()
        logger.info('Migracoes executadas com sucesso.')
    except Exception:
        logger.error('Falha na execucao das migracoes.')
        raise


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
