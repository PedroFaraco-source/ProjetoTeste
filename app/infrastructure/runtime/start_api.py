from __future__ import annotations

import logging
import sys

import uvicorn

from app.core.config.settings import get_settings
from app.infrastructure.runtime.migrate import main as migrate_main

logger = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    settings = get_settings()

    logger.info('Executando migracoes antes de iniciar a API.')
    migrate_main()

    logger.info('Iniciando API.')
    uvicorn.run('app.main:app', host='0.0.0.0', port=settings.api_http_port)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        logger.error('Falha ao iniciar a API apos migracoes.')
        sys.exit(1)
