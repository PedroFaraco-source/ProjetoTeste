from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config.settings import get_settings

Base = declarative_base()
SessionLocal = sessionmaker(autoflush=False, autocommit=False, expire_on_commit=False)

_ENGINE = None


def _build_engine():
    settings = get_settings()
    url = settings.sqlalchemy_url

    if url.startswith('sqlite') and ':memory:' in url:
        return create_engine(
            url,
            connect_args={'check_same_thread': False},
            poolclass=StaticPool,
            future=True,
        )

    if url.startswith('sqlite'):
        return create_engine(
            url,
            connect_args={'check_same_thread': False},
            future=True,
        )

    if url.startswith('mssql+pyodbc'):
        return create_engine(
            url,
            pool_pre_ping=True,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_timeout=settings.db_pool_timeout,
            pool_recycle=settings.db_pool_recycle,
            fast_executemany=True,
            future=True,
        )

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_timeout=settings.db_pool_timeout,
        pool_recycle=settings.db_pool_recycle,
        future=True,
    )


def get_engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = _build_engine()
        SessionLocal.configure(bind=_ENGINE)
    return _ENGINE


def init_db() -> None:
    import app.infrastructure.db.models  # noqa: F401

    engine = get_engine()
    settings = get_settings()
    if settings.sqlalchemy_url.startswith('sqlite'):
        Base.metadata.create_all(bind=engine)


def shutdown_db() -> None:
    global _ENGINE
    if _ENGINE is not None:
        _ENGINE.dispose()
        _ENGINE = None
        SessionLocal.configure(bind=None)


def get_session_factory():
    get_engine()
    return SessionLocal


def get_db():
    get_engine()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
