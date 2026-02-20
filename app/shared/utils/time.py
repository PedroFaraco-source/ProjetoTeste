from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.config.settings import get_settings


def utc_now() -> datetime:
    return app_now()


def get_app_timezone() -> tzinfo:
    settings = get_settings()
    timezone_name = (settings.app_timezone or settings.tz or 'America/Sao_Paulo').strip() or 'America/Sao_Paulo'
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return timezone(timedelta(hours=-3))


def app_now() -> datetime:
    return datetime.now(get_app_timezone())


def to_app_timezone(value: datetime) -> datetime:
    app_timezone = get_app_timezone()
    if value.tzinfo is None:
        return value.replace(tzinfo=app_timezone)
    return value.astimezone(app_timezone)


def to_rfc3339_app(value: datetime) -> str:
    return to_app_timezone(value).isoformat()
