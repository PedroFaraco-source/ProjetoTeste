from __future__ import annotations

from typing import Any

from fastapi import Request


class _NullRabbitBus:
    def publish_event(self, event: dict[str, Any], routing_key: str | None = None, headers: dict[str, Any] | None = None) -> bool:
        return False

    def close(self) -> None:
        return None


def get_publisher(request: Request):
    return getattr(request.app.state, 'rabbit_bus', _NullRabbitBus())


def get_correlation_id(request: Request) -> str:
    return str(getattr(request.state, 'correlation_id', '')).strip()
