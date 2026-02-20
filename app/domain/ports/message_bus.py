from __future__ import annotations

from typing import Protocol


class MessageBusPort(Protocol):
    def publish_event(self, event: dict, routing_key: str | None = None, headers: dict | None = None) -> bool:
        ...
