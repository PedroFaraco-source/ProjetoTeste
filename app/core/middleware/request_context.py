from __future__ import annotations

from fastapi import Request


def get_request_correlation_id(request: Request) -> str:
    return str(getattr(request.state, 'correlation_id', '')).strip()
