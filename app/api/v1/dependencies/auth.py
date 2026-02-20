from __future__ import annotations

from fastapi import Request


def get_optional_auth_token(request: Request) -> str | None:
    auth = request.headers.get('Authorization', '').strip()
    return auth or None
