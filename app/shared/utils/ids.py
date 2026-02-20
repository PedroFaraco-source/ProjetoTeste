from __future__ import annotations

import uuid


def new_uuid_str() -> str:
    return str(uuid.uuid4())
