"""normalize_user_ids_as_string

Revision ID: 20260220_0003
Revises: 20260220_0002
Create Date: 2026-02-20
"""

from __future__ import annotations

import json

from alembic import op
import sqlalchemy as sa

revision = '20260220_0003'
down_revision = '20260220_0002'
branch_labels = None
depends_on = None


def _normalize_to_plain_text(value: str | None) -> str:
    raw = (value or '').strip()
    if not raw:
        return ''

    try:
        parsed = json.loads(raw)
    except Exception:
        return raw

    if isinstance(parsed, list):
        cleaned = [str(item).strip() for item in parsed if str(item).strip()]
        return ','.join(cleaned)
    return raw


def _to_json_array_text(value: str | None) -> str:
    raw = (value or '').strip()
    if not raw:
        return '[]'

    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = None

    if isinstance(parsed, list):
        return json.dumps(parsed, ensure_ascii=False)

    values = [item.strip() for item in raw.split(',') if item.strip()]
    return json.dumps(values, ensure_ascii=False)


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'analysis_requests' not in inspector.get_table_names():
        return

    rows = bind.execute(sa.text('SELECT id, user_ids FROM analysis_requests')).fetchall()
    for row in rows:
        normalized = _normalize_to_plain_text(row.user_ids)
        bind.execute(
            sa.text('UPDATE analysis_requests SET user_ids = :user_ids WHERE id = :id'),
            {'id': row.id, 'user_ids': normalized},
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'analysis_requests' not in inspector.get_table_names():
        return

    rows = bind.execute(sa.text('SELECT id, user_ids FROM analysis_requests')).fetchall()
    for row in rows:
        as_json_array = _to_json_array_text(row.user_ids)
        bind.execute(
            sa.text('UPDATE analysis_requests SET user_ids = :user_ids WHERE id = :id'),
            {'id': row.id, 'user_ids': as_json_array},
        )
