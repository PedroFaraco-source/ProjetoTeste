"""add_outbox_events

Revision ID: 20260220_0005
Revises: 20260220_0004
Create Date: 2026-02-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '20260220_0005'
down_revision = '20260220_0004'
branch_labels = None
depends_on = None


def _table_exists(name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return name in inspector.get_table_names()


def _index_exists(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for item in inspector.get_indexes(table_name):
        if item.get('name') == index_name:
            return True
    return False


def upgrade() -> None:
    if not _table_exists('outbox_events'):
        op.create_table(
            'outbox_events',
            sa.Column('id', sa.String(length=36), nullable=False),
            sa.Column('message_id', sa.String(length=36), nullable=False),
            sa.Column('correlation_id', sa.String(length=64), nullable=False),
            sa.Column('event_type', sa.String(length=64), nullable=False),
            sa.Column('payload', sa.JSON(), nullable=False),
            sa.Column('status', sa.String(length=16), nullable=False),
            sa.Column('attempts', sa.Integer(), nullable=False, server_default=sa.text('0')),
            sa.Column('last_error', sa.Text(), nullable=True),
            sa.Column('available_at_utc', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column('locked_at_utc', sa.DateTime(timezone=True), nullable=True),
            sa.Column('locked_by', sa.String(length=128), nullable=True),
            sa.Column('created_at_utc', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column('updated_at_utc', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.ForeignKeyConstraint(['message_id'], ['messages.id']),
            sa.PrimaryKeyConstraint('id'),
        )

    if not _index_exists('outbox_events', 'ix_outbox_events_status_available_at_utc'):
        op.create_index('ix_outbox_events_status_available_at_utc', 'outbox_events', ['status', 'available_at_utc'], unique=False)
    if not _index_exists('outbox_events', 'ix_outbox_events_locked_at_utc'):
        op.create_index('ix_outbox_events_locked_at_utc', 'outbox_events', ['locked_at_utc'], unique=False)
    if not _index_exists('outbox_events', 'ix_outbox_events_correlation_id'):
        op.create_index('ix_outbox_events_correlation_id', 'outbox_events', ['correlation_id'], unique=False)


def downgrade() -> None:
    if _table_exists('outbox_events'):
        op.drop_table('outbox_events')
