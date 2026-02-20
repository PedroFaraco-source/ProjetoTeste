"""create_analysis_requests_table

Revision ID: 20260220_0001
Revises:
Create Date: 2026-02-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '20260220_0001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if 'analysis_requests' in inspector.get_table_names():
        return

    op.create_table(
        'analysis_requests',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('created_at_utc', sa.DateTime(timezone=True), nullable=False),
        sa.Column('correlation_id', sa.String(length=64), nullable=False),
        sa.Column('messages_count', sa.Integer(), nullable=False),
        sa.Column('user_ids', sa.Text(), nullable=False),
        sa.Column('time_window_minutes', sa.Integer(), nullable=False),
        sa.Column('analysis_json', sa.Text(), nullable=False),
        sa.Column('flags_json', sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_analysis_requests_created_at_utc', 'analysis_requests', ['created_at_utc'], unique=False)
    op.create_index('ix_analysis_requests_correlation_id', 'analysis_requests', ['correlation_id'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'analysis_requests' not in inspector.get_table_names():
        return

    op.drop_index('ix_analysis_requests_correlation_id', table_name='analysis_requests')
    op.drop_index('ix_analysis_requests_created_at_utc', table_name='analysis_requests')
    op.drop_table('analysis_requests')
