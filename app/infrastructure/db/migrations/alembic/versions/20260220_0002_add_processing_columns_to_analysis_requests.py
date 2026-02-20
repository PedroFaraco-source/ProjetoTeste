"""add_processing_columns_to_analysis_requests

Revision ID: 20260220_0002
Revises: 20260220_0001
Create Date: 2026-02-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '20260220_0002'
down_revision = '20260220_0001'
branch_labels = None
depends_on = None


def _get_column_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return {column['name'] for column in inspector.get_columns(table_name)}


def _get_index_names(table_name: str) -> set[str]:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return {index['name'] for index in inspector.get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'analysis_requests' not in inspector.get_table_names():
        return

    columns = _get_column_names('analysis_requests')

    if 'queue_message_id' not in columns:
        op.add_column('analysis_requests', sa.Column('queue_message_id', sa.String(length=128), nullable=True))
    if 'processing_success' not in columns:
        op.add_column(
            'analysis_requests',
            sa.Column('processing_success', sa.Boolean(), nullable=False, server_default=sa.text('0')),
        )
    if 'processing_status' not in columns:
        op.add_column(
            'analysis_requests',
            sa.Column('processing_status', sa.String(length=32), nullable=False, server_default=sa.text("'pending'")),
        )
    if 'failure_stage' not in columns:
        op.add_column('analysis_requests', sa.Column('failure_stage', sa.String(length=32), nullable=True))
    if 'failure_reason' not in columns:
        op.add_column('analysis_requests', sa.Column('failure_reason', sa.Text(), nullable=True))
    if 'elastic_saved' not in columns:
        op.add_column(
            'analysis_requests',
            sa.Column('elastic_saved', sa.Boolean(), nullable=False, server_default=sa.text('0')),
        )
    if 'elastic_index_name' not in columns:
        op.add_column('analysis_requests', sa.Column('elastic_index_name', sa.String(length=128), nullable=True))
    if 'processed_at_utc' not in columns:
        op.add_column('analysis_requests', sa.Column('processed_at_utc', sa.DateTime(timezone=True), nullable=True))

    indexes = _get_index_names('analysis_requests')
    if 'ix_analysis_requests_queue_message_id' not in indexes:
        op.create_index(
            'ix_analysis_requests_queue_message_id',
            'analysis_requests',
            ['queue_message_id'],
            unique=False,
        )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if 'analysis_requests' not in inspector.get_table_names():
        return

    indexes = _get_index_names('analysis_requests')
    if 'ix_analysis_requests_queue_message_id' in indexes:
        op.drop_index('ix_analysis_requests_queue_message_id', table_name='analysis_requests')

    columns = _get_column_names('analysis_requests')

    if 'processed_at_utc' in columns:
        op.drop_column('analysis_requests', 'processed_at_utc')
    if 'elastic_index_name' in columns:
        op.drop_column('analysis_requests', 'elastic_index_name')
    if 'elastic_saved' in columns:
        op.drop_column('analysis_requests', 'elastic_saved')
    if 'failure_reason' in columns:
        op.drop_column('analysis_requests', 'failure_reason')
    if 'failure_stage' in columns:
        op.drop_column('analysis_requests', 'failure_stage')
    if 'processing_status' in columns:
        op.drop_column('analysis_requests', 'processing_status')
    if 'processing_success' in columns:
        op.drop_column('analysis_requests', 'processing_success')
    if 'queue_message_id' in columns:
        op.drop_column('analysis_requests', 'queue_message_id')
