"""rebuild_normalized_schema

Revision ID: 20260220_0004
Revises: 20260220_0003
Create Date: 2026-02-20
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = '20260220_0004'
down_revision = '20260220_0003'
branch_labels = None
depends_on = None


def _table_exists(name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return name in inspector.get_table_names()


def upgrade() -> None:
    for table_name in [
        'message_topics',
        'influence_ranking_items',
        'message_processing',
        'message_anomalies',
        'message_flags',
        'message_sentiments',
        'messages',
        'topics',
        'users',
        'analysis_requests',
    ]:
        if _table_exists(table_name):
            op.drop_table(table_name)

    op.create_table(
        'users',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('external_user_key', sa.String(length=128), nullable=True),
        sa.Column('created_at_utc', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_users_external_user_key', 'users', ['external_user_key'], unique=True)

    op.create_table(
        'messages',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('user_id', sa.String(length=36), nullable=False),
        sa.Column('created_at_utc', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column('correlation_id', sa.String(length=64), nullable=False),
        sa.Column('request_raw', sa.Text(), nullable=True),
        sa.Column('engagement_score', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('ranking', sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column('influence_ranking_score', sa.Numeric(precision=18, scale=4), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('correlation_id'),
    )
    op.create_index('ix_messages_user_id', 'messages', ['user_id'], unique=False)
    op.create_index('ix_messages_created_at_utc', 'messages', ['created_at_utc'], unique=False)
    op.create_index('ix_messages_correlation_id', 'messages', ['correlation_id'], unique=True)

    op.create_table(
        'message_sentiments',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('message_id', sa.String(length=36), nullable=False),
        sa.Column('positive', sa.Numeric(precision=7, scale=4), nullable=False),
        sa.Column('negative', sa.Numeric(precision=7, scale=4), nullable=False),
        sa.Column('neutral', sa.Numeric(precision=7, scale=4), nullable=False),
        sa.ForeignKeyConstraint(['message_id'], ['messages.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('message_id'),
    )

    op.create_table(
        'message_flags',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('message_id', sa.String(length=36), nullable=False),
        sa.Column('mbras_employee', sa.Boolean(), server_default=sa.text('0'), nullable=False),
        sa.Column('special_pattern', sa.Boolean(), server_default=sa.text('0'), nullable=False),
        sa.Column('candidate_awareness', sa.Boolean(), server_default=sa.text('0'), nullable=False),
        sa.ForeignKeyConstraint(['message_id'], ['messages.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('message_id'),
    )

    op.create_table(
        'message_anomalies',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('message_id', sa.String(length=36), nullable=False),
        sa.Column('anomaly_detected', sa.Boolean(), server_default=sa.text('0'), nullable=False),
        sa.Column('anomaly_type', sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(['message_id'], ['messages.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('message_id'),
    )

    op.create_table(
        'message_processing',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('message_id', sa.String(length=36), nullable=False),
        sa.Column('queue_messaging', sa.String(length=256), nullable=True),
        sa.Column('processing_success', sa.Boolean(), nullable=True),
        sa.Column('processing_status', sa.String(length=32), nullable=False),
        sa.Column('failure_stage', sa.String(length=32), nullable=True),
        sa.Column('failed_reason', sa.Text(), nullable=True),
        sa.Column('elastic_name', sa.String(length=128), nullable=True),
        sa.Column('elastic_index_name', sa.String(length=128), nullable=True),
        sa.Column('updated_at_utc', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(['message_id'], ['messages.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('message_id'),
    )
    op.create_index('ix_message_processing_processing_status', 'message_processing', ['processing_status'], unique=False)

    op.create_table(
        'topics',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('name', sa.String(length=128), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('name'),
    )
    op.create_index('ix_topics_name', 'topics', ['name'], unique=True)

    op.create_table(
        'message_topics',
        sa.Column('message_id', sa.String(length=36), nullable=False),
        sa.Column('topic_id', sa.String(length=36), nullable=False),
        sa.ForeignKeyConstraint(['message_id'], ['messages.id']),
        sa.ForeignKeyConstraint(['topic_id'], ['topics.id']),
        sa.PrimaryKeyConstraint('message_id', 'topic_id'),
    )
    op.create_index('ix_message_topics_message_id', 'message_topics', ['message_id'], unique=False)

    op.create_table(
        'influence_ranking_items',
        sa.Column('id', sa.String(length=36), nullable=False),
        sa.Column('message_id', sa.String(length=36), nullable=False),
        sa.Column('external_user_key', sa.String(length=128), nullable=False),
        sa.Column('followers', sa.Integer(), nullable=False),
        sa.Column('engagement_rate', sa.Numeric(precision=9, scale=6), nullable=False),
        sa.Column('influence_score', sa.Numeric(precision=18, scale=4), nullable=False),
        sa.ForeignKeyConstraint(['message_id'], ['messages.id']),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('ix_influence_ranking_items_message_id', 'influence_ranking_items', ['message_id'], unique=False)


def downgrade() -> None:
    for table_name in [
        'influence_ranking_items',
        'message_topics',
        'topics',
        'message_processing',
        'message_anomalies',
        'message_flags',
        'message_sentiments',
        'messages',
        'users',
    ]:
        if _table_exists(table_name):
            op.drop_table(table_name)
