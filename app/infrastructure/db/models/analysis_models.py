from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.session import Base


class User(Base):
    __tablename__ = 'users'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    external_user_key: Mapped[str | None] = mapped_column(String(128), nullable=True, unique=True, index=True)
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    messages: Mapped[list['Message']] = relationship('Message', back_populates='user')


class Message(Base):
    __tablename__ = 'messages'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id: Mapped[str] = mapped_column(String(36), ForeignKey('users.id'), nullable=False, index=True)
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    request_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    engagement_score: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    ranking: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    influence_ranking_score: Mapped[float | None] = mapped_column(Numeric(18, 4), nullable=True)

    user: Mapped['User'] = relationship('User', back_populates='messages')
    sentiment: Mapped['MessageSentiment | None'] = relationship('MessageSentiment', back_populates='message', uselist=False)
    flags: Mapped['MessageFlags | None'] = relationship('MessageFlags', back_populates='message', uselist=False)
    anomaly: Mapped['MessageAnomaly | None'] = relationship('MessageAnomaly', back_populates='message', uselist=False)
    processing: Mapped['MessageProcessing | None'] = relationship('MessageProcessing', back_populates='message', uselist=False)
    outbox_events: Mapped[list['OutboxEvent']] = relationship('OutboxEvent', back_populates='message')
    influence_items: Mapped[list['InfluenceRankingItem']] = relationship('InfluenceRankingItem', back_populates='message')
    message_topics: Mapped[list['MessageTopic']] = relationship('MessageTopic', back_populates='message')


class MessageSentiment(Base):
    __tablename__ = 'message_sentiments'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    message_id: Mapped[str] = mapped_column(String(36), ForeignKey('messages.id'), nullable=False, unique=True)
    positive: Mapped[float] = mapped_column(Numeric(7, 4), nullable=False)
    negative: Mapped[float] = mapped_column(Numeric(7, 4), nullable=False)
    neutral: Mapped[float] = mapped_column(Numeric(7, 4), nullable=False)

    message: Mapped['Message'] = relationship('Message', back_populates='sentiment')


class MessageFlags(Base):
    __tablename__ = 'message_flags'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    message_id: Mapped[str] = mapped_column(String(36), ForeignKey('messages.id'), nullable=False, unique=True)
    mbras_employee: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default='0')
    special_pattern: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default='0')
    candidate_awareness: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default='0')

    message: Mapped['Message'] = relationship('Message', back_populates='flags')


class MessageAnomaly(Base):
    __tablename__ = 'message_anomalies'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    message_id: Mapped[str] = mapped_column(String(36), ForeignKey('messages.id'), nullable=False, unique=True)
    anomaly_detected: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False, server_default='0')
    anomaly_type: Mapped[str | None] = mapped_column(String(64), nullable=True)

    message: Mapped['Message'] = relationship('Message', back_populates='anomaly')


class MessageProcessing(Base):
    __tablename__ = 'message_processing'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    message_id: Mapped[str] = mapped_column(String(36), ForeignKey('messages.id'), nullable=False, unique=True)
    queue_messaging: Mapped[str | None] = mapped_column(String(256), nullable=True)
    processing_success: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    processing_status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    failure_stage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    failed_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    elastic_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    elastic_index_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    updated_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    message: Mapped['Message'] = relationship('Message', back_populates='processing')


class Topic(Base):
    __tablename__ = 'topics'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(128), nullable=False, unique=True, index=True)

    message_topics: Mapped[list['MessageTopic']] = relationship('MessageTopic', back_populates='topic')


class MessageTopic(Base):
    __tablename__ = 'message_topics'
    __table_args__ = (UniqueConstraint('message_id', 'topic_id', name='uq_message_topics_message_id_topic_id'),)

    message_id: Mapped[str] = mapped_column(String(36), ForeignKey('messages.id'), primary_key=True)
    topic_id: Mapped[str] = mapped_column(String(36), ForeignKey('topics.id'), primary_key=True)

    message: Mapped['Message'] = relationship('Message', back_populates='message_topics')
    topic: Mapped['Topic'] = relationship('Topic', back_populates='message_topics')


class InfluenceRankingItem(Base):
    __tablename__ = 'influence_ranking_items'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    message_id: Mapped[str] = mapped_column(String(36), ForeignKey('messages.id'), nullable=False, index=True)
    external_user_key: Mapped[str] = mapped_column(String(128), nullable=False)
    followers: Mapped[int] = mapped_column(Integer, nullable=False)
    engagement_rate: Mapped[float] = mapped_column(Numeric(9, 6), nullable=False)
    influence_score: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)

    message: Mapped['Message'] = relationship('Message', back_populates='influence_items')


class OutboxEvent(Base):
    __tablename__ = 'outbox_events'

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    message_id: Mapped[str] = mapped_column(String(36), ForeignKey('messages.id'), nullable=False, index=True)
    correlation_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default='0')
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    available_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), index=True)
    locked_at_utc: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    locked_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    message: Mapped['Message'] = relationship('Message', back_populates='outbox_events')
