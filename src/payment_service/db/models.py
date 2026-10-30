from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

from pydantic import JsonValue
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from payment_service.db.base import Base
from payment_service.domain import MAX_ATTEMPTS, FailureReason


class Payment(Base):
    __tablename__ = "payments"
    __table_args__ = (
        CheckConstraint("amount > 0", name="positive_amount"),
        CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name="currency"),
        CheckConstraint("status IN ('pending', 'succeeded', 'failed')", name="status"),
        CheckConstraint(
            f"failure_reason IN ({', '.join(repr(reason.value) for reason in FailureReason)})",
            name="failure_reason",
        ),
        CheckConstraint(
            "(status = 'failed' AND failure_reason IS NOT NULL) OR "
            "(status IN ('pending', 'succeeded') AND failure_reason IS NULL)",
            name="failure_reason_matches_status",
        ),
        CheckConstraint(
            "workflow_status IN ('active', 'completed', 'dead_lettered')", name="workflow_status"
        ),
        CheckConstraint(f"current_attempt BETWEEN 1 AND {MAX_ATTEMPTS}", name="current_attempt"),
        CheckConstraint(f"webhook_attempt BETWEEN 1 AND {MAX_ATTEMPTS}", name="webhook_attempt"),
        CheckConstraint("length(btrim(idempotency_key)) > 0", name="nonempty_idempotency_key"),
        CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name="processed_at_matches_status",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    description: Mapped[str] = mapped_column(String(1000))
    payment_metadata: Mapped[dict[str, JsonValue]] = mapped_column(
        "metadata", JSONB, default=dict, server_default=text("'{}'::jsonb")
    )
    status: Mapped[str] = mapped_column(String(9), server_default="pending")
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    webhook_url: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    workflow_status: Mapped[str] = mapped_column(String(13), server_default="active")
    current_attempt: Mapped[int] = mapped_column(SmallInteger, server_default="1")
    failure_reason: Mapped[str | None] = mapped_column(String(32))
    webhook_attempt: Mapped[int] = mapped_column(SmallInteger, server_default="1")
    webhook_event_id: Mapped[UUID] = mapped_column(default=uuid4, unique=True)
    webhook_delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    processing_token: Mapped[UUID | None] = mapped_column()


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        CheckConstraint("publish_attempts >= 0", name="nonnegative_publish_attempts"),
        Index(
            "ix_outbox_ready",
            "available_at",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index(
            "ix_outbox_published_at",
            "published_at",
            postgresql_where=text("published_at IS NOT NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    payment_id: Mapped[UUID | None] = mapped_column(ForeignKey("payments.id"), index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    exchange: Mapped[str] = mapped_column(String(255))
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, JsonValue]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    publish_attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)
