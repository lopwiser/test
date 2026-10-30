from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("description", sa.String(1000), nullable=False),
        sa.Column(
            "metadata", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False
        ),
        sa.Column("status", sa.String(9), server_default="pending", nullable=False),
        sa.Column("idempotency_key", sa.String(255), nullable=False),
        sa.Column("request_fingerprint", sa.String(64), nullable=False),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("workflow_status", sa.String(13), server_default="active", nullable=False),
        sa.Column("current_attempt", sa.SmallInteger(), server_default="1", nullable=False),
        sa.Column("failure_reason", sa.String(32), nullable=True),
        sa.Column("webhook_attempt", sa.SmallInteger(), server_default="1", nullable=False),
        sa.Column("webhook_event_id", sa.Uuid(), nullable=False),
        sa.Column("webhook_delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("processing_token", sa.Uuid(), nullable=True),
        sa.CheckConstraint("amount > 0", name=op.f("ck_payments_positive_amount")),
        sa.CheckConstraint("currency IN ('RUB', 'USD', 'EUR')", name=op.f("ck_payments_currency")),
        sa.CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed')", name=op.f("ck_payments_status")
        ),
        sa.CheckConstraint(
            "failure_reason IN ('declined', 'gateway_unavailable', 'processing_error')",
            name=op.f("ck_payments_failure_reason"),
        ),
        sa.CheckConstraint(
            "(status = 'failed' AND failure_reason IS NOT NULL) OR "
            "(status IN ('pending', 'succeeded') AND failure_reason IS NULL)",
            name=op.f("ck_payments_failure_reason_matches_status"),
        ),
        sa.CheckConstraint(
            "workflow_status IN ('active', 'completed', 'dead_lettered')",
            name=op.f("ck_payments_workflow_status"),
        ),
        sa.CheckConstraint(
            "current_attempt BETWEEN 1 AND 3", name=op.f("ck_payments_current_attempt")
        ),
        sa.CheckConstraint(
            "webhook_attempt BETWEEN 1 AND 3", name=op.f("ck_payments_webhook_attempt")
        ),
        sa.CheckConstraint(
            "length(btrim(idempotency_key)) > 0", name=op.f("ck_payments_nonempty_idempotency_key")
        ),
        sa.CheckConstraint(
            "(status = 'pending' AND processed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed') AND processed_at IS NOT NULL)",
            name=op.f("ck_payments_processed_at_matches_status"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_payments")),
        sa.UniqueConstraint("idempotency_key", name=op.f("uq_payments_idempotency_key")),
        sa.UniqueConstraint("webhook_event_id", name=op.f("uq_payments_webhook_event_id")),
    )
    op.create_table(
        "outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("payment_id", sa.Uuid(), nullable=True),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("exchange", sa.String(255), nullable=False),
        sa.Column("routing_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.CheckConstraint(
            "publish_attempts >= 0", name=op.f("ck_outbox_nonnegative_publish_attempts")
        ),
        sa.ForeignKeyConstraint(
            ["payment_id"], ["payments.id"], name=op.f("fk_outbox_payment_id_payments")
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox")),
    )
    op.create_index(
        "ix_outbox_ready",
        "outbox",
        ["available_at", "created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index("ix_outbox_payment_id", "outbox", ["payment_id"])
    op.create_index(
        "ix_outbox_published_at",
        "outbox",
        ["published_at"],
        postgresql_where=sa.text("published_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_published_at", table_name="outbox")
    op.drop_index("ix_outbox_payment_id", table_name="outbox")
    op.drop_index("ix_outbox_ready", table_name="outbox")
    op.drop_table("outbox")
    op.drop_table("payments")
