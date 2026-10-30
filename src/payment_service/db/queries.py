from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import OutboxEvent, Payment


async def insert_payment_if_absent(session: AsyncSession, **values: object) -> Payment | None:
    statement = (
        insert(Payment)
        .values(**values)
        .on_conflict_do_nothing(index_elements=[Payment.idempotency_key])
        .returning(Payment)
    )
    return (await session.scalars(statement)).one_or_none()


async def find_payment_by_key(session: AsyncSession, key: str) -> Payment:
    return (await session.scalars(select(Payment).where(Payment.idempotency_key == key))).one()


async def next_outbox_event(session: AsyncSession) -> OutboxEvent | None:
    statement = (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None), OutboxEvent.available_at <= func.now())
        .order_by(OutboxEvent.available_at, OutboxEvent.created_at, OutboxEvent.id)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    return (await session.scalars(statement)).one_or_none()
