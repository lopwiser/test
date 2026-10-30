from uuid import uuid4

from sqlalchemy import select

from payment_service.api.schemas import PaymentCreate
from payment_service.db.models import OutboxEvent
from payment_service.services.payments import create_payment


async def create_event(database, payment_body, routing_key="payments.new"):
    async with database.sessions() as session:
        payment = await create_payment(
            session, PaymentCreate.model_validate(payment_body).to_new_payment(), str(uuid4())
        )
    async with database.sessions.begin() as session:
        event = (
            await session.scalars(select(OutboxEvent).where(OutboxEvent.payment_id == payment.id))
        ).one()
        event.routing_key = routing_key
    return event
