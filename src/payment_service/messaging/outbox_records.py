from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import DateTime, func

from payment_service.db.models import OutboxEvent
from payment_service.domain import Stage
from payment_service.messaging.broker import PAYMENTS_EXCHANGE, PAYMENTS_QUEUE
from payment_service.messaging.schemas import PaymentProcessEvent


def processing_record(
    payment_id: UUID,
    *,
    stage: Stage = "gateway",
    attempt: int = 1,
    delay: float = 0,
) -> OutboxEvent:
    event = PaymentProcessEvent(
        event_id=uuid4(), payment_id=payment_id, stage=stage, attempt=attempt
    )
    return OutboxEvent(
        id=event.event_id,
        payment_id=payment_id,
        event_type=event.event_type,
        exchange=PAYMENTS_EXCHANGE.name,
        routing_key=PAYMENTS_QUEUE.routing_key,
        payload=event.model_dump(mode="json"),
        available_at=func.clock_timestamp(type_=DateTime(timezone=True)) + timedelta(seconds=delay),
    )
