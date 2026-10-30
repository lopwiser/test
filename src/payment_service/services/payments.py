import hashlib
import json
from dataclasses import asdict
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.db.models import Payment
from payment_service.db.queries import find_payment_by_key, insert_payment_if_absent
from payment_service.domain import NewPayment
from payment_service.errors import IdempotencyConflict, PaymentNotFound
from payment_service.messaging.outbox_records import processing_record


def request_fingerprint(request: NewPayment) -> str:
    canonical = json.dumps(
        asdict(request),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


async def create_payment(
    session: AsyncSession, request: NewPayment, idempotency_key: str
) -> Payment:
    fingerprint = request_fingerprint(request)
    async with session.begin():
        payment = await insert_payment_if_absent(
            session,
            id=uuid4(),
            amount=request.amount,
            currency=request.currency.value,
            description=request.description,
            payment_metadata=request.metadata,
            webhook_url=request.webhook_url,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
        )
        if payment is None:
            payment = await find_payment_by_key(session, idempotency_key)
            if payment.request_fingerprint != fingerprint:
                raise IdempotencyConflict
        else:
            session.add(processing_record(payment.id))
    return payment


async def get_payment(session: AsyncSession, payment_id: UUID) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFound
    return payment
