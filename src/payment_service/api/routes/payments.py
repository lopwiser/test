from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, status

from payment_service.api.dependencies import Session
from payment_service.api.schemas import ErrorResponse, PaymentAccepted, PaymentCreate, PaymentDetail
from payment_service.services import payments as service

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    responses={
        401: {"model": ErrorResponse, "description": "Invalid or missing API key"},
        503: {"model": ErrorResponse, "description": "Database temporarily unavailable"},
    },
)
IdempotencyKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[\x21-\x7e]+$")
]


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={
        409: {"model": ErrorResponse, "description": "Idempotency key used for a different request"}
    },
)
async def create_payment(
    body: PaymentCreate, idempotency_key: IdempotencyKey, session: Session
) -> PaymentAccepted:
    payment = await service.create_payment(session, body.to_new_payment(), idempotency_key)
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get(
    "/{payment_id}",
    response_model=PaymentDetail,
    responses={404: {"model": ErrorResponse, "description": "Payment not found"}},
)
async def get_payment(payment_id: UUID, session: Session) -> PaymentDetail:
    return PaymentDetail.model_validate(await service.get_payment(session, payment_id))
