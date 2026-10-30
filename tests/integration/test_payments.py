import asyncio
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import IntegrityError

from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain import FailureReason
from payment_service.messaging.schemas import PaymentProcessEvent

pytestmark = pytest.mark.integration


async def row_counts(database):
    async with database.sessions() as session:
        return (
            await session.scalar(select(func.count()).select_from(Payment)),
            await session.scalar(select(func.count()).select_from(OutboxEvent)),
        )


async def test_create_and_get_payment(client, database, payment_body):
    response = await client.post("/api/v1/payments", json=payment_body)
    assert response.status_code == 202
    created = response.json()
    assert set(created) == {"payment_id", "status", "created_at"}
    assert created["status"] == "pending"
    assert (
        datetime.fromisoformat(created["created_at"].replace("Z", "+00:00"))
        .utcoffset()
        .total_seconds()
        == 0
    )

    response = await client.get(f"/api/v1/payments/{created['payment_id']}")
    assert response.status_code == 200
    payment = response.json()
    assert payment["payment_id"] == created["payment_id"]
    assert payment["amount"] == "1500.00"
    assert payment["metadata"] == payment_body["metadata"]
    assert payment["processed_at"] is None
    assert "workflow_status" not in payment
    assert "current_attempt" not in payment
    assert payment["webhook_delivered_at"] is None
    assert "request_fingerprint" not in payment
    assert "last_error" not in payment

    async with database.sessions() as session:
        events = (await session.scalars(select(OutboxEvent))).all()
        assert len(events) == 1
        event = events[0]
        assert event.payment_id == UUID(created["payment_id"])
        assert event.routing_key == "payments.new"
        assert event.exchange == "payments.exchange"
        assert event.published_at is None
        envelope = PaymentProcessEvent.model_validate(event.payload)
        assert envelope.event_id == event.id
        assert envelope.payment_id == event.payment_id
        assert envelope.attempt == 1


async def test_equivalent_repeat_creates_no_extra_rows(client, database, payment_body):
    first = await client.post("/api/v1/payments", json=payment_body)
    equivalent = {**payment_body, "amount": "1500"}
    second = await client.post("/api/v1/payments", json=equivalent)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert await row_counts(database) == (1, 1)


async def test_same_key_with_different_body_conflicts(client, database, payment_body):
    await client.post("/api/v1/payments", json=payment_body)
    response = await client.post("/api/v1/payments", json={**payment_body, "amount": "1500.01"})
    assert response.status_code == 409
    assert await row_counts(database) == (1, 1)


async def test_different_keys_create_independent_payments(client, database, payment_body):
    first = await client.post("/api/v1/payments", json=payment_body)
    second = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "another-order"}
    )
    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] != second.json()["payment_id"]
    assert await row_counts(database) == (2, 2)


async def test_concurrent_duplicates_create_one_payment_and_event(client, database, payment_body):
    responses = await asyncio.gather(
        *(client.post("/api/v1/payments", json=payment_body) for _ in range(12))
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    assert len({response.json()["created_at"] for response in responses}) == 1
    assert await row_counts(database) == (1, 1)


async def test_concurrent_different_bodies_have_one_winner(client, database, payment_body):
    responses = await asyncio.gather(
        client.post("/api/v1/payments", json=payment_body),
        client.post("/api/v1/payments", json={**payment_body, "amount": "1500.01"}),
    )
    assert sorted(response.status_code for response in responses) == [202, 409]
    assert await row_counts(database) == (1, 1)


async def test_outbox_error_rolls_back_payment_and_does_not_return_202(
    client, database, payment_body
):
    async with database.engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT reject_insert CHECK (false)")
        )

    response = await client.post("/api/v1/payments", json=payment_body)
    assert response.status_code == 500
    assert await row_counts(database) == (0, 0)


async def test_repeat_returns_current_status(client, database, payment_body):
    first = (await client.post("/api/v1/payments", json=payment_body)).json()
    async with database.sessions.begin() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == UUID(first["payment_id"]))
            .values(status="succeeded", processed_at=func.now())
        )
    response = await client.post("/api/v1/payments", json=payment_body)
    assert response.status_code == 202
    assert response.json() == {**first, "status": "succeeded"}
    assert await row_counts(database) == (1, 1)


@pytest.mark.parametrize("key", ["", " ", "a" * 256])
async def test_rejects_invalid_idempotency_key(client, database, payment_body, key):
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": key}
    )
    assert response.status_code == 422
    assert await row_counts(database) == (0, 0)


async def test_requires_idempotency_key(client, database, payment_body):
    del client.headers["Idempotency-Key"]
    response = await client.post("/api/v1/payments", json=payment_body)
    assert response.status_code == 422
    assert await row_counts(database) == (0, 0)


async def test_missing_and_invalid_payment_id(client):
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404
    assert (await client.get("/api/v1/payments/not-a-uuid")).status_code == 422


async def test_health_checks_postgres(client):
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_largest_amount_preserves_precision(client, database, payment_body):
    response = await client.post(
        "/api/v1/payments", json={**payment_body, "amount": "9999999999999999.99"}
    )
    assert response.status_code == 202
    async with database.sessions() as session:
        payment = await session.get(Payment, UUID(response.json()["payment_id"]))
        assert payment.amount == Decimal("9999999999999999.99")


@pytest.mark.parametrize("reason", list(FailureReason))
async def test_every_failure_reason_is_stored_and_returned(client, database, payment_body, reason):
    created = (await client.post("/api/v1/payments", json=payment_body)).json()
    async with database.sessions.begin() as session:
        await session.execute(
            update(Payment)
            .where(Payment.id == UUID(created["payment_id"]))
            .values(status="failed", processed_at=func.now(), failure_reason=reason.value)
        )
    response = await client.get(f"/api/v1/payments/{created['payment_id']}")
    assert response.status_code == 200
    assert response.json()["failure_reason"] == reason.value


@pytest.mark.parametrize(
    "status,reason",
    [("pending", "declined"), ("succeeded", "declined"), ("failed", "unknown"), ("failed", None)],
)
async def test_database_rejects_inconsistent_failure_reason(
    client, database, payment_body, status, reason
):
    response = await client.post("/api/v1/payments", json=payment_body)
    payment_id = UUID(response.json()["payment_id"])
    with pytest.raises(IntegrityError):
        async with database.sessions.begin() as session:
            await session.execute(
                update(Payment)
                .where(Payment.id == payment_id)
                .values(
                    status=status,
                    processed_at=None if status == "pending" else func.now(),
                    failure_reason=reason,
                )
            )
