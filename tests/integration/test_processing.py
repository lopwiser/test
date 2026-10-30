import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain import PaymentStatus
from payment_service.errors import GatewayError, ProcessingOwnershipLost
from payment_service.messaging.schemas import PaymentProcessEvent
from payment_service.services.gateway import PaymentGateway
from payment_service.services.processing import PaymentProcessor
from payment_service.services.webhooks import WebhookSender
from tests.support.factories import create_event

pytestmark = pytest.mark.integration


@pytest.fixture
def settings(database):
    return WorkerSettings(
        _env_file=None, database_url=database.engine.url.render_as_string(hide_password=False)
    )


@pytest.fixture
def gateway():
    gateway = AsyncMock(spec=PaymentGateway)
    gateway.process.return_value = PaymentStatus.SUCCEEDED
    return gateway


@pytest.fixture
def webhooks():
    return AsyncMock(spec=WebhookSender)


@pytest.fixture
def processor(database, gateway, webhooks, settings):
    return PaymentProcessor(database, gateway, webhooks, settings)


async def load_payment(database, payment_id):
    async with database.sessions() as session:
        return (await session.scalars(select(Payment).where(Payment.id == payment_id))).one()


async def next_event(database, payment_id, stage, attempt):
    async with database.sessions() as session:
        events = (
            await session.scalars(select(OutboxEvent).where(OutboxEvent.payment_id == payment_id))
        ).all()
    return next(
        row for row in events if row.payload["stage"] == stage and row.payload["attempt"] == attempt
    )


@pytest.mark.parametrize("result", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_result_is_committed_before_webhook_and_duplicate_is_noop(
    database, processor, gateway, webhooks, payment_body, result
):
    initial = await create_event(database, payment_body)
    gateway.process.return_value = result

    async def send(payment):
        saved = await load_payment(database, payment.id)
        assert saved.status == result
        assert saved.processed_at is not None
        assert saved.failure_reason == ("declined" if result == PaymentStatus.FAILED else None)

    webhooks.send.side_effect = send
    event = PaymentProcessEvent.model_validate(initial.payload)
    assert await processor.process(event)
    assert await processor.process(event)
    payment = await load_payment(database, initial.payment_id)
    assert payment.workflow_status == "completed"
    assert payment.webhook_delivered_at >= payment.processed_at >= payment.created_at
    gateway.process.assert_awaited_once_with(payment.id, attempt=1)
    webhooks.send.assert_awaited_once()


async def test_webhook_retries_have_own_counter_and_2_4_backoff(
    database, processor, gateway, webhooks, payment_body
):
    initial = await create_event(database, payment_body)
    current = PaymentProcessEvent.model_validate(initial.payload)
    webhooks.send.side_effect = httpx.ConnectError("offline")
    for attempt, delay in [(1, 2), (2, 4)]:
        before = datetime.now(UTC)
        assert await processor.process(current)
        retry = await next_event(database, initial.payment_id, "webhook", attempt + 1)
        assert retry.available_at >= before + timedelta(seconds=delay)
        current = PaymentProcessEvent.model_validate(retry.payload)
    assert await processor.process(current) is False
    saved = await load_payment(database, initial.payment_id)
    assert saved.current_attempt == 1
    assert saved.webhook_attempt == 3
    assert saved.status == "succeeded"
    assert saved.workflow_status == "dead_lettered"
    assert await processor.process(current) is False
    assert await processor.process(PaymentProcessEvent.model_validate(initial.payload))
    gateway.process.assert_awaited_once()
    assert webhooks.send.await_count == 3


@pytest.mark.parametrize(
    "error",
    [
        GatewayError("offline"),
        OSError("offline"),
        httpx.ConnectError("offline"),
        TimeoutError("timeout"),
    ],
)
async def test_gateway_exhaustion_schedules_notification_with_three_fresh_attempts(
    database, processor, gateway, webhooks, payment_body, error
):
    initial = await create_event(database, payment_body)
    current = PaymentProcessEvent.model_validate(initial.payload)
    gateway.process.side_effect = error
    for attempt in range(1, 4):
        assert await processor.process(current) is (attempt < 3)
        if attempt < 3:
            current = PaymentProcessEvent.model_validate(
                (await next_event(database, initial.payment_id, "gateway", attempt + 1)).payload
            )
    assert await processor.process(current) is False
    assert await processor.process(PaymentProcessEvent.model_validate(initial.payload))
    webhooks.send.assert_not_awaited()
    webhooks.send.side_effect = [httpx.ConnectError("offline"), httpx.ConnectError("offline"), None]
    for attempt in range(1, 4):
        notification = await next_event(database, initial.payment_id, "webhook", attempt)
        assert await processor.process(PaymentProcessEvent.model_validate(notification.payload))
    saved = await load_payment(database, initial.payment_id)
    assert saved.status == "failed"
    assert saved.failure_reason == "gateway_unavailable"
    assert saved.current_attempt == saved.webhook_attempt == 3
    assert saved.workflow_status == "completed"
    assert saved.webhook_delivered_at is not None
    assert gateway.process.await_count == 3
    assert webhooks.send.await_count == 3


async def test_gateway_retry_does_not_reduce_webhook_budget(
    database, processor, gateway, webhooks, payment_body
):
    initial = await create_event(database, payment_body)
    gateway.process.side_effect = [GatewayError("offline"), PaymentStatus.SUCCEEDED]
    await processor.process(PaymentProcessEvent.model_validate(initial.payload))
    retry = await next_event(database, initial.payment_id, "gateway", 2)
    webhooks.send.side_effect = httpx.ConnectError("offline")
    await processor.process(PaymentProcessEvent.model_validate(retry.payload))
    assert await processor.process(PaymentProcessEvent.model_validate(retry.payload))
    for attempt in [2, 3]:
        notification = await next_event(database, initial.payment_id, "webhook", attempt)
        await processor.process(PaymentProcessEvent.model_validate(notification.payload))
    assert gateway.process.await_count == 2
    assert webhooks.send.await_count == 3


async def test_two_processors_serialize_same_payment_without_conflicting_results(
    database, processor, gateway, webhooks, payment_body, settings
):
    initial = await create_event(database, payment_body)
    event = PaymentProcessEvent.model_validate(initial.payload)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def send(payment):
        entered.set()
        await release.wait()

    webhooks.send.side_effect = send
    second = PaymentProcessor(database, gateway, webhooks, settings)
    first_task = asyncio.create_task(processor.process(event))
    await asyncio.wait_for(entered.wait(), timeout=3)
    second_task = asyncio.create_task(second.process(event))
    await asyncio.sleep(0.05)
    assert not second_task.done()
    release.set()
    await asyncio.wait_for(asyncio.gather(first_task, second_task), timeout=3)
    gateway.process.assert_awaited_once()
    webhooks.send.assert_awaited_once()
    assert (await load_payment(database, initial.payment_id)).workflow_status == "completed"


async def terminate_lock_holder(database, payment_id):
    key = int.from_bytes(payment_id.bytes[:8], signed=True)
    async with database.engine.begin() as connection:
        terminated = await connection.scalar(
            text(
                "SELECT bool_or(pg_terminate_backend(pid)) FROM pg_locks "
                "WHERE locktype = 'advisory' AND granted "
                "AND classid = :high AND objid = :low AND objsubid = 1"
            ),
            {"high": (key >> 32) & 0xFFFFFFFF, "low": key & 0xFFFFFFFF},
        )
    assert terminated


async def test_stale_processor_cannot_overwrite_after_losing_payment_lock(
    database, settings, payment_body
):
    initial = await create_event(database, payment_body)
    event = PaymentProcessEvent.model_validate(initial.payload)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_decline(payment_id, *, attempt):
        entered.set()
        await release.wait()
        return PaymentStatus.FAILED

    stale_gateway = AsyncMock(spec=PaymentGateway)
    stale_gateway.process.side_effect = slow_decline
    stale_webhooks = AsyncMock(spec=WebhookSender)
    stale = PaymentProcessor(database, stale_gateway, stale_webhooks, settings)
    owner_gateway = AsyncMock(spec=PaymentGateway)
    owner_gateway.process.return_value = PaymentStatus.SUCCEEDED
    owner_webhooks = AsyncMock(spec=WebhookSender)
    owner = PaymentProcessor(database, owner_gateway, owner_webhooks, settings)

    stale_task = asyncio.create_task(stale.process(event))
    await asyncio.wait_for(entered.wait(), timeout=3)
    await terminate_lock_holder(database, event.payment_id)
    assert await asyncio.wait_for(owner.process(event), timeout=10)
    release.set()
    with pytest.raises((ProcessingOwnershipLost, DBAPIError)):
        await asyncio.wait_for(stale_task, timeout=5)

    payment = await load_payment(database, event.payment_id)
    assert payment.status == "succeeded"
    assert payment.failure_reason is None
    assert payment.workflow_status == "completed"
    owner_webhooks.send.assert_awaited_once()
    stale_webhooks.send.assert_not_awaited()


async def test_cancellation_releases_payment_lock_and_keeps_saved_gateway_result(
    database, processor, gateway, webhooks, payment_body
):
    initial = await create_event(database, payment_body)
    event = PaymentProcessEvent.model_validate(initial.payload)
    entered = asyncio.Event()

    async def send(payment):
        entered.set()
        await asyncio.Event().wait()

    webhooks.send.side_effect = send
    task = asyncio.create_task(processor.process(event))
    await asyncio.wait_for(entered.wait(), timeout=3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    webhooks.send.side_effect = None
    await asyncio.wait_for(processor.process(event), timeout=3)
    gateway.process.assert_awaited_once()
    assert (await load_payment(database, initial.payment_id)).workflow_status == "completed"


async def test_abandoned_payment_fails_and_the_client_is_told(
    database, processor, gateway, webhooks, payment_body
):
    initial = await create_event(database, payment_body)
    event = PaymentProcessEvent.model_validate(initial.payload)
    await processor.abandon(event, RuntimeError("bug"))
    saved = await load_payment(database, initial.payment_id)
    assert saved.status == "failed"
    assert saved.failure_reason == "processing_error"
    assert saved.processed_at is not None
    assert saved.last_error == "RuntimeError: bug"
    notification = await next_event(database, initial.payment_id, "webhook", 1)
    assert await processor.process(PaymentProcessEvent.model_validate(notification.payload))
    assert (await load_payment(database, initial.payment_id)).workflow_status == "completed"
    webhooks.send.assert_awaited_once()
    gateway.process.assert_not_awaited()
    assert await processor.process(event) is False


async def test_abandon_after_gateway_result_keeps_the_result(
    database, processor, webhooks, payment_body
):
    initial = await create_event(database, payment_body)
    event = PaymentProcessEvent.model_validate(initial.payload)
    webhooks.send.side_effect = RuntimeError("bug")
    with pytest.raises(RuntimeError):
        await processor.process(event)
    await processor.abandon(event, RuntimeError("bug"))
    saved = await load_payment(database, initial.payment_id)
    assert saved.status == "succeeded"
    assert saved.failure_reason is None
    assert saved.workflow_status == "dead_lettered"
    assert saved.last_error == "RuntimeError: bug"
    assert await processor.process(event) is False
    assert webhooks.send.await_count == 1


async def test_abandon_ignores_a_finished_payment(database, processor, payment_body):
    initial = await create_event(database, payment_body)
    event = PaymentProcessEvent.model_validate(initial.payload)
    assert await processor.process(event)
    await processor.abandon(event, RuntimeError("late"))
    saved = await load_payment(database, initial.payment_id)
    assert saved.status == "succeeded"
    assert saved.workflow_status == "completed"
    assert saved.last_error is None


async def test_abandon_ignores_a_superseded_attempt(database, processor, gateway, payment_body):
    initial = await create_event(database, payment_body)
    event = PaymentProcessEvent.model_validate(initial.payload)
    gateway.process.side_effect = GatewayError("offline")
    assert await processor.process(event)
    await processor.abandon(event, RuntimeError("late"))
    saved = await load_payment(database, initial.payment_id)
    assert saved.status == "pending"
    assert saved.current_attempt == 2


async def test_abandon_ignores_an_unknown_payment(processor):
    await processor.abandon(
        PaymentProcessEvent(event_id=uuid4(), payment_id=uuid4()), RuntimeError("bug")
    )


@pytest.mark.parametrize("failing_call", ["gateway", "webhook"])
async def test_error_without_message_stays_diagnosable(
    database, processor, gateway, webhooks, payment_body, failing_call
):
    initial = await create_event(database, payment_body)
    call = gateway.process if failing_call == "gateway" else webhooks.send
    call.side_effect = TimeoutError()
    await processor.process(PaymentProcessEvent.model_validate(initial.payload))
    saved = await load_payment(database, initial.payment_id)
    assert saved.last_error.startswith("TimeoutError")


async def test_retry_insert_failure_rolls_back_attempt(database, processor, webhooks, payment_body):
    initial = await create_event(database, payment_body)
    async with database.engine.begin() as connection:
        await connection.execute(
            text("ALTER TABLE outbox ADD CONSTRAINT reject_retry CHECK (payload->>'attempt' = '1')")
        )
    webhooks.send.side_effect = httpx.ConnectError("offline")
    with pytest.raises(IntegrityError):
        await processor.process(PaymentProcessEvent.model_validate(initial.payload))
    saved = await load_payment(database, initial.payment_id)
    assert saved.status == "succeeded"
    assert saved.webhook_attempt == 1


async def test_webhook_event_before_a_gateway_result_is_rejected(
    database, processor, webhooks, payment_body
):
    initial = await create_event(database, payment_body)
    early = PaymentProcessEvent(event_id=uuid4(), payment_id=initial.payment_id, stage="webhook")
    assert await processor.process(early) is False
    webhooks.send.assert_not_awaited()


async def test_missing_or_future_payment_is_rejected_without_diagnostic_outbox(
    database, processor, gateway, payment_body
):
    assert (
        await processor.process(PaymentProcessEvent(event_id=uuid4(), payment_id=uuid4())) is False
    )
    initial = await create_event(database, payment_body)
    assert (
        await processor.process(
            PaymentProcessEvent(event_id=uuid4(), payment_id=initial.payment_id, attempt=3)
        )
        is False
    )
    gateway.process.assert_not_awaited()
    async with database.sessions() as session:
        assert len((await session.scalars(select(OutboxEvent))).all()) == 1
