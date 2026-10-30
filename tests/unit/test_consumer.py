import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from asyncpg import CannotConnectNowError, ConnectionDoesNotExistError, TooManyConnectionsError
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError

from payment_service.errors import DatabaseUnavailable
from payment_service.messaging.consumer import PaymentConsumer
from payment_service.messaging.schemas import PaymentProcessEvent
from payment_service.services.processing import PaymentProcessor


@pytest.fixture
def processor():
    processor = AsyncMock(spec=PaymentProcessor)
    processor.process.return_value = True
    processor.settings = SimpleNamespace(
        infrastructure_retry_base_delay=1, infrastructure_retry_max_delay=30
    )
    return processor


@pytest.fixture
def message():
    event = PaymentProcessEvent(event_id=uuid4(), payment_id=uuid4())
    return AsyncMock(body=event.model_dump_json().encode(), headers={})


async def test_ack_follows_successful_processing(processor, message):
    async def process(event):
        message.ack.assert_not_awaited()
        return True

    processor.process.side_effect = process
    await PaymentConsumer(processor).handle(message)
    processor.process.assert_awaited_once()
    message.ack.assert_awaited_once()
    message.reject.assert_not_awaited()


async def test_invalid_json_is_rejected_without_losing_original_body(processor, message):
    message.body = b"not-json\x00"
    await PaymentConsumer(processor).handle(message)
    processor.process.assert_not_awaited()
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


async def test_unexpected_error_is_bounded_and_does_not_stop_next_message(
    processor, message, monkeypatch
):
    sleep = AsyncMock()
    monkeypatch.setattr("payment_service.messaging.consumer.asyncio.sleep", sleep)
    processor.process.side_effect = [RuntimeError("bug")] * 3 + [True]
    consumer = PaymentConsumer(processor)
    for count in range(3):
        message.headers = {"x-delivery-count": count}
        await consumer.handle(message)
    assert [call.args[0] for call in sleep.await_args_list] == [1, 2]
    assert message.nack.await_count == 2
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    processor.abandon.assert_awaited_once()
    await consumer.handle(message)
    message.ack.assert_awaited_once()


async def test_final_delivery_records_the_failure_before_dead_lettering(processor, message):
    steps = []
    processor.process.side_effect = RuntimeError("bug")
    processor.abandon.side_effect = lambda event, error: steps.append(("abandon", str(error)))
    message.reject.side_effect = lambda requeue: steps.append(("reject", requeue))
    message.headers = {"x-delivery-count": 2}
    await PaymentConsumer(processor).handle(message)
    assert steps == [("abandon", "bug"), ("reject", False)]


async def test_failure_to_record_abandonment_still_dead_letters(processor, message):
    processor.process.side_effect = RuntimeError("bug")
    processor.abandon.side_effect = RuntimeError("also broken")
    message.headers = {"x-delivery-count": 2}
    await PaymentConsumer(processor).handle(message)
    message.reject.assert_awaited_once_with(requeue=False)


async def test_abandon_waits_out_a_database_outage(processor, message, monkeypatch):
    monkeypatch.setattr("payment_service.messaging.consumer.asyncio.sleep", AsyncMock())
    processor.process.side_effect = RuntimeError("bug")
    processor.abandon.side_effect = [DatabaseUnavailable("offline"), None]
    message.headers = {"x-delivery-count": 2}
    await PaymentConsumer(processor).handle(message)
    assert processor.abandon.await_count == 2
    message.reject.assert_awaited_once_with(requeue=False)


async def test_exhausted_processing_is_rejected_to_broker_dlq(processor, message):
    processor.process.return_value = False
    await PaymentConsumer(processor).handle(message)
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT 1", {}, OSError("offline")),
        InterfaceError("SELECT 1", {}, OSError("closed")),
        DBAPIError("SELECT 1", {}, OSError("closed"), connection_invalidated=True),
        ConnectionDoesNotExistError("connection closed"),
        CannotConnectNowError("database is starting up"),
        TooManyConnectionsError("too many connections"),
        DatabaseUnavailable("connection refused"),
    ],
)
async def test_database_outage_keeps_delivery_unacked_until_recovery(
    processor, message, monkeypatch, error
):
    delays = []

    async def sleep(delay):
        delays.append(delay)
        message.ack.assert_not_awaited()
        message.nack.assert_not_awaited()
        message.reject.assert_not_awaited()

    monkeypatch.setattr("payment_service.messaging.consumer.asyncio.sleep", sleep)
    processor.process.side_effect = [error] * 7 + [True]
    await PaymentConsumer(processor).handle(message)
    assert delays == [1, 2, 4, 8, 16, 30, 30]
    assert processor.process.await_count == 8
    message.ack.assert_awaited_once()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()


async def test_integrity_error_uses_bounded_poison_retry(processor, message, monkeypatch):
    monkeypatch.setattr("payment_service.messaging.consumer.asyncio.sleep", AsyncMock())
    processor.process.side_effect = IntegrityError("INSERT", {}, ValueError("bad data"))
    await PaymentConsumer(processor).handle(message)
    processor.process.assert_awaited_once()
    message.nack.assert_awaited_once_with(requeue=True)


async def test_os_error_outside_database_is_not_retried_forever(processor, message, monkeypatch):
    monkeypatch.setattr("payment_service.messaging.consumer.asyncio.sleep", AsyncMock())
    processor.process.side_effect = OSError("not a database failure")
    await PaymentConsumer(processor).handle(message)
    processor.process.assert_awaited_once()
    message.nack.assert_awaited_once_with(requeue=True)


async def test_cancellation_during_database_retry_leaves_message_unacked(processor, message):
    processor.process.side_effect = DatabaseUnavailable("offline")
    task = asyncio.create_task(PaymentConsumer(processor).handle(message))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    message.ack.assert_not_awaited()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()
