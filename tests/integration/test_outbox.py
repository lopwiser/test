import asyncio
import json
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from aio_pika import DeliveryMode
from faststream.rabbit import RabbitBroker
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent
from payment_service.messaging.broker import PAYMENTS_EXCHANGE
from payment_service.messaging.outbox import OutboxRelay, relay_until_stopped
from tests.support.factories import create_event

pytestmark = pytest.mark.integration


@pytest.fixture
def relay_settings(database):
    return WorkerSettings(
        _env_file=None,
        database_url=database.engine.url.render_as_string(hide_password=False),
        outbox_publish_timeout=5,
    )


@pytest.fixture
def broker():
    return AsyncMock(spec=RabbitBroker)


async def load_event(database, event_id):
    async with database.sessions() as session:
        return (await session.scalars(select(OutboxEvent).where(OutboxEvent.id == event_id))).one()


async def test_publish_marks_event_only_after_confirmation(
    database, broker, relay_settings, payment_body
):
    event = await create_event(database, payment_body)
    confirmed = asyncio.Event()
    publish_started = asyncio.Event()

    async def publish(*args, **kwargs):
        publish_started.set()
        await confirmed.wait()

    broker.publish.side_effect = publish
    relay = OutboxRelay(database, broker, relay_settings)
    async with asyncio.timeout(5):
        task = asyncio.create_task(relay.publish_next())
        await publish_started.wait()
        assert (await load_event(database, event.id)).published_at is None
        confirmed.set()
        assert await task is True

    saved = await load_event(database, event.id)
    assert saved.published_at is not None
    assert saved.publish_attempts == 1
    broker.publish.assert_awaited_once_with(
        event.payload,
        exchange=PAYMENTS_EXCHANGE,
        routing_key="payments.new",
        message_id=str(event.id),
        correlation_id=str(event.id),
        message_type="payment.process",
        persist=True,
        mandatory=True,
    )


async def test_future_and_published_events_are_skipped(
    database, broker, relay_settings, payment_body
):
    future = await create_event(database, payment_body)
    published = await create_event(database, payment_body)
    async with database.sessions.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == future.id)
            .values(available_at=datetime.now(UTC) + timedelta(hours=1))
        )
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == published.id)
            .values(published_at=datetime.now(UTC))
        )
    assert await OutboxRelay(database, broker, relay_settings).publish_next() is False
    broker.publish.assert_not_awaited()


async def test_publication_failure_is_persisted_and_retried(
    database, broker, relay_settings, payment_body
):
    event = await create_event(database, payment_body)
    broker.publish.side_effect = ConnectionError("RabbitMQ unavailable")
    relay = OutboxRelay(database, broker, relay_settings)
    before = datetime.now(UTC)
    assert await relay.publish_next() is True
    saved = await load_event(database, event.id)
    assert saved.published_at is None
    assert saved.publish_attempts == 1
    assert saved.available_at >= before + timedelta(seconds=1)
    assert saved.last_error == "ConnectionError: RabbitMQ unavailable"
    assert await relay.publish_next() is False

    async with database.sessions.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event.id)
            .values(available_at=datetime.now(UTC))
        )
    broker.publish.side_effect = None
    assert await relay.publish_next() is True
    saved = await load_event(database, event.id)
    assert saved.published_at is not None
    assert saved.publish_attempts == 2
    assert saved.last_error is None


async def test_timeout_releases_lock_and_schedules_retry(
    database, broker, relay_settings, payment_body
):
    event = await create_event(database, payment_body)
    relay_settings.outbox_publish_timeout = 0.05

    async def slow_publish(*args, **kwargs):
        await asyncio.sleep(10)

    broker.publish.side_effect = slow_publish
    async with asyncio.timeout(3):
        assert await OutboxRelay(database, broker, relay_settings).publish_next() is True
    saved = await load_event(database, event.id)
    assert saved.published_at is None
    assert saved.last_error.startswith("TimeoutError:")
    async with database.sessions.begin() as session:
        locked = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.id == event.id).with_for_update(skip_locked=True)
        )
        assert locked.id == event.id


async def test_parallel_relays_skip_locked_event(database, broker, relay_settings, payment_body):
    event = await create_event(database, payment_body)
    publish_started = asyncio.Event()
    confirmed = asyncio.Event()

    async def publish(*args, **kwargs):
        publish_started.set()
        await confirmed.wait()

    broker.publish.side_effect = publish
    relay_settings.outbox_publish_timeout = 5
    first = OutboxRelay(database, broker, relay_settings)
    second = OutboxRelay(database, broker, relay_settings)
    async with asyncio.timeout(5):
        task = asyncio.create_task(first.publish_next())
        await publish_started.wait()
        assert await second.publish_next() is False
        confirmed.set()
        assert await task is True
    broker.publish.assert_awaited_once()
    assert (await load_event(database, event.id)).published_at is not None


async def test_confirmed_publish_before_failed_commit_can_be_repeated(
    database, broker, relay_settings, payment_body, monkeypatch
):
    event = await create_event(database, payment_body)
    begin = database.sessions.begin

    @asynccontextmanager
    async def failed_commit() -> AsyncGenerator[AsyncSession, None]:
        async with begin() as session:
            yield session
            raise RuntimeError("Crash after publisher confirm, before commit")

    relay = OutboxRelay(database, broker, relay_settings)
    with monkeypatch.context() as patch:
        patch.setattr(database.sessions, "begin", failed_commit)
        with pytest.raises(RuntimeError, match="Crash after publisher confirm"):
            await relay.publish_next()

    assert (await load_event(database, event.id)).published_at is None
    assert await relay.publish_next() is True
    assert broker.publish.await_count == 2
    assert {call.kwargs["message_id"] for call in broker.publish.await_args_list} == {str(event.id)}


async def test_failed_event_does_not_block_other_payments(
    database, broker, relay_settings, payment_body
):
    first = await create_event(database, payment_body)
    second = await create_event(database, payment_body)
    broker.publish.side_effect = [ConnectionError("unavailable"), None]
    relay = OutboxRelay(database, broker, relay_settings)
    assert await relay.publish_next() is True
    assert await relay.publish_next() is True
    assert (await load_event(database, first.id)).published_at is None
    assert (await load_event(database, second.id)).published_at is not None


async def test_real_rabbitmq_receives_persistent_message(
    database, rabbitmq, relay_settings, payment_body
):
    broker, queue = rabbitmq
    event = await create_event(database, payment_body, routing_key=queue.name)
    relay_settings.outbox_publish_timeout = 5
    assert await OutboxRelay(database, broker, relay_settings).publish_next() is True

    message = await queue.get(timeout=3)
    assert json.loads(message.body) == event.payload
    assert message.message_id == str(event.id)
    assert message.delivery_mode == DeliveryMode.PERSISTENT
    assert (await load_event(database, event.id)).published_at is not None
    await message.ack()


async def test_relay_connects_publishes_and_stops(database, rabbitmq, relay_settings, payment_body):
    _broker, queue = rabbitmq
    event = await create_event(database, payment_body, routing_key=queue.name)
    relay_settings.rabbitmq_url = os.environ["TEST_RABBITMQ_URL"]
    stop = asyncio.Event()
    task = asyncio.create_task(relay_until_stopped(database, relay_settings, stop))
    async with asyncio.timeout(5):
        while (message := await queue.get(fail=False)) is None:
            await asyncio.sleep(0.01)
    assert message.message_id == str(event.id)
    await message.ack()
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert (await load_event(database, event.id)).published_at is not None


async def test_relay_keeps_retrying_an_unreachable_broker_until_stopped(
    database, relay_settings, caplog
):
    relay_settings.rabbitmq_url = "amqp://guest:guest@127.0.0.1:1/"
    relay_settings.outbox_publish_timeout = 0.2
    relay_settings.outbox_retry_base_delay = 0.01
    stop = asyncio.Event()
    task = asyncio.create_task(relay_until_stopped(database, relay_settings, stop))
    async with asyncio.timeout(5):
        while caplog.text.count("Outbox relay unavailable") < 2:
            await asyncio.sleep(0.01)
    assert not task.done()
    stop.set()
    await asyncio.wait_for(task, timeout=5)


async def test_unroutable_message_is_not_marked_as_published(
    database, rabbitmq, relay_settings, payment_body
):
    broker, _queue = rabbitmq
    event = await create_event(database, payment_body, routing_key=f"missing_{uuid4().hex}")
    relay_settings.outbox_publish_timeout = 5
    assert await OutboxRelay(database, broker, relay_settings).publish_next() is True
    saved = await load_event(database, event.id)
    assert saved.published_at is None
    assert saved.publish_attempts == 1
    assert "NO_ROUTE" in saved.last_error


async def test_real_rabbitmq_republishes_same_id_after_lost_commit(
    database, rabbitmq, relay_settings, payment_body
):
    broker, queue = rabbitmq
    event = await create_event(database, payment_body, routing_key=queue.name)
    relay_settings.outbox_publish_timeout = 5
    relay = OutboxRelay(database, broker, relay_settings)
    assert await relay.publish_next() is True
    async with database.sessions.begin() as session:
        await session.execute(
            update(OutboxEvent).where(OutboxEvent.id == event.id).values(published_at=None)
        )
    assert await relay.publish_next() is True
    first = await queue.get(timeout=3)
    second = await queue.get(timeout=3)
    assert first.message_id == second.message_id == str(event.id)
    assert UUID(json.loads(second.body)["event_id"]) == event.id
    await first.ack()
    await second.ack()


async def test_cleanup_removes_only_expired_published_records(
    database, broker, relay_settings, payment_body
):
    expired = await create_event(database, payment_body)
    pending = await create_event(database, payment_body)
    fresh = await create_event(database, payment_body)
    async with database.sessions.begin() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == expired.id)
            .values(published_at=datetime.now(UTC) - timedelta(days=8))
        )
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == fresh.id)
            .values(published_at=datetime.now(UTC))
        )
    await OutboxRelay(database, broker, relay_settings).cleanup()
    async with database.sessions() as session:
        remaining = set((await session.scalars(select(OutboxEvent.id))).all())
    assert remaining == {pending.id, fresh.id}


async def test_cleanup_backlog_does_not_block_publication(
    database, broker, relay_settings, payment_body
):
    async with database.sessions.begin() as session:
        await session.execute(
            insert(OutboxEvent),
            [
                {
                    "id": uuid4(),
                    "event_type": "payment.process",
                    "exchange": "payments.exchange",
                    "routing_key": "payments.new",
                    "payload": {},
                    "published_at": datetime.now(UTC) - timedelta(days=8),
                }
                for _ in range(1001)
            ],
        )
    pending = await create_event(database, payment_body)
    stop = asyncio.Event()
    broker.publish.side_effect = lambda *args, **kwargs: stop.set()
    await OutboxRelay(database, broker, relay_settings).run(stop)
    async with database.sessions() as session:
        remaining = (await session.scalars(select(OutboxEvent))).all()
    assert len(remaining) == 502
    assert (await load_event(database, pending.id)).published_at is not None
    broker.publish.assert_awaited_once()
