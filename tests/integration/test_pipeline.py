import asyncio
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from faststream.rabbit import RabbitQueue
from sqlalchemy import select

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent, Payment
from payment_service.domain import PaymentStatus
from payment_service.messaging.consumer import register_consumer
from payment_service.messaging.outbox import OutboxRelay
from payment_service.messaging.schemas import PaymentProcessEvent
from payment_service.services.gateway import PaymentGateway
from payment_service.services.processing import PaymentProcessor
from payment_service.services.webhooks import WebhookSender

pytestmark = pytest.mark.integration


def queue_schema(queue):
    return RabbitQueue(queue.name, durable=True, routing_key=queue.name, arguments=queue.arguments)


async def dead_queue(broker, queue):
    return await broker.declare_queue(
        RabbitQueue(queue.arguments["x-dead-letter-routing-key"], exclusive=True, auto_delete=True)
    )


async def next_dead_message(queue):
    async with asyncio.timeout(5):
        while (message := await queue.get(fail=False)) is None:
            await asyncio.sleep(0.01)
        return message


@pytest.mark.parametrize("webhook_status", [204, 503])
async def test_api_outbox_consumer_webhook_and_original_message_dlq(
    client, database, rabbitmq, payment_body, monkeypatch, webhook_status
):
    broker, queue = rabbitmq
    schema = queue_schema(queue)
    dead = await dead_queue(broker, queue)
    monkeypatch.setattr("payment_service.messaging.outbox_records.PAYMENTS_QUEUE", schema)
    settings = WorkerSettings(
        _env_file=None,
        database_url=database.engine.url.render_as_string(hide_password=False),
        processing_retry_base_delay=0.01,
    )
    gateway = AsyncMock(spec=PaymentGateway)
    gateway.process.return_value = PaymentStatus.SUCCEEDED
    deliveries = []

    def receive(request):
        deliveries.append(json.loads(request.content))
        return httpx.Response(webhook_status)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        processor = PaymentProcessor(
            database, gateway, WebhookSender(http, 1, "test-secret"), settings
        )
        relay = OutboxRelay(database, broker, settings)
        register_consumer(broker, processor, schema)
        try:
            await broker.start()
            response = await client.post("/api/v1/payments", json=payment_body)
            assert response.status_code == 202
            async with database.sessions.begin() as session:
                initial = (await session.scalars(select(OutboxEvent))).one()
                initial.routing_key = queue.name
                payment_id = initial.payment_id
            async with asyncio.timeout(10):
                while True:
                    await relay.publish_next()
                    async with database.sessions() as session:
                        payment = await session.get(Payment, payment_id)
                    if payment.workflow_status != "active":
                        break
                    await asyncio.sleep(0.02)
            gateway.process.assert_awaited_once()
            assert payment.status == "succeeded"
            if webhook_status == 204:
                assert payment.workflow_status == "completed"
                assert len(deliveries) == 1
            else:
                assert payment.workflow_status == "dead_lettered"
                assert len(deliveries) == 3
                assert deliveries[0] == deliveries[1] == deliveries[2]
                message = await next_dead_message(dead)
                original = PaymentProcessEvent.model_validate_json(message.body)
                assert original.payment_id == payment_id
                assert original.stage == "webhook"
                assert original.attempt == 3
                assert message.headers["x-death"][0]["reason"] == "rejected"
                await message.ack()
        finally:
            await broker.stop()


async def test_unexpected_error_fails_payment_tells_client_and_reaches_dlq(
    client, database, rabbitmq, payment_body, monkeypatch
):
    broker, queue = rabbitmq
    schema = queue_schema(queue)
    dead = await dead_queue(broker, queue)
    monkeypatch.setattr("payment_service.messaging.outbox_records.PAYMENTS_QUEUE", schema)
    settings = WorkerSettings(
        _env_file=None,
        database_url=database.engine.url.render_as_string(hide_password=False),
        infrastructure_retry_base_delay=0.01,
    )
    gateway = AsyncMock(spec=PaymentGateway)
    gateway.process.side_effect = RuntimeError("bug")
    deliveries = []

    def receive(request):
        deliveries.append(json.loads(request.content))
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as http:
        processor = PaymentProcessor(
            database, gateway, WebhookSender(http, 1, "test-secret"), settings
        )
        relay = OutboxRelay(database, broker, settings)
        register_consumer(broker, processor, schema)
        try:
            await broker.start()
            created = (await client.post("/api/v1/payments", json=payment_body)).json()
            async with asyncio.timeout(10):
                while not deliveries:
                    await relay.publish_next()
                    await asyncio.sleep(0.02)
            message = await next_dead_message(dead)
            assert message.headers["x-death"][0]["reason"] == "rejected"
            await message.ack()
            assert gateway.process.await_count == 3
            assert deliveries[0]["status"] == "failed"
            assert deliveries[0]["failure_reason"] == "processing_error"
            payment = (await client.get(f"/api/v1/payments/{created['payment_id']}")).json()
            assert payment["status"] == "failed"
            assert payment["failure_reason"] == "processing_error"
        finally:
            await broker.stop()


async def test_poison_message_is_bounded_and_healthy_message_still_completes(rabbitmq):
    broker, queue = rabbitmq
    dead = await dead_queue(broker, queue)
    poison = PaymentProcessEvent(event_id=uuid4(), payment_id=uuid4())
    healthy = PaymentProcessEvent(event_id=uuid4(), payment_id=uuid4())
    completed = asyncio.Event()
    attempts = []
    processor = AsyncMock(spec=PaymentProcessor)
    processor.settings = WorkerSettings(
        _env_file=None,
        database_url="postgresql+asyncpg://payments:payments@localhost/payments",
        infrastructure_retry_base_delay=0.01,
    )

    async def process(event):
        if event.payment_id == poison.payment_id:
            attempts.append(event.event_id)
            raise RuntimeError("poison")
        completed.set()
        return True

    processor.process.side_effect = process
    register_consumer(broker, processor, queue_schema(queue))
    try:
        await broker.start()
        await broker.publish(poison.model_dump_json().encode(), queue=queue.name, persist=True)
        await broker.publish(healthy.model_dump_json().encode(), queue=queue.name, persist=True)
        await asyncio.wait_for(completed.wait(), timeout=5)
        message = await next_dead_message(dead)
        assert message.body == poison.model_dump_json().encode()
        assert len(attempts) == 3
        await message.ack()
    finally:
        await broker.stop()


async def test_invalid_binary_message_reaches_dlq_unchanged(rabbitmq):
    broker, queue = rabbitmq
    dead = await dead_queue(broker, queue)
    processor = AsyncMock(spec=PaymentProcessor)
    register_consumer(broker, processor, queue_schema(queue))
    try:
        await broker.start()
        original = b"bad-json\x00\xff"
        await broker.publish(
            original, queue=queue.name, content_type="application/json", persist=True
        )
        message = await next_dead_message(dead)
        assert message.body == original
        processor.process.assert_not_awaited()
        await message.ack()
    finally:
        await broker.stop()
