import asyncio
import logging
from collections.abc import Awaitable, Callable
from functools import partial
from typing import TypeVar

import httpx
from asyncpg import CannotConnectNowError, PostgresConnectionError, TooManyConnectionsError
from asyncpg import InterfaceError as AsyncpgInterfaceError
from faststream import FastStream
from faststream.middlewares import AckPolicy
from faststream.rabbit import RabbitBroker, RabbitQueue
from faststream.rabbit.annotations import RabbitMessage
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

from payment_service.config import ConsumerSettings
from payment_service.db.session import Database
from payment_service.domain import MAX_ATTEMPTS
from payment_service.errors import DatabaseUnavailable
from payment_service.messaging.broker import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    create_broker,
    declare_topology,
)
from payment_service.messaging.runner import run_worker
from payment_service.messaging.schemas import PaymentProcessEvent
from payment_service.services.gateway import PaymentGateway
from payment_service.services.processing import PaymentProcessor
from payment_service.services.retry import retry_delay
from payment_service.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)

T = TypeVar("T")

DATABASE_OUTAGE_ERRORS = (
    OperationalError,
    InterfaceError,
    AsyncpgInterfaceError,
    PostgresConnectionError,
    CannotConnectNowError,
    TooManyConnectionsError,
    PoolTimeoutError,
    DatabaseUnavailable,
)


def is_database_outage(error: Exception) -> bool:
    if isinstance(error, DATABASE_OUTAGE_ERRORS):
        return True
    return isinstance(error, DBAPIError) and error.connection_invalidated


def delivery_attempt(message: RabbitMessage) -> int:
    return message.headers.get("x-delivery-count", 0) + 1


class PaymentConsumer:
    def __init__(self, processor: PaymentProcessor) -> None:
        self.processor = processor

    async def handle(self, message: RabbitMessage) -> None:
        try:
            event = PaymentProcessEvent.model_validate_json(message.body)
        except ValidationError:
            logger.exception("Invalid payment message; rejecting to DLQ")
            await message.reject(requeue=False)
            return

        try:
            accepted = await self.with_database_retry(event, self.processor.process)
        except Exception as error:
            logger.exception(
                "Processing error payment_id=%s broker_attempt=%s",
                event.payment_id,
                delivery_attempt(message),
            )
            await self.retry_or_dead_letter(message, event, error)
            return

        if accepted:
            await message.ack()
        else:
            await message.reject(requeue=False)

    async def with_database_retry(
        self, event: PaymentProcessEvent, operation: Callable[[PaymentProcessEvent], Awaitable[T]]
    ) -> T:
        failures = 0
        while True:
            try:
                return await operation(event)
            except Exception as error:
                if not is_database_outage(error):
                    raise
            failures += 1
            delay = self.backoff(failures)
            logger.warning(
                "Database unavailable payment_id=%s retry_in=%s", event.payment_id, delay
            )
            await asyncio.sleep(delay)

    async def retry_or_dead_letter(
        self, message: RabbitMessage, event: PaymentProcessEvent, error: Exception
    ) -> None:
        attempt = delivery_attempt(message)
        if attempt >= MAX_ATTEMPTS:
            await self.abandon(event, error)
            await message.reject(requeue=False)
            return
        await asyncio.sleep(self.backoff(attempt))
        await message.nack(requeue=True)

    async def abandon(self, event: PaymentProcessEvent, error: Exception) -> None:
        try:
            await self.with_database_retry(event, partial(self.processor.abandon, error=error))
        except Exception:
            logger.exception("Could not close abandoned payment_id=%s", event.payment_id)

    def backoff(self, attempt: int) -> float:
        settings = self.processor.settings
        return retry_delay(
            attempt,
            settings.infrastructure_retry_base_delay,
            settings.infrastructure_retry_max_delay,
        )


def register_consumer(
    broker: RabbitBroker,
    processor: PaymentProcessor,
    queue: RabbitQueue = PAYMENTS_QUEUE,
) -> None:
    consumer = PaymentConsumer(processor)

    @broker.subscriber(
        queue,
        exchange=PAYMENTS_EXCHANGE,
        ack_policy=AckPolicy.MANUAL,
        decoder=lambda message: message.body,
    )
    async def handle(message: RabbitMessage) -> None:
        await consumer.handle(message)


def build_processor(
    settings: ConsumerSettings, database: Database, client: httpx.AsyncClient
) -> PaymentProcessor:
    webhooks = WebhookSender(
        client, settings.webhook_timeout, settings.webhook_signing_secret.get_secret_value()
    )
    gateway = PaymentGateway(settings.gateway_error_mode)
    return PaymentProcessor(database, gateway, webhooks, settings)


async def run_consumer(settings: ConsumerSettings, stop: asyncio.Event) -> None:
    async with (
        Database(settings.database_url) as database,
        httpx.AsyncClient(trust_env=False) as client,
    ):
        broker = create_broker(str(settings.rabbitmq_url))
        register_consumer(broker, build_processor(settings, database, client))
        app = FastStream(broker, logger=logger)
        try:
            await broker.connect()
            await declare_topology(broker)
            await app.start()
            await stop.wait()
        finally:
            await app.stop()


def main() -> None:
    run_worker(ConsumerSettings(), run_consumer)


if __name__ == "__main__":
    main()
