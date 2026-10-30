import asyncio
import logging
from datetime import timedelta
from time import monotonic

from aio_pika.exceptions import AMQPException
from faststream.rabbit import RabbitBroker
from sqlalchemy import delete, func, select
from sqlalchemy.exc import InterfaceError, OperationalError

from payment_service.config import WorkerSettings
from payment_service.db.models import OutboxEvent
from payment_service.db.queries import next_outbox_event
from payment_service.db.session import Database
from payment_service.errors import DatabaseUnavailable, describe
from payment_service.messaging.broker import EXCHANGES, create_broker, declare_topology
from payment_service.messaging.runner import run_worker
from payment_service.services.retry import retry_delay

logger = logging.getLogger(__name__)


class OutboxRelay:
    def __init__(self, database: Database, broker: RabbitBroker, settings: WorkerSettings) -> None:
        self.database = database
        self.broker = broker
        self.settings = settings

    async def publish_next(self) -> bool:
        async with self.database.sessions.begin() as session:
            event = await next_outbox_event(session)
            if event is None:
                return False

            event.publish_attempts += 1
            try:
                async with asyncio.timeout(self.settings.outbox_publish_timeout):
                    await self.broker.publish(
                        event.payload,
                        exchange=EXCHANGES[event.exchange],
                        routing_key=event.routing_key,
                        message_id=str(event.id),
                        correlation_id=str(event.id),
                        message_type=event.event_type,
                        persist=True,
                        mandatory=True,
                    )
            except (AMQPException, OSError) as exc:
                delay = retry_delay(
                    event.publish_attempts,
                    self.settings.outbox_retry_base_delay,
                    self.settings.outbox_retry_max_delay,
                )
                event.available_at = await session.scalar(
                    select(func.clock_timestamp())
                ) + timedelta(seconds=delay)
                event.last_error = describe(exc)
                logger.warning(
                    "Publish failed event_id=%s payment_id=%s "
                    "publish_attempt=%s retry_in=%s error=%s",
                    event.id,
                    event.payment_id,
                    event.publish_attempts,
                    delay,
                    event.last_error,
                )
            else:
                event.published_at = await session.scalar(select(func.clock_timestamp()))
                event.last_error = None
                logger.info(
                    "Published event_id=%s payment_id=%s publish_attempt=%s",
                    event.id,
                    event.payment_id,
                    event.publish_attempts,
                )
        return True

    async def cleanup(self) -> bool:
        async with self.database.sessions.begin() as session:
            cutoff = func.now() - timedelta(days=self.settings.outbox_retention_days)
            expired = (
                select(OutboxEvent.id)
                .where(OutboxEvent.published_at < cutoff)
                .order_by(OutboxEvent.published_at)
                .limit(500)
                .with_for_update(skip_locked=True)
            )
            removed = await session.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(expired)))
        return removed.rowcount == 500

    async def run(self, stop: asyncio.Event) -> None:
        next_cleanup = 0.0
        while not stop.is_set():
            if monotonic() >= next_cleanup:
                more_expired = await self.cleanup()
                next_cleanup = monotonic() + (0 if more_expired else 60)
            if not await self.publish_next():
                await asyncio.sleep(self.settings.outbox_poll_interval)


async def relay_until_stopped(
    database: Database, settings: WorkerSettings, stop: asyncio.Event
) -> None:
    failures = 0
    while not stop.is_set():
        broker = create_broker(str(settings.rabbitmq_url), settings.outbox_publish_timeout)
        try:
            async with broker:
                async with asyncio.timeout(settings.outbox_publish_timeout):
                    await declare_topology(broker)
                failures = 0
                logger.info("Outbox relay connected")
                await OutboxRelay(database, broker, settings).run(stop)
        except (AMQPException, DatabaseUnavailable, OperationalError, InterfaceError, OSError):
            failures += 1
            delay = retry_delay(
                failures, settings.outbox_retry_base_delay, settings.outbox_retry_max_delay
            )
            logger.exception("Outbox relay unavailable; reconnect_in=%s", delay)
            await asyncio.sleep(delay)


async def run_relay(settings: WorkerSettings, stop: asyncio.Event) -> None:
    async with Database(settings.database_url) as database:
        await relay_until_stopped(database, settings, stop)


def main() -> None:
    run_worker(WorkerSettings(), run_relay)


if __name__ == "__main__":
    main()
