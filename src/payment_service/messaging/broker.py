import logging

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.schemas import Channel

from payment_service.domain import MAX_ATTEMPTS

logger = logging.getLogger(__name__)

PAYMENTS_EXCHANGE = RabbitExchange("payments.exchange", durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", durable=True)
DEAD_LETTER_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key="payments.failed")
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-queue-type": "quorum",
        "x-delivery-limit": MAX_ATTEMPTS - 1,
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": DEAD_LETTER_QUEUE.routing_key,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    },
)
EXCHANGES = {
    PAYMENTS_EXCHANGE.name: PAYMENTS_EXCHANGE,
    DEAD_LETTER_EXCHANGE.name: DEAD_LETTER_EXCHANGE,
}


def create_broker(url: str, timeout: float = 5) -> RabbitBroker:
    return RabbitBroker(
        url,
        timeout=timeout,
        graceful_timeout=30,
        logger=logger,
        default_channel=Channel(prefetch_count=1, publisher_confirms=True, on_return_raises=True),
    )


async def declare_topology(broker: RabbitBroker) -> None:
    for queue_schema, exchange_schema in (
        (PAYMENTS_QUEUE, PAYMENTS_EXCHANGE),
        (DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE),
    ):
        exchange = await broker.declare_exchange(exchange_schema)
        queue = await broker.declare_queue(queue_schema)
        await queue.bind(exchange, routing_key=queue_schema.routing_key)
