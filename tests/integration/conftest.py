import os
from collections.abc import AsyncGenerator
from uuid import uuid4

import pytest
from aio_pika import RobustQueue, connect_robust
from alembic.config import Config
from faststream.rabbit import RabbitBroker, RabbitQueue
from httpx import ASGITransport, AsyncClient
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateSchema, DropSchema

from alembic import command
from payment_service.api.app import create_app
from payment_service.api.dependencies import get_database
from payment_service.config import Settings, get_settings
from payment_service.db.session import Database
from payment_service.messaging.broker import (
    DEAD_LETTER_EXCHANGE,
    EXCHANGES,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    create_broker,
)
from tests.conftest import ROOT


def run_alembic(connection: Connection, operation: str, revision: str) -> None:
    config = Config(str(ROOT / "alembic.ini"))
    config.attributes["connection"] = connection
    getattr(command, operation)(config, revision)


@pytest.fixture
async def database() -> AsyncGenerator[Database, None]:
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set TEST_DATABASE_URL to run PostgreSQL integration tests")

    admin = create_async_engine(url, poolclass=NullPool)
    schema = f"test_{uuid4().hex}"
    async with admin.begin() as connection:
        await connection.execute(CreateSchema(schema))

    database = Database(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with database.engine.begin() as connection:
            await connection.run_sync(run_alembic, "upgrade", "head")
        yield database
    finally:
        await database.close()
        async with admin.begin() as connection:
            await connection.execute(DropSchema(schema, cascade=True))
        await admin.dispose()


@pytest.fixture
async def client(database: Database) -> AsyncGenerator[AsyncClient, None]:
    settings = Settings(
        _env_file=None,
        database_url=database.engine.url.render_as_string(hide_password=False),
        api_key="test-api-key",
    )
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_database] = lambda: database
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
        headers={"X-API-Key": "test-api-key", "Idempotency-Key": "order-123"},
    ) as client:
        yield client


@pytest.fixture
async def rabbitmq() -> AsyncGenerator[tuple[RabbitBroker, RobustQueue], None]:
    url = os.environ.get("TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("Set TEST_RABBITMQ_URL to run RabbitMQ integration tests")

    async with create_broker(url) as broker:
        for schema in EXCHANGES.values():
            await broker.declare_exchange(schema)
        dead_name = f"test_dead_{uuid4().hex}"
        dead = await broker.declare_queue(RabbitQueue(dead_name, exclusive=True, auto_delete=True))
        await dead.bind(await broker.declare_exchange(DEAD_LETTER_EXCHANGE), routing_key=dead_name)
        name = f"test_{uuid4().hex}"
        schema = RabbitQueue(
            name,
            durable=True,
            routing_key=name,
            arguments={**PAYMENTS_QUEUE.arguments, "x-dead-letter-routing-key": dead_name},
        )
        queue = await broker.declare_queue(schema)
        await queue.bind(await broker.declare_exchange(PAYMENTS_EXCHANGE), routing_key=name)
        try:
            yield broker, queue
        finally:
            async with await connect_robust(url) as cleanup_connection:
                channel = await cleanup_connection.channel()
                await channel.queue_delete(name, if_unused=False, if_empty=False)
