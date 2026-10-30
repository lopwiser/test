from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any, Self
from uuid import UUID

from pydantic import PostgresDsn
from sqlalchemy import event, text
from sqlalchemy.engine import Dialect
from sqlalchemy.engine.interfaces import DBAPIConnection
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import ConnectionPoolEntry

from payment_service.errors import DatabaseUnavailable


def connect_to_database(
    dialect: Dialect,
    _connection_record: ConnectionPoolEntry,
    args: list[Any],
    kwargs: dict[str, Any],
) -> DBAPIConnection:
    try:
        return dialect.connect(*args, **kwargs)
    except OSError as exc:
        raise DatabaseUnavailable("Database connection unavailable") from exc


class Database:
    def __init__(
        self, url: str | PostgresDsn, *, connect_args: dict[str, Any] | None = None
    ) -> None:
        self.engine = create_async_engine(
            str(url), pool_pre_ping=True, hide_parameters=True, connect_args=connect_args or {}
        )
        event.listen(self.engine.sync_engine, "do_connect", connect_to_database)
        self.sessions = async_sessionmaker(self.engine, expire_on_commit=False)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    @asynccontextmanager
    async def payment_lock(self, payment_id: UUID) -> AsyncGenerator[None, None]:
        key = int.from_bytes(payment_id.bytes[:8], signed=True)
        async with self.engine.begin() as connection:
            await connection.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            yield

    async def close(self) -> None:
        await self.engine.dispose()
