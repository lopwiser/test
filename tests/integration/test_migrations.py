import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import inspect

from payment_service.db.base import Base
from tests.integration.conftest import run_alembic

pytestmark = pytest.mark.integration


async def test_migration_matches_models(database):
    async with database.engine.connect() as connection:
        differences = await connection.run_sync(
            lambda sync: compare_metadata(MigrationContext.configure(sync), Base.metadata)
        )
    assert differences == []


async def test_downgrade_and_upgrade(database):
    async with database.engine.begin() as connection:
        await connection.run_sync(run_alembic, "downgrade", "base")
        tables = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        assert set(tables) == {"alembic_version"}
        await connection.run_sync(run_alembic, "upgrade", "head")
        tables = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        assert set(tables) == {"payments", "outbox", "alembic_version"}
