from unittest.mock import Mock

import pytest

from payment_service.db.session import Database
from payment_service.errors import DatabaseUnavailable


@pytest.mark.parametrize("error", [ConnectionRefusedError("offline"), TimeoutError("timeout")])
async def test_database_connect_errors_are_translated_at_engine_boundary(monkeypatch, error):
    database = Database("postgresql+asyncpg://user:password@localhost/unused")
    monkeypatch.setattr(
        database.engine.sync_engine.dialect.loaded_dbapi, "connect", Mock(side_effect=error)
    )
    try:
        with pytest.raises(DatabaseUnavailable) as caught:
            async with database.engine.connect():
                pytest.fail("Connection should not succeed")
        assert caught.value.__cause__ is error
    finally:
        await database.close()
