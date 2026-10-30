from collections.abc import AsyncGenerator
from hmac import compare_digest
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import Settings, get_settings
from payment_service.db.session import Database

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_database(request: Request) -> Database:
    return request.app.state.database


async def require_api_key(
    settings: Annotated[Settings, Depends(get_settings)],
    supplied_key: Annotated[str | None, Security(api_key_header)],
) -> None:
    expected = settings.api_key.get_secret_value().encode("utf-8")
    if supplied_key is None or not compare_digest(supplied_key.encode("utf-8"), expected):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid or missing API key")


async def get_session(
    database: Annotated[Database, Depends(get_database)],
) -> AsyncGenerator[AsyncSession, None]:
    async with database.sessions() as session:
        yield session


Session = Annotated[AsyncSession, Depends(get_session)]
