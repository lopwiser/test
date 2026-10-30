from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI

from payment_service.api.dependencies import require_api_key
from payment_service.api.exception_handlers import register_exception_handlers
from payment_service.api.routes import health, payments
from payment_service.config import get_settings
from payment_service.db.session import Database
from payment_service.logging import configure_logging


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    configure_logging(settings.log_level)
    async with Database(settings.database_url) as database:
        app.state.database = database
        yield


def include_routers(app: FastAPI) -> None:
    for router in (health.router, payments.router):
        app.include_router(router, dependencies=[Depends(require_api_key)])


def create_app() -> FastAPI:
    app = FastAPI(title="Payment service", version="0.1.0", lifespan=lifespan, redoc_url=None)
    include_routers(app)
    register_exception_handlers(app)
    return app


app = create_app()
