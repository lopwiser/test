import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import InterfaceError, OperationalError

from payment_service.errors import DatabaseUnavailable, IdempotencyConflict, PaymentNotFound

logger = logging.getLogger(__name__)


async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = [
        {"type": error["type"], "loc": error["loc"], "msg": error["msg"]} for error in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"detail": errors})


async def idempotency_conflict(request: Request, exc: IdempotencyConflict) -> JSONResponse:
    return JSONResponse(
        status_code=409, content={"detail": "Idempotency key was used for a different request"}
    )


async def payment_not_found(request: Request, exc: PaymentNotFound) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": "Payment not found"})


async def database_unavailable(
    request: Request, exc: OperationalError | InterfaceError | DatabaseUnavailable
) -> JSONResponse:
    logger.exception("Database operation failed")
    return JSONResponse(status_code=503, content={"detail": "Database temporarily unavailable"})


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(RequestValidationError, validation_error)
    app.add_exception_handler(IdempotencyConflict, idempotency_conflict)
    app.add_exception_handler(PaymentNotFound, payment_not_found)
    for error in (OperationalError, InterfaceError, DatabaseUnavailable):
        app.add_exception_handler(error, database_unavailable)
