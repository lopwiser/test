from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from payment_service.domain import PaymentStatus
from payment_service.errors import GatewayError
from payment_service.services.gateway import PaymentGateway


async def test_gateway_has_stable_results_and_90_10_distribution(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr("payment_service.services.gateway.asyncio.sleep", sleep)
    gateway = PaymentGateway()
    results = [await gateway.process(UUID(int=value)) for value in range(100)]
    assert results.count(PaymentStatus.SUCCEEDED) == 90
    assert results.count(PaymentStatus.FAILED) == 10
    assert await gateway.process(UUID(int=20)) == results[20]
    assert await gateway.process(UUID(int=21)) == results[21]
    assert all(2 <= call.args[0] <= 5 for call in sleep.await_args_list)


async def test_transient_gateway_error_preserves_operation_identity(monkeypatch):
    monkeypatch.setattr("payment_service.services.gateway.asyncio.sleep", AsyncMock())
    gateway = PaymentGateway("transient")
    payment_id = UUID(int=21)
    for attempt in [1, 2]:
        with pytest.raises(GatewayError):
            await gateway.process(payment_id, attempt=attempt)
    assert await gateway.process(payment_id, attempt=3) == PaymentStatus.SUCCEEDED
    assert await gateway.process(payment_id, attempt=3) == PaymentStatus.SUCCEEDED


async def test_unavailable_gateway_can_exhaust_all_attempts(monkeypatch):
    monkeypatch.setattr("payment_service.services.gateway.asyncio.sleep", AsyncMock())
    gateway = PaymentGateway("unavailable")
    for attempt in [1, 2, 3]:
        with pytest.raises(GatewayError):
            await gateway.process(UUID(int=21), attempt=attempt)
