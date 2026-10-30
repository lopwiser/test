import asyncio
import hashlib
import hmac
import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest

from payment_service.db.models import Payment
from payment_service.domain import FailureReason
from payment_service.services.webhooks import WebhookSender


@pytest.fixture
def payment():
    return Payment(
        id=uuid4(),
        webhook_event_id=uuid4(),
        status="succeeded",
        amount=Decimal("10.50"),
        currency="RUB",
        description="Order",
        processed_at=datetime.now(UTC),
        payment_metadata={"order_id": 123},
        webhook_url="https://client.example/webhook",
    )


async def test_repeated_webhook_has_same_event_id_and_no_api_key(payment):
    requests = []

    def receive(request):
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        sender = WebhookSender(client, timeout=1, signing_secret="test-secret")
        await sender.send(payment)
        await sender.send(payment)

    first, second = (json.loads(request.content) for request in requests)
    assert first == second
    assert first["event_id"] == str(payment.webhook_event_id)
    assert first["payment_id"] == str(payment.id)
    assert first["amount"] == "10.50"
    assert first["status"] == "succeeded"
    assert first["metadata"] == {"order_id": 123}
    assert all("X-API-Key" not in request.headers for request in requests)
    assert all("X-Webhook-Signature" in request.headers for request in requests)


async def test_signature_covers_timestamp_and_exact_body(payment, monkeypatch):
    requests = []
    monkeypatch.setattr("payment_service.services.webhooks.time", lambda: 1700000000)
    payment.status = "failed"
    payment.failure_reason = "gateway_unavailable"

    def receive(request):
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await WebhookSender(client, timeout=1, signing_secret="test-secret").send(payment)
    request = requests[0]
    expected = hmac.new(
        b"test-secret", b"1700000000." + request.content, hashlib.sha256
    ).hexdigest()
    assert request.headers["X-Webhook-Timestamp"] == "1700000000"
    assert request.headers["X-Webhook-Signature"] == "sha256=" + expected
    assert json.loads(request.content)["failure_reason"] == "gateway_unavailable"
    changed = hmac.new(
        b"test-secret", b"1700000000." + request.content + b" ", hashlib.sha256
    ).hexdigest()
    assert not hmac.compare_digest(expected, changed)


@pytest.mark.parametrize("reason", list(FailureReason))
async def test_every_failure_reason_reaches_the_client(payment, reason):
    requests = []
    payment.status = "failed"
    payment.failure_reason = reason.value

    def receive(request):
        requests.append(request)
        return httpx.Response(204)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        await WebhookSender(client, timeout=1, signing_secret="test-secret").send(payment)
    assert json.loads(requests[0].content)["failure_reason"] == reason.value


@pytest.mark.parametrize("status", [200, 201, 202, 204, 299])
async def test_any_2xx_is_success(payment, status):
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status))
    ) as client:
        await WebhookSender(client, timeout=1, signing_secret="test-secret").send(payment)


@pytest.mark.parametrize("status", [301, 400, 429, 500, 503])
async def test_non_2xx_fails_without_following_redirect(payment, status):
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(status, headers={"location": "https://redirect.example/webhook"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await WebhookSender(client, timeout=1, signing_secret="test-secret").send(payment)
    assert len(calls) == 1


async def test_total_timeout_bounds_even_a_slow_http_transport(payment):
    async def receive(request):
        await asyncio.sleep(5)
        return httpx.Response(200)

    async with httpx.AsyncClient(transport=httpx.MockTransport(receive)) as client:
        async with asyncio.timeout(1):
            with pytest.raises(TimeoutError):
                await WebhookSender(client, timeout=0.01, signing_secret="test-secret").send(
                    payment
                )
