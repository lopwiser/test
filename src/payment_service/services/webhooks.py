import asyncio
import hashlib
import hmac
from datetime import datetime
from decimal import Decimal
from time import time
from typing import Literal
from uuid import UUID

import httpx
from pydantic import BaseModel, JsonValue

from payment_service.db.models import Payment
from payment_service.domain import Currency, FailureReason, PaymentStatus


class WebhookEvent(BaseModel):
    event_id: UUID
    event_type: Literal["payment.completed"] = "payment.completed"
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    description: str
    processed_at: datetime
    metadata: dict[str, JsonValue]
    failure_reason: FailureReason | None = None


class WebhookSender:
    def __init__(self, client: httpx.AsyncClient, timeout: float, signing_secret: str) -> None:
        self.client = client
        self.timeout = timeout
        self.signing_secret = signing_secret

    async def send(self, payment: Payment) -> None:
        event = WebhookEvent(
            event_id=payment.webhook_event_id,
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            processed_at=payment.processed_at,
            metadata=payment.payment_metadata,
            failure_reason=payment.failure_reason,
        )
        body = event.model_dump_json().encode("utf-8")
        headers = {"Content-Type": "application/json"}
        timestamp = str(int(time()))
        signature = hmac.new(
            self.signing_secret.encode("utf-8"),
            timestamp.encode("ascii") + b"." + body,
            hashlib.sha256,
        ).hexdigest()
        headers["X-Webhook-Timestamp"] = timestamp
        headers["X-Webhook-Signature"] = "sha256=" + signature
        async with asyncio.timeout(self.timeout):
            async with self.client.stream(
                "POST",
                payment.webhook_url,
                content=body,
                headers=headers,
                timeout=httpx.Timeout(self.timeout, connect=min(2, self.timeout)),
                follow_redirects=False,
            ) as response:
                response.raise_for_status()
