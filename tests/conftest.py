from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def payment_body() -> dict:
    return {
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Оплата заказа 123",
        "metadata": {"order_id": "123", "nested": {"a": 1, "b": [True, None]}},
        "webhook_url": "https://client.example/webhooks/payments",
    }
