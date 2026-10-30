import pytest
from pydantic import ValidationError

from payment_service.api.schemas import PaymentCreate
from payment_service.config import ConsumerSettings, DatabaseSettings, Settings, WorkerSettings


@pytest.mark.parametrize("amount", ["0", "-1", "1.001", "10000000000000000", "NaN", "Infinity"])
def test_rejects_invalid_amounts(payment_body, amount):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**payment_body, "amount": amount})


@pytest.mark.parametrize("amount", ["0.01", "9999999999999999.99", "1500", 1500])
def test_accepts_valid_amounts(payment_body, amount):
    payment = PaymentCreate.model_validate({**payment_body, "amount": amount})
    assert payment.amount > 0
    assert payment.amount.as_tuple().exponent == -2


@pytest.mark.parametrize(
    "update",
    [
        {"currency": "GBP"},
        {"description": "x" * 1001},
        {"description": "nul\x00character"},
        {"metadata": []},
        {"metadata": {"nested": [float("nan")]}},
        {"metadata": {"nul\x00key": True}},
        {"metadata": {"nested": ["nul\x00value"]}},
        {"webhook_url": "ftp://client.example/webhook"},
        {"webhook_url": "https://client.example/" + "x" * 2048},
        {"unexpected": "field"},
    ],
)
def test_rejects_invalid_fields(payment_body, update):
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate({**payment_body, **update})


@pytest.mark.parametrize("api_key", ["", "   "])
def test_rejects_blank_api_key(api_key):
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            database_url="postgresql+asyncpg://user:password@localhost/db",
            api_key=api_key,
        )


def test_requires_asyncpg_driver():
    with pytest.raises(ValidationError):
        Settings(
            _env_file=None,
            database_url="postgresql://user:password@localhost/db",
            api_key="test-key",
        )


@pytest.mark.parametrize("settings_type", [DatabaseSettings, WorkerSettings])
def test_background_settings_do_not_require_api_secret(settings_type):
    settings = settings_type(
        _env_file=None, database_url="postgresql+asyncpg://user:password@localhost/db"
    )
    assert "api_key" not in type(settings).model_fields


def test_consumer_requires_webhook_signing_secret(monkeypatch):
    monkeypatch.delenv("WEBHOOK_SIGNING_SECRET", raising=False)
    with pytest.raises(ValidationError, match="webhook_signing_secret"):
        ConsumerSettings(
            _env_file=None, database_url="postgresql+asyncpg://user:password@localhost/db"
        )


@pytest.mark.parametrize("secret", ["", "   "])
def test_consumer_rejects_blank_webhook_signing_secret(secret):
    with pytest.raises(ValidationError, match="must not be blank"):
        ConsumerSettings(
            _env_file=None,
            database_url="postgresql+asyncpg://user:password@localhost/db",
            webhook_signing_secret=secret,
        )
