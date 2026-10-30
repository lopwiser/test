from uuid import uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import InterfaceError, OperationalError

from payment_service.api.app import create_app
from payment_service.api.dependencies import get_database, get_session
from payment_service.config import Settings, get_settings
from payment_service.db.session import Database
from payment_service.errors import DatabaseUnavailable

UNUSED_DATABASE_URL = "postgresql+asyncpg://user:password@localhost:1/unused"


@pytest.fixture
def settings():
    return Settings(_env_file=None, database_url=UNUSED_DATABASE_URL, api_key="test-api-key")


@pytest.fixture
async def app(settings):
    database = Database(UNUSED_DATABASE_URL)
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_database] = lambda: database
    yield app
    await database.close()


@pytest.fixture
async def client(app):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.mark.parametrize("key", [None, "incorrect-key"])
@pytest.mark.parametrize("endpoint", ["/health", "/api/v1/payments/{id}"])
async def test_read_endpoints_require_key_before_database_access(client, key, endpoint):
    headers = {} if key is None else {"X-API-Key": key}
    response = await client.get(endpoint.format(id=uuid4()), headers=headers)
    assert response.status_code == 401


async def test_create_requires_key(client, payment_body):
    response = await client.post(
        "/api/v1/payments", json=payment_body, headers={"Idempotency-Key": "order-123"}
    )
    assert response.status_code == 401


async def test_openapi_schema_is_public_and_describes_api_key_auth(client):
    response = await client.get("/openapi.json")
    assert response.status_code == 200
    assert "/api/v1/payments" in response.json()["paths"]
    assert response.json()["components"]["securitySchemes"]["APIKeyHeader"]["name"] == "X-API-Key"
    assert response.json()["paths"]["/api/v1/payments"]["post"]["security"] == [
        {"APIKeyHeader": []}
    ]
    paths = response.json()["paths"]
    assert set(paths["/api/v1/payments"]["post"]["responses"]) == {
        "202",
        "401",
        "409",
        "422",
        "503",
    }
    assert set(paths["/api/v1/payments/{payment_id}"]["get"]["responses"]) == {
        "200",
        "401",
        "404",
        "422",
        "503",
    }
    assert paths["/api/v1/payments"]["post"]["responses"]["409"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/ErrorResponse"}


async def test_swagger_is_public(client):
    response = await client.get("/docs")
    assert response.status_code == 200
    assert "SwaggerUIBundle" in response.text


async def test_nonfinite_metadata_returns_422_instead_of_serialization_error(client):
    response = await client.post(
        "/api/v1/payments",
        headers={
            "X-API-Key": "test-api-key",
            "Idempotency-Key": "order-123",
            "Content-Type": "application/json",
        },
        content='{"amount":"1.00","currency":"RUB","description":"test",'
        '"metadata":{"bad":NaN},"webhook_url":"https://client.example"}',
    )
    assert response.status_code == 422


@pytest.mark.parametrize(
    "error",
    [
        OperationalError("SELECT 1", {}, OSError("offline")),
        InterfaceError("SELECT 1", {}, OSError("offline")),
        DatabaseUnavailable("offline"),
    ],
)
async def test_connection_failure_returns_503(app, client, error):
    async def unavailable():
        raise error

    app.dependency_overrides[get_session] = unavailable
    response = await client.get("/health", headers={"X-API-Key": "test-api-key"})
    assert response.status_code == 503
    assert response.json() == {"detail": "Database temporarily unavailable"}


async def test_lifespan_opens_and_closes_database(monkeypatch, settings):
    closed = []
    close = Database.close

    async def track_close(database):
        closed.append(database)
        await close(database)

    monkeypatch.setattr("payment_service.api.app.get_settings", lambda: settings)
    monkeypatch.setattr(Database, "close", track_close)
    app = create_app()
    async with app.router.lifespan_context(app):
        database = app.state.database
        assert database.engine.url.render_as_string(hide_password=False) == UNUSED_DATABASE_URL
    assert closed == [database]
