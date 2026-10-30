# Payment Service

An asynchronous payment processing service built with FastAPI, PostgreSQL, and
RabbitMQ. Payments are processed through a simulated payment gateway, and results
are delivered to clients via webhooks.

## Local setup

For local testing you need:

- Python 3.11.
- Poetry 2.5.1.
- Docker with Docker Compose.


Install the dependencies:

```shell
poetry env use 3.11
poetry install --no-root
```

Create the environment file:

```shell
cp .env.example .env
```

Set `API_KEY` in `.env` to a static key of your choice. All API endpoints require
the same value in the `X-API-Key` header. Set `WEBHOOK_SIGNING_SECRET` to a separate
secret shared with the webhook receiver; the example value is for local testing.

Start the services:

```shell
docker compose --profile demo up --build -d
```

This starts PostgreSQL, RabbitMQ, the API, the payment consumer, the outbox
publisher, and a local webhook receiver. A one-off `migrate` service applies
database migrations before the API and workers start.

Default service addresses:

- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15673 (username and password: `payments`)
- Webhook receiver: http://localhost:8081

After changing `API_KEY` or other environment settings, apply the configuration:

```shell
docker compose up -d api consumer outbox
```

View application logs:

```shell
docker compose logs -f api consumer outbox webhook-demo
```

Stop the services:

```shell
docker compose --profile demo down
```

`down` keeps the PostgreSQL and RabbitMQ volumes, so payments, queued messages and dead
letters survive the next `up`. Add `-v` to delete them.

## API examples

Amounts are accepted as decimals and returned as JSON strings, such as `"100.50"`.
Use your `.env` API key in the examples below. Every new payment needs a unique
`Idempotency-Key`. Repeating the same request returns the existing payment;
reusing its key with a different body returns `409`.

Bash / curl:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: YOUR_API_KEY' \
  -H 'Idempotency-Key: order-123' \
  -H 'Content-Type: application/json' \
  --data '{"amount":"100.50","currency":"RUB","description":"Order 123","metadata":{"order_id":"123"},"webhook_url":"http://webhook-demo:8080/success"}'

curl http://localhost:8000/api/v1/payments/PAYMENT_ID \
  -H 'X-API-Key: YOUR_API_KEY'
```

The POST response is `202 Accepted` with `payment_id`, `status`, and `created_at`.
Replace `PAYMENT_ID` in the GET URL with the returned identifier.

PowerShell:

```powershell
$headers = @{ 'X-API-Key' = 'YOUR_API_KEY'; 'Idempotency-Key' = 'order-123' }
$body = @{
    amount = '100.50'
    currency = 'RUB'
    description = 'Order 123'
    metadata = @{ order_id = '123' }
    webhook_url = 'http://webhook-demo:8080/success'
} | ConvertTo-Json -Depth 3
$payment = Invoke-RestMethod -Method Post -Uri 'http://localhost:8000/api/v1/payments' -Headers $headers -ContentType 'application/json; charset=utf-8' -Body $body
$payment
Invoke-RestMethod -Method Get -Uri "http://localhost:8000/api/v1/payments/$($payment.payment_id)" -Headers $headers
```

In Postman, use the same URLs and headers; select `Body` -> `raw` -> `JSON` for POST.
Swagger UI and `/openapi.json` are public. In Swagger, click **Authorize**, enter
your API key, and use **Try it out** to send requests.

## How it works

- **Outbox.** `POST /api/v1/payments` stores the payment and its `payments.new` event in
  one transaction; the `outbox` service publishes pending events to RabbitMQ.
- **Consumer.** Emulates the gateway (2-5 s, ~90% `succeeded`, ~10% declined as `failed`),
  saves the result, then sends the webhook. Processing of one payment is serialized with
  a PostgreSQL lock, so duplicate messages are safe.
- **Retries.** Gateway errors and webhook failures each get 3 attempts with 2 s and 4 s
  delays; retry events are scheduled through the outbox.
- **DLQ.** Every dead letter takes one route: the consumer rejects the message and RabbitMQ
  moves it through `payments.dlx` to `payments.dlq`, adding an `x-death` header. That covers
  exhausted retries, invalid messages and unexpected errors after 3 deliveries. The cause is
  in the payment's `last_error` column.
- **Failure reasons.** `failed` payments carry `failure_reason`: `declined` (gateway said no),
  `gateway_unavailable` (3 gateway attempts failed) or `processing_error` (unexpected errors
  stopped processing). The client gets a webhook in all three cases.
- **Emulated decline.** The ~10% is derived from `payment_id` rather than drawn at random, so
  a retried or redelivered payment always gets the same answer, like a real idempotent gateway.
- **Webhook targets.** `webhook_url` may point at internal hosts, and the demo depends on that.
  A production deployment should restrict targets to prevent server-side request forgery.
- **Signed webhooks.** `X-Webhook-Signature` is `sha256=` plus an HMAC-SHA256 of
  `timestamp + "." + body` with `WEBHOOK_SIGNING_SECRET`; the timestamp is sent in
  `X-Webhook-Timestamp`. Receivers should deduplicate by `event_id`.

To see gateway retries, set `GATEWAY_ERROR_MODE=transient` (first two attempts fail) or
`unavailable` (all attempts fail) in `.env` and run `docker compose up -d consumer`.

## Glossary

| Term | Meaning |
| --- | --- |
| Status | What happened to the payment: `pending`, `succeeded` or `failed`. Clients see this. |
| Workflow status | Whether the service still has work to do: `active`, `completed` or `dead_lettered`. Internal. |
| Stage | One step of processing, `gateway` or `webhook`. Each has its own 3 attempts. |
| Attempt | One try at a stage, counted in the database and spaced by backoff. |
| Delivery | One hand-over of a message by RabbitMQ, counted by the broker. |
| Dead letter | A message that will not be retried, kept in `payments.dlq` for inspection. |
| Abandon | Closing a payment after unexpected errors, just before its message is dead-lettered. |

## Demo webhooks and DLQ

| URL | Behavior |
| --- | --- |
| `http://webhook-demo:8080/success` | Always returns `200`. |
| `http://webhook-demo:8080/flaky` | Returns `503` twice per event, then `200`. |
| `http://webhook-demo:8080/fail` | Always returns `503`; the event ends up in the DLQ. |

Received notifications: http://localhost:8081/events. Dead letters: RabbitMQ Management
(http://localhost:15673, `payments` / `payments`) → **Queues and Streams** → `payments.dlq`
→ **Get messages**.

## Tests

Run the entire suite in a one-off container without local Python or Poetry:

```shell
docker compose run --build --rm tests
```

The command starts PostgreSQL and RabbitMQ, installs the locked test dependencies
in a separate image, and runs both unit and integration tests. To run only unit
tests in the container:

```shell
docker compose run --rm tests python -B -m pytest tests/unit
```

For local Python execution, install dependencies as described above.
Run unit tests:

```shell
poetry run python -B -m pytest tests/unit
```

Integration tests require PostgreSQL and RabbitMQ:

```shell
docker compose up -d --wait postgres rabbitmq
```

Set the test connection URLs in your terminal. Test fixtures do not load `.env`
automatically. Adjust the ports if you changed them in `.env`.

PowerShell:

```powershell
$env:TEST_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:55432/payments"
$env:TEST_RABBITMQ_URL = "amqp://payments:payments@localhost:5673/"
poetry run python -B -m pytest tests/integration
```

Bash:

```bash
export TEST_DATABASE_URL="postgresql+asyncpg://payments:payments@localhost:55432/payments"
export TEST_RABBITMQ_URL="amqp://payments:payments@localhost:5673/"
poetry run python -B -m pytest tests/integration
```

Tests use temporary PostgreSQL schemas and uniquely named RabbitMQ queues.
Tests requiring an unset connection URL are skipped.

To run the entire suite with these variables set:

```shell
poetry run python -B -m pytest
```
