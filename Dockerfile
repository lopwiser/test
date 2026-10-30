FROM python:3.11-slim AS builder

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_IN_PROJECT=true

RUN pip install --no-cache-dir poetry==2.5.1
COPY pyproject.toml poetry.lock README.md ./
RUN poetry install --only main --no-root --no-ansi
COPY src ./src
RUN poetry install --only-root --no-ansi

FROM builder AS test

ENV PATH="/app/.venv/bin:$PATH"
RUN poetry install --with dev --no-root --no-ansi
COPY tests ./tests
COPY alembic ./alembic
COPY alembic.ini ./
CMD ["python", "-B", "-m", "pytest"]

FROM python:3.11-slim AS runtime

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

RUN useradd --create-home app
COPY --from=builder /app/.venv /app/.venv
COPY src ./src
COPY alembic ./alembic
COPY alembic.ini ./
USER app

EXPOSE 8000
CMD ["uvicorn", "payment_service.api.app:app", "--host", "0.0.0.0", "--port", "8000"]

