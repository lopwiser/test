from functools import lru_cache
from typing import Annotated, Literal

from pydantic import AfterValidator, AmqpDsn, Field, PostgresDsn, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def require_non_blank(value: SecretStr) -> SecretStr:
    if not value.get_secret_value().strip():
        raise ValueError("must not be blank")
    return value


NonBlankSecret = Annotated[SecretStr, AfterValidator(require_non_blank)]


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: PostgresDsn
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"

    @field_validator("database_url")
    @classmethod
    def require_asyncpg(cls, value: PostgresDsn) -> PostgresDsn:
        if value.scheme != "postgresql+asyncpg":
            raise ValueError("DATABASE_URL must use postgresql+asyncpg")
        return value


class Settings(DatabaseSettings):
    api_key: NonBlankSecret


@lru_cache
def get_settings() -> Settings:
    return Settings()


class WorkerSettings(DatabaseSettings):
    rabbitmq_url: AmqpDsn = AmqpDsn("amqp://payments:payments@localhost:5673/")
    outbox_poll_interval: float = Field(default=0.5, gt=0, le=60)
    outbox_publish_timeout: float = Field(default=5, gt=0, le=60)
    outbox_retry_base_delay: float = Field(default=1, ge=0.01, le=3600)
    outbox_retry_max_delay: float = Field(default=60, ge=0.01, le=3600)
    processing_retry_base_delay: float = Field(default=2, gt=0, le=60)
    processing_retry_max_delay: float = Field(default=30, gt=0, le=300)
    webhook_timeout: float = Field(default=5, gt=0, le=60)

    infrastructure_retry_base_delay: float = Field(default=1, gt=0, le=60)
    infrastructure_retry_max_delay: float = Field(default=30, gt=0, le=300)
    outbox_retention_days: int = Field(default=7, ge=1, le=365)
    gateway_error_mode: Literal["none", "transient", "unavailable"] = "none"


class ConsumerSettings(WorkerSettings):
    webhook_signing_secret: NonBlankSecret
