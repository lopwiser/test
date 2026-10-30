from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Literal

from pydantic import JsonValue

MAX_ATTEMPTS = 3

Stage = Literal["gateway", "webhook"]


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class FailureReason(StrEnum):
    DECLINED = "declined"
    GATEWAY_UNAVAILABLE = "gateway_unavailable"
    PROCESSING_ERROR = "processing_error"


NO_GATEWAY_RESULT = {FailureReason.GATEWAY_UNAVAILABLE, FailureReason.PROCESSING_ERROR}


class WorkflowStatus(StrEnum):
    ACTIVE = "active"
    COMPLETED = "completed"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True)
class NewPayment:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue]
    webhook_url: str
