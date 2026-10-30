from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from payment_service.domain import MAX_ATTEMPTS, Stage


class PaymentProcessEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1] = 1
    event_type: Literal["payment.process"] = "payment.process"
    event_id: UUID
    payment_id: UUID
    stage: Stage = "gateway"
    attempt: int = Field(default=1, ge=1, le=MAX_ATTEMPTS)
