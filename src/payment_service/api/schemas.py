from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import UUID

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    FiniteFloat,
    JsonValue,
    UrlConstraints,
    field_validator,
)
from typing_extensions import TypeAliasType

from payment_service.domain import Currency, FailureReason, NewPayment, PaymentStatus

Money = Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
WebhookUrl = Annotated[AnyHttpUrl, UrlConstraints(max_length=2048)]
JsonString = Annotated[str, Field(pattern=r"^[^\x00]*$")]
MetadataValue = TypeAliasType(
    "MetadataValue",
    dict[JsonString, "MetadataValue"]
    | list["MetadataValue"]
    | JsonString
    | bool
    | int
    | FiniteFloat
    | None,
)


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Money
    currency: Currency
    description: JsonString = Field(max_length=1000)
    metadata: dict[JsonString, MetadataValue] = Field(default_factory=dict)
    webhook_url: WebhookUrl

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))

    def to_new_payment(self) -> NewPayment:
        return NewPayment(
            amount=self.amount,
            currency=self.currency,
            description=self.description,
            metadata=self.metadata,
            webhook_url=str(self.webhook_url),
        )


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class ErrorResponse(BaseModel):
    detail: str


class PaymentDetail(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    payment_id: UUID = Field(validation_alias="id")
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, JsonValue] = Field(validation_alias="payment_metadata")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
    failure_reason: FailureReason | None
    webhook_delivered_at: datetime | None
