import asyncio
from random import uniform
from uuid import UUID

from payment_service.domain import MAX_ATTEMPTS, PaymentStatus
from payment_service.errors import GatewayError


class PaymentGateway:
    def __init__(self, error_mode: str = "none") -> None:
        self.error_mode = error_mode

    async def process(self, payment_id: UUID, *, attempt: int = 1) -> PaymentStatus:
        await asyncio.sleep(uniform(2, 5))
        if self.error_mode == "unavailable" or (
            self.error_mode == "transient" and attempt < MAX_ATTEMPTS
        ):
            raise GatewayError("Simulated gateway connection failure")
        if payment_id.int % 10 == 0:
            return PaymentStatus.FAILED
        return PaymentStatus.SUCCEEDED
