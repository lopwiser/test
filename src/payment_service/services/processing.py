import logging
from uuid import UUID, uuid4

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from payment_service.config import WorkerSettings
from payment_service.db.models import Payment
from payment_service.db.session import Database
from payment_service.domain import (
    MAX_ATTEMPTS,
    NO_GATEWAY_RESULT,
    FailureReason,
    PaymentStatus,
    Stage,
    WorkflowStatus,
)
from payment_service.errors import GatewayError, ProcessingOwnershipLost, describe
from payment_service.messaging.outbox_records import processing_record
from payment_service.messaging.schemas import PaymentProcessEvent
from payment_service.services.gateway import PaymentGateway
from payment_service.services.retry import retry_delay
from payment_service.services.webhooks import WebhookSender

logger = logging.getLogger(__name__)

ATTEMPT_FIELDS: dict[Stage, str] = {"gateway": "current_attempt", "webhook": "webhook_attempt"}


class PaymentProcessor:
    def __init__(
        self,
        database: Database,
        gateway: PaymentGateway,
        webhooks: WebhookSender,
        settings: WorkerSettings,
    ) -> None:
        self.database = database
        self.gateway = gateway
        self.webhooks = webhooks
        self.settings = settings

    async def process(self, event: PaymentProcessEvent) -> bool:
        async with self.database.payment_lock(event.payment_id):
            return await self.process_locked(event)

    async def process_locked(self, event: PaymentProcessEvent) -> bool:
        payment = await self.claim(event.payment_id)
        if payment is None:
            logger.error("Payment missing payment_id=%s", event.payment_id)
            return False
        result = self.result_without_work(event, payment)
        if result is not None:
            return result
        if event.stage == "gateway":
            return await self.run_gateway(event, payment)
        return await self.send_webhook(payment)

    async def claim(self, payment_id: UUID) -> Payment | None:
        async with self.database.sessions.begin() as session:
            return await session.scalar(
                update(Payment)
                .where(Payment.id == payment_id)
                .values(processing_token=uuid4())
                .returning(Payment)
            )

    @staticmethod
    async def owned(session: AsyncSession, payment: Payment) -> Payment:
        saved = await session.scalar(
            select(Payment)
            .where(Payment.id == payment.id, Payment.processing_token == payment.processing_token)
            .with_for_update()
        )
        if saved is None:
            raise ProcessingOwnershipLost(f"payment_id={payment.id}")
        return saved

    @staticmethod
    def stage_gave_up(event: PaymentProcessEvent, payment: Payment) -> bool:
        if event.stage == "gateway" and payment.failure_reason in NO_GATEWAY_RESULT:
            return True
        sent_this_webhook = event.stage == "webhook" or payment.webhook_attempt == 1
        return sent_this_webhook and payment.workflow_status == WorkflowStatus.DEAD_LETTERED

    @classmethod
    def result_without_work(cls, event: PaymentProcessEvent, payment: Payment) -> bool | None:
        """Outcome for stale, duplicate or out-of-order events; None when the event needs work.

        The final attempt of a stage that gave up returns False again when a crash made the broker
        redeliver it, so it still reaches the dead letter queue.
        """
        expected = getattr(payment, ATTEMPT_FIELDS[event.stage])
        if event.attempt < expected:
            logger.debug("Ignored duplicate event_id=%s payment_id=%s", event.event_id, payment.id)
            return True
        if event.attempt > expected:
            logger.error("Unexpected attempt event_id=%s payment_id=%s", event.event_id, payment.id)
            return False
        if cls.stage_gave_up(event, payment):
            return False
        if payment.workflow_status != WorkflowStatus.ACTIVE:
            return True
        if event.stage == "webhook" and payment.status == PaymentStatus.PENDING:
            return False
        return None

    async def run_gateway(self, event: PaymentProcessEvent, payment: Payment) -> bool:
        if payment.status == PaymentStatus.PENDING:
            logger.debug(
                "Calling gateway payment_id=%s gateway_attempt=%s", payment.id, event.attempt
            )
            try:
                result = await self.gateway.process(payment.id, attempt=event.attempt)
            except (GatewayError, httpx.TransportError, OSError) as exc:
                return await self.attempt_failed("gateway", payment, exc)
            await self.save_gateway_result(payment, result)
        if payment.webhook_attempt != 1:
            return True
        return await self.send_webhook(payment)

    async def save_gateway_result(self, payment: Payment, result: PaymentStatus) -> None:
        async with self.database.sessions.begin() as session:
            saved = await self.owned(session, payment)
            saved.status = result.value
            saved.failure_reason = (
                FailureReason.DECLINED.value if result == PaymentStatus.FAILED else None
            )
            saved.processed_at = await session.scalar(select(func.clock_timestamp()))
            saved.last_error = None
            payment.status = saved.status
            payment.processed_at = saved.processed_at
            payment.failure_reason = saved.failure_reason

    async def send_webhook(self, payment: Payment) -> bool:
        logger.debug(
            "Sending webhook payment_id=%s webhook_attempt=%s", payment.id, payment.webhook_attempt
        )
        try:
            await self.webhooks.send(payment)
        except (httpx.HTTPError, OSError) as exc:
            return await self.attempt_failed("webhook", payment, exc)
        async with self.database.sessions.begin() as session:
            saved = await self.owned(session, payment)
            saved.workflow_status = WorkflowStatus.COMPLETED.value
            saved.webhook_delivered_at = await session.scalar(select(func.clock_timestamp()))
            saved.last_error = None
        logger.info("Completed payment_id=%s status=%s", payment.id, payment.status)
        return True

    async def attempt_failed(self, stage: Stage, payment: Payment, error: Exception) -> bool:
        attempt = getattr(payment, ATTEMPT_FIELDS[stage])
        retrying = attempt < MAX_ATTEMPTS
        async with self.database.sessions.begin() as session:
            saved = await self.owned(session, payment)
            saved.last_error = describe(error)
            if retrying:
                setattr(saved, ATTEMPT_FIELDS[stage], attempt + 1)
                session.add(
                    processing_record(
                        saved.id,
                        stage=stage,
                        attempt=attempt + 1,
                        delay=self.processing_delay(attempt),
                    )
                )
            elif stage == "gateway":
                await self.fail_without_result(session, saved, FailureReason.GATEWAY_UNAVAILABLE)
            else:
                saved.workflow_status = WorkflowStatus.DEAD_LETTERED.value
        logger.warning(
            "Attempt failed stage=%s payment_id=%s attempt=%s error=%s",
            stage,
            payment.id,
            attempt,
            describe(error),
        )
        return retrying

    async def abandon(self, event: PaymentProcessEvent, error: Exception) -> None:
        """Close a payment whose message is going to the dead letter queue after unexpected errors.

        Runs under the payment lock so it cannot race another delivery of the same payment.
        """
        async with self.database.payment_lock(event.payment_id):
            payment = await self.claim(event.payment_id)
            if payment is None or self.result_without_work(event, payment) is not None:
                return
            async with self.database.sessions.begin() as session:
                saved = await self.owned(session, payment)
                saved.last_error = describe(error)
                if saved.status == PaymentStatus.PENDING:
                    await self.fail_without_result(session, saved, FailureReason.PROCESSING_ERROR)
                else:
                    saved.workflow_status = WorkflowStatus.DEAD_LETTERED.value
        logger.error("Abandoned payment_id=%s error=%s", event.payment_id, describe(error))

    @staticmethod
    async def fail_without_result(
        session: AsyncSession, saved: Payment, reason: FailureReason
    ) -> None:
        saved.status = PaymentStatus.FAILED.value
        saved.processed_at = await session.scalar(select(func.clock_timestamp()))
        saved.failure_reason = reason.value
        session.add(processing_record(saved.id, stage="webhook"))

    def processing_delay(self, attempt: int) -> float:
        return retry_delay(
            attempt,
            self.settings.processing_retry_base_delay,
            self.settings.processing_retry_max_delay,
        )
