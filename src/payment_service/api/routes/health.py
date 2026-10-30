from fastapi import APIRouter
from sqlalchemy import text

from payment_service.api.dependencies import Session

router = APIRouter(tags=["operations"])


@router.get("/health")
async def health(session: Session) -> dict[str, str]:
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}
