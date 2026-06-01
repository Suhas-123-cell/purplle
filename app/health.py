from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import func, select, and_, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from .analytics import get_reference_now
    from .database import EventRow
    from .models import CameraStatus, HealthResponse
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from analytics import get_reference_now
    from database import EventRow
    from models import CameraStatus, HealthResponse


_STALE_THRESHOLD_SECONDS = 600  # 10 minutes


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def compute_health(session: AsyncSession, store_id: str) -> HealthResponse:
    # Use the latest event timestamp as the reference so that historical/replay
    # data is not treated as stale relative to the wall-clock system time.
    now = await get_reference_now(session, store_id)

    try:
        await session.execute(text("SELECT 1"))
        db_status = "ok"
    except SQLAlchemyError:
        db_status = "unavailable"

    last_event_ts_result = await session.scalar(
        select(func.max(EventRow.timestamp)).where(
            EventRow.store_id == store_id
        )
    )

    camera_ts_result = await session.execute(
        select(
            EventRow.camera_id,
            func.max(EventRow.timestamp).label("last_ts"),
        )
        .where(EventRow.store_id == store_id)
        .group_by(EventRow.camera_id)
    )

    camera_statuses: List[CameraStatus] = []
    for camera_id, last_ts in camera_ts_result.fetchall():
        if last_ts is None:
            camera_statuses.append(
                CameraStatus(
                    camera_id=camera_id,
                    last_event_ts=None,
                    is_stale=True,
                    lag_seconds=None,
                )
            )
        else:
            lag = (now - last_ts).total_seconds()
            camera_statuses.append(
                CameraStatus(
                    camera_id=camera_id,
                    last_event_ts=last_ts,
                    is_stale=lag > _STALE_THRESHOLD_SECONDS,
                    lag_seconds=round(lag, 1),
                )
            )

    overall_status = "ok"
    if db_status != "ok":
        overall_status = "degraded"
    elif any(c.is_stale for c in camera_statuses):
        overall_status = "warn"

    return HealthResponse(
        status=overall_status,
        db_status=db_status,
        store_id=store_id,
        last_event_ts=last_event_ts_result,
        camera_statuses=camera_statuses,
        checked_at=datetime.utcnow(),
    )
