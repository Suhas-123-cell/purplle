from __future__ import annotations

from datetime import datetime

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from .analytics import (
        MAX_DWELL_MS,
        get_billing_queue_visitors,
        get_current_queue_visitors,
        get_presence_visitors,
        get_purchase_visitors,
        get_reference_now,
    )
    from .database import EventRow
    from .models import StoreMetrics, ZoneDwellStat
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from analytics import (
        MAX_DWELL_MS,
        get_billing_queue_visitors,
        get_current_queue_visitors,
        get_presence_visitors,
        get_purchase_visitors,
        get_reference_now,
    )
    from database import EventRow
    from models import StoreMetrics, ZoneDwellStat


async def get_unique_visitors(session: AsyncSession, store_id: str) -> int:
    return len(await get_presence_visitors(session, store_id))


async def get_conversion_rate(
    session: AsyncSession,
    store_id: str,
    unique_visitors: int,
    reference_now: datetime | None = None,
) -> float:
    if unique_visitors == 0:
        return 0.0

    if reference_now is None:
        reference_now = await get_reference_now(session, store_id)

    queue_visitors = await get_billing_queue_visitors(session, store_id)
    converters = await get_purchase_visitors(
        session,
        store_id,
        queue_visitors=queue_visitors,
        reference_now=reference_now,
    )
    return min(len(converters) / unique_visitors, 1.0)


async def get_avg_dwell_per_zone(
    session: AsyncSession, store_id: str
) -> list[ZoneDwellStat]:
    rows = await session.execute(
        select(
            EventRow.zone_id,
            func.avg(EventRow.dwell_ms).label("avg_dwell"),
            func.count(EventRow.event_id).label("visit_count"),
        )
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "ZONE_DWELL",
                EventRow.zone_id.isnot(None),
                EventRow.dwell_ms <= MAX_DWELL_MS,
            )
        )
        .group_by(EventRow.zone_id)
    )

    return [
        ZoneDwellStat(
            zone_id=row.zone_id,
            avg_dwell_ms=round(row.avg_dwell or 0.0, 2),
            visit_count=row.visit_count,
        )
        for row in rows.fetchall()
    ]


async def get_current_queue_depth(session: AsyncSession, store_id: str) -> int:
    return len(await get_current_queue_visitors(session, store_id))


async def get_abandonment_rate(session: AsyncSession, store_id: str) -> float:
    joins = await session.scalar(
        select(func.count(EventRow.event_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "BILLING_QUEUE_JOIN",
            )
        )
    )
    if not joins:
        return 0.0

    abandons = await session.scalar(
        select(func.count(EventRow.event_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "BILLING_QUEUE_ABANDON",
            )
        )
    )

    return min((abandons or 0) / joins, 1.0)


async def compute_store_metrics(session: AsyncSession, store_id: str) -> StoreMetrics:
    reference_now = await get_reference_now(session, store_id)
    unique_visitors = await get_unique_visitors(session, store_id)
    conversion_rate = await get_conversion_rate(
        session, store_id, unique_visitors, reference_now=reference_now
    )
    avg_dwell = await get_avg_dwell_per_zone(session, store_id)
    queue_depth = await get_current_queue_depth(session, store_id)
    abandonment_rate = await get_abandonment_rate(session, store_id)

    return StoreMetrics(
        store_id=store_id,
        as_of=reference_now,
        unique_visitors=unique_visitors,
        conversion_rate=round(conversion_rate, 4),
        avg_dwell_per_zone=avg_dwell,
        queue_depth=queue_depth,
        abandonment_rate=round(abandonment_rate, 4),
    )
