from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import func, select, and_, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from .database import EventRow, POSTransaction, VisitorSession
from .models import StoreMetrics, ZoneDwellStat


def _today_range() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, now


async def get_unique_visitors(session: AsyncSession, store_id: str) -> int:
    start, end = _today_range()
    result = await session.scalar(
        select(func.count(distinct(EventRow.visitor_id))).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "ENTRY",
                EventRow.is_staff.is_(False),
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
            )
        )
    )
    return result or 0


async def get_conversion_rate(
    session: AsyncSession, store_id: str, unique_visitors: int
) -> float:
    if unique_visitors == 0:
        return 0.0

    start, end = _today_range()

    pos_rows = await session.execute(
        select(POSTransaction.transaction_ts).where(
            and_(
                POSTransaction.store_id == store_id,
                POSTransaction.transaction_ts >= start,
                POSTransaction.transaction_ts <= end,
            )
        )
    )
    pos_timestamps = [row[0] for row in pos_rows.fetchall() if row[0] is not None]

    if not pos_timestamps:
        return 0.0

    converters: set[str] = set()
    for txn_ts in pos_timestamps:
        window_start = txn_ts - timedelta(minutes=5)
        visitors_before_purchase = await session.execute(
            select(distinct(EventRow.visitor_id)).where(
                and_(
                    EventRow.store_id == store_id,
                    EventRow.event_type == "BILLING_QUEUE_JOIN",
                    EventRow.is_staff.is_(False),
                    EventRow.timestamp >= window_start,
                    EventRow.timestamp <= txn_ts,
                )
            )
        )
        for (vid,) in visitors_before_purchase.fetchall():
            converters.add(vid)

    return min(len(converters) / unique_visitors, 1.0)


async def get_avg_dwell_per_zone(
    session: AsyncSession, store_id: str
) -> List[ZoneDwellStat]:
    start, end = _today_range()

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
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
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
    subq = (
        select(
            EventRow.visitor_id,
            func.max(EventRow.timestamp).label("last_ts"),
        )
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type.in_(
                    ["BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON", "EXIT"]
                ),
            )
        )
        .group_by(EventRow.visitor_id)
        .subquery()
    )

    latest_events = await session.execute(
        select(EventRow.visitor_id, EventRow.event_type).join(
            subq,
            and_(
                EventRow.visitor_id == subq.c.visitor_id,
                EventRow.timestamp == subq.c.last_ts,
                EventRow.store_id == store_id,
            ),
        )
    )

    depth = sum(
        1
        for _, et in latest_events.fetchall()
        if et == "BILLING_QUEUE_JOIN"
    )
    return depth


async def get_abandonment_rate(session: AsyncSession, store_id: str) -> float:
    start, end = _today_range()

    joins = await session.scalar(
        select(func.count(EventRow.event_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "BILLING_QUEUE_JOIN",
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
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
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
            )
        )
    )

    return min((abandons or 0) / joins, 1.0)


async def compute_store_metrics(session: AsyncSession, store_id: str) -> StoreMetrics:
    unique_visitors = await get_unique_visitors(session, store_id)
    conversion_rate = await get_conversion_rate(session, store_id, unique_visitors)
    avg_dwell = await get_avg_dwell_per_zone(session, store_id)
    queue_depth = await get_current_queue_depth(session, store_id)
    abandonment_rate = await get_abandonment_rate(session, store_id)

    return StoreMetrics(
        store_id=store_id,
        as_of=datetime.utcnow(),
        unique_visitors=unique_visitors,
        conversion_rate=round(conversion_rate, 4),
        avg_dwell_per_zone=avg_dwell,
        queue_depth=queue_depth,
        abandonment_rate=round(abandonment_rate, 4),
    )
