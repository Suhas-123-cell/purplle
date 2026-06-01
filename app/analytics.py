from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

from sqlalchemy import and_, distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from .database import EventRow, POSTransaction
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from database import EventRow, POSTransaction


MAX_DWELL_MS = 3_600_000

VISITOR_PRESENCE_EVENTS = (
    "ENTRY",
    "GROUP_ENTRY",
    "ZONE_ENTER",
    "ZONE_DWELL",
    "BILLING_QUEUE_JOIN",
)


async def get_reference_now(session: AsyncSession, store_id: str) -> datetime:
    ts = await session.scalar(
        select(func.max(EventRow.timestamp)).where(EventRow.store_id == store_id)
    )
    return ts or datetime.utcnow()


async def get_presence_visitors(session: AsyncSession, store_id: str) -> set[str]:
    rows = await session.execute(
        select(distinct(EventRow.visitor_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type.in_(VISITOR_PRESENCE_EVENTS),
                EventRow.is_staff.is_(False),
            )
        )
    )
    return {row[0] for row in rows.fetchall()}


async def get_zone_visitors(
    session: AsyncSession, store_id: str, base_visitors: Iterable[str] | None = None
) -> set[str]:
    visitor_set = set(base_visitors) if base_visitors is not None else None
    rows = await session.execute(
        select(distinct(EventRow.visitor_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type.in_(["ZONE_ENTER", "ZONE_DWELL"]),
                EventRow.is_staff.is_(False),
                EventRow.visitor_id.in_(visitor_set) if visitor_set is not None else True,
            )
        )
    )
    return {row[0] for row in rows.fetchall()}


async def get_billing_queue_visitors(
    session: AsyncSession, store_id: str, base_visitors: Iterable[str] | None = None
) -> set[str]:
    visitor_set = set(base_visitors) if base_visitors is not None else None
    rows = await session.execute(
        select(distinct(EventRow.visitor_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "BILLING_QUEUE_JOIN",
                EventRow.is_staff.is_(False),
                EventRow.visitor_id.in_(visitor_set) if visitor_set is not None else True,
            )
        )
    )
    return {row[0] for row in rows.fetchall()}


async def get_purchase_visitors(
    session: AsyncSession,
    store_id: str,
    queue_visitors: Iterable[str] | None = None,
    reference_now: datetime | None = None,
) -> set[str]:
    allowed_queue_visitors = set(queue_visitors) if queue_visitors is not None else None

    pos_rows = await session.execute(
        select(POSTransaction.transaction_ts).where(
            and_(
                POSTransaction.store_id == store_id,
                POSTransaction.transaction_ts.isnot(None),
            )
        )
    )
    pos_timestamps = [row[0] for row in pos_rows.fetchall() if row[0] is not None]
    if not pos_timestamps:
        return set()

    converters: set[str] = set()
    for txn_ts in pos_timestamps:
        rows = await session.execute(
            select(distinct(EventRow.visitor_id)).where(
                and_(
                    EventRow.store_id == store_id,
                    EventRow.event_type == "BILLING_QUEUE_JOIN",
                    EventRow.is_staff.is_(False),
                    EventRow.timestamp >= txn_ts - timedelta(minutes=5),
                    EventRow.timestamp <= txn_ts,
                    EventRow.visitor_id.in_(allowed_queue_visitors)
                    if allowed_queue_visitors is not None
                    else True,
                )
            )
        )
        converters.update(row[0] for row in rows.fetchall())

    if converters:
        return converters

    data_now = reference_now or await get_reference_now(session, store_id)
    pos_day_count = await session.scalar(
        select(func.count(distinct(POSTransaction.order_id))).where(
            and_(
                POSTransaction.store_id == store_id,
                func.date(POSTransaction.transaction_ts) == str(data_now.date()),
            )
        )
    ) or 0
    if pos_day_count == 0:
        return set()

    if allowed_queue_visitors is None:
        allowed_queue_visitors = await get_billing_queue_visitors(session, store_id)

    # Replay datasets can have CCTV timestamps compressed into a short clip while
    # POS keeps store-day wall clock times. In that case, keep conversion
    # session-based by capping same-day POS orders to observed billing visitors.
    return set(sorted(allowed_queue_visitors)[:pos_day_count])


async def get_current_queue_visitors(session: AsyncSession, store_id: str) -> set[str]:
    subq = (
        select(
            EventRow.visitor_id,
            func.max(EventRow.timestamp).label("last_ts"),
        )
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.is_staff.is_(False),
                EventRow.event_type.in_(
                    ["BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON", "EXIT", "ZONE_EXIT"]
                ),
                ~(
                    (EventRow.event_type == "ZONE_EXIT")
                    & (EventRow.zone_id != "BILLING")
                ),
            )
        )
        .group_by(EventRow.visitor_id)
        .subquery()
    )

    rows = await session.execute(
        select(EventRow.visitor_id, EventRow.event_type).join(
            subq,
            and_(
                EventRow.visitor_id == subq.c.visitor_id,
                EventRow.timestamp == subq.c.last_ts,
                EventRow.store_id == store_id,
            ),
        )
    )
    in_queue = {
        visitor_id
        for visitor_id, event_type in rows.fetchall()
        if event_type == "BILLING_QUEUE_JOIN"
    }
    converted = await get_purchase_visitors(
        session,
        store_id,
        queue_visitors=in_queue,
        reference_now=await get_reference_now(session, store_id),
    )
    return in_queue - converted
