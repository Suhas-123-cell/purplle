from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from sqlalchemy import func, select, and_, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from .database import EventRow, POSTransaction, VisitorSession
from .models import FunnelData, FunnelStage


def _today_range() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, now


async def compute_funnel(session: AsyncSession, store_id: str) -> FunnelData:
    start, end = _today_range()

    # Stage 1: Unique visitors who entered today (ENTRY only, no REENTRY to avoid double-count)
    entry_visitors_result = await session.execute(
        select(distinct(EventRow.visitor_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "ENTRY",
                EventRow.is_staff.is_(False),
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
            )
        )
    )
    entry_visitors: set[str] = {row[0] for row in entry_visitors_result.fetchall()}
    total_entries = len(entry_visitors)

    # Stage 2: Visitors who visited at least one zone
    zone_visitors_result = await session.execute(
        select(distinct(EventRow.visitor_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type.in_(["ZONE_ENTER", "ZONE_DWELL"]),
                EventRow.visitor_id.in_(entry_visitors) if entry_visitors else False,
                EventRow.is_staff.is_(False),
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
            )
        )
    )
    zone_visitors: set[str] = {row[0] for row in zone_visitors_result.fetchall()}
    total_zone = len(zone_visitors) if entry_visitors else 0

    # Stage 3: Visitors who joined billing queue
    queue_visitors_result = await session.execute(
        select(distinct(EventRow.visitor_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "BILLING_QUEUE_JOIN",
                EventRow.visitor_id.in_(entry_visitors) if entry_visitors else False,
                EventRow.is_staff.is_(False),
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
            )
        )
    )
    queue_visitors: set[str] = {row[0] for row in queue_visitors_result.fetchall()}
    total_queue = len(queue_visitors) if entry_visitors else 0

    # Stage 4: Visitors who completed purchase (present in POS for the same day)
    pos_result = await session.execute(
        select(distinct(POSTransaction.customer_number)).where(
            and_(
                POSTransaction.store_id == store_id,
                POSTransaction.transaction_ts >= start,
                POSTransaction.transaction_ts <= end,
            )
        )
    )
    pos_customer_numbers: set[str] = {
        row[0] for row in pos_result.fetchall() if row[0]
    }

    # Match via visitor sessions: check if visitor_id overlaps with customer_number
    # (direct match when camera system provides visitor_id == customer_number or phone)
    purchase_visitors_result = await session.execute(
        select(distinct(VisitorSession.visitor_id)).where(
            and_(
                VisitorSession.store_id == store_id,
                VisitorSession.entered_billing_queue.is_(True),
                VisitorSession.completed_purchase.is_(True),
                VisitorSession.session_start >= start,
                VisitorSession.session_start <= end,
            )
        )
    )
    direct_purchase_visitors = {row[0] for row in purchase_visitors_result.fetchall()}

    # Fallback: count distinct POS transactions as purchase count when visitor linkage unavailable
    pos_count = await session.scalar(
        select(func.count(func.distinct(POSTransaction.order_id))).where(
            and_(
                POSTransaction.store_id == store_id,
                POSTransaction.transaction_ts >= start,
                POSTransaction.transaction_ts <= end,
            )
        )
    ) or 0

    total_purchase = len(direct_purchase_visitors) if direct_purchase_visitors else min(
        pos_count, total_queue
    )

    def _drop_off(current: int, previous: int) -> float:
        if previous == 0:
            return 0.0
        return round((1 - current / previous) * 100, 2)

    stages: List[FunnelStage] = [
        FunnelStage(stage="Entry", count=total_entries, drop_off_pct=0.0),
        FunnelStage(
            stage="Zone Visit",
            count=total_zone,
            drop_off_pct=_drop_off(total_zone, total_entries),
        ),
        FunnelStage(
            stage="Billing Queue",
            count=total_queue,
            drop_off_pct=_drop_off(total_queue, total_zone),
        ),
        FunnelStage(
            stage="Purchase",
            count=total_purchase,
            drop_off_pct=_drop_off(total_purchase, total_queue),
        ),
    ]

    return FunnelData(
        store_id=store_id,
        as_of=datetime.utcnow(),
        date=start.strftime("%Y-%m-%d"),
        stages=stages,
    )
