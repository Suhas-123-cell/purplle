from __future__ import annotations

from typing import List

from sqlalchemy.ext.asyncio import AsyncSession

try:
    from .analytics import (
        get_billing_queue_visitors,
        get_presence_visitors,
        get_purchase_visitors,
        get_reference_now,
        get_zone_visitors,
    )
    from .models import FunnelData, FunnelStage
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from analytics import (
        get_billing_queue_visitors,
        get_presence_visitors,
        get_purchase_visitors,
        get_reference_now,
        get_zone_visitors,
    )
    from models import FunnelData, FunnelStage


async def compute_funnel(session: AsyncSession, store_id: str) -> FunnelData:
    all_visitors = await get_presence_visitors(session, store_id)
    total_entries = len(all_visitors)

    zone_visitors = await get_zone_visitors(session, store_id, all_visitors)
    total_zone = len(zone_visitors) if all_visitors else 0

    queue_visitors = await get_billing_queue_visitors(session, store_id, all_visitors)
    total_queue = len(queue_visitors) if all_visitors else 0

    reference_now = await get_reference_now(session, store_id)
    purchase_visitors = await get_purchase_visitors(
        session,
        store_id,
        queue_visitors=queue_visitors,
        reference_now=reference_now,
    )
    total_purchase = len(purchase_visitors)

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
        as_of=reference_now,
        date=reference_now.strftime("%Y-%m-%d"),
        stages=stages,
    )
