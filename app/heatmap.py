from __future__ import annotations

from typing import List

from sqlalchemy import and_, distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from .analytics import MAX_DWELL_MS, get_reference_now
    from .database import EventRow
    from .models import HeatmapCell, HeatmapData
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from analytics import MAX_DWELL_MS, get_reference_now
    from database import EventRow
    from models import HeatmapCell, HeatmapData

_LOW_CONFIDENCE_THRESHOLD = 20


def _normalize(value: float, min_val: float, max_val: float) -> float:
    if max_val == min_val:
        return 50.0
    return round((value - min_val) / (max_val - min_val) * 100, 2)


async def compute_heatmap(session: AsyncSession, store_id: str) -> HeatmapData:
    reference_now = await get_reference_now(session, store_id)
    # Query 1: visit_count and unique_visitors from ZONE_ENTER + ZONE_DWELL
    # (captures all traffic frequency, including entries with dwell_ms == 0)
    traffic_rows = await session.execute(
        select(
            EventRow.zone_id,
            func.count(EventRow.event_id).label("visit_count"),
            func.count(distinct(EventRow.visitor_id)).label("unique_visitors"),
        )
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.zone_id.isnot(None),
                EventRow.event_type.in_(["ZONE_ENTER", "ZONE_DWELL"]),
            )
        )
        .group_by(EventRow.zone_id)
    )
    traffic_data = {r.zone_id: r for r in traffic_rows.fetchall()}

    # Query 2: avg_dwell_ms from ZONE_DWELL events ONLY
    # (ZONE_ENTER events have dwell_ms == 0 and would dilute the average)
    # Rows with dwell_ms > MAX_DWELL_MS are corrupted (time.time() bug) and
    # are excluded to prevent inflated averages.
    dwell_rows = await session.execute(
        select(
            EventRow.zone_id,
            func.avg(EventRow.dwell_ms).label("avg_dwell"),
        )
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.zone_id.isnot(None),
                EventRow.event_type == "ZONE_DWELL",
                EventRow.dwell_ms <= MAX_DWELL_MS,
            )
        )
        .group_by(EventRow.zone_id)
    )
    dwell_data = {r.zone_id: r.avg_dwell for r in dwell_rows.fetchall()}

    if not traffic_data:
        return HeatmapData(
            store_id=store_id,
            as_of=reference_now,
            date=reference_now.strftime("%Y-%m-%d"),
            cells=[],
        )

    visit_counts = [float(r.visit_count) for r in traffic_data.values()]
    min_visits = min(visit_counts)
    max_visits = max(visit_counts)

    cells: List[HeatmapCell] = []
    for zone_id, row in traffic_data.items():
        intensity = _normalize(float(row.visit_count), min_visits, max_visits)
        avg_dwell = dwell_data.get(zone_id)
        cells.append(
            HeatmapCell(
                zone_id=zone_id,
                visit_count=row.visit_count,
                avg_dwell_ms=round(avg_dwell or 0.0, 2),
                intensity=intensity,
                data_confidence=row.unique_visitors >= _LOW_CONFIDENCE_THRESHOLD,
            )
        )

    cells.sort(key=lambda c: c.intensity, reverse=True)

    return HeatmapData(
        store_id=store_id,
        as_of=reference_now,
        date=reference_now.strftime("%Y-%m-%d"),
        cells=cells,
    )
