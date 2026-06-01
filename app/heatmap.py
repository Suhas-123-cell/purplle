from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from sqlalchemy import func, select, and_, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from .database import EventRow
from .models import HeatmapCell, HeatmapData

_LOW_CONFIDENCE_THRESHOLD = 20


def _today_range() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, now


def _normalize(value: float, min_val: float, max_val: float) -> float:
    if max_val == min_val:
        return 50.0
    return round((value - min_val) / (max_val - min_val) * 100, 2)


async def compute_heatmap(session: AsyncSession, store_id: str) -> HeatmapData:
    start, end = _today_range()

    rows = await session.execute(
        select(
            EventRow.zone_id,
            func.count(EventRow.event_id).label("visit_count"),
            func.avg(EventRow.dwell_ms).label("avg_dwell"),
            func.count(distinct(EventRow.visitor_id)).label("unique_visitors"),
        )
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.zone_id.isnot(None),
                EventRow.event_type.in_(["ZONE_ENTER", "ZONE_DWELL"]),
                EventRow.timestamp >= start,
                EventRow.timestamp <= end,
            )
        )
        .group_by(EventRow.zone_id)
    )
    data = rows.fetchall()

    if not data:
        return HeatmapData(
            store_id=store_id,
            as_of=datetime.utcnow(),
            date=start.strftime("%Y-%m-%d"),
            cells=[],
        )

    visit_counts = [float(r.visit_count) for r in data]
    min_visits = min(visit_counts)
    max_visits = max(visit_counts)

    cells: List[HeatmapCell] = []
    for row in data:
        intensity = _normalize(float(row.visit_count), min_visits, max_visits)
        cells.append(
            HeatmapCell(
                zone_id=row.zone_id,
                visit_count=row.visit_count,
                avg_dwell_ms=round(row.avg_dwell or 0.0, 2),
                intensity=intensity,
                data_confidence=row.unique_visitors >= _LOW_CONFIDENCE_THRESHOLD,
            )
        )

    cells.sort(key=lambda c: c.intensity, reverse=True)

    return HeatmapData(
        store_id=store_id,
        as_of=datetime.utcnow(),
        date=start.strftime("%Y-%m-%d"),
        cells=cells,
    )
