from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List

from sqlalchemy import func, select, and_, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from .database import EventRow, POSTransaction
from .models import Anomaly, AnomalyData, AnomalyType, Severity


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _today_range() -> tuple[datetime, datetime]:
    now = _now()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, now


async def _check_billing_queue_spike(
    session: AsyncSession, store_id: str
) -> Anomaly | None:
    # Current queue depth
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
    current_depth = sum(
        1 for _, et in latest_events.fetchall() if et == "BILLING_QUEUE_JOIN"
    )

    # 7-day average queue depth — approximate via daily JOIN counts
    now = _now()
    seven_days_ago = now - timedelta(days=7)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    daily_counts = await session.execute(
        select(
            func.date(EventRow.timestamp).label("day"),
            func.count(EventRow.event_id).label("joins"),
        )
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "BILLING_QUEUE_JOIN",
                EventRow.timestamp >= seven_days_ago,
                EventRow.timestamp < today_start,
            )
        )
        .group_by(func.date(EventRow.timestamp))
    )
    rows = daily_counts.fetchall()
    avg_daily_joins = sum(r.joins for r in rows) / len(rows) if rows else 0

    # Normalise to an hourly proxy for fair comparison
    current_hour_joins = await session.scalar(
        select(func.count(EventRow.event_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.event_type == "BILLING_QUEUE_JOIN",
                EventRow.timestamp >= now - timedelta(hours=1),
                EventRow.timestamp <= now,
            )
        )
    ) or 0
    avg_hourly = avg_daily_joins / 12 if avg_daily_joins else 0  # ~12 active hours

    threshold = 2 * avg_hourly if avg_hourly > 0 else None

    if threshold is not None and current_hour_joins > threshold:
        severity = Severity.CRITICAL if current_hour_joins > 3 * avg_hourly else Severity.WARN
        return Anomaly(
            anomaly_type=AnomalyType.BILLING_QUEUE_SPIKE,
            severity=severity,
            description=(
                f"Current queue depth {current_depth} / hourly joins {current_hour_joins} "
                f"exceeds 2x 7-day hourly average ({avg_hourly:.1f})"
            ),
            suggested_action="Open additional billing counters or deploy staff to queue management.",
            detected_at=now,
            context={
                "current_depth": current_depth,
                "current_hour_joins": current_hour_joins,
                "avg_hourly_7d": round(avg_hourly, 2),
                "threshold": round(threshold, 2),
            },
        )
    return None


async def _check_conversion_drop(
    session: AsyncSession, store_id: str
) -> Anomaly | None:
    now = _now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    seven_days_ago = now - timedelta(days=7)

    async def _day_conversion(day_start: datetime, day_end: datetime) -> float:
        visitors = await session.scalar(
            select(func.count(distinct(EventRow.visitor_id))).where(
                and_(
                    EventRow.store_id == store_id,
                    EventRow.event_type == "ENTRY",
                    EventRow.is_staff.is_(False),
                    EventRow.timestamp >= day_start,
                    EventRow.timestamp <= day_end,
                )
            )
        ) or 0
        if visitors == 0:
            return 0.0
        purchases = await session.scalar(
            select(func.count(func.distinct(POSTransaction.order_id))).where(
                and_(
                    POSTransaction.store_id == store_id,
                    POSTransaction.transaction_ts >= day_start,
                    POSTransaction.transaction_ts <= day_end,
                )
            )
        ) or 0
        return min(purchases / visitors, 1.0)

    today_conversion = await _day_conversion(today_start, now)

    historical_rates: List[float] = []
    for days_back in range(1, 8):
        d_start = today_start - timedelta(days=days_back)
        d_end = d_start.replace(hour=23, minute=59, second=59)
        rate = await _day_conversion(d_start, d_end)
        if rate > 0:
            historical_rates.append(rate)

    if not historical_rates:
        return None

    avg_historical = sum(historical_rates) / len(historical_rates)
    threshold = 0.5 * avg_historical

    if avg_historical > 0 and today_conversion < threshold:
        severity = (
            Severity.CRITICAL
            if today_conversion < 0.25 * avg_historical
            else Severity.WARN
        )
        return Anomaly(
            anomaly_type=AnomalyType.CONVERSION_DROP,
            severity=severity,
            description=(
                f"Today's conversion rate {today_conversion:.2%} is below 50% "
                f"of 7-day average {avg_historical:.2%}"
            ),
            suggested_action=(
                "Investigate staff deployment, check for product availability issues, "
                "or review promotional effectiveness."
            ),
            detected_at=now,
            context={
                "today_conversion": round(today_conversion, 4),
                "avg_7d_conversion": round(avg_historical, 4),
                "threshold": round(threshold, 4),
            },
        )
    return None


async def _check_dead_zones(
    session: AsyncSession, store_id: str
) -> List[Anomaly]:
    now = _now()
    cutoff = now - timedelta(minutes=30)

    # Find all zones active in the last 24h
    active_zones_result = await session.execute(
        select(distinct(EventRow.zone_id)).where(
            and_(
                EventRow.store_id == store_id,
                EventRow.zone_id.isnot(None),
                EventRow.event_type.in_(["ZONE_ENTER", "ZONE_DWELL"]),
                EventRow.timestamp >= now - timedelta(hours=24),
            )
        )
    )
    all_zones = {row[0] for row in active_zones_result.fetchall()}

    anomalies: List[Anomaly] = []
    for zone_id in all_zones:
        last_visit = await session.scalar(
            select(func.max(EventRow.timestamp)).where(
                and_(
                    EventRow.store_id == store_id,
                    EventRow.zone_id == zone_id,
                    EventRow.event_type.in_(["ZONE_ENTER", "ZONE_DWELL"]),
                )
            )
        )
        if last_visit and last_visit < cutoff:
            idle_minutes = (now - last_visit).total_seconds() / 60
            anomalies.append(
                Anomaly(
                    anomaly_type=AnomalyType.DEAD_ZONE,
                    severity=Severity.WARN,
                    description=f"Zone '{zone_id}' has had no visits for {idle_minutes:.0f} minutes.",
                    suggested_action=(
                        f"Check camera coverage for zone '{zone_id}'. "
                        "Consider repositioning staff or adding a promotional display."
                    ),
                    detected_at=now,
                    context={
                        "zone_id": zone_id,
                        "last_visit_ts": last_visit.isoformat(),
                        "idle_minutes": round(idle_minutes, 1),
                    },
                )
            )
    return anomalies


async def _check_stale_feeds(
    session: AsyncSession, store_id: str
) -> List[Anomaly]:
    now = _now()
    cutoff = now - timedelta(minutes=10)

    camera_last_ts = await session.execute(
        select(EventRow.camera_id, func.max(EventRow.timestamp).label("last_ts")).where(
            EventRow.store_id == store_id
        ).group_by(EventRow.camera_id)
    )

    anomalies: List[Anomaly] = []
    for camera_id, last_ts in camera_last_ts.fetchall():
        if last_ts and last_ts < cutoff:
            lag_seconds = (now - last_ts).total_seconds()
            severity = Severity.CRITICAL if lag_seconds > 600 else Severity.WARN
            anomalies.append(
                Anomaly(
                    anomaly_type=AnomalyType.STALE_FEED,
                    severity=severity,
                    description=(
                        f"No events from camera '{camera_id}' for "
                        f"{lag_seconds / 60:.1f} minutes."
                    ),
                    suggested_action=(
                        f"Verify network connectivity and health of camera '{camera_id}'. "
                        "Check edge device logs."
                    ),
                    detected_at=now,
                    context={
                        "camera_id": camera_id,
                        "last_event_ts": last_ts.isoformat() if last_ts else None,
                        "lag_seconds": round(lag_seconds, 1),
                    },
                )
            )
    return anomalies


async def compute_anomalies(session: AsyncSession, store_id: str) -> AnomalyData:
    anomalies: List[Anomaly] = []

    queue_spike = await _check_billing_queue_spike(session, store_id)
    if queue_spike:
        anomalies.append(queue_spike)

    conv_drop = await _check_conversion_drop(session, store_id)
    if conv_drop:
        anomalies.append(conv_drop)

    dead_zones = await _check_dead_zones(session, store_id)
    anomalies.extend(dead_zones)

    stale_feeds = await _check_stale_feeds(session, store_id)
    anomalies.extend(stale_feeds)

    # Sort: CRITICAL first, then WARN, then INFO
    severity_order = {Severity.CRITICAL: 0, Severity.WARN: 1, Severity.INFO: 2}
    anomalies.sort(key=lambda a: severity_order.get(a.severity, 99))

    return AnomalyData(
        store_id=store_id,
        as_of=_now(),
        anomalies=anomalies,
    )
