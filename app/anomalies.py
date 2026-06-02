from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, distinct, func, select
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from .analytics import get_current_queue_visitors, get_purchase_visitors, get_reference_now
    from .database import EventRow
    from .models import Anomaly, AnomalyData, AnomalyType, Severity
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from analytics import get_current_queue_visitors, get_purchase_visitors, get_reference_now
    from database import EventRow
    from models import Anomaly, AnomalyData, AnomalyType, Severity


_STALE_WARN_SECONDS = 600    # 10 min
_STALE_CRITICAL_SECONDS = 1200  # 20 min


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _check_billing_queue_spike(
    session: AsyncSession, store_id: str, now: datetime
) -> Anomaly | None:
    current_depth = len(await get_current_queue_visitors(session, store_id))

    # 7-day average queue depth — approximate via daily JOIN counts.
    # Use data-anchored reference time so historical/replay data is evaluated
    # relative to the data's own "now", not the system clock.
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

    # No historical baseline: emit a WARN when queue depth is notable so the
    # system is useful on day-one / replay datasets with only one day of data.
    if avg_hourly == 0 and current_depth >= 4:
        return Anomaly(
            anomaly_type=AnomalyType.BILLING_QUEUE_SPIKE,
            severity=Severity.WARN,
            description=(
                f"Queue depth {current_depth} with no historical baseline — "
                "monitoring recommended."
            ),
            suggested_action="Open additional billing counters or deploy staff to queue management.",
            detected_at=now,
            context={
                "current_depth": current_depth,
                "current_hour_joins": current_hour_joins,
                "avg_hourly_7d": 0,
                "threshold": None,
            },
        )

    return None


async def _check_conversion_drop(
    session: AsyncSession, store_id: str, now: datetime
) -> Anomaly | None:
    # Use data-anchored reference time so the "today" window aligns with the
    # date of the actual data rather than the current system date (June 1 vs
    # April 10).
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    async def _day_conversion(day_start: datetime, day_end: datetime) -> float:
        visitor_rows = await session.execute(
            select(distinct(EventRow.visitor_id)).where(
                and_(
                    EventRow.store_id == store_id,
                    EventRow.event_type.in_(
                        ["ENTRY", "GROUP_ENTRY", "ZONE_ENTER", "ZONE_DWELL", "BILLING_QUEUE_JOIN"]
                    ),
                    EventRow.is_staff.is_(False),
                    EventRow.timestamp >= day_start,
                    EventRow.timestamp <= day_end,
                )
            )
        )
        visitors = {row[0] for row in visitor_rows.fetchall()}
        if len(visitors) == 0:
            return 0.0
        purchases = await get_purchase_visitors(
            session,
            store_id,
            queue_visitors=visitors,
            reference_now=day_end,
        )
        return min(len(purchases) / len(visitors), 1.0)

    today_conversion = await _day_conversion(today_start, now)

    historical_rates: list[float] = []
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
    session: AsyncSession, store_id: str, now: datetime
) -> list[Anomaly]:
    # Use data-anchored reference time so the 24h active-zone window and the
    # 30-minute dead-zone cutoff are computed relative to the data's own "now",
    # not the system clock (which would find zero activity from April data).
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

    zone_last_ts_result = await session.execute(
        select(EventRow.zone_id, func.max(EventRow.timestamp).label("last_ts"))
        .where(
            and_(
                EventRow.store_id == store_id,
                EventRow.zone_id.in_(all_zones),
                EventRow.event_type.in_(["ZONE_ENTER", "ZONE_DWELL"]),
            )
        )
        .group_by(EventRow.zone_id)
    )
    zone_last_ts = {row.zone_id: row.last_ts for row in zone_last_ts_result.fetchall()}

    anomalies: list[Anomaly] = []
    for zone_id in all_zones:
        last_visit = zone_last_ts.get(zone_id)
        if last_visit is None or last_visit < cutoff:
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
    session: AsyncSession, store_id: str, now: datetime
) -> list[Anomaly]:
    # Use the latest event timestamp as reference so historical/replay data
    # is not treated as stale relative to the wall-clock system time.
    cutoff = now - timedelta(seconds=_STALE_WARN_SECONDS)

    camera_last_ts = await session.execute(
        select(EventRow.camera_id, func.max(EventRow.timestamp).label("last_ts")).where(
            EventRow.store_id == store_id
        ).group_by(EventRow.camera_id)
    )

    anomalies: List[Anomaly] = []
    for camera_id, last_ts in camera_last_ts.fetchall():
        if last_ts and last_ts < cutoff:
            lag_seconds = (now - last_ts).total_seconds()
            severity = Severity.CRITICAL if lag_seconds > _STALE_CRITICAL_SECONDS else Severity.WARN
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
    reference_now = await get_reference_now(session, store_id)
    anomalies: list[Anomaly] = []

    queue_spike = await _check_billing_queue_spike(session, store_id, reference_now)
    if queue_spike:
        anomalies.append(queue_spike)

    conv_drop = await _check_conversion_drop(session, store_id, reference_now)
    if conv_drop:
        anomalies.append(conv_drop)

    dead_zones = await _check_dead_zones(session, store_id, reference_now)
    anomalies.extend(dead_zones)

    stale_feeds = await _check_stale_feeds(session, store_id, reference_now)
    anomalies.extend(stale_feeds)

    # Sort: CRITICAL first, then WARN, then INFO
    severity_order = {Severity.CRITICAL: 0, Severity.WARN: 1, Severity.INFO: 2}
    anomalies.sort(key=lambda a: severity_order.get(a.severity, 99))

    return AnomalyData(
        store_id=store_id,
        as_of=reference_now,
        anomalies=anomalies,
    )
