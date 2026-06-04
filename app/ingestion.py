from __future__ import annotations

import csv
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dateutil import parser as dateutil_parser
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

try:
    from .database import AsyncSessionLocal, EventRow, POSTransaction, VisitorSession
    from .models import Event, EventType, IngestResponse
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from database import AsyncSessionLocal, EventRow, POSTransaction, VisitorSession
    from models import Event, EventType, IngestResponse

logger = logging.getLogger(__name__)

CANONICAL_STORE_ID = "STORE_BLR_002"

# Maps every known alias (uppercased) → canonical store ID.
# Unknown store IDs pass through unchanged.
_ALIAS_MAP: dict[str, str] = {
    "STORE_BLR_002": "STORE_BLR_002",
    "ST1008": "STORE_BLR_002",
    "STORE_1": "STORE_1",
    "ST1": "STORE_1",
    "STORE_2": "STORE_2",
    "ST2": "STORE_2",
    "STORE_1076": "STORE_1076",
    "ST1076": "STORE_1076",
}


def normalize_store_id(raw: str) -> str:
    return _ALIAS_MAP.get(raw.upper(), raw)


def _safe_float(val: str) -> float | None:
    try:
        return float(val.strip()) if val.strip() else None
    except ValueError:
        return None


def _safe_int(val: str) -> int | None:
    try:
        return int(float(val.strip())) if val.strip() else None
    except ValueError:
        return None


def _parse_pos_timestamp(date_str: str, time_str: str) -> datetime | None:
    try:
        combined = f"{date_str.strip()} {time_str.strip()}"
        return dateutil_parser.parse(combined, dayfirst=True)
    except (ValueError, OverflowError):
        return None


async def load_pos_from_csv(csv_path: str) -> Tuple[int, int]:
    path = Path(csv_path)
    if not path.exists():
        logger.warning("POS CSV not found at %s — skipping load", csv_path)
        return 0, 0

    loaded = 0
    skipped = 0

    async with AsyncSessionLocal() as session:
        existing_ids: set[str] = {
            row[0]
            for row in (await session.execute(select(POSTransaction.order_id))).all()
        }

        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            batch: List[POSTransaction] = []
            seen_in_run: set[str] = set()

            for row in reader:
                order_id = row.get("order_id", "").strip()
                if not order_id or order_id in existing_ids or order_id in seen_in_run:
                    skipped += 1
                    continue
                seen_in_run.add(order_id)

                ts = _parse_pos_timestamp(
                    row.get("order_date", ""), row.get("order_time", "")
                )

                raw_store_id = row.get("store_id", "").strip()

                txn = POSTransaction(
                    order_id=order_id,
                    order_date=row.get("order_date", "").strip(),
                    order_time=row.get("order_time", "").strip(),
                    store_id=normalize_store_id(raw_store_id) if raw_store_id else raw_store_id,
                    store_name=row.get("store_name", "").strip() or None,
                    city=row.get("city", "").strip() or None,
                    sku=row.get("sku", "").strip() or None,
                    product_name=row.get("product_name", "").strip() or None,
                    brand_name=row.get("brand_name", "").strip() or None,
                    dep_name=row.get("dep_name", "").strip() or None,
                    sub_category=row.get("sub_category", "").strip() or None,
                    qty=_safe_int(row.get("qty", "")),
                    gmv=_safe_float(row.get("GMV", "")),
                    nmv=_safe_float(row.get("NMV", "")),
                    total_amount=_safe_float(row.get("total_amount", "")),
                    salesperson_id=row.get("salesperson_id", "").strip() or None,
                    transaction_ts=ts,
                )
                batch.append(txn)

                if len(batch) >= 500:
                    try:
                        session.add_all(batch)
                        await session.commit()
                        loaded += len(batch)
                    except Exception:
                        await session.rollback()
                        logger.error('{"event": "pos_csv_batch_failed"}', exc_info=True)
                        raise
                    batch = []

            if batch:
                try:
                    session.add_all(batch)
                    await session.commit()
                    loaded += len(batch)
                except Exception:
                    await session.rollback()
                    logger.error('{"event": "pos_csv_batch_failed"}', exc_info=True)
                    raise

    logger.info("POS load complete: %d loaded, %d skipped", loaded, skipped)
    return loaded, skipped


async def _upsert_visitor_session(
    session: AsyncSession, event: Event, canonical_store_id: str
) -> None:
    existing = await session.scalar(
        select(VisitorSession)
        .where(
            VisitorSession.store_id == canonical_store_id,
            VisitorSession.visitor_id == event.visitor_id,
        )
        .order_by(VisitorSession.session_start.desc())
        .limit(1)
    )

    if event.event_type == EventType.ENTRY:
        if existing is None:
            session.add(
                VisitorSession(
                    store_id=canonical_store_id,
                    visitor_id=event.visitor_id,
                    session_start=event.timestamp,
                    is_staff=event.is_staff,
                )
            )
        # REENTRY does not create a new session record; it updates the existing one's start only
        # if session_seq resets to 1, which we detect via metadata.
    elif event.event_type == EventType.REENTRY:
        if existing is None:
            session.add(
                VisitorSession(
                    store_id=canonical_store_id,
                    visitor_id=event.visitor_id,
                    session_start=event.timestamp,
                    is_staff=event.is_staff,
                )
            )
        # existing session continues — no new row
    elif event.event_type in {EventType.ZONE_ENTER, EventType.ZONE_EXIT, EventType.ZONE_DWELL}:
        if existing and not existing.entered_zone:
            existing.entered_zone = True
    elif event.event_type == EventType.BILLING_QUEUE_JOIN:
        if existing and not existing.entered_billing_queue:
            existing.entered_billing_queue = True
    elif event.event_type == EventType.EXIT:
        if existing and existing.session_end is None:
            existing.session_end = event.timestamp


async def ingest_events(events: List[Event]) -> IngestResponse:
    accepted = 0
    rejected = 0
    duplicate = 0
    errors: List[Dict[str, Any]] = []

    valid_events: List[Event] = []

    for ev in events:
        ev.store_id = normalize_store_id(ev.store_id)
        valid_events.append(ev)

    async with AsyncSessionLocal() as session:
        async with session.begin():
            for ev in valid_events:
                try:
                    row = EventRow(
                        event_id=ev.event_id,
                        store_id=ev.store_id,
                        camera_id=ev.camera_id,
                        visitor_id=ev.visitor_id,
                        event_type=ev.event_type.value,
                        timestamp=ev.timestamp,
                        zone_id=ev.zone_id,
                        dwell_ms=ev.dwell_ms,
                        is_staff=ev.is_staff,
                        confidence=ev.confidence,
                        queue_depth=ev.metadata.queue_depth,
                        sku_zone=ev.metadata.sku_zone,
                        session_seq=ev.metadata.session_seq,
                        ingested_at=datetime.now(timezone.utc),
                    )
                    async with session.begin_nested():  # savepoint per row
                        session.add(row)
                        await _upsert_visitor_session(session, ev, ev.store_id)
                    accepted += 1
                except IntegrityError:
                    duplicate += 1
                    errors.append({"event_id": ev.event_id, "reason": "duplicate_event"})
                except Exception:
                    rejected += 1
                    logger.exception("Ingest error for event %s", ev.event_id)
                    errors.append({"event_id": ev.event_id, "reason": "processing_error"})

    return IngestResponse(
        accepted=accepted,
        rejected=rejected,
        duplicate=duplicate,
        errors=errors,
    )
