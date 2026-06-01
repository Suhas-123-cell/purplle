"""
pytest conftest for Store Intelligence API tests.

Provides async HTTP client and realistic event fixtures for STORE_BLR_002
(Brigade Road, Bangalore — Purplle Tech Challenge 2026 Round 2).
"""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta
from typing import List

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

# ── environment must be set before app modules are imported ─────────────────
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("POS_CSV_PATH", "/dev/null")

# lazy import so env vars land first
from app.main import app  # noqa: E402  (app must exist by the time tests run)
from app.database import Base, engine  # noqa: E402
from app.models import Event, EventMetadata, EventType  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

STORE_ID = "STORE_BLR_002"
BASE_TS = datetime(2026, 4, 10, 11, 0, 0)  # 10-04-2026 11:00:00


def _ev(
    visitor_id: str,
    event_type: EventType,
    delta_minutes: int = 0,
    zone_id: str | None = None,
    camera_id: str = "CAM_ENTRY_01",
    is_staff: bool = False,
    confidence: float = 0.92,
    dwell_ms: int = 0,
    queue_depth: int | None = None,
    session_seq: int | None = None,
) -> Event:
    """Build a valid Event with sensible defaults."""
    ts = BASE_TS + timedelta(minutes=delta_minutes)
    return Event(
        event_id=str(uuid.uuid4()),
        store_id=STORE_ID,
        camera_id=camera_id,
        visitor_id=visitor_id,
        event_type=event_type,
        timestamp=ts,
        zone_id=zone_id,
        dwell_ms=dwell_ms,
        is_staff=is_staff,
        confidence=confidence,
        metadata=EventMetadata(
            queue_depth=queue_depth,
            session_seq=session_seq,
        ),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Core fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture()
async def reset_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield


@pytest_asyncio.fixture()
async def async_client(reset_db):
    """Async HTTP client backed by the FastAPI app (in-process, no network)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture()
def sample_events() -> List[Event]:
    """
    20 realistic CCTV events for STORE_BLR_002 spanning one shopping hour.

    Traffic pattern:
    - 6 unique visitors (V001–V006)
    - Each enters, browses 2–3 zones, some join billing queue, all exit
    - Covers ENTRY, ZONE_ENTER, ZONE_DWELL, BILLING_QUEUE_JOIN, EXIT event types
    """
    events: List[Event] = []

    # ── V001: full funnel (ENTRY → Skincare → Billing Queue → EXIT) ──────────
    events.append(_ev("V001", EventType.ENTRY,              delta_minutes=0,  session_seq=1))
    events.append(_ev("V001", EventType.ZONE_ENTER,         delta_minutes=2,  zone_id="SKINCARE",  camera_id="CAM_FLOOR_01", session_seq=2))
    events.append(_ev("V001", EventType.ZONE_DWELL,         delta_minutes=8,  zone_id="SKINCARE",  camera_id="CAM_FLOOR_01", dwell_ms=360_000, session_seq=3))
    events.append(_ev("V001", EventType.BILLING_QUEUE_JOIN, delta_minutes=12, camera_id="CAM_BILLING_01", queue_depth=1, session_seq=4))
    events.append(_ev("V001", EventType.EXIT,               delta_minutes=18, session_seq=5))

    # ── V002: browses Makeup, skips billing, exits ───────────────────────────
    events.append(_ev("V002", EventType.ENTRY,              delta_minutes=1,  session_seq=1))
    events.append(_ev("V002", EventType.ZONE_ENTER,         delta_minutes=3,  zone_id="MAKEUP",    camera_id="CAM_FLOOR_01", session_seq=2))
    events.append(_ev("V002", EventType.ZONE_DWELL,         delta_minutes=10, zone_id="MAKEUP",    camera_id="CAM_FLOOR_01", dwell_ms=420_000, session_seq=3))
    events.append(_ev("V002", EventType.EXIT,               delta_minutes=15, session_seq=4))

    # ── V003: haircare → fragrance → exit (window shopper) ───────────────────
    events.append(_ev("V003", EventType.ENTRY,              delta_minutes=5,  session_seq=1))
    events.append(_ev("V003", EventType.ZONE_ENTER,         delta_minutes=7,  zone_id="HAIRCARE",  camera_id="CAM_FLOOR_01", session_seq=2))
    events.append(_ev("V003", EventType.ZONE_ENTER,         delta_minutes=13, zone_id="FRAGRANCE", camera_id="CAM_FLOOR_02", session_seq=3))
    events.append(_ev("V003", EventType.EXIT,               delta_minutes=20, session_seq=4))

    # ── V004: skincare → billing queue join → exit ────────────────────────────
    events.append(_ev("V004", EventType.ENTRY,              delta_minutes=8,  session_seq=1))
    events.append(_ev("V004", EventType.ZONE_ENTER,         delta_minutes=10, zone_id="SKINCARE",  camera_id="CAM_FLOOR_01", session_seq=2))
    events.append(_ev("V004", EventType.BILLING_QUEUE_JOIN, delta_minutes=20, camera_id="CAM_BILLING_01", queue_depth=2, session_seq=3))
    events.append(_ev("V004", EventType.EXIT,               delta_minutes=25, session_seq=4))

    # ── V005: quick entry/exit (bounce) ───────────────────────────────────────
    events.append(_ev("V005", EventType.ENTRY,              delta_minutes=15, session_seq=1))
    events.append(_ev("V005", EventType.EXIT,               delta_minutes=17, session_seq=2))

    # ── V006: just an ENTRY so far (still in store at snapshot time) ─────────
    events.append(_ev("V006", EventType.ENTRY,              delta_minutes=25, session_seq=1))

    return events


@pytest.fixture()
def sample_events_with_staff(sample_events: List[Event]) -> List[Event]:
    """
    Extends sample_events with 3 staff member events interspersed among
    the customer traffic.  Staff entries are marked is_staff=True and simulate
    rapid multi-zone movement typical of floor associates.
    """
    staff_events: List[Event] = [
        # Staff S001 opens the floor, moves through all zones quickly
        _ev("S001", EventType.ENTRY,      delta_minutes=0,  is_staff=True,  confidence=0.97, session_seq=1),
        _ev("S001", EventType.ZONE_ENTER, delta_minutes=1,  zone_id="SKINCARE",  camera_id="CAM_FLOOR_01", is_staff=True, session_seq=2),
        _ev("S001", EventType.ZONE_ENTER, delta_minutes=2,  zone_id="MAKEUP",    camera_id="CAM_FLOOR_01", is_staff=True, session_seq=3),
        _ev("S001", EventType.ZONE_ENTER, delta_minutes=3,  zone_id="HAIRCARE",  camera_id="CAM_FLOOR_01", is_staff=True, session_seq=4),
        _ev("S001", EventType.ZONE_ENTER, delta_minutes=4,  zone_id="BILLING",   camera_id="CAM_BILLING_01", is_staff=True, session_seq=5),
        # Staff S002 at billing counter all day
        _ev("S002", EventType.ENTRY,      delta_minutes=0,  is_staff=True,  confidence=0.95, camera_id="CAM_BILLING_01", session_seq=1),
        _ev("S002", EventType.ZONE_ENTER, delta_minutes=1,  zone_id="BILLING",   camera_id="CAM_BILLING_01", is_staff=True, session_seq=2),
    ]
    return sample_events + staff_events


@pytest.fixture()
def sample_events_with_reentry() -> List[Event]:
    """
    V007 enters, exits, and then re-enters the store — triggering REENTRY logic.
    Used to validate that re-entries are counted once in the unique_visitors metric
    and that the second appearance is tagged REENTRY, not ENTRY.
    """
    return [
        # First visit
        _ev("V007", EventType.ENTRY,      delta_minutes=0,  session_seq=1),
        _ev("V007", EventType.ZONE_ENTER, delta_minutes=3,  zone_id="SKINCARE", camera_id="CAM_FLOOR_01", session_seq=2),
        _ev("V007", EventType.EXIT,       delta_minutes=10, session_seq=3),
        # Gap — visitor steps out briefly, then comes back
        _ev("V007", EventType.REENTRY,    delta_minutes=18, session_seq=1),  # session_seq resets on re-entry
        _ev("V007", EventType.ZONE_ENTER, delta_minutes=21, zone_id="BILLING", camera_id="CAM_BILLING_01", session_seq=2),
        _ev("V007", EventType.EXIT,       delta_minutes=30, session_seq=3),
    ]
