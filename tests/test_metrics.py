"""
Tests for store metrics computation via the FastAPI endpoints.

# PROMPT: Write tests for store metrics computation covering conversion rate, funnel,
#         and heatmap.
# Test: unique visitor counting, session-based conversion, zero-purchase stores,
#       re-entry deduplication.
# CHANGES MADE: Added parametrize for multiple store scenarios including all-staff clips.

All tests run against the in-process ASGI app backed by an in-memory SQLite database.
Each test function is isolated: fixtures create fresh event sets and the async_client
fixture resets the database state via app lifespan or table truncation.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import List

import pytest
import pytest_asyncio

from app.models import Event, EventMetadata, EventType

STORE_ID = "STORE_BLR_002"
BASE_TS = datetime(2026, 4, 10, 14, 0, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _ev(
    visitor_id: str,
    event_type: EventType,
    delta_minutes: int = 0,
    zone_id: str | None = None,
    camera_id: str = "CAM_ENTRY_01",
    is_staff: bool = False,
    confidence: float = 0.91,
    dwell_ms: int = 0,
    queue_depth: int | None = None,
    session_seq: int | None = None,
) -> dict:
    """Return a JSON-serialisable dict matching the Event schema."""
    ts = BASE_TS + timedelta(minutes=delta_minutes)
    return {
        "event_id": str(uuid.uuid4()),
        "store_id": STORE_ID,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type.value,
        "timestamp": ts.isoformat(),
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": confidence,
        "metadata": {
            "queue_depth": queue_depth,
            "session_seq": session_seq,
        },
    }


async def _ingest(client, events: List[dict]) -> dict:
    """POST events to /events/ingest and return the JSON response."""
    resp = await client.post("/events/ingest", json={"events": events})
    assert resp.status_code == 200, f"Ingest failed: {resp.text}"
    return resp.json()


# ─────────────────────────────────────────────────────────────────────────────
# 1. Unique visitor count
# ─────────────────────────────────────────────────────────────────────────────


class TestUniqueVisitorCount:
    """GET /stores/{id}/metrics must return the correct unique_visitors count."""

    @pytest.mark.asyncio
    async def test_unique_visitors_counts_entry_events(self, async_client, sample_events):
        """6 unique customers in sample_events → unique_visitors == 6."""
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == 6

    @pytest.mark.asyncio
    async def test_unique_visitors_excludes_staff(
        self, async_client, sample_events_with_staff
    ):
        """Staff events must not inflate unique_visitors."""
        payload = [e.model_dump(mode="json") for e in sample_events_with_staff]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        # sample_events has 6 customers; sample_events_with_staff adds 2 staff
        assert data["unique_visitors"] == 6

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "n_visitors,n_staff",
        [(0, 5), (1, 0), (3, 3), (10, 0)],
    )
    async def test_unique_visitors_parametrized(self, async_client, n_visitors, n_staff):
        """Parametrized: unique_visitors is always exactly n_visitors."""
        events = []
        for i in range(n_visitors):
            events.append(_ev(f"CUST_{i:03d}", EventType.ENTRY, delta_minutes=i))
        for j in range(n_staff):
            events.append(_ev(f"STAFF_{j:03d}", EventType.ENTRY, is_staff=True, delta_minutes=j))
        if events:
            await _ingest(async_client, events)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == n_visitors

    @pytest.mark.asyncio
    async def test_all_staff_clip_unique_visitors_zero(
        self, async_client, sample_events_with_staff
    ):
        """A recording containing only staff → unique_visitors == 0."""
        # Build an all-staff batch
        staff_only = [
            _ev(f"STAFF_{i:02d}", EventType.ENTRY, is_staff=True, delta_minutes=i)
            for i in range(5)
        ]
        await _ingest(async_client, staff_only)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == 0

    @pytest.mark.asyncio
    async def test_unique_visitors_falls_back_to_zone_activity(self, async_client):
        """Challenge replay data may miss ENTRY events; zone activity still counts a visitor."""
        events = [
            _ev(
                "V_ZONE_ONLY",
                EventType.ZONE_ENTER,
                zone_id="SKINCARE",
                camera_id="CAM_FLOOR_01",
            )
        ]
        await _ingest(async_client, events)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        assert resp.json()["unique_visitors"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# 2. Conversion rate computation
# ─────────────────────────────────────────────────────────────────────────────


class TestConversionRate:
    """conversion_rate = visitors who reached billing zone within 5 min of a
    POS transaction / total unique customer visitors."""

    @pytest.mark.asyncio
    async def test_conversion_rate_zero_no_purchases(self, async_client):
        """No POS data → conversion_rate must be 0, not a crash."""
        events = [
            _ev("V001", EventType.ENTRY, delta_minutes=0),
            _ev("V001", EventType.ZONE_ENTER, delta_minutes=2, zone_id="SKINCARE", camera_id="CAM_FLOOR_01"),
            _ev("V001", EventType.EXIT, delta_minutes=10),
        ]
        await _ingest(async_client, events)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["conversion_rate"] == pytest.approx(0.0, abs=1e-6)

    @pytest.mark.asyncio
    async def test_conversion_rate_within_bounds(self, async_client, sample_events):
        """conversion_rate must always be in [0, 1]."""
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        rate = data["conversion_rate"]
        assert 0.0 <= rate <= 1.0, f"conversion_rate out of bounds: {rate}"

    @pytest.mark.asyncio
    async def test_conversion_rate_no_crash_no_visitors(self, async_client):
        """Zero visitors → conversion_rate must be 0.0 with no ZeroDivisionError."""
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["conversion_rate"] == pytest.approx(0.0, abs=1e-6)

    @pytest.mark.asyncio
    async def test_conversion_rate_field_present(self, async_client):
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        assert "conversion_rate" in resp.json()


# ─────────────────────────────────────────────────────────────────────────────
# 3. Funnel endpoint
# ─────────────────────────────────────────────────────────────────────────────


class TestFunnelEndpoint:
    """GET /stores/{id}/funnel must return ordered stages with drop-off percentages."""

    @pytest.mark.asyncio
    async def test_funnel_stages_present(self, async_client, sample_events):
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/funnel")
        assert resp.status_code == 200
        data = resp.json()
        assert "stages" in data
        assert isinstance(data["stages"], list)
        assert len(data["stages"]) >= 1

    @pytest.mark.asyncio
    async def test_funnel_stage_schema(self, async_client, sample_events):
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/funnel")
        data = resp.json()
        for stage in data["stages"]:
            assert "stage" in stage
            assert "count" in stage
            assert "drop_off_pct" in stage
            assert 0.0 <= stage["drop_off_pct"] <= 100.0

    @pytest.mark.asyncio
    async def test_funnel_top_stage_is_entry(self, async_client, sample_events):
        """The first funnel stage must capture store entry (highest count)."""
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/funnel")
        data = resp.json()
        if data["stages"]:
            top = data["stages"][0]
            # The entry stage should have the largest count
            counts = [s["count"] for s in data["stages"]]
            assert top["count"] == max(counts)

    @pytest.mark.asyncio
    async def test_funnel_reentry_visitor_counted_once(
        self, async_client, sample_events_with_reentry
    ):
        """V007 (who re-enters) must appear once in the funnel, not twice."""
        payload = [e.model_dump(mode="json") for e in sample_events_with_reentry]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/funnel")
        assert resp.status_code == 200
        data = resp.json()
        # Entry count should reflect 1 unique visitor (V007), not 2
        if data["stages"]:
            entry_stage = data["stages"][0]
            assert entry_stage["count"] == 1

    @pytest.mark.asyncio
    async def test_funnel_store_id_in_response(self, async_client):
        resp = await async_client.get(f"/stores/{STORE_ID}/funnel")
        assert resp.status_code == 200
        assert resp.json()["store_id"] == STORE_ID


# ─────────────────────────────────────────────────────────────────────────────
# 4. Heatmap endpoint
# ─────────────────────────────────────────────────────────────────────────────


class TestHeatmapEndpoint:
    """GET /stores/{id}/heatmap must return per-zone intensity cells."""

    @pytest.mark.asyncio
    async def test_heatmap_returns_cells(self, async_client, sample_events):
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/heatmap")
        assert resp.status_code == 200
        data = resp.json()
        assert "cells" in data

    @pytest.mark.asyncio
    async def test_heatmap_cell_schema(self, async_client, sample_events):
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/heatmap")
        data = resp.json()
        for cell in data["cells"]:
            assert "zone_id" in cell
            assert "visit_count" in cell
            assert "avg_dwell_ms" in cell
            assert "intensity" in cell
            assert 0.0 <= cell["intensity"] <= 100.0
            assert "data_confidence" in cell

    @pytest.mark.asyncio
    async def test_heatmap_low_data_confidence_flag(self, async_client):
        """
        Zones with < 20 sessions must have data_confidence=False.
        We ingest only a handful of events so all zones are below threshold.
        """
        events = [
            _ev("V_HEAT_01", EventType.ENTRY),
            _ev("V_HEAT_01", EventType.ZONE_ENTER, zone_id="SKINCARE", camera_id="CAM_FLOOR_01", delta_minutes=2),
        ]
        await _ingest(async_client, events)
        resp = await async_client.get(f"/stores/{STORE_ID}/heatmap")
        data = resp.json()
        for cell in data["cells"]:
            if cell["zone_id"] == "SKINCARE":
                assert cell["data_confidence"] is False, (
                    "Should be low confidence with <20 sessions"
                )

    @pytest.mark.asyncio
    async def test_heatmap_store_id_in_response(self, async_client):
        resp = await async_client.get(f"/stores/{STORE_ID}/heatmap")
        assert resp.status_code == 200
        assert resp.json()["store_id"] == STORE_ID

    @pytest.mark.asyncio
    async def test_heatmap_ignores_impossible_dwell_values(self, async_client):
        events = [
            _ev(
                "V_BAD_DWELL",
                EventType.ZONE_ENTER,
                zone_id="SKINCARE",
                camera_id="CAM_FLOOR_01",
            ),
            _ev(
                "V_BAD_DWELL",
                EventType.ZONE_DWELL,
                delta_minutes=1,
                zone_id="SKINCARE",
                camera_id="CAM_FLOOR_01",
                dwell_ms=4_500_000_000,
            ),
        ]
        await _ingest(async_client, events)
        resp = await async_client.get(f"/stores/{STORE_ID}/heatmap")
        assert resp.status_code == 200
        skincare = next(c for c in resp.json()["cells"] if c["zone_id"] == "SKINCARE")
        assert skincare["avg_dwell_ms"] == 0.0

    @pytest.mark.asyncio
    async def test_queue_depth_clears_on_billing_zone_exit(self, async_client):
        events = [
            _ev("V_QUEUE", EventType.ENTRY),
            _ev(
                "V_QUEUE",
                EventType.BILLING_QUEUE_JOIN,
                delta_minutes=5,
                zone_id="BILLING",
                camera_id="CAM_BILLING_01",
            ),
            _ev(
                "V_QUEUE",
                EventType.ZONE_EXIT,
                delta_minutes=6,
                zone_id="BILLING",
                camera_id="CAM_BILLING_01",
            ),
        ]
        await _ingest(async_client, events)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        assert resp.json()["queue_depth"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# 5. Re-entry deduplication
# ─────────────────────────────────────────────────────────────────────────────


class TestReentryDeduplication:
    """
    A visitor who exits and re-enters on the same day should be counted once
    in unique_visitors, not twice.
    """

    @pytest.mark.asyncio
    async def test_reentry_counted_once_in_unique_visitors(
        self, async_client, sample_events_with_reentry
    ):
        payload = [e.model_dump(mode="json") for e in sample_events_with_reentry]
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        # V007 should count as 1, not 2
        assert data["unique_visitors"] == 1

    @pytest.mark.asyncio
    async def test_reentry_visitor_counted_once_combined_traffic(
        self, async_client, sample_events, sample_events_with_reentry
    ):
        """6 base visitors + V007 (re-entrant) = 7 unique visitors total."""
        base = [e.model_dump(mode="json") for e in sample_events]
        reentry = [e.model_dump(mode="json") for e in sample_events_with_reentry]
        await _ingest(async_client, base)
        await _ingest(async_client, reentry)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == 7


# ─────────────────────────────────────────────────────────────────────────────
# 6. Idempotent ingest
# ─────────────────────────────────────────────────────────────────────────────


class TestIdempotentIngest:
    """
    POSTing the same event batch twice must not create duplicate records.
    The second ingest should report all events as duplicates.
    """

    @pytest.mark.asyncio
    async def test_double_ingest_accepted_count_same(self, async_client, sample_events):
        payload = [e.model_dump(mode="json") for e in sample_events]
        first = await _ingest(async_client, payload)
        second = await _ingest(async_client, payload)
        # First ingest: all accepted; second: all duplicates
        assert first["accepted"] == len(sample_events)
        assert second["duplicate"] == len(sample_events)
        assert second["accepted"] == 0

    @pytest.mark.asyncio
    async def test_double_ingest_no_unique_visitor_inflation(
        self, async_client, sample_events
    ):
        """After double-ingesting, unique_visitors must not double."""
        payload = [e.model_dump(mode="json") for e in sample_events]
        await _ingest(async_client, payload)
        await _ingest(async_client, payload)
        resp = await async_client.get(f"/stores/{STORE_ID}/metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["unique_visitors"] == 6

    @pytest.mark.asyncio
    async def test_single_event_idempotent(self, async_client):
        """A single event ingested twice: second call returns duplicate=1."""
        ev = _ev("V_IDEM", EventType.ENTRY)
        first = await _ingest(async_client, [ev])
        # Re-use same event_id (idempotency key)
        second = await _ingest(async_client, [ev])
        assert first["accepted"] == 1
        assert second["duplicate"] == 1

    @pytest.mark.asyncio
    async def test_ingest_response_schema(self, async_client, sample_events):
        payload = [e.model_dump(mode="json") for e in sample_events]
        result = await _ingest(async_client, payload)
        for field in ("accepted", "rejected", "duplicate", "errors"):
            assert field in result, f"Missing field: {field}"
        assert isinstance(result["errors"], list)


# ─────────────────────────────────────────────────────────────────────────────
# 7. Health endpoint
# ─────────────────────────────────────────────────────────────────────────────


class TestHealthEndpoint:
    """GET /health must return valid JSON with required fields."""

    @pytest.mark.asyncio
    async def test_health_returns_200(self, async_client):
        resp = await async_client.get("/health")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_health_response_is_json(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        assert isinstance(data, dict)

    @pytest.mark.asyncio
    async def test_health_required_fields(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        for field in ("status", "db_status", "store_id", "checked_at", "camera_statuses"):
            assert field in data, f"Missing field in /health response: {field}"

    @pytest.mark.asyncio
    async def test_health_status_ok_when_db_up(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        assert data["status"] in ("ok", "healthy", "degraded")

    @pytest.mark.asyncio
    async def test_health_camera_statuses_is_list(self, async_client):
        resp = await async_client.get("/health")
        data = resp.json()
        assert isinstance(data["camera_statuses"], list)

    @pytest.mark.asyncio
    async def test_health_store_id_correct(self, async_client):
        resp = await async_client.get("/health")
        assert resp.json()["store_id"] == STORE_ID
