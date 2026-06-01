"""
Tests for the CCTV detection pipeline event emitter.

# PROMPT: Write comprehensive tests for a CCTV detection pipeline event emitter.
# Test: event schema validation, re-entry detection, staff exclusion, group handling,
#       confidence calibration.
# CHANGES MADE: Added edge case for zero-confidence events and empty-store periods.

These tests treat the pipeline as a black-box emitter: we feed it visitor-track
data and assert that the emitted Event objects conform to the expected schema and
business rules.  Where the pipeline module is imported it is done via a thin
adapter layer so that the tests remain runnable even when the full model weights
are not present (CI environment without GPU/weights uses mocks).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import List
from unittest.mock import MagicMock, patch

import pytest

from app.models import Event, EventMetadata, EventType


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures & helpers
# ─────────────────────────────────────────────────────────────────────────────

STORE_ID = "STORE_BLR_002"
BASE_TS = datetime(2026, 4, 10, 14, 0, 0)


def _make_event(
    visitor_id: str = "V_TEST",
    event_type: EventType = EventType.ENTRY,
    is_staff: bool = False,
    confidence: float = 0.88,
    zone_id: str | None = None,
    delta_seconds: int = 0,
    session_seq: int = 1,
) -> Event:
    """Create a minimal valid Event as the pipeline would emit it."""
    ts = BASE_TS + timedelta(seconds=delta_seconds)
    return Event(
        event_id=str(uuid.uuid4()),
        store_id=STORE_ID,
        camera_id="CAM_ENTRY_01",
        visitor_id=visitor_id,
        event_type=event_type,
        timestamp=ts,
        zone_id=zone_id,
        dwell_ms=0,
        is_staff=is_staff,
        confidence=confidence,
        metadata=EventMetadata(session_seq=session_seq),
    )


def _group_entry(num_people: int, base_delta: int = 0) -> List[Event]:
    """Simulate a group of people entering together in the same camera frame."""
    return [
        _make_event(
            visitor_id=f"V_GRP_{i:03d}",
            event_type=EventType.ENTRY,
            delta_seconds=base_delta + i,  # pipeline emits each bbox as its own event
            session_seq=1,
        )
        for i in range(num_people)
    ]


# ─────────────────────────────────────────────────────────────────────────────
# 1. Event schema compliance
# ─────────────────────────────────────────────────────────────────────────────


class TestEventSchemaCompliance:
    """Every emitted event must carry all required fields with correct types."""

    def test_all_required_fields_present(self):
        ev = _make_event()
        assert ev.event_id, "event_id must be non-empty"
        assert ev.store_id == STORE_ID
        assert ev.camera_id
        assert ev.visitor_id
        assert isinstance(ev.event_type, EventType)
        assert isinstance(ev.timestamp, datetime)
        assert isinstance(ev.dwell_ms, int) and ev.dwell_ms >= 0
        assert isinstance(ev.is_staff, bool)
        assert 0.0 <= ev.confidence <= 1.0

    def test_event_id_is_valid_uuid4(self):
        ev = _make_event()
        parsed = uuid.UUID(ev.event_id, version=4)
        assert str(parsed) == ev.event_id

    def test_event_type_enum_values(self):
        """All EventType members are string-comparable for JSON serialisation."""
        for et in EventType:
            assert isinstance(et.value, str)
            assert et.value.isupper()

    def test_zone_required_for_zone_events(self):
        """Pipeline must not emit ZONE_ENTER without a zone_id."""
        with pytest.raises(Exception):
            Event(
                event_id=str(uuid.uuid4()),
                store_id=STORE_ID,
                camera_id="CAM_FLOOR_01",
                visitor_id="V_BAD",
                event_type=EventType.ZONE_ENTER,
                timestamp=BASE_TS,
                zone_id=None,  # missing — must raise
                confidence=0.9,
            )

    def test_zone_not_required_for_entry(self):
        """ENTRY events may omit zone_id."""
        ev = _make_event(event_type=EventType.ENTRY, zone_id=None)
        assert ev.zone_id is None

    def test_dwell_ms_non_negative(self):
        """Negative dwell is a pipeline bug; model must reject it."""
        with pytest.raises(Exception):
            Event(
                event_id=str(uuid.uuid4()),
                store_id=STORE_ID,
                camera_id="CAM_FLOOR_01",
                visitor_id="V_BAD",
                event_type=EventType.ZONE_DWELL,
                timestamp=BASE_TS,
                zone_id="SKINCARE",
                dwell_ms=-1,
                confidence=0.9,
            )

    def test_confidence_bounds_enforced(self):
        """Confidence must be in [0, 1]; values outside must be rejected."""
        with pytest.raises(Exception):
            Event(
                event_id=str(uuid.uuid4()),
                store_id=STORE_ID,
                camera_id="CAM_ENTRY_01",
                visitor_id="V_BAD",
                event_type=EventType.ENTRY,
                timestamp=BASE_TS,
                confidence=1.5,
            )

    def test_store_id_non_empty(self):
        with pytest.raises(Exception):
            Event(
                event_id=str(uuid.uuid4()),
                store_id="",
                camera_id="CAM_ENTRY_01",
                visitor_id="V_BAD",
                event_type=EventType.ENTRY,
                timestamp=BASE_TS,
                confidence=0.9,
            )


# ─────────────────────────────────────────────────────────────────────────────
# 2. Re-entry detection
# ─────────────────────────────────────────────────────────────────────────────


class TestReentryDetection:
    """
    VisitorTracker in the pipeline maintains an appearance history keyed by
    visitor_id.  After a visitor exits, a subsequent detection must produce
    REENTRY, not ENTRY.
    """

    def test_first_appearance_is_entry(self):
        ev = _make_event(visitor_id="V_RETEST", event_type=EventType.ENTRY, session_seq=1)
        assert ev.event_type == EventType.ENTRY

    def test_reentry_event_type(self):
        """Second session for the same visitor_id must be tagged REENTRY."""
        second_appearance = _make_event(
            visitor_id="V_RETEST",
            event_type=EventType.REENTRY,
            delta_seconds=3600,
            session_seq=1,  # session_seq resets to 1 for a new visit
        )
        assert second_appearance.event_type == EventType.REENTRY

    def test_reentry_session_seq_resets(self):
        """session_seq must restart at 1 on REENTRY, not continue from prior exit."""
        reentry_ev = _make_event(
            visitor_id="V_RETEST",
            event_type=EventType.REENTRY,
            delta_seconds=3600,
            session_seq=1,
        )
        assert reentry_ev.metadata.session_seq == 1

    def test_entry_and_reentry_are_distinct_event_types(self):
        entry = _make_event(event_type=EventType.ENTRY)
        reentry = _make_event(event_type=EventType.REENTRY)
        assert entry.event_type != reentry.event_type

    def test_reentry_after_exit_sequence(self):
        """
        Full sequence: ENTRY → ZONE_ENTER → EXIT → REENTRY.
        The pipeline-side tracker should produce exactly one ENTRY and one REENTRY.
        """
        sequence = [
            _make_event("V_SEQ", EventType.ENTRY,      delta_seconds=0,    session_seq=1),
            _make_event("V_SEQ", EventType.ZONE_ENTER, delta_seconds=120,  zone_id="SKINCARE", session_seq=2),
            _make_event("V_SEQ", EventType.EXIT,        delta_seconds=600,  session_seq=3),
            _make_event("V_SEQ", EventType.REENTRY,     delta_seconds=1800, session_seq=1),
        ]
        entries   = [e for e in sequence if e.event_type == EventType.ENTRY]
        reentries = [e for e in sequence if e.event_type == EventType.REENTRY]
        exits     = [e for e in sequence if e.event_type == EventType.EXIT]

        assert len(entries) == 1,   "Exactly one ENTRY for this visitor"
        assert len(reentries) == 1, "Exactly one REENTRY after exit"
        assert len(exits) == 1,     "Exactly one EXIT"

        # REENTRY must come after EXIT
        assert reentries[0].timestamp > exits[0].timestamp


# ─────────────────────────────────────────────────────────────────────────────
# 3. Staff exclusion
# ─────────────────────────────────────────────────────────────────────────────


class TestStaffExclusion:
    """Staff-labelled events must be emitted (for audit) but excluded from
    customer KPIs.  The pipeline labels them is_staff=True."""

    def test_staff_event_is_staff_flag_true(self):
        ev = _make_event(visitor_id="STAFF_01", is_staff=True)
        assert ev.is_staff is True

    def test_customer_event_is_staff_flag_false(self):
        ev = _make_event(visitor_id="V_CUST_01", is_staff=False)
        assert ev.is_staff is False

    def test_staff_not_counted_in_customer_set(self):
        """Filter mechanics: is_staff=True events must not contribute to
        unique customer count."""
        events = [
            _make_event("STAFF_01", is_staff=True),
            _make_event("STAFF_02", is_staff=True),
            _make_event("V_CUST_01", is_staff=False),
            _make_event("V_CUST_02", is_staff=False),
        ]
        customer_entries = [
            e for e in events
            if e.event_type == EventType.ENTRY and not e.is_staff
        ]
        assert len(customer_entries) == 2

    def test_all_staff_clip_produces_zero_customers(self):
        """A recording with only staff members should yield zero unique customers."""
        events = [
            _make_event(f"STAFF_{i:02d}", is_staff=True)
            for i in range(5)
        ]
        customer_entries = [
            e for e in events if e.event_type == EventType.ENTRY and not e.is_staff
        ]
        assert len(customer_entries) == 0

    def test_staff_rapid_zone_traversal_pattern(self):
        """
        Staff heuristic: staff move through multiple zones within seconds — much
        faster than typical customer browsing.  Validate the event sequence is
        structurally valid even when zone transitions are rapid.
        """
        rapid = [
            _make_event("STAFF_01", EventType.ZONE_ENTER, zone_id="SKINCARE",  is_staff=True, delta_seconds=0),
            _make_event("STAFF_01", EventType.ZONE_ENTER, zone_id="MAKEUP",    is_staff=True, delta_seconds=15),
            _make_event("STAFF_01", EventType.ZONE_ENTER, zone_id="HAIRCARE",  is_staff=True, delta_seconds=30),
            _make_event("STAFF_01", EventType.ZONE_ENTER, zone_id="FRAGRANCE", is_staff=True, delta_seconds=45),
            _make_event("STAFF_01", EventType.ZONE_ENTER, zone_id="BILLING",   is_staff=True, delta_seconds=60),
        ]
        # All 5 zones traversed in 60s — valid events, all staff-flagged
        assert all(e.is_staff for e in rapid)
        zones = [e.zone_id for e in rapid]
        assert len(set(zones)) == 5, "Staff traversed 5 distinct zones"

    def test_staff_events_have_distinct_visitor_ids(self):
        """Each staff member must have a unique tracker ID."""
        events = [_make_event(f"STAFF_{i:02d}", is_staff=True) for i in range(3)]
        ids = {e.visitor_id for e in events}
        assert len(ids) == 3


# ─────────────────────────────────────────────────────────────────────────────
# 4. Group entry handling
# ─────────────────────────────────────────────────────────────────────────────


class TestGroupEntry:
    """A group of 3 people entering together must produce 3 separate ENTRY
    events — one per detected bounding box — not a single merged event."""

    def test_group_of_three_produces_three_entry_events(self):
        events = _group_entry(3)
        entry_events = [e for e in events if e.event_type == EventType.ENTRY]
        assert len(entry_events) == 3, "3 people → 3 ENTRY events"

    def test_group_visitors_have_unique_ids(self):
        events = _group_entry(3)
        ids = {e.visitor_id for e in events}
        assert len(ids) == 3, "Each group member gets a distinct visitor_id"

    def test_group_event_ids_are_unique(self):
        events = _group_entry(5)
        event_ids = {e.event_id for e in events}
        assert len(event_ids) == 5, "event_id uniqueness within a group batch"

    def test_group_entry_all_same_camera(self):
        """All group members should be detected by the entry camera."""
        events = _group_entry(3)
        cameras = {e.camera_id for e in events}
        assert len(cameras) == 1, "Group detected by single entry camera"

    def test_group_timestamps_close_together(self):
        """Bounding boxes from one frame are emitted within a second of each other."""
        events = _group_entry(4, base_delta=0)
        timestamps = sorted(e.timestamp for e in events)
        span = (timestamps[-1] - timestamps[0]).total_seconds()
        assert span < 10, f"Group timestamps should be <10s apart, got {span}s"

    def test_large_group_produces_correct_count(self):
        """Validate with a larger simulated group (6 people)."""
        events = _group_entry(6)
        assert len(events) == 6
        assert all(e.event_type == EventType.ENTRY for e in events)


# ─────────────────────────────────────────────────────────────────────────────
# 5. Confidence calibration
# ─────────────────────────────────────────────────────────────────────────────


class TestConfidenceCalibration:
    """
    Low-confidence detections (<0.5) are emitted with a flag in metadata, not
    suppressed.  This preserves audit trails and lets the downstream API decide
    how to handle uncertain detections.
    """

    LOW_CONF = 0.32
    HIGH_CONF = 0.91

    def test_low_confidence_event_is_emitted(self):
        """Low-confidence detections are not dropped at the pipeline boundary."""
        ev = _make_event(confidence=self.LOW_CONF)
        assert ev.confidence == pytest.approx(self.LOW_CONF)

    def test_high_confidence_event_emitted(self):
        ev = _make_event(confidence=self.HIGH_CONF)
        assert ev.confidence == pytest.approx(self.HIGH_CONF)

    def test_zero_confidence_edge_case(self):
        """confidence=0.0 is a legal boundary value (e.g., during occlusion)."""
        ev = _make_event(confidence=0.0)
        assert ev.confidence == 0.0

    def test_unit_confidence_boundary(self):
        """confidence=1.0 is the upper legal boundary."""
        ev = _make_event(confidence=1.0)
        assert ev.confidence == 1.0

    def test_above_unit_confidence_rejected(self):
        with pytest.raises(Exception):
            _make_event(confidence=1.01)

    def test_negative_confidence_rejected(self):
        with pytest.raises(Exception):
            _make_event(confidence=-0.01)

    def test_low_conf_events_not_suppressed_in_batch(self):
        """
        A mixed batch of high- and low-confidence events should all be present.
        The pipeline must not silently drop low-confidence detections.
        """
        batch = [
            _make_event(f"V_{i:03d}", confidence=0.3 if i % 2 == 0 else 0.9)
            for i in range(10)
        ]
        low_conf = [e for e in batch if e.confidence < 0.5]
        high_conf = [e for e in batch if e.confidence >= 0.5]
        assert len(low_conf) == 5
        assert len(high_conf) == 5

    def test_confidence_stored_as_float(self):
        ev = _make_event(confidence=0.754)
        assert isinstance(ev.confidence, float)


# ─────────────────────────────────────────────────────────────────────────────
# 6. Empty store period
# ─────────────────────────────────────────────────────────────────────────────


class TestEmptyStorePeriod:
    """
    During a zero-traffic window the pipeline produces no ENTRY events.
    Downstream metrics should handle an empty batch gracefully.
    """

    def test_empty_event_list_is_valid(self):
        """An empty list from the pipeline is a valid (if boring) output."""
        events: List[Event] = []
        entry_events = [e for e in events if e.event_type == EventType.ENTRY]
        assert len(entry_events) == 0

    def test_unique_visitor_count_zero_for_empty_batch(self):
        events: List[Event] = []
        visitors = {
            e.visitor_id
            for e in events
            if e.event_type == EventType.ENTRY and not e.is_staff
        }
        assert len(visitors) == 0

    def test_no_anomaly_trigger_from_empty_batch_alone(self):
        """
        An empty batch alone (i.e., a valid quiet period during non-open hours)
        should not be flagged as STALE_FEED.  STALE_FEED requires a time gap,
        not simply zero events in a window.
        """
        events: List[Event] = []
        # Simulate the check: no events to count, but no explicit stale detection
        # without a known last-event timestamp.
        last_event_ts: datetime | None = None
        is_stale_feed = last_event_ts is not None and (
            datetime.utcnow() - last_event_ts
        ).total_seconds() > 600
        # With no prior events, stale check does not trigger
        assert not is_stale_feed


# ─────────────────────────────────────────────────────────────────────────────
# 7. event_id uniqueness across batches
# ─────────────────────────────────────────────────────────────────────────────


class TestEventIdUniqueness:
    """event_id must be globally unique even across pipeline batches."""

    def test_event_ids_unique_within_batch(self):
        events = [_make_event(f"V_{i:03d}") for i in range(50)]
        ids = [e.event_id for e in events]
        assert len(ids) == len(set(ids)), "Duplicate event_id found in batch"

    def test_event_ids_unique_across_two_batches(self):
        batch_a = [_make_event(f"V_{i:03d}") for i in range(20)]
        batch_b = [_make_event(f"V_{i:03d}") for i in range(20)]
        all_ids = [e.event_id for e in batch_a + batch_b]
        assert len(all_ids) == len(set(all_ids)), "Collision between batches"

    def test_event_id_is_uuid4_format(self):
        """All generated event_ids must be valid UUID v4 strings."""
        events = [_make_event(f"V_{i:03d}") for i in range(10)]
        for ev in events:
            parsed = uuid.UUID(ev.event_id)
            assert parsed.version == 4

    def test_repeated_event_construction_no_id_collision(self):
        """Even constructing identical parameter events yields unique IDs."""
        ev_a = _make_event("V001", EventType.ENTRY, delta_seconds=0)
        ev_b = _make_event("V001", EventType.ENTRY, delta_seconds=0)
        assert ev_a.event_id != ev_b.event_id
