"""
detect.py – Main detection + tracking script for the Purplle CCTV pipeline.

Processes CCTV footage frame-by-frame:
  - Person detection via YOLOv8n (auto-downloaded) with graceful fallback to
    a rule-based mock generator when ultralytics is not installed.
  - Entry/Exit line-crossing detection on entry cameras.
  - Zone classification via frame-quadrant mapping.
  - Group detection, occlusion handling, dwell tracking, billing queue events.
  - Re-ID via VisitorTracker (tracker.py).
  - Event emission via EventEmitter (emit.py).

Usage
-----
    python detect.py \\
        --video     "/path/to/CAM 1.mp4" \\
        --camera-id CAM_ENTRY_01 \\
        --store-id  STORE_BLR_002 \\
        --layout    pipeline/store_layout.json \\
        --sample-every 5 \\
        --start-time 2026-04-10T10:00:00Z \\
        --output-file events/CAM_ENTRY_01_events.jsonl \\
        [--api-url http://localhost:8000]
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from emit import EventEmitter, EVENT_ENTRY, EVENT_EXIT, EVENT_ZONE_ENTER, \
    EVENT_ZONE_EXIT, EVENT_ZONE_DWELL, EVENT_BILLING_QUEUE_JOIN, \
    EVENT_BILLING_QUEUE_ABANDON, EVENT_REENTRY, EVENT_GROUP_ENTRY, \
    DEFAULT_CLIP_START_TIME
from tracker import VisitorTracker

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional YOLOv8 import with graceful fallback
# ---------------------------------------------------------------------------

try:
    from ultralytics import YOLO  # type: ignore
    YOLO_AVAILABLE = True
    logger.info("YOLOv8 (ultralytics) is available.")
except ImportError:
    YOLO_AVAILABLE = False
    logger.warning(
        "ultralytics not installed. Running in MOCK mode – "
        "realistic synthetic events will be generated for API testing."
    )

try:
    import cv2  # type: ignore
    CV2_AVAILABLE = True
except ImportError:
    CV2_AVAILABLE = False
    logger.warning("OpenCV (cv2) not found. Video decoding will use mock mode.")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_SAMPLE_EVERY = 5           # process every Nth frame
ENTRY_LINE_FRACTION = 0.80         # virtual line at 80% of frame height
ENTRY_LINE_HYSTERESIS = 0.05       # ±5% band around line to avoid jitter
DWELL_EMIT_INTERVAL_SECONDS = 30   # emit ZONE_DWELL every 30 s of presence
BILLING_QUEUE_ABANDON_WINDOW = 600 # 10 min without billing exit → abandonment
PERSON_CLASS_ID = 0                # YOLO COCO class id for "person"
LOW_CONFIDENCE_THRESHOLD = 0.40    # below this → emit with low_confidence flag
GROUP_IOU_THRESHOLD = 0.05         # overlapping or very-close boxes → group
MIN_DETECTION_CONF = 0.25          # minimum box confidence to consider


def _review_metadata(
    session: Dict[str, Any],
    low_confidence: bool = False,
    ambiguous_reentry: bool = False,
) -> Dict[str, Any]:
    """Build optional audit metadata for downstream human review workflows."""
    metadata: Dict[str, Any] = {
        "low_confidence": low_confidence,
        "ambiguous_reentry": ambiguous_reentry,
    }
    if session.get("review_flags"):
        metadata["review_flags"] = session["review_flags"]
    if session.get("reentry_match_confidence") is not None:
        metadata["reentry_match_confidence"] = session["reentry_match_confidence"]
    if session.get("staff_reason"):
        metadata["staff_reason"] = session["staff_reason"]
    return metadata


# ---------------------------------------------------------------------------
# Zone mapping helpers
# ---------------------------------------------------------------------------

def load_store_layout(layout_path: str) -> Dict[str, Any]:
    """Load and return the store_layout.json as a dict."""
    with open(layout_path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _quadrant_zone_map(camera_id: str, layout: Dict[str, Any]) -> Dict[str, str]:
    """
    Build a simple quadrant → zone_id mapping for a given camera.

    The frame is divided into 4 quadrants (TL, TR, BL, BR).
    Zones assigned to the camera are spread across quadrants.
    For cameras with > 4 zones, later zones share quadrants.

    Returns dict: {"TL": zone_id, "TR": zone_id, "BL": zone_id, "BR": zone_id}
    """
    quadrants = ["TL", "TR", "BL", "BR"]
    camera_zones = [
        z["id"] for z in layout.get("zones", []) if z.get("camera") == camera_id
    ]

    if not camera_zones:
        return {q: "UNKNOWN" for q in quadrants}

    # Round-robin assign zones to quadrants
    mapping: Dict[str, str] = {}
    for i, q in enumerate(quadrants):
        mapping[q] = camera_zones[i % len(camera_zones)]

    return mapping


def _bbox_to_quadrant(
    bbox: Tuple[float, float, float, float],
    frame_w: int,
    frame_h: int,
) -> str:
    """
    Map a bounding box centre to one of TL/TR/BL/BR quadrants.

    Args:
        bbox:    (x1, y1, x2, y2) in pixels.
        frame_w: Frame width in pixels.
        frame_h: Frame height in pixels.
    """
    cx = (bbox[0] + bbox[2]) / 2.0
    cy = (bbox[1] + bbox[3]) / 2.0
    half_w = frame_w / 2.0
    half_h = frame_h / 2.0
    left = cx < half_w
    top = cy < half_h
    if top and left:
        return "TL"
    if top and not left:
        return "TR"
    if not top and left:
        return "BL"
    return "BR"


# ---------------------------------------------------------------------------
# Line-crossing detection
# ---------------------------------------------------------------------------

class LineCrossingDetector:
    """
    Tracks whether person bounding-box centroids cross the virtual entry/exit line.

    The line is horizontal at `line_y` (0-1 normalised fraction of frame height).
    Crossing top-to-bottom (y increases) → ENTRY.
    Crossing bottom-to-top (y decreases) → EXIT.
    """

    def __init__(self, line_y_fraction: float = ENTRY_LINE_FRACTION) -> None:
        self.line_y = line_y_fraction
        # track_id → last centroid_y (normalised)
        self._last_y: Dict[int, float] = {}

    def update(
        self, track_id: int, bbox_norm: Tuple[float, float, float, float]
    ) -> Optional[str]:
        """
        Update centroid position for track and detect crossing.

        Args:
            track_id:  Integer track id.
            bbox_norm: Normalised (x1, y1, x2, y2) in [0, 1].

        Returns:
            "ENTRY", "EXIT", or None.
        """
        cy = (bbox_norm[1] + bbox_norm[3]) / 2.0
        prev_y = self._last_y.get(track_id)
        self._last_y[track_id] = cy

        if prev_y is None:
            return None

        lo = self.line_y - ENTRY_LINE_HYSTERESIS
        hi = self.line_y + ENTRY_LINE_HYSTERESIS

        if prev_y < lo and cy >= hi:
            return "ENTRY"   # moving downward across line
        if prev_y > hi and cy <= lo:
            return "EXIT"    # moving upward across line
        return None

    def remove(self, track_id: int) -> None:
        self._last_y.pop(track_id, None)


# ---------------------------------------------------------------------------
# Group detection
# ---------------------------------------------------------------------------

def _iou(
    a: Tuple[float, float, float, float],
    b: Tuple[float, float, float, float],
) -> float:
    """Intersection-over-union for two (x1, y1, x2, y2) boxes."""
    ix1 = max(a[0], b[0])
    iy1 = max(a[1], b[1])
    ix2 = min(a[2], b[2])
    iy2 = min(a[3], b[3])
    inter = max(0.0, ix2 - ix1) * max(0.0, iy2 - iy1)
    if inter == 0:
        return 0.0
    area_a = (a[2] - a[0]) * (a[3] - a[1])
    area_b = (b[2] - b[0]) * (b[3] - b[1])
    return inter / (area_a + area_b - inter)


def _proximity_score(
    a: Tuple[float, float, float, float],
    b: Tuple[float, float, float, float],
    threshold: float = GROUP_IOU_THRESHOLD,
) -> bool:
    """
    True if boxes overlap (IoU > threshold) OR their centres are closer than
    50% of the average box width (very close, touching persons).
    """
    if _iou(a, b) > threshold:
        return True
    cx_a = (a[0] + a[2]) / 2
    cx_b = (b[0] + b[2]) / 2
    cy_a = (a[1] + a[3]) / 2
    cy_b = (b[1] + b[3]) / 2
    avg_w = ((a[2] - a[0]) + (b[2] - b[0])) / 2
    dist = ((cx_a - cx_b) ** 2 + (cy_a - cy_b) ** 2) ** 0.5
    return dist < 0.5 * avg_w


def detect_groups(
    track_ids: List[int],
    bboxes: List[Tuple[float, float, float, float]],
) -> List[List[int]]:
    """
    Return list of groups (each group is a list of track_ids that are close/overlapping).
    Single-member groups are excluded.
    """
    n = len(track_ids)
    if n < 2:
        return []

    visited = [False] * n
    groups: List[List[int]] = []

    for i in range(n):
        if visited[i]:
            continue
        group = [track_ids[i]]
        visited[i] = True
        for j in range(i + 1, n):
            if not visited[j] and _proximity_score(bboxes[i], bboxes[j]):
                group.append(track_ids[j])
                visited[j] = True
        if len(group) > 1:
            groups.append(group)

    return groups


# ---------------------------------------------------------------------------
# Zone dwell state machine
# ---------------------------------------------------------------------------

class ZoneDwellTracker:
    """
    Tracks per-visitor dwell time and fires ZONE_DWELL events at intervals.

    State: visitor_id → (zone_id, unix_ts_entered, last_dwell_event_ts)
    """

    def __init__(self, dwell_interval: float = DWELL_EMIT_INTERVAL_SECONDS) -> None:
        self.dwell_interval = dwell_interval
        # visitor_id → {"zone": str, "entered_ts": float, "last_event_ts": float}
        self._state: Dict[str, Dict[str, Any]] = {}

    def update(
        self,
        visitor_id: str,
        zone_id: str,
        current_ts: float,
    ) -> Optional[int]:
        """
        Update zone presence. Returns dwell_ms if a ZONE_DWELL event should be emitted,
        else None.
        """
        state = self._state.get(visitor_id)
        if state is None or state["zone"] != zone_id:
            # New zone
            self._state[visitor_id] = {
                "zone": zone_id,
                "entered_ts": current_ts,
                "last_event_ts": current_ts,
            }
            return None

        elapsed = current_ts - state["last_event_ts"]
        if elapsed >= self.dwell_interval:
            dwell_ms = int(elapsed * 1000)
            state["last_event_ts"] = current_ts
            return dwell_ms

        return None

    def get_current_zone(self, visitor_id: str) -> Optional[str]:
        s = self._state.get(visitor_id)
        return s["zone"] if s else None

    def total_dwell_ms(self, visitor_id: str, zone_id: str, current_ts: float) -> int:
        s = self._state.get(visitor_id)
        if s and s["zone"] == zone_id:
            return int((current_ts - s["entered_ts"]) * 1000)
        return 0

    def remove(self, visitor_id: str, current_ts: float) -> Optional[int]:
        """Remove visitor, returning final dwell_ms if > 0."""
        state = self._state.pop(visitor_id, None)
        if state:
            elapsed = current_ts - state["last_event_ts"]
            if elapsed > 1:
                return int(elapsed * 1000)
        return None


# ---------------------------------------------------------------------------
# Billing queue state machine
# ---------------------------------------------------------------------------

class BillingQueueTracker:
    """
    Tracks visitors in the billing zone for queue depth and abandonment.
    """

    def __init__(self, abandon_window: float = BILLING_QUEUE_ABANDON_WINDOW) -> None:
        self.abandon_window = abandon_window
        # visitor_id → unix_ts entered billing zone
        self._in_queue: Dict[str, float] = {}

    def enter(self, visitor_id: str, ts: float) -> int:
        """Record visitor joining queue. Returns queue depth after joining."""
        self._in_queue[visitor_id] = ts
        return len(self._in_queue)

    def exit(self, visitor_id: str) -> bool:
        """
        Record visitor leaving billing zone.
        Returns True if the visitor had been waiting (clean exit, not abandonment).
        """
        return self._in_queue.pop(visitor_id, None) is not None

    def check_abandonments(self, current_ts: float) -> List[str]:
        """
        Return list of visitor_ids who entered billing but have been waiting
        longer than the abandon window (no POS transaction detected).
        """
        stale = [
            vid for vid, entered_ts in self._in_queue.items()
            if current_ts - entered_ts > self.abandon_window
        ]
        for vid in stale:
            del self._in_queue[vid]
        return stale

    @property
    def queue_depth(self) -> int:
        return len(self._in_queue)


# ---------------------------------------------------------------------------
# Real YOLO detection path
# ---------------------------------------------------------------------------

def _run_yolo_detection(
    video_path: str,
    camera_id: str,
    store_id: str,
    layout: Dict[str, Any],
    emitter: EventEmitter,
    tracker: VisitorTracker,
    sample_every: int,
    is_entry_camera: bool,
    clip_start_time: str,
) -> List[Dict[str, Any]]:
    """
    Process video with YOLOv8n + ByteTrack, emit events, return event list.
    """
    model = YOLO("yolov8n.pt")
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        logger.error("Cannot open video: %s", video_path)
        return []

    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    emitter.fps = fps

    frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    quadrant_map = _quadrant_zone_map(camera_id, layout)
    line_detector = LineCrossingDetector() if is_entry_camera else None
    dwell_tracker = ZoneDwellTracker()
    billing_tracker = BillingQueueTracker()

    # Which zone is billing?
    billing_zone_id = next(
        (z["id"] for z in layout["zones"] if z.get("type") == "billing"), "BILLING"
    )

    frame_idx = 0
    events: List[Dict[str, Any]] = []
    active_zones: Dict[str, str] = {}  # visitor_id → current zone_id
    entry_emitted: set[str] = set()

    logger.info("Processing %s with YOLO (sample_every=%d)…", video_path, sample_every)

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        if frame_idx % sample_every != 0:
            frame_idx += 1
            continue

        current_unix_ts = _frame_ts_unix(frame_idx, fps, clip_start_time)
        frame_ts_iso = emitter.emit_event.__func__  # not used directly, we use frame_number

        # Run YOLO inference
        results = model.track(frame, persist=True, classes=[PERSON_CLASS_ID], verbose=False)

        track_ids_in_frame: List[int] = []
        bboxes_pixel: List[Tuple[float, float, float, float]] = []
        bboxes_norm: List[Tuple[float, float, float, float]] = []
        confs: List[float] = []

        if results and results[0].boxes is not None:
            boxes = results[0].boxes
            for box in boxes:
                conf = float(box.conf[0])
                if conf < MIN_DETECTION_CONF:
                    continue
                tid = int(box.id[0]) if box.id is not None else -1
                x1, y1, x2, y2 = map(float, box.xyxy[0])
                bboxes_pixel.append((x1, y1, x2, y2))
                bboxes_norm.append((
                    x1 / frame_w, y1 / frame_h,
                    x2 / frame_w, y2 / frame_h,
                ))
                track_ids_in_frame.append(tid)
                confs.append(conf)

        # --- Group detection at entry zone ---
        if is_entry_camera and len(track_ids_in_frame) >= 2:
            groups = detect_groups(track_ids_in_frame, bboxes_pixel)
            for group in groups:
                group_vis_ids = []
                for tid in group:
                    idx = track_ids_in_frame.index(tid)
                    vid, _ = tracker.assign_visitor_id(
                        tid, bboxes_norm[idx], frame, current_unix_ts
                    )
                    group_vis_ids.append(vid)
                tracker.mark_group_members(group_vis_ids)
                for member_vid in group_vis_ids:
                    if member_vid in entry_emitted:
                        continue
                    tracker.record_entry(
                        member_vid,
                        _frame_ts_dt(frame_idx, fps, clip_start_time),
                    )
                    ev = emitter.emit_event(
                        EVENT_ENTRY, camera_id,
                        visitor_id=member_vid,
                        frame_number=frame_idx,
                        zone_id="ENTRY_ZONE",
                        confidence=confs[track_ids_in_frame.index(group[0])],
                        group_size=len(group),
                        group_members=group_vis_ids,
                        **_review_metadata(tracker.get_session(member_vid) or {}),
                    )
                    emitter.print_event(ev)
                    events.append(ev)
                    entry_emitted.add(member_vid)
                ev = emitter.emit_event(
                    EVENT_GROUP_ENTRY, camera_id,
                    visitor_id=group_vis_ids[0],
                    frame_number=frame_idx,
                    group_size=len(group),
                    group_members=group_vis_ids,
                    zone_id="ENTRY_ZONE",
                )
                emitter.print_event(ev)
                events.append(ev)

        # --- Per-detection processing ---
        for i, tid in enumerate(track_ids_in_frame):
            bbox_norm = bboxes_norm[i]
            bbox_px = bboxes_pixel[i]
            conf = confs[i]
            low_conf = conf < LOW_CONFIDENCE_THRESHOLD

            visitor_id, is_reentry = tracker.assign_visitor_id(
                tid, bbox_norm, frame, current_unix_ts
            )

            session = tracker.get_session(visitor_id) or {}
            is_staff = session.get("is_staff", False)
            session_seq = session.get("session_seq", 0)
            reentry_conf = session.get("reentry_match_confidence")
            ambiguous_reentry = bool(
                is_reentry and reentry_conf is not None and reentry_conf < 0.85
            )

            if is_entry_camera and not is_reentry and visitor_id not in entry_emitted:
                tracker.record_entry(visitor_id, _frame_ts_dt(frame_idx, fps, clip_start_time))
                ev = emitter.emit_event(
                    EVENT_ENTRY, camera_id, visitor_id,
                    frame_number=frame_idx,
                    zone_id="ENTRY_ZONE",
                    is_staff=is_staff,
                    confidence=conf,
                    session_seq=session_seq,
                    **_review_metadata(session, low_confidence=low_conf),
                )
                emitter.print_event(ev)
                events.append(ev)
                entry_emitted.add(visitor_id)

            # --- Re-entry event ---
            if is_reentry:
                ev = emitter.emit_event(
                    EVENT_REENTRY, camera_id, visitor_id,
                    frame_number=frame_idx,
                    is_staff=is_staff,
                    confidence=conf,
                    session_seq=session_seq,
                    **_review_metadata(
                        session,
                        low_confidence=low_conf,
                        ambiguous_reentry=ambiguous_reentry,
                    ),
                )
                emitter.print_event(ev)
                events.append(ev)

            # --- Entry/Exit line crossing ---
            if line_detector:
                crossing = line_detector.update(tid, bbox_norm)
                if crossing == "ENTRY":
                    if visitor_id not in entry_emitted:
                        tracker.record_entry(visitor_id, _frame_ts_dt(frame_idx, fps, clip_start_time))
                        ev = emitter.emit_event(
                            EVENT_ENTRY, camera_id, visitor_id,
                            frame_number=frame_idx,
                            zone_id="ENTRY_ZONE",
                            is_staff=is_staff,
                            confidence=conf,
                            session_seq=session_seq,
                            **_review_metadata(session, low_confidence=low_conf),
                        )
                        emitter.print_event(ev)
                        events.append(ev)
                        entry_emitted.add(visitor_id)

                elif crossing == "EXIT":
                    tracker.record_exit(
                        visitor_id,
                        _frame_ts_dt(frame_idx, fps, clip_start_time),
                        track_id=tid,
                    )
                    ev = emitter.emit_event(
                        EVENT_EXIT, camera_id, visitor_id,
                        frame_number=frame_idx,
                        zone_id="ENTRY_ZONE",
                        is_staff=is_staff,
                        confidence=conf,
                        session_seq=session_seq,
                        **_review_metadata(session, low_confidence=low_conf),
                    )
                    emitter.print_event(ev)
                    events.append(ev)

            # --- Zone classification ---
            quadrant = _bbox_to_quadrant(bbox_px, frame_w, frame_h)
            zone_id = quadrant_map.get(quadrant, "UNKNOWN")
            prev_zone = active_zones.get(visitor_id)

            if prev_zone != zone_id:
                # Zone exit
                if prev_zone:
                    final_dwell = dwell_tracker.remove(visitor_id, current_unix_ts)
                    if final_dwell and final_dwell > 1000:
                        ev = emitter.emit_event(
                            EVENT_ZONE_DWELL, camera_id, visitor_id,
                            frame_number=frame_idx,
                            zone_id=prev_zone,
                            dwell_ms=final_dwell,
                            is_staff=is_staff,
                            confidence=conf,
                            **_review_metadata(session, low_confidence=low_conf),
                        )
                        emitter.print_event(ev)
                        events.append(ev)
                    ev = emitter.emit_event(
                        EVENT_ZONE_EXIT, camera_id, visitor_id,
                        frame_number=frame_idx,
                        zone_id=prev_zone,
                        is_staff=is_staff,
                        confidence=conf,
                        **_review_metadata(session, low_confidence=low_conf),
                    )
                    emitter.print_event(ev)
                    events.append(ev)
                    if prev_zone == billing_zone_id:
                        billing_tracker.exit(visitor_id)

                # Zone enter
                active_zones[visitor_id] = zone_id
                tracker.record_zone(visitor_id, zone_id, _frame_ts_dt(frame_idx, fps, clip_start_time))
                session = tracker.get_session(visitor_id) or session
                is_staff = session.get("is_staff", False)
                session_seq = session.get("session_seq", session_seq)
                queue_depth = billing_tracker.queue_depth

                if zone_id == billing_zone_id:
                    depth = billing_tracker.enter(visitor_id, current_unix_ts)
                    ev = emitter.emit_event(
                        EVENT_BILLING_QUEUE_JOIN, camera_id, visitor_id,
                        frame_number=frame_idx,
                        zone_id=zone_id,
                        queue_depth=depth,
                        is_staff=is_staff,
                        confidence=conf,
                        session_seq=session_seq,
                        **_review_metadata(session, low_confidence=low_conf),
                    )
                    emitter.print_event(ev)
                    events.append(ev)
                else:
                    ev = emitter.emit_event(
                        EVENT_ZONE_ENTER, camera_id, visitor_id,
                        frame_number=frame_idx,
                        zone_id=zone_id,
                        queue_depth=queue_depth,
                        is_staff=is_staff,
                        confidence=conf,
                        session_seq=session_seq,
                        **_review_metadata(session, low_confidence=low_conf),
                    )
                    emitter.print_event(ev)
                    events.append(ev)

            # --- Dwell interval check ---
            dwell_ms = dwell_tracker.update(visitor_id, zone_id, current_unix_ts)
            if dwell_ms is not None:
                ev = emitter.emit_event(
                    EVENT_ZONE_DWELL, camera_id, visitor_id,
                    frame_number=frame_idx,
                    zone_id=zone_id,
                    dwell_ms=dwell_ms,
                    is_staff=is_staff,
                    confidence=conf,
                    session_seq=session_seq,
                    **_review_metadata(session, low_confidence=low_conf),
                )
                emitter.print_event(ev)
                events.append(ev)

        # --- Billing queue abandonment check (every 60 frames) ---
        if frame_idx % (sample_every * 60) == 0:
            abandoned = billing_tracker.check_abandonments(current_unix_ts)
            for vid in abandoned:
                ev = emitter.emit_event(
                    EVENT_BILLING_QUEUE_ABANDON, camera_id, vid,
                    frame_number=frame_idx,
                    zone_id=billing_zone_id,
                    queue_depth=billing_tracker.queue_depth,
                    is_staff=False,
                    confidence=0.9,
                )
                emitter.print_event(ev)
                events.append(ev)

        # --- Release stale tracks ---
        released = tracker.release_stale_tracks(current_unix_ts, timeout=15.0)
        for vid in released:
            active_zones.pop(vid, None)

        frame_idx += 1

    cap.release()
    logger.info("Finished processing %s. Total events: %d", video_path, len(events))
    return events


# ---------------------------------------------------------------------------
# Mock detection path (no YOLOv8 / OpenCV)
# ---------------------------------------------------------------------------

def _run_mock_detection(
    video_path: str,
    camera_id: str,
    store_id: str,
    layout: Dict[str, Any],
    emitter: EventEmitter,
    tracker: VisitorTracker,
    clip_start_time: str,
) -> List[Dict[str, Any]]:
    """
    Generate realistic synthetic events for a given camera when YOLO/CV2 is unavailable.

    Simulates a full day of store traffic using configurable probability distributions.
    """
    rng = random.Random(hash(camera_id) & 0xFFFFFFFF)

    start_dt = datetime.fromisoformat(clip_start_time.replace("Z", "+00:00"))
    store_open_ts = start_dt.timestamp()
    # Simulate 12 hours of footage
    store_close_ts = store_open_ts + 12 * 3600

    # Determine camera role
    is_entry_cam = "ENTRY" in camera_id
    is_billing_cam = "BILLING" in camera_id

    # Zones associated with this camera
    cam_zones = [z["id"] for z in layout.get("zones", []) if z.get("camera") == camera_id]
    if not cam_zones:
        cam_zones = ["UNKNOWN"]

    billing_zone = next(
        (z["id"] for z in layout.get("zones", []) if z.get("type") == "billing"), "BILLING"
    )

    events: List[Dict[str, Any]] = []

    # Simulate ~150 visitors over the day with realistic arrival distribution
    n_visitors = rng.randint(120, 180)
    # Peak hours: 12-14 and 17-20
    arrival_times = _simulate_arrivals(store_open_ts, store_close_ts, n_visitors, rng)

    visitor_pool: List[str] = []
    for _ in range(n_visitors):
        import hashlib
        seed = f"{camera_id}_{rng.random()}"
        vid = "VIS_" + hashlib.sha256(seed.encode()).hexdigest()[:8].upper()
        visitor_pool.append(vid)

    # Process each visitor
    for visitor_id, arrival_ts in zip(visitor_pool, arrival_times):
        # --- ENTRY event (entry cameras only) ---
        if is_entry_cam:
            entry_dt = datetime.fromtimestamp(arrival_ts, tz=timezone.utc)
            is_staff = rng.random() < 0.03
            ev = emitter.emit_event(
                EVENT_ENTRY, camera_id, visitor_id,
                timestamp=entry_dt.isoformat(),
                zone_id="ENTRY_ZONE",
                is_staff=is_staff,
                confidence=round(rng.uniform(0.6, 0.99), 3),
            )
            emitter.print_event(ev)
            events.append(ev)

        # --- Zone visits ---
        n_zones = rng.randint(1, len(cam_zones))
        zones_to_visit = rng.sample(cam_zones, n_zones)
        current_ts = arrival_ts + rng.uniform(30, 120)

        for zone_id in zones_to_visit:
            zone_enter_dt = datetime.fromtimestamp(current_ts, tz=timezone.utc)

            if zone_id == billing_zone:
                # Billing queue join
                queue_depth = rng.randint(0, 5)
                ev = emitter.emit_event(
                    EVENT_BILLING_QUEUE_JOIN, camera_id, visitor_id,
                    timestamp=zone_enter_dt.isoformat(),
                    zone_id=zone_id,
                    queue_depth=queue_depth,
                    confidence=round(rng.uniform(0.7, 0.99), 3),
                )
                emitter.print_event(ev)
                events.append(ev)

                # 10% chance of abandonment
                if rng.random() < 0.10:
                    abandon_ts = current_ts + rng.uniform(120, 600)
                    abandon_dt = datetime.fromtimestamp(abandon_ts, tz=timezone.utc)
                    ev = emitter.emit_event(
                        EVENT_BILLING_QUEUE_ABANDON, camera_id, visitor_id,
                        timestamp=abandon_dt.isoformat(),
                        zone_id=zone_id,
                        queue_depth=max(0, queue_depth - 1),
                        confidence=round(rng.uniform(0.6, 0.95), 3),
                    )
                    emitter.print_event(ev)
                    events.append(ev)
                    current_ts = abandon_ts
                    continue
            else:
                # Zone enter
                ev = emitter.emit_event(
                    EVENT_ZONE_ENTER, camera_id, visitor_id,
                    timestamp=zone_enter_dt.isoformat(),
                    zone_id=zone_id,
                    confidence=round(rng.uniform(0.55, 0.99), 3),
                )
                emitter.print_event(ev)
                events.append(ev)

            # Dwell time: 2-10 min per zone
            dwell_seconds = rng.uniform(120, 600)
            # Emit ZONE_DWELL every 30 s of presence
            dwell_emitted = 0
            dwell_cursor = current_ts
            while dwell_cursor - current_ts < dwell_seconds - 30:
                dwell_cursor += 30.0
                dwell_dt = datetime.fromtimestamp(dwell_cursor, tz=timezone.utc)
                ev = emitter.emit_event(
                    EVENT_ZONE_DWELL, camera_id, visitor_id,
                    timestamp=dwell_dt.isoformat(),
                    zone_id=zone_id,
                    dwell_ms=30000,
                    confidence=round(rng.uniform(0.6, 0.99), 3),
                )
                emitter.print_event(ev)
                events.append(ev)
                dwell_emitted += 1
                # Limit to avoid event explosion in mock
                if dwell_emitted >= 3:
                    break

            current_ts += dwell_seconds

        # --- EXIT event (entry cameras only) ---
        if is_entry_cam:
            exit_dt = datetime.fromtimestamp(current_ts + rng.uniform(10, 60), tz=timezone.utc)
            ev = emitter.emit_event(
                EVENT_EXIT, camera_id, visitor_id,
                timestamp=exit_dt.isoformat(),
                zone_id="ENTRY_ZONE",
                confidence=round(rng.uniform(0.6, 0.99), 3),
            )
            emitter.print_event(ev)
            events.append(ev)

    logger.info(
        "[MOCK] Generated %d synthetic events for %s", len(events), camera_id
    )
    return events


def _simulate_arrivals(
    open_ts: float, close_ts: float, n: int, rng: random.Random
) -> List[float]:
    """
    Generate n realistic arrival timestamps with peak-hour weighting.

    Peaks at 12:00-14:00 and 17:00-20:00 (offsets from open_ts).
    """
    duration = close_ts - open_ts
    arrivals: List[float] = []
    for _ in range(n):
        # Choose peak or off-peak
        r = rng.random()
        if r < 0.35:
            # Morning peak (2-4h into day)
            frac = rng.gauss(3 / 12, 0.5 / 12)
        elif r < 0.70:
            # Evening peak (7-10h into day)
            frac = rng.gauss(8.5 / 12, 0.8 / 12)
        else:
            frac = rng.uniform(0, 1)
        frac = max(0.0, min(1.0, frac))
        arrivals.append(open_ts + frac * duration)
    arrivals.sort()
    return arrivals


# ---------------------------------------------------------------------------
# Timestamp helpers (used inside detection loop)
# ---------------------------------------------------------------------------

def _frame_ts_unix(frame_idx: int, fps: float, clip_start_time: str) -> float:
    """Return Unix timestamp for a frame index."""
    start_dt = datetime.fromisoformat(clip_start_time.replace("Z", "+00:00"))
    return start_dt.timestamp() + frame_idx / fps


def _frame_ts_dt(frame_idx: int, fps: float, clip_start_time: str) -> datetime:
    """Return aware datetime for a frame index."""
    return datetime.fromtimestamp(
        _frame_ts_unix(frame_idx, fps, clip_start_time), tz=timezone.utc
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Purplle CCTV detection pipeline")
    parser.add_argument("--video", required=True, help="Path to input MP4 file")
    parser.add_argument("--camera-id", required=True, help="Logical camera ID (e.g. CAM_ENTRY_01)")
    parser.add_argument("--store-id", default="STORE_BLR_002", help="Store identifier")
    parser.add_argument(
        "--layout",
        default=str(Path(__file__).parent / "store_layout.json"),
        help="Path to store_layout.json",
    )
    parser.add_argument("--sample-every", type=int, default=DEFAULT_SAMPLE_EVERY,
                        help="Process every Nth frame (default: 5)")
    parser.add_argument("--start-time", default=DEFAULT_CLIP_START_TIME,
                        help="Clip wall-clock start time (ISO-8601 UTC)")
    parser.add_argument("--output-file", default=None,
                        help="Write events to this JSONL file (in addition to stdout)")
    parser.add_argument("--api-url", default=None,
                        help="POST events to this base URL (e.g. http://localhost:8000)")
    parser.add_argument("--mock", action="store_true",
                        help="Force mock mode even if YOLO is available")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stderr,
    )

    layout = load_store_layout(args.layout)

    emitter = EventEmitter(
        store_id=args.store_id,
        clip_start_time=args.start_time,
    )

    tracker = VisitorTracker(
        clip_start_time=datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
    )

    is_entry_camera = "ENTRY" in args.camera_id

    use_mock = args.mock or not YOLO_AVAILABLE or not CV2_AVAILABLE

    if use_mock:
        logger.info("Running in MOCK mode for camera %s", args.camera_id)
        events = _run_mock_detection(
            video_path=args.video,
            camera_id=args.camera_id,
            store_id=args.store_id,
            layout=layout,
            emitter=emitter,
            tracker=tracker,
            clip_start_time=args.start_time,
        )
    else:
        logger.info("Running YOLO detection for camera %s", args.camera_id)
        events = _run_yolo_detection(
            video_path=args.video,
            camera_id=args.camera_id,
            store_id=args.store_id,
            layout=layout,
            emitter=emitter,
            tracker=tracker,
            sample_every=args.sample_every,
            is_entry_camera=is_entry_camera,
            clip_start_time=args.start_time,
        )

    # Write to file
    if args.output_file:
        emitter.emit_to_file(events, args.output_file)
        logger.info("Wrote %d events to %s", len(events), args.output_file)

    # POST to API
    if args.api_url:
        result = emitter.emit_to_api(events, args.api_url)
        logger.info("API result: sent=%d failed=%d", result["sent"], result["failed"])

    logger.info("Done. Total events: %d", len(events))


if __name__ == "__main__":
    main()
