"""
tracker.py – Re-ID and visitor session management for Purplle CCTV pipeline.

Each YOLO track_id is ephemeral (lost on occlusion or frame gap). This module
maps ephemeral track_ids → stable visitor_ids using appearance-feature Re-ID,
then maintains a full session lifecycle per visitor.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import DefaultDict, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

RE_ENTRY_WINDOW_SECONDS: int = 30 * 60          # 30 minutes
STAFF_MIN_SESSION_HOURS: float = 4.0             # sessions longer than this → staff
STAFF_ZONE_CROSSING_RATE: float = 0.5            # zone changes per minute above this → staff
STAFF_FAST_ZONE_WINDOW_SECONDS: float = 90.0      # staff-like multi-zone sweep window
STAFF_FAST_ZONE_DISTINCT_MIN: int = 3             # distinct zones within the sweep window
FEATURE_MATCH_THRESHOLD: float = 0.75            # cosine similarity to re-id
REENTRY_REVIEW_THRESHOLD: float = 0.85            # below this, keep but flag for review
LOST_TRACK_TIMEOUT_SECONDS: float = 10.0         # seconds before a track is considered lost


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TrackRecord:
    """Short-lived track kept until it is confirmed lost."""
    track_id: int
    visitor_id: str
    last_bbox: Tuple[float, float, float, float]   # x1, y1, x2, y2 (normalised 0-1)
    last_seen_ts: float                             # Unix timestamp
    feature: Optional[np.ndarray] = None
    bbox_history: List[Tuple[float, float, float, float]] = field(default_factory=list)


@dataclass
class VisitorSession:
    """Full session for a single store visit."""
    visitor_id: str
    entry_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    zones_visited: List[str] = field(default_factory=list)
    zone_entry_times: Dict[str, datetime] = field(default_factory=dict)
    zone_dwell_ms: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))
    is_staff: bool = False
    staff_reason: Optional[str] = None
    is_group_member: bool = False
    session_seq: int = 0                            # increments on re-entry
    reentry_count: int = 0
    last_reentry_match_confidence: Optional[float] = None
    review_flags: List[str] = field(default_factory=list)
    _zone_change_times: List[Tuple[str, float]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Feature extraction helpers
# ---------------------------------------------------------------------------

def _extract_histogram_feature(frame: np.ndarray, bbox: Tuple[float, float, float, float]) -> np.ndarray:
    """
    Extract a simple colour histogram from the bounding-box crop.

    Uses HSV colour space, 16 bins per channel, concatenated and L2-normalised.
    This is the fallback when a deep Re-ID encoder is not available.

    Args:
        frame: BGR uint8 image.
        bbox: Normalised (x1, y1, x2, y2) in [0, 1].

    Returns:
        1-D float32 numpy array (L2-normalised histogram feature).
    """
    try:
        import cv2  # type: ignore
        h, w = frame.shape[:2]
        x1 = max(0, int(bbox[0] * w))
        y1 = max(0, int(bbox[1] * h))
        x2 = min(w, int(bbox[2] * w))
        y2 = min(h, int(bbox[3] * h))

        if x2 <= x1 or y2 <= y1:
            return np.zeros(48, dtype=np.float32)

        crop = frame[y1:y2, x1:x2]
        hsv = cv2.cvtColor(crop, cv2.COLOR_BGR2HSV)

        features = []
        for ch in range(3):
            hist = cv2.calcHist([hsv], [ch], None, [16], [0, 256])
            features.append(hist.flatten())

        feat = np.concatenate(features).astype(np.float32)
        norm = np.linalg.norm(feat)
        if norm > 0:
            feat /= norm
        return feat

    except Exception as exc:  # pragma: no cover
        logger.warning("Feature extraction failed: %s", exc)
        return np.zeros(48, dtype=np.float32)


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Return cosine similarity in [-1, 1]. Returns 0.0 on zero vectors."""
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na < 1e-9 or nb < 1e-9:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def _visitor_id_from_hash(seed: str) -> str:
    """Derive a deterministic 8-char uppercase visitor hash from a seed string."""
    digest = hashlib.sha256(seed.encode()).hexdigest()[:8].upper()
    return f"VIS_{digest}"


# ---------------------------------------------------------------------------
# Main tracker class
# ---------------------------------------------------------------------------

class VisitorTracker:
    """
    Maps ephemeral YOLO track_ids to stable visitor_ids and manages sessions.

    Usage
    -----
    tracker = VisitorTracker(clip_start_time=datetime(..., tzinfo=timezone.utc))
    visitor_id, is_reentry = tracker.assign_visitor_id(track_id, bbox, frame)
    session = tracker.get_session(visitor_id)
    tracker.record_zone(visitor_id, zone_id, timestamp)
    tracker.record_exit(visitor_id, timestamp)
    """

    def __init__(
        self,
        clip_start_time: Optional[datetime] = None,
        re_entry_window: int = RE_ENTRY_WINDOW_SECONDS,
        feature_match_threshold: float = FEATURE_MATCH_THRESHOLD,
    ) -> None:
        self.clip_start_time: datetime = clip_start_time or datetime(
            2026, 4, 10, 10, 0, 0, tzinfo=timezone.utc
        )
        self.re_entry_window = re_entry_window
        self.feature_match_threshold = feature_match_threshold

        # track_id → TrackRecord (active in current frame window)
        self._active_tracks: Dict[int, TrackRecord] = {}

        # visitor_id → VisitorSession
        self._sessions: Dict[str, VisitorSession] = {}

        # Graveyard: recently exited visitor_ids with their features + exit_time
        # Used for re-entry matching.  List of (visitor_id, feature, exit_unix_ts)
        self._exited: List[Tuple[str, Optional[np.ndarray], float]] = []

        # Counter used to generate unique seeds when no frame available
        self._uid_counter: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def assign_visitor_id(
        self,
        track_id: int,
        bbox: Tuple[float, float, float, float],
        frame: Optional[np.ndarray],
        timestamp: Optional[float] = None,
    ) -> Tuple[str, bool]:
        """
        Given a (possibly new) YOLO track_id and current bounding box, return a
        stable visitor_id and a flag indicating whether this is a re-entry.

        The function:
        1. If track_id already known → return existing visitor_id.
        2. Extract appearance feature from frame crop.
        3. Search exited visitors within re_entry_window for a feature match.
           If found → mark as re-entry and resume session.
        4. Otherwise create a new visitor_id / session.

        Args:
            track_id:  YOLO sort/bytetrack integer ID.
            bbox:      Normalised (x1, y1, x2, y2) bounding box.
            frame:     BGR numpy image (may be None in mock mode).
            timestamp: Unix timestamp of this frame (defaults to now).

        Returns:
            (visitor_id, is_reentry)
        """
        now = timestamp or time.time()

        # --- Already tracking this track_id ---
        if track_id in self._active_tracks:
            record = self._active_tracks[track_id]
            record.last_bbox = bbox
            record.last_seen_ts = now
            record.bbox_history.append(bbox)
            if frame is not None:
                feature = _extract_histogram_feature(frame, bbox)
                if record.feature is None:
                    record.feature = feature
                elif np.linalg.norm(feature) > 1e-9:
                    blended = (record.feature * 0.8) + (feature * 0.2)
                    norm = np.linalg.norm(blended)
                    if norm > 1e-9:
                        record.feature = blended / norm
            return record.visitor_id, False

        # --- New track_id: extract feature ---
        feature: Optional[np.ndarray] = None
        if frame is not None:
            feature = _extract_histogram_feature(frame, bbox)

        # --- Check re-entry against exited pool ---
        visitor_id, is_reentry = self._match_reentry(feature, now)

        if not is_reentry:
            visitor_id = self._create_visitor_id(track_id, now)
            session = VisitorSession(visitor_id=visitor_id)
            self._sessions[visitor_id] = session

        # --- Register active track ---
        record = TrackRecord(
            track_id=track_id,
            visitor_id=visitor_id,
            last_bbox=bbox,
            last_seen_ts=now,
            feature=feature,
            bbox_history=[bbox],
        )
        self._active_tracks[track_id] = record

        return visitor_id, is_reentry

    def get_session(self, visitor_id: str) -> Optional[Dict]:
        """
        Return session information for visitor_id as a plain dict.

        Keys: entry_time, exit_time, zones_visited, zone_dwell_ms,
              is_staff, is_group_member, session_seq.
        Returns None if visitor_id not found.
        """
        session = self._sessions.get(visitor_id)
        if session is None:
            return None
        return {
            "visitor_id": visitor_id,
            "entry_time": session.entry_time.isoformat() if session.entry_time else None,
            "exit_time": session.exit_time.isoformat() if session.exit_time else None,
            "zones_visited": list(session.zones_visited),
            "zone_dwell_ms": dict(session.zone_dwell_ms),
            "is_staff": session.is_staff,
            "staff_reason": session.staff_reason,
            "is_group_member": session.is_group_member,
            "session_seq": session.session_seq,
            "reentry_count": session.reentry_count,
            "reentry_match_confidence": session.last_reentry_match_confidence,
            "review_flags": list(session.review_flags),
        }

    def record_entry(self, visitor_id: str, timestamp: datetime) -> None:
        """Mark the visitor's entry time (call once per visit)."""
        session = self._sessions.get(visitor_id)
        if session and session.entry_time is None:
            session.entry_time = timestamp

    def record_zone(self, visitor_id: str, zone_id: str, timestamp: datetime) -> None:
        """
        Record a zone visit.  Tracks dwell start time and accumulates dwell_ms.
        Also updates zone-crossing rate to detect staff.
        """
        session = self._sessions.get(visitor_id)
        if session is None:
            return

        if zone_id not in session.zones_visited:
            session.zones_visited.append(zone_id)

        if zone_id not in session.zone_entry_times:
            session.zone_entry_times[zone_id] = timestamp
        session._zone_change_times.append((zone_id, timestamp.timestamp()))

        self._evaluate_staff(session)

    def record_exit(self, visitor_id: str, timestamp: datetime, track_id: Optional[int] = None) -> None:
        """
        Mark the visitor as exited, move them to the graveyard for re-entry matching.
        """
        session = self._sessions.get(visitor_id)
        if session:
            session.exit_time = timestamp
            self._evaluate_staff(session)

        # Move feature to exited pool
        feature: Optional[np.ndarray] = None
        if track_id is not None and track_id in self._active_tracks:
            feature = self._active_tracks[track_id].feature
            del self._active_tracks[track_id]

        self._exited.append((visitor_id, feature, timestamp.timestamp()))
        self._prune_exited_pool(timestamp.timestamp())

    def release_stale_tracks(self, current_unix_ts: float, timeout: float = LOST_TRACK_TIMEOUT_SECONDS) -> List[str]:
        """
        Remove tracks that haven't been seen for `timeout` seconds.

        Returns list of visitor_ids whose tracks were released (not exited —
        caller may want to emit implicit EXIT events).
        """
        stale = [
            tid for tid, rec in self._active_tracks.items()
            if current_unix_ts - rec.last_seen_ts > timeout
        ]
        released_visitors: List[str] = []
        for tid in stale:
            rec = self._active_tracks.pop(tid)
            released_visitors.append(rec.visitor_id)
            # Move to exited pool
            self._exited.append((rec.visitor_id, rec.feature, current_unix_ts))
        self._prune_exited_pool(current_unix_ts)
        return released_visitors

    def mark_group_members(self, visitor_ids: List[str]) -> None:
        """Flag a set of visitor_ids as belonging to a group."""
        for vid in visitor_ids:
            session = self._sessions.get(vid)
            if session:
                session.is_group_member = True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _match_reentry(
        self, feature: Optional[np.ndarray], now: float
    ) -> Tuple[str, bool]:
        """
        Search the exited pool for a feature match within the re-entry window.
        Returns (visitor_id, True) on match, or (empty_string, False) on miss.
        """
        best_sim = -1.0
        best_vid = ""
        best_idx: Optional[int] = None

        for idx, (visitor_id, ex_feature, exit_ts) in enumerate(self._exited):
            if now - exit_ts > self.re_entry_window:
                continue
            if feature is None or ex_feature is None:
                continue
            sim = _cosine_similarity(feature, ex_feature)
            if sim > best_sim:
                best_sim = sim
                best_vid = visitor_id
                best_idx = idx

        if best_sim >= self.feature_match_threshold and best_vid:
            if best_idx is not None:
                self._exited.pop(best_idx)
            # Resume existing session with incremented seq
            session = self._sessions.get(best_vid)
            if session:
                session.session_seq += 1
                session.reentry_count += 1
                session.last_reentry_match_confidence = round(best_sim, 4)
                session.exit_time = None  # re-opened
                if best_sim < REENTRY_REVIEW_THRESHOLD:
                    self._add_review_flag(session, "AMBIGUOUS_REENTRY_MATCH")
            return best_vid, True

        return "", False

    def _create_visitor_id(self, track_id: int, ts: float) -> str:
        """Generate a new unique visitor_id."""
        self._uid_counter += 1
        seed = f"{track_id}_{ts}_{self._uid_counter}"
        return _visitor_id_from_hash(seed)

    def _evaluate_staff(self, session: VisitorSession) -> None:
        """
        Heuristic staff detection:
        - Session duration > STAFF_MIN_SESSION_HOURS, OR
        - Zone crossing rate > STAFF_ZONE_CROSSING_RATE changes/minute.
        """
        if session.is_staff:
            return  # already flagged

        # Duration check
        if session.entry_time and session.exit_time:
            duration_hours = (
                session.exit_time - session.entry_time
            ).total_seconds() / 3600.0
            if duration_hours >= STAFF_MIN_SESSION_HOURS:
                self._mark_staff(session, "LONG_SESSION")
                return

        # Zone crossing rate check
        times = session._zone_change_times
        if len(times) >= 4:
            window = times[-1][1] - times[0][1]
            if window > 0:
                rate = (len(times) - 1) / (window / 60.0)  # changes per minute
                if rate > STAFF_ZONE_CROSSING_RATE:
                    self._mark_staff(session, "HIGH_ZONE_CROSSING_RATE")

        # Staff often sweep across multiple departments quickly; customers usually dwell.
        if len(times) >= STAFF_FAST_ZONE_DISTINCT_MIN:
            latest_ts = times[-1][1]
            recent = [
                zone for zone, ts in times
                if latest_ts - ts <= STAFF_FAST_ZONE_WINDOW_SECONDS
            ]
            if len(set(recent)) >= STAFF_FAST_ZONE_DISTINCT_MIN:
                self._mark_staff(session, "FAST_MULTI_ZONE_SWEEP")

    def _mark_staff(self, session: VisitorSession, reason: str) -> None:
        session.is_staff = True
        session.staff_reason = session.staff_reason or reason
        self._add_review_flag(session, f"STAFF_{reason}")

    def _add_review_flag(self, session: VisitorSession, flag: str) -> None:
        if flag not in session.review_flags:
            session.review_flags.append(flag)

    def _prune_exited_pool(self, now: float) -> None:
        """Remove entries from the exited pool that are outside the re-entry window."""
        cutoff = now - self.re_entry_window
        self._exited = [
            entry for entry in self._exited if entry[2] >= cutoff
        ]
