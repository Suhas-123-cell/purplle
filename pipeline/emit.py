"""
emit.py – Event schema definition and emission utilities for the Purplle CCTV pipeline.

All events produced by detect.py pass through this module before being written
to JSONL files or POSTed to the ingestion API.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests  # type: ignore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Event type constants
# ---------------------------------------------------------------------------

EVENT_ENTRY = "ENTRY"
EVENT_EXIT = "EXIT"
EVENT_ZONE_ENTER = "ZONE_ENTER"
EVENT_ZONE_EXIT = "ZONE_EXIT"
EVENT_ZONE_DWELL = "ZONE_DWELL"
EVENT_BILLING_QUEUE_JOIN = "BILLING_QUEUE_JOIN"
EVENT_BILLING_QUEUE_ABANDON = "BILLING_QUEUE_ABANDON"
EVENT_REENTRY = "REENTRY"
EVENT_GROUP_ENTRY = "GROUP_ENTRY"

KNOWN_EVENT_TYPES = {
    EVENT_ENTRY, EVENT_EXIT, EVENT_ZONE_ENTER, EVENT_ZONE_EXIT,
    EVENT_ZONE_DWELL, EVENT_BILLING_QUEUE_JOIN, EVENT_BILLING_QUEUE_ABANDON,
    EVENT_REENTRY, EVENT_GROUP_ENTRY,
}

# Default clip start time assumed when no wall-clock metadata is embedded
DEFAULT_CLIP_START_TIME = "2026-04-10T10:00:00Z"

# API configuration
API_BATCH_SIZE = 100
API_TIMEOUT_SECONDS = 15
LOW_CONFIDENCE_REVIEW_THRESHOLD = 0.40
MEDIUM_CONFIDENCE_THRESHOLD = 0.65


def _confidence_bucket(confidence: float) -> str:
    if confidence < LOW_CONFIDENCE_REVIEW_THRESHOLD:
        return "LOW"
    if confidence < MEDIUM_CONFIDENCE_THRESHOLD:
        return "MEDIUM"
    return "HIGH"


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def frame_to_timestamp(
    frame_number: int,
    fps: float,
    clip_start_time: Optional[str] = None,
) -> str:
    """
    Convert a frame number + FPS into an ISO-8601 UTC timestamp string.

    Args:
        frame_number:    Zero-based frame index within the clip.
        fps:             Frames per second of the source video.
        clip_start_time: ISO-8601 string representing when the clip started.
                         Defaults to DEFAULT_CLIP_START_TIME.

    Returns:
        ISO-8601 UTC timestamp, e.g. "2026-04-10T10:01:30.500000+00:00".
    """
    start_str = clip_start_time or DEFAULT_CLIP_START_TIME
    # Parse to aware datetime
    start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
    offset_seconds = frame_number / fps if fps > 0 else 0.0
    # Use timedelta for sub-second precision
    from datetime import timedelta
    ts = start_dt + timedelta(seconds=offset_seconds)
    return ts.isoformat()


def now_utc_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Event schema builder
# ---------------------------------------------------------------------------

def build_event(
    event_type: str,
    store_id: str,
    camera_id: str,
    visitor_id: str,
    timestamp: str,
    zone_id: Optional[str] = None,
    dwell_ms: int = 0,
    is_staff: bool = False,
    confidence: float = 1.0,
    queue_depth: int = 0,
    sku_zone: Optional[str] = None,
    session_seq: int = 0,
    **extra_metadata: Any,
) -> Dict[str, Any]:
    """
    Construct a validated event dict matching the canonical schema.

    Schema
    ------
    {
        "event_id":   str (uuid4),
        "store_id":   str,
        "camera_id":  str,
        "visitor_id": str,
        "event_type": str,
        "timestamp":  str (ISO-8601 UTC),
        "zone_id":    str | null,
        "dwell_ms":   int,
        "is_staff":   bool,
        "confidence": float [0, 1],
        "metadata": {
            "queue_depth": int,
            "sku_zone":    str | null,
            "session_seq": int,
            ... (extra_metadata)
        }
    }

    Raises:
        ValueError: if event_type is not a known type.
    """
    if event_type not in KNOWN_EVENT_TYPES:
        raise ValueError(f"Unknown event_type '{event_type}'. Valid: {KNOWN_EVENT_TYPES}")

    confidence = max(0.0, min(1.0, float(confidence)))

    explicit_flags = extra_metadata.pop("review_flags", None) or []
    review_flags = list(dict.fromkeys(str(flag) for flag in explicit_flags))
    if confidence < LOW_CONFIDENCE_REVIEW_THRESHOLD:
        review_flags.append("LOW_DETECTION_CONFIDENCE")
    if extra_metadata.pop("low_confidence", False):
        review_flags.append("LOW_DETECTION_CONFIDENCE")
    if extra_metadata.get("ambiguous_reentry"):
        review_flags.append("AMBIGUOUS_REENTRY_MATCH")
    if is_staff:
        review_flags.append("STAFF_HEURISTIC")

    review_flags = list(dict.fromkeys(review_flags))
    confidence_bucket = extra_metadata.pop(
        "confidence_bucket", _confidence_bucket(confidence)
    )
    confidence_reason = extra_metadata.pop("confidence_reason", None)
    if confidence_reason is None and "LOW_DETECTION_CONFIDENCE" in review_flags:
        confidence_reason = "Detection confidence below review threshold"

    metadata: Dict[str, Any] = {
        "queue_depth": queue_depth,
        "sku_zone": sku_zone,
        "session_seq": session_seq,
        "review_required": bool(review_flags),
        "review_flags": review_flags,
        "confidence_bucket": confidence_bucket,
        "confidence_reason": confidence_reason,
    }
    metadata.update(extra_metadata)

    return {
        "event_id": str(uuid.uuid4()),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": round(confidence, 4),
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------
# EventEmitter class
# ---------------------------------------------------------------------------

class EventEmitter:
    """
    Centralised event emitter for the Purplle CCTV pipeline.

    Wraps event construction, batching, file output, and API posting.

    Args:
        store_id:        Store identifier (e.g. "STORE_BLR_002").
        clip_start_time: ISO-8601 string for the start of the clip being processed.
        fps:             Frames per second of source video (used for timestamp derivation).
    """

    def __init__(
        self,
        store_id: str = "STORE_BLR_002",
        clip_start_time: Optional[str] = None,
        fps: float = 25.0,
    ) -> None:
        self.store_id = store_id
        self.clip_start_time = clip_start_time or DEFAULT_CLIP_START_TIME
        self.fps = fps
        self._buffer: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # Core emit
    # ------------------------------------------------------------------

    def emit_event(
        self,
        event_type: str,
        camera_id: str,
        visitor_id: str,
        frame_number: Optional[int] = None,
        timestamp: Optional[str] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """
        Build and buffer a single event.

        Exactly one of frame_number or timestamp should be supplied. If
        frame_number is given, the timestamp is derived from the clip start
        time + frame offset.  If neither is provided, the current UTC time
        is used.

        Args:
            event_type:   One of the EVENT_* constants.
            camera_id:    Camera identifier (e.g. "CAM_ENTRY_01").
            visitor_id:   Stable visitor identifier from VisitorTracker.
            frame_number: Zero-based frame index (mutually exclusive with timestamp).
            timestamp:    ISO-8601 UTC string (mutually exclusive with frame_number).
            **kwargs:     Forwarded to build_event (zone_id, dwell_ms, confidence, …).

        Returns:
            The constructed event dict (also appended to internal buffer).
        """
        if timestamp is None:
            if frame_number is not None:
                timestamp = frame_to_timestamp(frame_number, self.fps, self.clip_start_time)
            else:
                timestamp = now_utc_iso()

        event = build_event(
            event_type=event_type,
            store_id=self.store_id,
            camera_id=camera_id,
            visitor_id=visitor_id,
            timestamp=timestamp,
            **kwargs,
        )
        self._buffer.append(event)
        return event

    def flush(self) -> List[Dict[str, Any]]:
        """Return and clear the internal event buffer."""
        events = list(self._buffer)
        self._buffer.clear()
        return events

    def buffer_size(self) -> int:
        """Return the current number of buffered (not yet emitted) events."""
        return len(self._buffer)

    # ------------------------------------------------------------------
    # File output
    # ------------------------------------------------------------------

    def emit_to_file(
        self,
        events: List[Dict[str, Any]],
        output_path: str,
        append: bool = False,
    ) -> int:
        """
        Write events to a JSONL file (one JSON object per line).

        Args:
            events:      List of event dicts.
            output_path: Destination file path.
            append:      If True, append to existing file; otherwise overwrite.

        Returns:
            Number of events written.
        """
        mode = "a" if append else "w"
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        written = 0
        with path.open(mode, encoding="utf-8") as fh:
            for event in events:
                fh.write(json.dumps(event, default=str) + "\n")
                written += 1

        logger.info("Wrote %d events to %s", written, output_path)
        return written

    # ------------------------------------------------------------------
    # API output
    # ------------------------------------------------------------------

    def emit_to_api(
        self,
        events: List[Dict[str, Any]],
        api_url: str,
        batch_size: int = API_BATCH_SIZE,
        timeout: int = API_TIMEOUT_SECONDS,
    ) -> Dict[str, int]:
        """
        POST events to {api_url}/events/ingest in batches.

        Each batch is sent as {"events": [...]} JSON body.

        Args:
            events:     List of event dicts.
            api_url:    Base URL, e.g. "http://localhost:8000".
            batch_size: Events per POST request (default 100).
            timeout:    HTTP request timeout in seconds.

        Returns:
            {"sent": N, "failed": M}
        """
        endpoint = api_url.rstrip("/") + "/events/ingest"
        headers = {"Content-Type": "application/json"}
        sent = 0
        failed = 0

        for i in range(0, len(events), batch_size):
            batch = events[i: i + batch_size]
            payload = json.dumps({"events": batch}, default=str)
            try:
                resp = requests.post(
                    endpoint, data=payload, headers=headers, timeout=timeout
                )
                if resp.ok:
                    sent += len(batch)
                    logger.debug("POSTed batch %d events → %s", len(batch), resp.status_code)
                else:
                    failed += len(batch)
                    logger.warning(
                        "API rejected batch (status %s): %s",
                        resp.status_code,
                        resp.text[:200],
                    )
            except requests.RequestException as exc:
                failed += len(batch)
                logger.error("Failed to POST batch to API: %s", exc)

        return {"sent": sent, "failed": failed}

    # ------------------------------------------------------------------
    # Convenience: emit + write to stdout
    # ------------------------------------------------------------------

    def print_event(self, event: Dict[str, Any]) -> None:
        """Print a single event as a JSONL line to stdout."""
        print(json.dumps(event, default=str), flush=True)
