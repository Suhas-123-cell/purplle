"""normalize_sample.py – Convert organizer-format JSONL to canonical API schema.

Reads the organizer's sample event format (entry/exit, zone, and queue events)
and outputs the canonical schema expected by /events/ingest.

Usage:
    python3 normalize_sample.py input.jsonl [output.jsonl]

If output.jsonl is omitted, canonical events are written to stdout.
The output can be piped directly to ingest_events.py:

    python3 normalize_sample.py input.jsonl | python3 ingest_events.py --stdin
"""

from __future__ import annotations

import json
import re
import sys
import uuid
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Store alias map (mirrors app/ingestion.py _ALIAS_MAP, case-insensitive).
# ---------------------------------------------------------------------------
STORE_ALIAS_MAP: dict[str, str] = {
    "STORE_BLR_002": "STORE_BLR_002",
    "ST1008": "STORE_BLR_002",
    "STORE_1": "STORE_1",
    "ST1": "STORE_1",
    "STORE_2": "STORE_2",
    "ST2": "STORE_2",
    "STORE_1076": "STORE_1076",
    "ST1076": "STORE_1076",
}

# ---------------------------------------------------------------------------
# Event-type mapping from organizer strings → canonical strings.
# ---------------------------------------------------------------------------
EVENT_TYPE_MAP: dict[str, str] = {
    "entry": "ENTRY",
    "exit": "EXIT",
    "zone_entered": "ZONE_ENTER",
    "zone_exited": "ZONE_EXIT",
    "queue_completed": "BILLING_QUEUE_JOIN",
    "queue_abandoned": "BILLING_QUEUE_ABANDON",
}

_SANITIZE_RE = re.compile(r"[^\w\-]")


def _sanitize(s: str) -> str:
    """Replace characters not matching ^[\\w\\-]+ with '_', cap at 64 chars."""
    return _SANITIZE_RE.sub("_", s)[:64]


def normalize_store_id(raw: str) -> str:
    return STORE_ALIAS_MAP.get(raw.upper(), raw)


def _build_canonical(src: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Return a canonical event dict or None if the event should be skipped."""

    raw_type = src.get("event_type", "")
    canonical_type = EVENT_TYPE_MAP.get(raw_type)
    if canonical_type is None:
        print(f"WARNING: unknown event_type '{raw_type}' — skipping", file=sys.stderr)
        return None

    # Visitor ID
    id_token = src.get("id_token")
    track_id = src.get("track_id")
    if id_token is not None:
        visitor_id = str(id_token)
    elif track_id is not None:
        visitor_id = f"TRACK_{int(track_id)}"
    else:
        print(f"WARNING: no visitor id in event (type={raw_type}) — skipping", file=sys.stderr)
        return None

    # Timestamp
    timestamp = (
        src.get("event_timestamp")
        or src.get("event_time")
        or src.get("queue_join_ts")
    )
    if not timestamp:
        print(f"WARNING: no timestamp in event (type={raw_type}, visitor={visitor_id}) — skipping", file=sys.stderr)
        return None

    # Store ID
    raw_store = src.get("store_code") or src.get("store_id") or ""
    store_id = normalize_store_id(raw_store) if raw_store else raw_store

    # Camera ID
    raw_cam = src.get("camera_id") or "CAM_UNKNOWN"
    camera_id = _sanitize(raw_cam)

    # Zone ID
    raw_zone = src.get("zone_id")
    zone_id = _sanitize(raw_zone) if raw_zone else None

    # dwell_ms: queue events use wait_seconds * 1000
    wait_seconds = src.get("wait_seconds")
    if canonical_type in ("BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON") and wait_seconds is not None:
        dwell_ms = int(float(wait_seconds) * 1000)
    else:
        dwell_ms = 0

    # is_staff
    is_staff = bool(src.get("is_staff", False))

    # queue_depth
    queue_depth = src.get("queue_position_at_join") if canonical_type in (
        "BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON"
    ) else None

    return {
        "event_id": str(uuid.uuid4()),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": canonical_type,
        "timestamp": timestamp,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": 1.0,
        "metadata": {
            "queue_depth": queue_depth,
            "sku_zone": None,
            "session_seq": None,
            "review_required": False,
            "review_flags": [],
        },
    }


def main() -> None:
    args = sys.argv[1:]
    if not args:
        print(f"Usage: {sys.argv[0]} input.jsonl [output.jsonl]", file=sys.stderr)
        sys.exit(1)

    input_path = args[0]
    output_path = args[1] if len(args) >= 2 else None

    converted: list[dict[str, Any]] = []
    skipped = 0

    with open(input_path, encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                src = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"WARNING: line {lineno}: invalid JSON ({exc}) — skipping", file=sys.stderr)
                skipped += 1
                continue

            result = _build_canonical(src)
            if result is None:
                skipped += 1
            else:
                converted.append(result)

    dest_label = output_path if output_path else "<stdout>"
    print(
        f"Converted {len(converted)} events → {dest_label} ({skipped} skipped)",
        file=sys.stderr,
    )

    if not converted:
        sys.exit(1)

    out_lines = [json.dumps(ev) for ev in converted]

    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(out_lines) + "\n")
    else:
        sys.stdout.write("\n".join(out_lines) + "\n")


if __name__ == "__main__":
    main()
