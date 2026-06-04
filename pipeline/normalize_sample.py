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
}

_SANITIZE_RE = re.compile(r"[^\w\-]")


def _sanitize(s: str) -> str:
    """Replace characters not matching ^[\\w\\-]+ with '_', cap at 64 chars."""
    return _SANITIZE_RE.sub("_", s)[:64]


def normalize_store_id(raw: str) -> str:
    return STORE_ALIAS_MAP.get(raw.upper(), raw)


def _base_event(
    src: dict[str, Any],
    event_type: str,
    visitor_id: str,
    store_id: str,
    camera_id: str,
    timestamp: str,
    zone_id: Optional[str],
    dwell_ms: int = 0,
) -> dict[str, Any]:
    queue_depth = src.get("queue_position_at_join") if event_type in (
        "BILLING_QUEUE_JOIN",
        "BILLING_QUEUE_ABANDON",
    ) else None

    return {
        "event_id": str(uuid.uuid4()),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": bool(src.get("is_staff", False)),
        "confidence": 1.0,
        "metadata": {
            "queue_depth": queue_depth,
            "sku_zone": None,
            "session_seq": None,
            "review_required": False,
            "review_flags": [],
        },
    }


def _build_canonical(src: dict[str, Any]) -> list[dict[str, Any]]:
    """Return one or more canonical event dicts. Empty list means skip."""

    raw_type = src.get("event_type", "")
    canonical_type = EVENT_TYPE_MAP.get(raw_type)
    is_queue_event = raw_type in {"queue_completed", "queue_abandoned"}
    if canonical_type is None:
        if not is_queue_event:
            print(f"WARNING: unknown event_type '{raw_type}' — skipping", file=sys.stderr)
            return []

    # Visitor ID
    id_token = src.get("id_token")
    track_id = src.get("track_id")
    if id_token is not None:
        visitor_id = str(id_token)
    elif track_id is not None:
        visitor_id = f"TRACK_{int(track_id)}"
    else:
        print(f"WARNING: no visitor id in event (type={raw_type}) — skipping", file=sys.stderr)
        return []

    # Timestamp
    timestamp = src.get("event_timestamp") or src.get("event_time") or src.get("queue_join_ts")
    if not timestamp:
        print(f"WARNING: no timestamp in event (type={raw_type}, visitor={visitor_id}) — skipping", file=sys.stderr)
        return []

    # Store ID
    raw_store = src.get("store_code") or src.get("store_id") or ""
    store_id = normalize_store_id(raw_store) if raw_store else raw_store

    # Camera ID
    raw_cam = src.get("camera_id") or "CAM_UNKNOWN"
    camera_id = _sanitize(raw_cam)

    # Zone ID
    raw_zone = src.get("zone_id")
    zone_id = "BILLING" if is_queue_event else (_sanitize(raw_zone) if raw_zone else None)

    if is_queue_event:
        if not src.get("queue_join_ts"):
            print(f"WARNING: queue event missing queue_join_ts (visitor={visitor_id}) — skipping", file=sys.stderr)
            return []
        wait_seconds = int(float(src.get("wait_seconds") or 0))
        events = [
            _base_event(
                src,
                "BILLING_QUEUE_JOIN",
                visitor_id,
                store_id,
                camera_id,
                src["queue_join_ts"],
                zone_id,
            )
        ]
        if raw_type == "queue_abandoned":
            events.append(
                _base_event(
                    src,
                    "BILLING_QUEUE_ABANDON",
                    visitor_id,
                    store_id,
                    camera_id,
                    src.get("queue_exit_ts") or timestamp,
                    zone_id,
                    dwell_ms=wait_seconds * 1000,
                )
            )
        else:
            events.append(
                _base_event(
                    src,
                    "ZONE_EXIT",
                    visitor_id,
                    store_id,
                    camera_id,
                    src.get("queue_exit_ts") or src.get("queue_served_ts") or timestamp,
                    zone_id,
                    dwell_ms=wait_seconds * 1000,
                )
            )
        return events

    return [
        _base_event(
            src,
            canonical_type,
            visitor_id,
            store_id,
            camera_id,
            timestamp,
            zone_id,
        )
    ]


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
            if not result:
                skipped += 1
            else:
                converted.extend(result)

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
