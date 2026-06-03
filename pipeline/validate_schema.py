"""
validate_schema.py — validates JSONL event files against the required schema.

Usage:
    python validate_schema.py path/to/events.jsonl [path2.jsonl ...]
    python validate_schema.py data/events/store1/   # directory of JSONL files

Exit code 0 = all valid, 1 = failures found.
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

REQUIRED_TOP_KEYS = {
    "event_id", "store_id", "camera_id", "visitor_id",
    "event_type", "timestamp", "dwell_ms", "is_staff", "confidence",
}
REQUIRED_METADATA_KEYS = {"queue_depth", "sku_zone", "session_seq"}

VALID_EVENT_TYPES = {
    "ENTRY", "EXIT", "ZONE_ENTER", "ZONE_EXIT", "ZONE_DWELL",
    "BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON", "REENTRY", "GROUP_ENTRY",
}

ZONE_REQUIRED_TYPES = {"ZONE_ENTER", "ZONE_EXIT", "ZONE_DWELL"}


def _check_event(ev: Any, line_no: int) -> list[str]:
    errors: list[str] = []

    if not isinstance(ev, dict):
        return [f"line {line_no}: not a JSON object"]

    missing = REQUIRED_TOP_KEYS - ev.keys()
    if missing:
        errors.append(f"line {line_no}: missing keys {sorted(missing)}")

    eid = ev.get("event_id", "")
    try:
        uuid.UUID(str(eid), version=4)
    except (ValueError, AttributeError):
        errors.append(f"line {line_no}: event_id '{eid}' is not a valid UUID v4")

    etype = ev.get("event_type", "")
    if etype not in VALID_EVENT_TYPES:
        errors.append(f"line {line_no}: unknown event_type '{etype}'")

    ts = ev.get("timestamp", "")
    try:
        datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        errors.append(f"line {line_no}: timestamp '{ts}' not ISO-8601")

    conf = ev.get("confidence")
    if conf is not None:
        try:
            if not (0.0 <= float(conf) <= 1.0):
                errors.append(f"line {line_no}: confidence {conf} out of [0,1]")
        except (TypeError, ValueError):
            errors.append(f"line {line_no}: confidence '{conf}' not a number")

    dwell = ev.get("dwell_ms", 0)
    if dwell is not None:
        try:
            if int(dwell) < 0:
                errors.append(f"line {line_no}: dwell_ms {dwell} is negative")
        except (TypeError, ValueError):
            errors.append(f"line {line_no}: dwell_ms '{dwell}' not an integer")

    if etype in ZONE_REQUIRED_TYPES and not ev.get("zone_id"):
        errors.append(f"line {line_no}: zone_id required for {etype}")

    meta = ev.get("metadata")
    if meta is not None:
        if not isinstance(meta, dict):
            errors.append(f"line {line_no}: metadata must be an object")
        else:
            missing_meta = REQUIRED_METADATA_KEYS - meta.keys()
            if missing_meta:
                errors.append(f"line {line_no}: metadata missing keys {sorted(missing_meta)}")

    return errors


def validate_file(path: Path) -> tuple[int, int, list[str]]:
    total = 0
    failures = 0
    all_errors: list[str] = []
    seen_ids: set[str] = set()

    with path.open(encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                ev = json.loads(line)
            except json.JSONDecodeError as exc:
                all_errors.append(f"line {line_no}: invalid JSON — {exc}")
                failures += 1
                continue

            errs = _check_event(ev, line_no)
            if errs:
                all_errors.extend(errs)
                failures += 1

            eid = ev.get("event_id", "")
            if eid in seen_ids:
                all_errors.append(f"line {line_no}: duplicate event_id '{eid}'")
                failures += 1
            else:
                seen_ids.add(eid)

    return total, failures, all_errors


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print("Usage: validate_schema.py <file.jsonl|directory> [...]")
        return 1

    paths: list[Path] = []
    for arg in args:
        p = Path(arg)
        if p.is_dir():
            paths.extend(sorted(p.glob("**/*.jsonl")))
        else:
            paths.append(p)

    if not paths:
        print("No JSONL files found.")
        return 1

    overall_pass = True
    for path in paths:
        total, failures, errors = validate_file(path)
        status = "PASS" if failures == 0 else "FAIL"
        print(f"[{status}] {path.name}  —  {total} events, {failures} invalid")
        for err in errors[:20]:
            print(f"       {err}")
        if len(errors) > 20:
            print(f"       ... and {len(errors) - 20} more errors")
        if failures:
            overall_pass = False

    print()
    print("Result:", "ALL PASS" if overall_pass else "FAILURES FOUND")
    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
