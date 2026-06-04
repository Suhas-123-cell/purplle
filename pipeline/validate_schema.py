"""
validate_schema.py — validates JSONL event files against the required schema.

Usage:
    python validate_schema.py path/to/events.jsonl [path2.jsonl ...]
    python validate_schema.py data/events/store1/   # directory of JSONL files
    python validate_schema.py data/events/store1/ data/events/store2/

Exit code 0 = all valid, 1 = failures found.
"""

from __future__ import annotations

import json
import sys
import uuid
from collections import defaultdict
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


def collect_store_stats(paths: list[Path], valid_counts: dict[Path, int]) -> dict:
    """Read all events from the given files and compute per-store analytics."""
    total_events = 0
    valid_events = 0
    camera_ids: set[str] = set()
    visitor_ids: set[str] = set()
    event_type_counts: dict[str, int] = defaultdict(int)
    max_dwell_ms = 0
    dwell_over_hour = 0
    bq_join = 0
    bq_abandon = 0
    low_conf_total = 0
    low_conf_flagged = 0
    min_ts: str | None = None
    max_ts: str | None = None

    for path in paths:
        file_valid = valid_counts.get(path, 0)
        valid_events += file_valid

        try:
            with path.open(encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        ev = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if not isinstance(ev, dict):
                        continue

                    total_events += 1

                    cam = ev.get("camera_id")
                    if cam:
                        camera_ids.add(str(cam))

                    vid = ev.get("visitor_id")
                    if vid:
                        visitor_ids.add(str(vid))

                    etype = ev.get("event_type", "")
                    if etype:
                        event_type_counts[str(etype)] += 1

                    dwell = ev.get("dwell_ms")
                    if dwell is not None:
                        try:
                            d = int(dwell)
                            if d > max_dwell_ms:
                                max_dwell_ms = d
                            if d > 3_600_000:
                                dwell_over_hour += 1
                        except (TypeError, ValueError):
                            pass

                    if etype == "BILLING_QUEUE_JOIN":
                        bq_join += 1
                    elif etype == "BILLING_QUEUE_ABANDON":
                        bq_abandon += 1

                    conf = ev.get("confidence")
                    if conf is not None:
                        try:
                            if float(conf) < 0.40:
                                low_conf_total += 1
                                meta = ev.get("metadata") or {}
                                flags = meta.get("review_flags") or []
                                if meta.get("review_required") and "LOW_DETECTION_CONFIDENCE" in flags:
                                    low_conf_flagged += 1
                        except (TypeError, ValueError):
                            pass

                    ts = ev.get("timestamp")
                    if ts:
                        ts_str = str(ts)
                        if min_ts is None or ts_str < min_ts:
                            min_ts = ts_str
                        if max_ts is None or ts_str > max_ts:
                            max_ts = ts_str
        except OSError:
            pass

    return {
        "total_events": total_events,
        "valid_events": valid_events,
        "camera_ids": camera_ids,
        "visitor_ids": visitor_ids,
        "event_type_counts": dict(event_type_counts),
        "max_dwell_ms": max_dwell_ms,
        "dwell_over_hour": dwell_over_hour,
        "bq_join": bq_join,
        "bq_abandon": bq_abandon,
        "low_conf_total": low_conf_total,
        "low_conf_flagged": low_conf_flagged,
        "min_ts": min_ts,
        "max_ts": max_ts,
    }


def print_store_summary(label: str, stats: dict) -> None:
    """Print a formatted per-store analytics summary block."""
    total = stats["total_events"]
    valid = stats["valid_events"]
    cameras = sorted(stats["camera_ids"])
    unique_visitors = len(stats["visitor_ids"])
    et = stats["event_type_counts"]
    max_dwell = stats["max_dwell_ms"]
    dwell_over_hour = stats["dwell_over_hour"]
    bq_join = stats["bq_join"]
    bq_abandon = stats["bq_abandon"]
    low_conf_total = stats["low_conf_total"]
    low_conf_flagged = stats["low_conf_flagged"]
    min_ts = stats["min_ts"] or "n/a"
    max_ts = stats["max_ts"] or "n/a"

    print(f"\nStore Summary: {label}")

    print(f"  {'Events':<18}: {total}")
    print(f"  {'Schema valid':<18}: {valid}/{total}")

    cam_str = ", ".join(cameras) if cameras else "(none)"
    print(f"  {'Cameras':<18}: {cam_str} ({len(cameras)})")

    print(f"  {'Unique visitors':<18}: {unique_visitors}")

    # Event types — print in a stable order, known types first
    known_order = [
        "ENTRY", "EXIT", "ZONE_ENTER", "ZONE_EXIT", "ZONE_DWELL",
        "BILLING_QUEUE_JOIN", "BILLING_QUEUE_ABANDON", "REENTRY", "GROUP_ENTRY",
    ]
    et_parts = []
    for k in known_order:
        if k in et:
            et_parts.append(f"{k}={et[k]}")
    for k in sorted(et):
        if k not in known_order:
            et_parts.append(f"{k}={et[k]}")
    print(f"  {'Event types':<18}: {', '.join(et_parts) if et_parts else '(none)'}")

    # Funnel
    entry_count = et.get("ENTRY", 0) + et.get("GROUP_ENTRY", 0)
    zone_count = et.get("ZONE_ENTER", 0)
    billing_count = bq_join
    funnel_str = f"ENTRY={entry_count} → ZONE={zone_count} → BILLING_QUEUE={billing_count}"
    if billing_count > entry_count * 2:
        funnel_note = "WARN — BILLING_QUEUE_JOIN exceeds 2× ENTRY (suspicious)"
    elif zone_count > entry_count:
        funnel_note = "WARN — more zone events than entries, expected for multi-zone cameras"
    else:
        funnel_note = "OK"
    print(f"  {'Funnel (offline)':<18}: {funnel_str} [monotonic: {funnel_note}]")

    # Dwell sanity
    if dwell_over_hour > 0:
        dwell_status = f"WARN — {dwell_over_hour} event(s) exceed 1 hour dwell"
    else:
        dwell_min = max_dwell // 60000
        dwell_status = f"OK — max dwell {max_dwell} ms ({dwell_min} min)"
    print(f"  {'Dwell sanity':<18}: {dwell_status}")

    # Queue sanity
    if bq_join >= bq_abandon:
        queue_status = f"OK — BILLING_QUEUE_JOIN={bq_join} >= BILLING_QUEUE_ABANDON={bq_abandon}"
    else:
        queue_status = f"WARN — BILLING_QUEUE_JOIN={bq_join} < BILLING_QUEUE_ABANDON={bq_abandon}"
    print(f"  {'Queue sanity':<18}: {queue_status}")

    # Low-conf flags
    if low_conf_total == 0:
        lc_status = "PASS — no low-confidence events"
    elif low_conf_flagged == low_conf_total:
        lc_status = (
            f"PASS — {low_conf_total} event(s) below 0.40 confidence, "
            f"{low_conf_flagged} flagged review_required=True"
        )
    else:
        lc_status = (
            f"FAIL — {low_conf_total} event(s) below 0.40 confidence, "
            f"only {low_conf_flagged} flagged review_required=True"
        )
    print(f"  {'Low-conf flags':<18}: {lc_status}")

    print(f"  {'Timestamp range':<18}: {min_ts} → {max_ts}")


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print("Usage: validate_schema.py <file.jsonl|directory> [...]")
        return 1

    # Resolve input paths and group them by "store label"
    # If arg is a directory → all JSONL files in it, label = the directory path string
    # If arg is a file → group by parent dir; files with the same parent share a label
    raw_paths: list[Path] = []
    store_groups: dict[str, list[Path]] = {}  # label → list of file paths

    for arg in args:
        p = Path(arg)
        if p.is_dir():
            files = sorted(p.glob("**/*.jsonl"))
            label = str(p)
            store_groups.setdefault(label, []).extend(files)
            raw_paths.extend(files)
        else:
            label = str(p.parent)
            store_groups.setdefault(label, []).append(p)
            raw_paths.append(p)

    if not raw_paths:
        print("No JSONL files found.")
        return 1

    # ── Per-file schema validation ────────────────────────────────────────────
    overall_pass = True
    # Track valid event counts per file so collect_store_stats can use them
    file_valid_counts: dict[Path, int] = {}

    for path in raw_paths:
        total, failures, errors = validate_file(path)
        file_valid_counts[path] = total - failures
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

    # ── Per-store analytics summary ───────────────────────────────────────────
    if store_groups:
        print()
        print("─" * 64)
        for label, files in store_groups.items():
            stats = collect_store_stats(files, file_valid_counts)
            print_store_summary(label, stats)

    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
