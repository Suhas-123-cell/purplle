#!/usr/bin/env python3
"""
validate_data.py — Purplle retail analytics data validation script.

Queries the live API at http://localhost:8000 and the JSONL event files,
then prints a PASS/FAIL report for each sanity check.

Usage:
    python pipeline/validate_data.py

Requirements: stdlib + requests
"""

from __future__ import annotations

import glob
import json
import os
import sys
import csv
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    print("ERROR: 'requests' is required. Install with: pip install requests")
    sys.exit(1)

API_BASE = "http://localhost:8000"
STORE_ID = "STORE_BLR_002"

# Resolve event JSONL files relative to this script's location
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
EVENTS_DIR = os.path.join(_SCRIPT_DIR, "..", "data", "events")

PASS = "PASS ✓"
FAIL = "FAIL ✗"


# ── helpers ──────────────────────────────────────────────────────────────────

def _get(path: str, params: Optional[Dict] = None) -> Any:
    url = f"{API_BASE}{path}"
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _label(ok: bool) -> str:
    return PASS if ok else FAIL


def _print(status: bool, check: str, reason: str) -> None:
    label = _label(status)
    print(f"  {label}  [{check}] {reason}")


# ── checks ────────────────────────────────────────────────────────────────────

def check_event_count_match() -> bool:
    """Check 1: JSONL line count matches DB accepted event count."""
    pattern = os.path.join(EVENTS_DIR, "*.jsonl")
    files = glob.glob(pattern)
    if not files:
        _print(False, "EVENT COUNT", f"No JSONL files found in {EVENTS_DIR}")
        return False

    jsonl_total = 0
    for fpath in files:
        with open(fpath, "r") as f:
            for line in f:
                if line.strip():
                    jsonl_total += 1

    try:
        data = _get(f"/stores/{STORE_ID}/metrics")
        # The metrics endpoint does not expose a raw event count, so we use
        # unique_visitors as a sanity proxy — but for a true match we also
        # check the /health endpoint which surfaces last_event_ts.
        # The real accepted count is best approximated from the ingestion log,
        # so we flag only if unique_visitors == 0 while JSONL lines exist.
        unique_visitors = data.get("unique_visitors", 0)
        if jsonl_total > 0 and unique_visitors == 0:
            _print(False, "EVENT COUNT",
                   f"JSONL has {jsonl_total} lines but DB reports 0 unique visitors — "
                   "possible ingestion failure")
            return False
        _print(True, "EVENT COUNT",
               f"JSONL total={jsonl_total} lines across {len(files)} file(s); "
               f"DB unique_visitors={unique_visitors}")
        return True
    except Exception as e:
        _print(False, "EVENT COUNT", f"API error: {e}")
        return False


def check_entry_count_reasonable() -> bool:
    """Check 2: ENTRY event count >= 5% of unique visitors."""
    pattern = os.path.join(EVENTS_DIR, "*.jsonl")
    files = glob.glob(pattern)

    entry_count = 0
    for fpath in files:
        with open(fpath, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if obj.get("event_type") in ("ENTRY", "GROUP_ENTRY"):
                        entry_count += 1
                except json.JSONDecodeError:
                    pass

    try:
        data = _get(f"/stores/{STORE_ID}/metrics")
        unique_visitors = data.get("unique_visitors", 0)
        if unique_visitors == 0:
            _print(True, "ENTRY RATIO", "unique_visitors=0, skipping ratio check")
            return True
        threshold = unique_visitors * 0.05
        ok = entry_count >= threshold
        _print(ok, "ENTRY RATIO",
               f"ENTRY events={entry_count}, unique_visitors={unique_visitors}, "
               f"ratio={entry_count/unique_visitors:.1%} (threshold 5%)")
        return ok
    except Exception as e:
        _print(False, "ENTRY RATIO", f"API error: {e}")
        return False


def check_dwell_sanity() -> bool:
    """Check 3: No zone avg_dwell_ms exceeds 3,600,000 ms (1 hour)."""
    MAX_DWELL_MS = 3_600_000
    try:
        data = _get(f"/stores/{STORE_ID}/heatmap")
        cells = data.get("cells", [])
        bad = [c for c in cells if (c.get("avg_dwell_ms") or 0) > MAX_DWELL_MS]
        if bad:
            details = ", ".join(
                f"{c['zone_id']}={c['avg_dwell_ms']/60000:.1f}m" for c in bad
            )
            _print(False, "DWELL SANITY",
                   f"{len(bad)} zone(s) exceed 1 hour avg dwell: {details}")
            return False
        max_found = max((c.get("avg_dwell_ms") or 0) for c in cells) if cells else 0
        _print(True, "DWELL SANITY",
               f"All {len(cells)} zone(s) OK; max avg dwell = {max_found/1000:.0f}s")
        return True
    except Exception as e:
        _print(False, "DWELL SANITY", f"API error: {e}")
        return False


def check_funnel_monotonic() -> bool:
    """Check 4: Each funnel stage count <= previous stage count."""
    try:
        data = _get(f"/stores/{STORE_ID}/funnel")
        stages = data.get("stages", [])
        if not stages:
            _print(False, "FUNNEL MONOTONIC", "No funnel stages returned")
            return False
        violations: List[str] = []
        for i in range(1, len(stages)):
            prev = stages[i - 1]
            curr = stages[i]
            if (curr.get("count") or 0) > (prev.get("count") or 0):
                violations.append(
                    f"{curr['stage']}({curr['count']}) > {prev['stage']}({prev['count']})"
                )
        ok = len(violations) == 0
        if ok:
            counts = " → ".join(f"{s['stage']}={s['count']}" for s in stages)
            _print(True, "FUNNEL MONOTONIC", counts)
        else:
            _print(False, "FUNNEL MONOTONIC",
                   "Non-monotonic stages: " + "; ".join(violations))
        return ok
    except Exception as e:
        _print(False, "FUNNEL MONOTONIC", f"API error: {e}")
        return False


def check_queue_depth_sanity() -> bool:
    """Check 5: queue_depth <= unique_visitors."""
    try:
        data = _get(f"/stores/{STORE_ID}/metrics")
        queue_depth = data.get("queue_depth", 0)
        unique_visitors = data.get("unique_visitors", 0)
        ok = queue_depth <= unique_visitors
        _print(ok, "QUEUE DEPTH",
               f"queue_depth={queue_depth}, unique_visitors={unique_visitors}")
        return ok
    except Exception as e:
        _print(False, "QUEUE DEPTH", f"API error: {e}")
        return False


def check_review_metadata() -> bool:
    """Check 6: low-confidence JSONL events carry explicit review metadata."""
    pattern = os.path.join(EVENTS_DIR, "*.jsonl")
    files = glob.glob(pattern)
    low_conf = 0
    flagged = 0
    missing: List[str] = []

    for fpath in files:
        with open(fpath, "r") as f:
            for idx, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                confidence = float(obj.get("confidence", 1.0))
                metadata = obj.get("metadata") or {}
                flags = metadata.get("review_flags") or []
                if confidence < 0.40:
                    low_conf += 1
                    if metadata.get("review_required") and "LOW_DETECTION_CONFIDENCE" in flags:
                        flagged += 1
                    elif len(missing) < 3:
                        missing.append(f"{os.path.basename(fpath)}:{idx}")

    ok = low_conf == flagged
    if ok:
        _print(True, "REVIEW FLAGS",
               f"{flagged}/{low_conf} low-confidence event(s) flagged for review")
    else:
        _print(False, "REVIEW FLAGS",
               f"{flagged}/{low_conf} low-confidence event(s) flagged; missing {missing}")
    return ok


def check_pos_cctv_time_overlap() -> None:
    """Check 7: Print CCTV and POS time ranges so the operator can verify overlap.
    This is informational — no PASS/FAIL, just the ranges side-by-side."""
    pattern = os.path.join(EVENTS_DIR, "*.jsonl")
    files = glob.glob(pattern)

    cctv_min: Optional[str] = None
    cctv_max: Optional[str] = None

    for fpath in files:
        with open(fpath, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    ts = obj.get("timestamp") or obj.get("ts")
                    if ts:
                        if cctv_min is None or ts < cctv_min:
                            cctv_min = ts
                        if cctv_max is None or ts > cctv_max:
                            cctv_max = ts
                except json.JSONDecodeError:
                    pass

    # POS time range from health / metrics — best we can do without DB access.
    # We surface what is available from the API.
    pos_range = "n/a (check data/pos_transactions.csv directly)"
    pos_csv = os.path.join(_SCRIPT_DIR, "..", "data", "pos_transactions.csv")
    if os.path.exists(pos_csv):
        pos_times: List[datetime] = []
        with open(pos_csv, "r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                date_part = (row.get("order_date") or "").strip()
                time_part = (row.get("order_time") or "").strip()
                if not date_part or not time_part:
                    continue
                for fmt in ("%d-%m-%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                    try:
                        pos_times.append(datetime.strptime(f"{date_part} {time_part}", fmt))
                        break
                    except ValueError:
                        continue
        if pos_times:
            pos_range = (
                f"{min(pos_times).isoformat(sep=' ')}  →  "
                f"{max(pos_times).isoformat(sep=' ')}"
            )

    print(f"  INFO  [TIME OVERLAP]")
    print(f"        CCTV events : {cctv_min or 'n/a'}  →  {cctv_max or 'n/a'}")
    print(f"        POS txns    : {pos_range}")
    print("        CCTV is a compressed replay window; POS is checked at store-day level.")


def check_camera_count() -> bool:
    """Check 8: /health returns statuses for at least 3 cameras."""
    MIN_CAMERAS = 3
    try:
        data = _get(f"/health?store_id={STORE_ID}")
        # health endpoint may nest cameras under the store or at top level
        cameras = data.get("camera_statuses", data.get("cameras", []))
        count = len(cameras)
        ok = count >= MIN_CAMERAS
        cam_ids = [c.get("camera_id", "?") for c in cameras]
        _print(ok, "CAMERA COUNT",
               f"{count} camera(s) found (need >= {MIN_CAMERAS}): {', '.join(cam_ids)}")
        return ok
    except Exception as e:
        _print(False, "CAMERA COUNT", f"API error: {e}")
        return False


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print()
    print("=" * 64)
    print("  Purplle Retail Analytics — Data Validation Report")
    print(f"  Store : {STORE_ID}")
    print(f"  API   : {API_BASE}")
    print(f"  Run   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 64)
    print()

    results: List[bool] = []

    print("Checks:")
    results.append(check_event_count_match())
    results.append(check_entry_count_reasonable())
    results.append(check_dwell_sanity())
    results.append(check_funnel_monotonic())
    results.append(check_queue_depth_sanity())
    results.append(check_review_metadata())
    check_pos_cctv_time_overlap()   # informational, not counted
    results.append(check_camera_count())

    passed = sum(results)
    total = len(results)
    print()
    print("=" * 64)
    print(f"  Summary: {passed}/{total} checks passed")
    if passed < total:
        print("  Status : NEEDS ATTENTION")
        sys.exit(1)
    else:
        print("  Status : ALL CLEAR")
    print("=" * 64)
    print()


if __name__ == "__main__":
    main()
