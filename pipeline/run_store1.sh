#!/usr/bin/env bash
# run_store1.sh – Detection pipeline for Store 1
# Usage: ./run_store1.sh [--video-dir DIR] [--api-url URL] [--output-dir DIR] [--mock]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VIDEO_DIR="/Users/suhasdev/Downloads/Store 1"
API_URL="http://localhost:8000"
OUTPUT_DIR="${SCRIPT_DIR}/../data/events/store1"
START_TIME="2026-04-10T10:00:00Z"
SAMPLE_EVERY=5
MOCK_FLAG=""
LOG_LEVEL="INFO"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --video-dir)   VIDEO_DIR="$2";   shift 2 ;;
    --api-url)     API_URL="$2";     shift 2 ;;
    --output-dir)  OUTPUT_DIR="$2";  shift 2 ;;
    --start-time)  START_TIME="$2";  shift 2 ;;
    --mock)        MOCK_FLAG="--mock"; shift ;;
    --log-level)   LOG_LEVEL="$2";   shift 2 ;;
    *) echo "[ERROR] Unknown arg: $1" >&2; exit 1 ;;
  esac
done

VENV_PYTHON="${SCRIPT_DIR}/../.venv/bin/python"
PYTHON=$( [[ -x "$VENV_PYTHON" ]] && echo "$VENV_PYTHON" || echo "python3" )
DETECT_PY="${SCRIPT_DIR}/detect.py"
LAYOUT="${SCRIPT_DIR}/store1_layout.json"
STORE_ID="STORE_1"

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" >&2; }

run_detect() {
  local video="$1" cam_id="$2"
  local out="${OUTPUT_DIR}/${cam_id}_events.jsonl"
  [[ ! -f "$video" ]] && { log "SKIP (not found): $video"; return 0; }
  log "Processing $cam_id <- $video"
  $PYTHON "$DETECT_PY" \
    --video        "$video" \
    --camera-id    "$cam_id" \
    --store-id     "$STORE_ID" \
    --layout       "$LAYOUT" \
    --sample-every "$SAMPLE_EVERY" \
    --start-time   "$START_TIME" \
    --output-file  "$out" \
    --api-url      "$API_URL" \
    --log-level    "$LOG_LEVEL" \
    $MOCK_FLAG
  log "Done $cam_id: $(wc -l < "$out" 2>/dev/null || echo 0) events"
}

mkdir -p "$OUTPUT_DIR"
log "=== Store 1 pipeline | store=$STORE_ID | api=$API_URL ==="

run_detect "${VIDEO_DIR}/CAM 3 - entry.mp4"   "CAM_ENTRY_01"

pids=()
run_detect "${VIDEO_DIR}/CAM 1 - zone.mp4"    "CAM_ZONE_01"    & pids+=($!)
run_detect "${VIDEO_DIR}/CAM 2 - zone.mp4"    "CAM_ZONE_02"    & pids+=($!)
run_detect "${VIDEO_DIR}/CAM 5 - billing.mp4" "CAM_BILLING_01" & pids+=($!)

EXIT=0
for pid in "${pids[@]}"; do wait "$pid" || EXIT=1; done

TOTAL=0
for f in "$OUTPUT_DIR"/*_events.jsonl; do
  [[ -f "$f" ]] && TOTAL=$(( TOTAL + $(wc -l < "$f") ))
done
log "Total events: $TOTAL  |  Output: $OUTPUT_DIR"
exit $EXIT
