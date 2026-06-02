#!/usr/bin/env bash
# =============================================================================
# run.sh – One-command CCTV processing pipeline for Purplle Tech Challenge 2026
#
# Usage:
#   ./run.sh [OPTIONS]
#
# Options:
#   --video-dir  DIR     Directory containing CAM *.mp4 files
#                        (default: /data/footage)
#   --api-url    URL     Base URL of the ingestion API
#                        (default: http://localhost:8000)
#   --output-dir DIR     Directory for output JSONL event files
#                        (default: /data/events)
#   --start-time ISO     Wall-clock start time of the clips (ISO-8601 UTC)
#                        (default: 2026-04-10T10:00:00Z)
#   --sample-every N     Process every Nth frame (default: 5)
#   --mock               Force mock mode (no YOLO / OpenCV required)
#   --log-level  LEVEL   Logging verbosity: DEBUG|INFO|WARNING|ERROR
#                        (default: INFO)
#   -h, --help           Show this help message
#
# Camera processing order:
#   CAM 1 (CAM_ENTRY_01) is processed first (entry camera – primary).
#   CAM 2, 3, 4, 5 are then processed in parallel.
#
# Outputs:
#   {output-dir}/CAM_ENTRY_01_events.jsonl
#   {output-dir}/CAM_FLOOR_01_events.jsonl
#   {output-dir}/CAM_BILLING_01_events.jsonl
#   {output-dir}/CAM_FLOOR_02_events.jsonl
#   {output-dir}/CAM_ENTRY_02_events.jsonl
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Default configuration
# ---------------------------------------------------------------------------
VIDEO_DIR="/data/footage"
API_URL="http://localhost:8000"
OUTPUT_DIR="/data/events"
START_TIME="2026-04-10T10:00:00Z"
SAMPLE_EVERY=5
MOCK_FLAG=""
LOG_LEVEL="INFO"

# Script location – detect.py lives next to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DETECT_PY="${SCRIPT_DIR}/detect.py"
LAYOUT_JSON="${SCRIPT_DIR}/store_layout.json"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --video-dir)   VIDEO_DIR="$2";   shift 2 ;;
    --api-url)     API_URL="$2";     shift 2 ;;
    --output-dir)  OUTPUT_DIR="$2";  shift 2 ;;
    --start-time)  START_TIME="$2";  shift 2 ;;
    --sample-every) SAMPLE_EVERY="$2"; shift 2 ;;
    --mock)        MOCK_FLAG="--mock"; shift ;;
    --log-level)   LOG_LEVEL="$2";   shift 2 ;;
    -h|--help)
      head -40 "${BASH_SOURCE[0]}" | grep "^#" | sed 's/^# \?//'
      exit 0
      ;;
    *)
      echo "[ERROR] Unknown argument: $1" >&2
      exit 1
      ;;
  esac
done

# ---------------------------------------------------------------------------
# Derived paths
# ---------------------------------------------------------------------------
CAM1_VIDEO="${VIDEO_DIR}/CAM 1.mp4"
CAM2_VIDEO="${VIDEO_DIR}/CAM 2.mp4"
CAM3_VIDEO="${VIDEO_DIR}/CAM 3.mp4"
CAM4_VIDEO="${VIDEO_DIR}/CAM 4.mp4"
CAM5_VIDEO="${VIDEO_DIR}/CAM 5.mp4"

CAM1_ID="CAM_ENTRY_01"
CAM2_ID="CAM_FLOOR_01"
CAM3_ID="CAM_BILLING_01"
CAM4_ID="CAM_FLOOR_02"
CAM5_ID="CAM_ENTRY_02"

STORE_ID="STORE_BLR_002"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() {
  echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] $*" >&2
}

check_python() {
  if ! command -v python3 &>/dev/null; then
    log "ERROR: python3 not found in PATH"
    exit 1
  fi
  log "Python: $(python3 --version)"
}

run_detect() {
  local video_path="$1"
  local camera_id="$2"
  local output_file="${OUTPUT_DIR}/${camera_id}_events.jsonl"

  if [[ ! -f "${video_path}" ]]; then
    log "WARNING: Video not found, skipping: ${video_path}"
    return 0
  fi

  log "Processing ${camera_id} → ${output_file}"

  python3 "${DETECT_PY}" \
    --video        "${video_path}" \
    --camera-id    "${camera_id}" \
    --store-id     "${STORE_ID}" \
    --layout       "${LAYOUT_JSON}" \
    --sample-every "${SAMPLE_EVERY}" \
    --start-time   "${START_TIME}" \
    --output-file  "${output_file}" \
    --api-url      "${API_URL}" \
    --log-level    "${LOG_LEVEL}" \
    ${MOCK_FLAG}

  local n_events=0
  if [[ -f "${output_file}" ]]; then
    n_events=$(wc -l < "${output_file}" | tr -d ' ')
  fi
  log "Done ${camera_id}: ${n_events} events written to ${output_file}"
}

# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
log "============================================================"
log "Purplle CCTV Pipeline – Purplle Tech Challenge 2026 Round 2"
log "Store: ${STORE_ID}  Start: ${START_TIME}"
log "Video dir:  ${VIDEO_DIR}"
log "Output dir: ${OUTPUT_DIR}"
log "API URL:    ${API_URL}"
log "============================================================"

check_python
mkdir -p "${OUTPUT_DIR}"

# --- Step 1: Process entry camera first (sequential) ---
log "=== Step 1/2: Processing primary entry camera (CAM_ENTRY_01) ==="
run_detect "${CAM1_VIDEO}" "${CAM1_ID}"

# --- Step 2: Process remaining cameras in parallel ---
log "=== Step 2/2: Processing remaining cameras in parallel ==="

pids=()

run_detect "${CAM2_VIDEO}" "${CAM2_ID}" &
pids+=($!)

run_detect "${CAM3_VIDEO}" "${CAM3_ID}" &
pids+=($!)

run_detect "${CAM4_VIDEO}" "${CAM4_ID}" &
pids+=($!)

run_detect "${CAM5_VIDEO}" "${CAM5_ID}" &
pids+=($!)

# Wait for all background jobs, collect exit codes
EXIT_CODE=0
for pid in "${pids[@]}"; do
  if ! wait "${pid}"; then
    log "WARNING: A background camera process (PID ${pid}) exited with error"
    EXIT_CODE=1
  fi
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
log "============================================================"
log "Pipeline complete."
TOTAL_EVENTS=0
for cam_id in "${CAM1_ID}" "${CAM2_ID}" "${CAM3_ID}" "${CAM4_ID}" "${CAM5_ID}"; do
  outfile="${OUTPUT_DIR}/${cam_id}_events.jsonl"
  if [[ -f "${outfile}" ]]; then
    n=$(wc -l < "${outfile}" | tr -d ' ')
    log "  ${cam_id}: ${n} events"
    TOTAL_EVENTS=$(( TOTAL_EVENTS + n ))
  else
    log "  ${cam_id}: (no output file)"
  fi
done
log "Total events across all cameras: ${TOTAL_EVENTS}"
log "Output directory: ${OUTPUT_DIR}"
log "============================================================"

exit "${EXIT_CODE}"
