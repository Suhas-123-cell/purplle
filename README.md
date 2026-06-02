# Purplle Store Intelligence — Brigade Road Bangalore

Real-time and replay-safe CCTV analytics for **STORE_BLR_002 (ST1008)**.  
Detects visitors, tracks zone dwell, correlates billing visits with POS orders, spots anomalies, and streams metrics to a live dashboard.

## Setup in 6 commands

```bash
# 1. Clone the repo
git clone <repo> && cd purplle

# 2. Drop the POS export into the data directory
cp /path/to/pos_transactions.csv data/

# 3. Drop footage clips (optional — pipeline runs on live RTSP or recorded files)
cp -r /path/to/footage data/footage/

# 4. Start API + dashboard. Compose mounts ./app and ./dashboard so local
#    source edits are reflected without rebuilding the images.
docker compose up -d

# 5. Install pipeline dependencies into the project venv
.venv/bin/pip install -r pipeline/requirements.txt

# 6. Start the detection pipeline (automatically uses the venv)
cd pipeline && ./run.sh
```

Dashboard is at **http://localhost:3000** · API at **http://localhost:8000**

---

## Architecture

```
CCTV cameras (RTSP / MP4)
        │
        ▼
  pipeline/detect.py          ← YOLOv8n bounding-box detection
  pipeline/tracker.py         ← ByteTrack multi-object tracker
  pipeline/emit.py            ← Event builder (ENTRY / ZONE_ENTER / EXIT …)
        │  HTTP POST (batch, ≤500 events)
        ▼
  app/main.py  (FastAPI)
  ├── POST /events/ingest     ← idempotent, deduped by event_id UUID
  ├── GET  /stores/{id}/metrics
  ├── GET  /stores/{id}/funnel
  ├── GET  /stores/{id}/heatmap
  ├── GET  /stores/{id}/anomalies
  └── GET  /health
        │
  SQLite  (aiosqlite + SQLAlchemy async)
  ├── events             ← raw event log
  ├── visitor_sessions   ← per-visitor session state
  └── pos_transactions   ← loaded from POS CSV at startup
        │
  dashboard/index.html   ← vanilla JS, polls API every 10 s
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events/ingest` | Batch-ingest camera events. Body: `{"events": [...]}`. Returns accepted/rejected/duplicate counts. |
| `GET`  | `/stores/{id}/metrics` | Replay-aware KPIs: unique_visitors, conversion_rate, avg dwell per zone, queue_depth, abandonment_rate. |
| `GET`  | `/stores/{id}/funnel` | Conversion funnel stages with drop-off percentages. |
| `GET`  | `/stores/{id}/heatmap` | Per-zone visit counts, avg dwell time, and intensity (0–100). |
| `GET`  | `/stores/{id}/anomalies` | Active anomalies: BILLING_QUEUE_SPIKE, CONVERSION_DROP, DEAD_ZONE, STALE_FEED. |
| `GET`  | `/health` | Service health + per-camera staleness status. |

Use store id `STORE_BLR_002` or the alias `ST1008` — both are accepted.

## How the pipeline feeds the API

1. `detect.py` reads each video frame (live RTSP or recorded `.mp4`) and runs **YOLOv8n** to detect `person` bounding boxes.
2. `tracker.py` wraps **ByteTrack** to maintain stable `visitor_id` strings across frames, including re-entry matching and staff-like movement heuristics.
3. `detect.py` maps spatial zones from `store_layout.json` onto pixel coordinates. When a tracked person crosses a zone boundary it emits the appropriate `EventType` (ENTRY, ZONE_ENTER, ZONE_DWELL, BILLING_QUEUE_JOIN, EXIT, REENTRY).
4. Events are batched (≤ 500) and POSTed to `/events/ingest`. The endpoint is idempotent — re-sending the same `event_id` increments the `duplicate` counter without writing a second row.
5. Every event keeps its raw `confidence` and includes review metadata for low-confidence detections, ambiguous re-entry matches, and staff heuristics.

## Replay and validation mode

The challenge clips are historical and compressed into a short camera window, while the POS file contains store-day order timestamps. To keep outputs meaningful for review:

- API `as_of` values are anchored to the latest event timestamp for the store.
- `/health` and stale-feed checks compare cameras against that replay timestamp, not the wall clock.
- Visitor counts use distinct non-staff visitor IDs from presence events, not only `ENTRY`, so under-emitted entry-line crossings do not collapse the funnel.
- Dwell calculations ignore impossible values above one hour, protecting the dashboard from corrupted legacy rows generated before the frame-time dwell fix.
- Conversion first applies the required 5-minute billing-zone-to-POS window. If replay timestamps do not overlap POS wall-clock time, it falls back to same-day order correlation capped by observed billing-queue visitors.
- Low-confidence detections are retained but marked with `review_required` and `LOW_DETECTION_CONFIDENCE`, so the replay remains auditable instead of silently dropping uncertain people.

Current validation target after loading the included sample events:

```text
Visitors: 35
Funnel: Entry 35 → Zone Visit 35 → Billing Queue 5 → Purchase 5
Conversion: 14.29%
Queue depth: 0
Max zone avg dwell: ~70 seconds
Health: ok across 5 cameras
```

## Running tests

```bash
pip install -r app/requirements.txt pytest-asyncio httpx
pytest tests/ -v --tb=short
```

Run the challenge sanity checks against the live API:

```bash
python3 pipeline/validate_data.py
```
