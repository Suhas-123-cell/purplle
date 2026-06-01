# Purplle Store Intelligence — Brigade Road Bangalore

Real-time CCTV analytics for **STORE_BLR_002 (ST1008)**.  
Detects visitors, tracks zone dwell, spots anomalies, and streams metrics to a live dashboard.

## Setup in 5 commands

```bash
# 1. Clone the repo
git clone <repo> && cd purplle

# 2. Drop the POS export into the data directory
cp /path/to/pos_transactions.csv data/

# 3. Drop footage clips (optional — pipeline runs on live RTSP or recorded files)
cp -r /path/to/footage data/footage/

# 4. Start API + dashboard
docker compose up -d

# 5. Start the detection pipeline
cd pipeline && pip install -r requirements.txt && ./run.sh
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
  dashboard/index.html   ← vanilla JS, polls API every 5 s
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events/ingest` | Batch-ingest camera events. Body: `{"events": [...]}`. Returns accepted/rejected/duplicate counts. |
| `GET`  | `/stores/{id}/metrics` | Real-time KPIs: unique_visitors, conversion_rate, queue_depth, abandonment_rate. |
| `GET`  | `/stores/{id}/funnel` | Conversion funnel stages with drop-off percentages. |
| `GET`  | `/stores/{id}/heatmap` | Per-zone visit counts, avg dwell time, and intensity (0–100). |
| `GET`  | `/stores/{id}/anomalies` | Active anomalies: BILLING_QUEUE_SPIKE, CONVERSION_DROP, DEAD_ZONE, STALE_FEED. |
| `GET`  | `/health` | Service health + per-camera staleness status. |

Use store id `STORE_BLR_002` or the alias `ST1008` — both are accepted.

## How the pipeline feeds the API

1. `detect.py` reads each video frame (live RTSP or recorded `.mp4`) and runs **YOLOv8n** to detect `person` bounding boxes.
2. `tracker.py` wraps **ByteTrack** to maintain stable `visitor_id` strings across frames.
3. `emit.py` contains `VisitorTracker`, which maps spatial zones from `store_layout.json` onto pixel coordinates. When a tracked person crosses a zone boundary it emits the appropriate `EventType` (ENTRY, ZONE_ENTER, ZONE_DWELL, BILLING_QUEUE_JOIN, EXIT, REENTRY).
4. Events are batched (≤ 500) and POSTed to `/events/ingest`. The endpoint is idempotent — re-sending the same `event_id` increments the `duplicate` counter without writing a second row.

## Running tests

```bash
pip install -r app/requirements.txt pytest-asyncio httpx
pytest tests/ -v --tb=short
```
