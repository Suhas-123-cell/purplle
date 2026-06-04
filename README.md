# Purplle Store Intelligence — Brigade Road Bangalore

Real-time and replay-safe CCTV analytics for **STORE_BLR_002 (ST1008)**.  
Detects visitors, tracks zone dwell, correlates billing visits with POS orders, spots anomalies, and streams metrics to a live dashboard.

## Quick Start (5 commands)

```bash
# 1. Clone the repo
git clone <repo> && cd purplle

# 2. Add POS data and start the API + dashboard
cp /path/to/pos_transactions.csv data/ && docker compose up -d

# 3. Install detection pipeline dependencies
python3 -m venv .venv && .venv/bin/pip install -r pipeline/requirements.txt

# 4. Run the detection pipeline against footage (feeds API automatically)
pipeline/run_store1.sh --video-dir "/path/to/Store 1"

# 5. Confirm everything works
curl http://localhost:8000/stores/STORE_1/metrics
# Dashboard at http://localhost:3000
```

> **No footage?** Replace step 4 with `pipeline/run_store1.sh --mock` to generate
> synthetic events and still exercise the full API and dashboard.

---

## All stores

```bash
# Store 1 (STORE_1 / ST1)
pipeline/run_store1.sh --video-dir "/path/to/Store 1"

# Store 2 (STORE_2 / ST2)
pipeline/run_store2.sh --video-dir "/path/to/Store 2"

# Brigade Road store (STORE_BLR_002 / ST1008)
pipeline/run.sh --video-dir /data/footage
```

### Ingesting organizer sample events

If you received a `sample_events.jsonl` from the challenge organiser in a different schema, normalise it first:

```bash
python3 pipeline/normalize_sample.py /path/to/sample_events.jsonl data/events/canonical.jsonl
python3 pipeline/ingest_events.py data/events/canonical.jsonl
```

### Validating event schema and API (optional)

```bash
# Offline schema check — no API needed
python3 pipeline/validate_schema.py data/events/store1 data/events/store2

# Live API sanity check
python3 pipeline/validate_data.py                   # defaults to STORE_BLR_002
python3 pipeline/validate_data.py --store STORE_1
python3 pipeline/validate_data.py --store STORE_2
```

### Resetting the database

```bash
rm data/store_intelligence.db && docker compose restart api
```

Re-run the pipeline scripts after restart to re-ingest events.

### Pipeline flags (all run scripts)

| Flag | Default | Description |
|---|---|---|
| `--video-dir DIR` | Script default (user-specific) | Path to the store's footage directory |
| `--output-dir DIR` | `data/events/storeN/` | Where to write generated JSONL files |
| `--api-url URL` | `http://localhost:8000` | API base URL |
| `--start-time ISO` | `2026-04-10T10:00:00Z` | Replay start timestamp injected into events |
| `--sample-every N` | `5` | Process every Nth frame (higher = faster, fewer events) |
| `--mock` | Off | Emit synthetic events without running YOLO (for dry-run) |

---

## Architecture

Supports 3 stores: **STORE_BLR_002**, **STORE_1**, and **STORE_2**. The dashboard includes a store switcher dropdown that switches all 5 API calls live.

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
                            store switcher toggles active store for all calls
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events/ingest` | Batch-ingest camera events. Body: `{"events": [...]}`. Returns accepted/rejected/duplicate counts. |
| `GET`  | `/stores/{id}/metrics` | Replay-aware KPIs: unique_visitors, conversion_rate, avg dwell per zone, queue_depth, abandonment_rate. |
| `GET`  | `/stores/{id}/funnel` | Conversion funnel stages with drop-off percentages. |
| `GET`  | `/stores/{id}/heatmap` | Per-zone visit counts, avg dwell time, and intensity (0–100). |
| `GET`  | `/stores/{id}/anomalies` | Active anomalies: BILLING_QUEUE_SPIKE, CONVERSION_DROP, DEAD_ZONE, STALE_FEED. |
| `GET`  | `/health` | Service health + per-camera staleness status. Accepts optional `?store_id=` query param to scope the check to a single store. |

Use store id `STORE_BLR_002` or the alias `ST1008` — both are accepted. For the two additional stores use `STORE_1` / `ST1` and `STORE_2` / `ST2`.

## Using the Dashboard

Open **http://localhost:3000** in your browser after `docker compose up -d`.

### Store switcher
The dropdown in the top-right corner lets you switch between all three stores. Selecting a store immediately re-fires all five API calls and updates every section — metrics, funnel, heatmap, anomalies, and camera health — for that store. The subtitle under the logo updates to match.

### Live Metrics strip
Four KPI cards at the top, refreshed every 10 seconds:

| Card | What it shows | When to act |
|---|---|---|
| **Tracked Visitors** | Unique non-staff visitors seen today (presence-based, not just entry line) | Baseline check |
| **Conversion Rate** | Billing-queue visitors who match a POS transaction within 5 min | Below 10% is a concern |
| **Queue Depth** | People currently in the billing zone with no exit/purchase yet | ≥ 4 triggers a yellow warning; open an extra counter |
| **Abandonment Rate** | Visitors who left billing without purchasing | Rising trend = checkout friction |

### Conversion Funnel
Shows the four-stage journey: **Entry → Zone Visit → Billing Queue → Purchase**.  
Each stage shows visitor count and the drop-off percentage from the previous stage. Red bars (>40% drop) and yellow bars (>15% drop) highlight problem stages at a glance. Re-entries are deduplicated — the same visitor counted once regardless of how many times they appear.

### Zone Activity (Heatmap)
One row per zone, sorted busiest-to-quietest by intensity (0–100). Each row shows:
- Zone name + busy/quiet label
- Average dwell time in that zone
- Total visit count
- A **low data** badge if fewer than 20 sessions contributed — treat intensity numbers with caution for those zones.

### Active Anomalies
Each card shows:
- **Severity badge** — CRITICAL (red) / WARN (yellow) / INFO (blue)
- **Anomaly type** — BILLING_QUEUE_SPIKE, STALE_FEED, DEAD_ZONE, CONVERSION_DROP
- **Detected at** — exact date and time the anomaly was computed (anchored to the store's latest event, not wall clock)
- **Description** — human-readable explanation with specific numbers
- **Suggested action** — what to do operationally
- **Context chips** — raw numbers at a glance: `lag Xs` for stale feeds, `idle X min` for dead zones, `depth N` for queue spikes, `cam` or `zone` identifier

Anomaly types explained:

| Type | Triggers when | Severity |
|---|---|---|
| `STALE_FEED` | Camera silent for >10 min | WARN; CRITICAL at >20 min |
| `DEAD_ZONE` | Zone had no visits for 30 min despite activity in past 24 h | WARN |
| `BILLING_QUEUE_SPIKE` | Current-hour joins exceed 2× 7-day hourly average; or depth ≥ 4 with no baseline | WARN / CRITICAL |
| `CONVERSION_DROP` | Today's conversion below 50% of 7-day average | WARN / CRITICAL |

### Camera Feeds
Bottom section lists every camera the API has seen events from, with lag in seconds and a green (live) / amber (stale) chip. Lag is computed relative to the store's own latest event timestamp — historical replay data is never falsely shown as stale.

---

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

Current validation target after loading the included sample events (all 3 stores):

```text
STORE_BLR_002 (ST1008)
Visitors: 35
Funnel: Entry 35 → Zone Visit 35 → Billing Queue 5 → Purchase 5
Conversion: 14.29%
Queue depth: 0
Max zone avg dwell: ~70 seconds
Health: ok across 5 cameras

STORE_1 (ST1) and STORE_2 (ST2)
Events validated via: .venv/bin/python pipeline/validate_schema.py data/events/
```

## Running tests

```bash
pip install -r app/requirements.txt
pytest tests/ -v
# Coverage threshold is 70% — enforced by pyproject.toml
```

Run the challenge sanity checks against the live API:

```bash
python3 pipeline/validate_data.py
```
