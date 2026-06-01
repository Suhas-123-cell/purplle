# Store Intelligence — System Design

## System architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        STORE FLOOR                              │
│  CAM_ENTRY_01  CAM_FLOOR_01  CAM_FLOOR_02  CAM_BILLING_01      │
└────────┬───────────┬──────────────┬───────────────┬────────────┘
         │  RTSP     │              │               │
         ▼           ▼              ▼               ▼
┌──────────────────────────────────────────────────────────────┐
│                   pipeline/detect.py                         │
│   YOLOv8n  ──►  ByteTrack  ──►  VisitorTracker (emit.py)    │
│   (bounding boxes)  (stable IDs)   (zone mapping + events)  │
└────────────────────────┬─────────────────────────────────────┘
                         │  HTTP POST /events/ingest
                         │  (batches ≤ 500 events, JSON)
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                   app/main.py  (FastAPI)                     │
│  ┌──────────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │  ingestion   │  │ metrics  │  │  funnel  │  │heatmap  │ │
│  │  (idempotent)│  │          │  │          │  │         │ │
│  └──────┬───────┘  └────┬─────┘  └────┬─────┘  └────┬────┘ │
│         │               │             │              │       │
│  ┌──────▼───────────────▼─────────────▼──────────────▼────┐ │
│  │            SQLite via aiosqlite (async)                 │ │
│  │  events · visitor_sessions · pos_transactions           │ │
│  └─────────────────────────────────────────────────────────┘ │
└────────────────────────┬─────────────────────────────────────┘
                         │  fetch() every 5 s
                         ▼
              dashboard/index.html
              (vanilla JS operations console)
```

## Component descriptions

### Detection pipeline (`pipeline/`)

`detect.py` runs a per-camera inference loop. Each frame is passed to **YOLOv8n**, which returns bounding boxes for the `person` class. The model runs at the native frame rate of the feed; on a laptop CPU with a 1280×720 RTSP stream, inference sits around 12–18 fps, which is sufficient for retail analytics (people move slowly). Detections are filtered by a minimum confidence of 0.30 to prevent background objects triggering false events.

`tracker.py` wraps **ByteTrack**, a multi-object tracking algorithm that handles occlusion by maintaining a two-stage buffer: high-confidence detections update existing tracks immediately; low-confidence ones are held until a matching high-confidence detection confirms them in a later frame. This means a person briefly obscured by a shelf does not cause a spurious EXIT event.

`emit.py` contains `VisitorTracker`, which maintains per-visitor state keyed on the ByteTrack ID. It maps pixel coordinates to named zones using the bounding polygons in `store_layout.json` and emits typed events:
- **ENTRY** — bounding box centroid crosses the store boundary inward for the first time for this visit
- **ZONE_ENTER / ZONE_DWELL / ZONE_EXIT** — centroid enters/lingers in/leaves a product zone
- **BILLING_QUEUE_JOIN / BILLING_QUEUE_ABANDON** — visitor reaches the billing zone; abandon if they leave without a POS match
- **EXIT** — centroid crosses the store boundary outward
- **REENTRY** — same ByteTrack appearance signature seen after a prior EXIT (handled by an appearance history cache)

### Event stream

Events are accumulated into batches of up to 500 and POSTed to `/events/ingest`. Batching amortises HTTP round-trip overhead. Each event carries a UUID v4 `event_id` generated at emit time; the API deduplicates on this key, making the pipeline safe to restart or replay without inflating metrics.

### Intelligence API (`app/`)

The FastAPI backend is structured around five thin computation modules (`metrics.py`, `funnel.py`, `heatmap.py`, `anomalies.py`, `health.py`) each issued a read-only `AsyncSession`. Shared business semantics live in `analytics.py`, so visitor base, conversion correlation, replay reference time, and current queue state are not reimplemented differently per endpoint. All writes go through `ingestion.py`.

Store ID normalisation (`normalize_store_id`) transparently maps the POS alias `ST1008` to the canonical `STORE_BLR_002`, so POS records and CCTV events join correctly even when the CSV uses a different identifier than the camera system.

### Dashboard (`dashboard/`)

A single `index.html` with no build step. It polls metrics, funnel, heatmap, anomalies, and health in parallel every 10 seconds, renders the results into DOM nodes, and exits gracefully with an error banner if the API is unreachable. The interface is an operations console: KPI strip, conversion funnel, zone dwell heatmap, anomaly detail, and camera feed status.

## Data flow (end-to-end)

```
CCTV frame
  → detect.py (YOLOv8n inference)
  → tracker.py (ByteTrack: assign stable visitor_id)
  → emit.py    (zone mapping: emit Event objects)
  → POST /events/ingest  (JSON batch)
  → ingestion.py (dedup on event_id, write EventRow + update VisitorSession)
  → SQLite
  → GET /stores/STORE_BLR_002/metrics  (read EventRow + VisitorSession aggregates)
  → dashboard (display)
```

POS data flows separately: the CSV is loaded into `pos_transactions` on API startup. The shared analytics layer joins CCTV-derived billing-zone events against POS timestamps to compute conversion rate. For compressed replay clips with no direct timestamp overlap, same-day POS orders are capped by observed billing-queue visitors.

## Edge case handling

**Re-entry.** `VisitorTracker` maintains an `appearance_history` dict keyed on visitor appearance hash (derived from ReID embeddings or, in fallback mode, ByteTrack ID). When a visitor is detected after their EXIT, the tracker emits `REENTRY` rather than `ENTRY`. On the API side, `_upsert_visitor_session` keeps a single `VisitorSession` row per logical visit day, so the unique_visitors count is not inflated. The `session_seq` field resets to 1 on REENTRY, allowing the funnel to treat the re-entry as a continuation rather than a new visit.

**Staff exclusion.** The pipeline applies a heuristic: any tracked person who traverses more than three distinct zones within 90 seconds is tagged `is_staff=True`. A secondary rule flags persons who appear at store open time before any customer events. Staff events are still emitted (audit trail) but the `is_staff` flag causes them to be excluded from all customer-facing KPIs.

**Group entry.** The detector operates at the bounding-box level. Three people entering together produce three separate detections, each assigned a unique ByteTrack ID and therefore a unique `visitor_id`. No merging occurs. This is intentional: each person is an independent shopping decision.

**Occlusion.** ByteTrack's low-confidence buffer handles brief occlusion (under ~1 s). For longer occlusions the track is marked `lost` and held for up to 3 seconds before being terminated. If the person re-appears within 3 s, the track is re-linked and no EXIT/ENTRY pair is emitted. Confidence degrades gracefully: low-confidence events are emitted with their actual confidence value rather than being suppressed, so the API has full information to decide how to weight them.

**Stale camera feed.** The `/health` endpoint computes `lag_seconds` per camera as `reference_now - max(event.timestamp)`, where `reference_now` is the latest event timestamp for the store. For live streams this is effectively wall-clock time; for historical replay data it prevents false stale-feed alarms caused by running April data on a later date.

**Corrupted dwell guardrail.** Earlier event generation can produce impossible dwell values if real wall-clock time is mixed with frame timestamps. The pipeline now computes final dwell from frame time, and API aggregations ignore dwell rows above one hour so legacy bad rows cannot dominate heatmap or metrics output.

## AI-Assisted Decisions

**1. Event schema design (session_seq field).** The initial schema draft omitted `session_seq`. After discussing the re-entry problem with an LLM — specifically how to distinguish "visitor browsed twice" from a tracker glitch — the LLM suggested attaching a monotonic sequence number that resets on REENTRY. This allows the funnel query to use `MIN(session_seq)` to find the earliest stage per visit without a complex window function.

**2. SQLite vs PostgreSQL.** When evaluating the persistence layer the LLM articulated the argument that SQLite with WAL mode is adequate for single-writer, single-store workloads up to ~100 k events/day, and that the operational simplicity (zero separate process, trivial Docker volume mount, no connection pool configuration) outweighs the horizontal-scale benefits of Postgres for this submission scope. The trade-off is documented in `docs/CHOICES.md`.

**3. Anomaly detection thresholds.** The 2× multiplier for `BILLING_QUEUE_SPIKE` and the 50% drop threshold for `CONVERSION_DROP` were proposed by the LLM based on typical retail analytics literature. The LLM noted that real deployments would calibrate these thresholds per store using historical p95 values, and suggested storing the 7-day rolling averages in a separate `daily_stats` table (not yet implemented) rather than recomputing them on every request.
