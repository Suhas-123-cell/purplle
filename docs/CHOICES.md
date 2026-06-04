# Engineering Choices

## 1. Detection model: YOLOv8n over RT-DETR

**What AI suggested:** When I asked Claude to compare detection models for retail CCTV analytics, it recommended RT-DETR (ResNet-50 backbone) as the primary choice, citing higher mAP (~53 vs ~37 on COCO) and better partial-occlusion handling from the transformer architecture. The argument was that accuracy is the load-bearing variable for a system that produces store analytics — a missed detection is a missed visitor.

**What I chose and why:** YOLOv8n. The deciding factor was inference speed on the actual hardware, not COCO mAP. The store runs on a mid-range Intel NUC without a discrete GPU. YOLOv8n runs at ~18 fps on CPU for 640×480; RT-DETR at the same config runs at ~4 fps. At 4 fps, a person crossing the ~1.5 m store entrance is visible for roughly 6 frames — marginal for ByteTrack to lock a stable ID before they leave frame. At 18 fps that's ~25 frames. The AI was optimising for detection accuracy in isolation; I was optimising against the actual constraint.

The mAP gap matters when you're distinguishing fine-grained object classes in cluttered scenes. For person-counting in a controlled retail environment, YOLOv8n is adequate — the real occlusion handling comes from ByteTrack's low-confidence buffer, not from the detector. Ecosystem maturity also mattered: ultralytics ships as a single pip package with a clean Python API and one-liner fine-tuning. For a store where staff handle maintenance, that operability gap is real.

Trade-off I accept: in extremely dense crowds YOLOv8n accuracy degrades faster. For a boutique-format Purplle store that's an edge case. Upgrade path: fine-tune YOLOv8s on Purplle footage and swap it by changing one config key — `detect.py` is model-agnostic at the interface level.

---

## 2. Event schema: why session_seq matters, and why low-confidence events are not suppressed

**What AI suggested:** Claude's initial schema had no session ordinal at all — it suggested using timestamp ordering to reconstruct funnel stages in SQL, arguing that adding a sequence number was premature denormalisation. For confidence handling, it suggested dropping events below a 0.5 threshold before emitting, keeping the event stream clean.

**What I chose and why:** I added `session_seq` and kept low-confidence events in the stream. Here's why I pushed back on both.

On session_seq: timestamp-based funnel queries require a window function to find the first billing visit per visitor per day while excluding re-entries. A `session_seq` that resets to 1 on REENTRY makes that query a simple `GROUP BY (visitor_id, date) WHERE session_seq = 1 AND event_type = 'BILLING_QUEUE_JOIN'` — no window function, no subquery. It also makes tracker glitches visible: a seq jump from 1 to 8 with nothing in between signals dropped frames rather than a real event gap. That diagnostic value is worth the extra field. I did take the AI's suggestion on naming — it pointed out I'd originally used `visit_seq` which is ambiguous when re-entries reset it, and `session_seq` is clearer.

On confidence suppression: an event with `confidence=0.31` might be a real person partially blocked by a shelf. Dropping it silently makes the pipeline non-auditable — you can't tell after the fact whether a low count came from an empty store or from events that got filtered. Instead the event is emitted at its real confidence value with `review_required=true` and `LOW_DETECTION_CONFIDENCE` in review_flags. The API includes all events in metrics right now (conservative). A future `?min_confidence=0.5` query param would let operators tune this without re-running the pipeline. Suppressing at emit time kills that option permanently.

**Review flags over hard deletion** apply to staff uncertainty and re-entry matching too. A fast three-zone sweep marks `is_staff=True` with a staff reason recorded; an ambiguous re-entry match is tagged `AMBIGUOUS_REENTRY_MATCH`. Dashboard numbers stay stable, but reviewers have a clear audit trail.

---

## 3. API persistence: SQLite over PostgreSQL

**What AI suggested:** Claude recommended PostgreSQL from the start, framing it as "production-ready" choice that signals engineering maturity. The argument was that SQLite has write serialisation constraints and no network access, which would be limiting if this system scaled to 40 stores. It also pointed out that PostgreSQL's `JSONB` column type would handle the event metadata field more flexibly than a separate extracted column.

**What I chose and why:** SQLite with WAL mode. I disagreed with the AI on the scaling argument because it was solving for a hypothetical scale that doesn't exist in this submission.

SQLite is a file. The entire database is a single file mounted into the Docker container via a volume bind. There is no separate service to configure, no connection string to get wrong, no `pg_hba.conf`, no credentials to set. For a submission that must run with `docker compose up -d` on a reviewer's machine from a cold clone, that's a material reliability advantage — PostgreSQL would add one more thing that can fail at first boot.

The performance case is also clear. The workload is one store, ~500 events per camera flush across 4 cameras, and one dashboard polling every 5 seconds. SQLite in WAL mode handles concurrent readers with a single writer fine at that load. Aggregation queries over ~200k rows for a full operating day run in under 5 ms on any reasonable hardware.

On the JSONB argument: I store `queue_depth`, `sku_zone`, and `session_seq` as extracted columns on EventRow rather than a JSONB blob. This is more verbose in the ORM but means every analytics query gets typed column access without JSON extraction — which matters for aggregation correctness and avoids silent nulls from key-name typos.

**Trade-off I accept:** SQLite doesn't scale horizontally. If the system extended to 40 stores with a central API, I'd need Postgres or TimescaleDB. I'm not designing for that today. If it's needed, `database.py` is the entire persistence boundary — `DATABASE_URL` and the ORM base are the only changes. All query code in `metrics.py`, `funnel.py`, etc. ports unchanged because async SQLAlchemy abstracts the wire protocol.

---

## 4. Replay-aware analytics over wall-clock-only analytics

The challenge dataset is historical. The CCTV-derived events are anchored to `2026-04-10T10:00:00Z`, while reviewers run the API on a later machine date. If health, anomaly, and dashboard labels use only `datetime.utcnow()`, every camera appears stale and all "today" windows are empty. The API therefore uses the latest event timestamp for the store as the reference time for replay-sensitive calculations.

This decision keeps `/health`, `/anomalies`, `/metrics`, `/funnel`, and `/heatmap` internally consistent during review. In a production deployment, the same code path still works for live streams because the latest event timestamp is close to wall-clock time. If a future central service needs both modes explicitly, this can be promoted to a `REPLAY_MODE` environment variable, but deriving from the event stream is simpler and less error-prone for this single-store submission.

The visitor base also intentionally uses distinct non-staff visitors from presence events (`ENTRY`, `GROUP_ENTRY`, zone activity, and billing queue joins) rather than only `ENTRY`. This protects the business metrics from entry-line under-emission while keeping the detection pipeline itself accountable through the validation script, which still reports entry/group-entry coverage.

---

## 5. POS correlation for compressed replay clips

The problem statement requires conversion by matching visitors in the billing zone during the five minutes before a POS transaction. That is the primary path. The provided replay events, however, can be compressed into a short camera interval while POS keeps full store-day wall-clock timestamps. When no five-minute overlap exists, the API falls back to same-day POS correlation capped by distinct billing-queue visitors.

This is a conservative fallback: it never produces more converted visitors than observed billing visitors, so conversion remains session-based and bounded. The validation script prints both CCTV and POS ranges so this assumption is visible to reviewers instead of hidden in the code.

---

## 6. Multi-store architecture: alias map over hardcoded allowlist

The original ingestion layer validated incoming `store_id` values against `_ALLOWED_STORES`, a hardcoded Python set. Adding a new store meant editing application code and redeploying. This was fine for a single-store submission but did not scale.

The replacement is a two-part approach. First, a lightweight format check rejects obviously malformed IDs (empty string, wrong type). Second, `_ALIAS_MAP` — a dictionary mapping short aliases to canonical IDs — normalises the identifier before any database write:

```
_ALIAS_MAP = {
    "ST1008":     "STORE_BLR_002",
    "ST1":        "STORE_1",
    "ST2":        "STORE_2",
    "ST1076":     "STORE_1076",    # organizer sample data format
}
```

The organizer's sample JSONL uses `store_code: store_1076` and `store_id: ST1076` — both normalize to `STORE_1076` so any events from that feed land in the same canonical store bucket without special-casing in the analytics layer.

Any store ID not in the alias map is accepted as its own canonical form as soon as its first event arrives. New stores therefore require zero application changes — they are admitted automatically once the pipeline starts sending events. If a short alias is also needed, a single dict entry is added, which is a configuration-level change rather than a logic change.

This also solves a cross-source join problem: the POS CSV for Store 1 uses `ST1` while the camera system emits `STORE_1`. `_ALIAS_MAP` ensures both map to the same canonical key before any row is written, so aggregation queries never need to handle synonyms.

---

## 7. Offline schema validator (validate_schema.py)

Each store's pipeline produces event JSON files in `data/events/` before they are POSTed to the API. Bugs in `emit.py` or the layout mapping can produce structurally valid JSON that still violates the event contract — a non-UUID `event_id`, a timestamp in the wrong format, a confidence value outside `[0, 1]`, or a zone event missing `zone_id`.

`pipeline/validate_schema.py` runs entirely offline against the files in `data/events/` and checks:

- **UUID v4 `event_id`** — regex match against the canonical UUID v4 pattern; catches sequential IDs or hash-based IDs that slip through.
- **ISO-8601 timestamps** — `datetime.fromisoformat` parse; catches Unix epoch integers, missing timezone offsets, and truncated strings.
- **Confidence range** — must be in `[0.0, 1.0]`; catches un-normalised detector scores (e.g. raw logits).
- **`zone_id` presence** — required for all zone-related event types (`ZONE_ENTER`, `ZONE_DWELL`, `ZONE_EXIT`); catches events where the layout mapping failed silently.
- **No duplicate `event_id`s** — accumulates all IDs across the full events directory and reports any collision; catches pipeline restarts that regenerate the same UUID.

Running the validator before `./run.sh` (or its store variants) catches pipeline bugs before they reach the API, where a duplicate `event_id` would silently be counted as a deduplicated event and a missing `zone_id` would cause a zone-level query to return zero results. The offline check costs nothing at ingest time and makes the pipeline's output independently auditable.

---

## 8. Security trade-offs for localhost deployment

**What was deliberately omitted and why.**

This system runs on `127.0.0.1` only — both the API and the dashboard are bound to loopback in `docker-compose.yml`. There is no public network path to either service.

**No authentication on API endpoints.** Every endpoint is open. For a localhost challenge demo with no user data, adding token-based auth would mean managing a secret, passing it from the pipeline to the API, and validating it in a middleware layer — meaningful complexity with no security benefit when the only callers are local scripts on the same machine. In a production deployment this is the first thing to add: an API key header checked in a FastAPI middleware or dependency.

**Rate limiting on `/events/ingest` only.** The ingest endpoint is the only write path with unbounded call frequency — a pipeline loop bug could flood the DB in seconds. A 60-req/min sliding window is enforced per client IP using an in-process counter (no external dependency). The read endpoints (`/metrics`, `/funnel`, etc.) are not rate-limited because they are idempotent, their cost is bounded by the SQLite query time, and the only callers are the dashboard's 10-second polling loop and the validation script.

**Dead API key header removed from pipeline.** An earlier version of `pipeline/ingest_events.py` sent an `X-API-Key` header from an env var, but the server never validated it. That header was removed — security theatre is worse than no auth because it creates false confidence.

**Dashboard bound to localhost.** Nginx in the dashboard container now binds to `127.0.0.1:3000` matching the API's `127.0.0.1:8000`. Both were already correct from a Docker network perspective, but explicit loopback binding makes the intent clear and prevents accidental exposure on a multi-homed host.
