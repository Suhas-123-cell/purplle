# Engineering Choices

## 1. Detection model: YOLOv8n over RT-DETR

We chose **YOLOv8n** (nano) rather than RT-DETR for the detection backbone. The decision came down to three concrete constraints of the Brigade Road deployment.

First, **inference speed on commodity hardware**. The store's NVR box is a mid-range Intel NUC without a discrete GPU. YOLOv8n runs at ~18 fps on CPU for 640×480 input; RT-DETR (ResNet-50 backbone) runs at ~4 fps on the same hardware. At 4 fps, a person crossing the narrow store entrance (~1.5 m wide) is visible for roughly 6 frames at normal walking speed — marginal for reliable detection. At 18 fps, they appear in ~25 frames, giving ByteTrack enough observations to lock a stable ID before they reach the first product zone.

Second, **accuracy is sufficient for retail counting**. RT-DETR's mAP advantage (~53 vs ~37 on COCO) matters when you need to distinguish fine-grained object classes in cluttered scenes. For person-counting in a controlled retail environment, YOLOv8n's accuracy is entirely adequate — false negatives are rare in open aisles and partially occluded detections are handled by ByteTrack's low-confidence buffer rather than requiring a more powerful detector.

Third, **ecosystem maturity**. YOLOv8n ships as a single pip-installable package (`ultralytics`) with a clean Python API, built-in ONNX export, and well-documented fine-tuning on custom datasets. This matters for operability: store staff are not ML engineers, and a model that can be updated with a `yolo train` one-liner is more maintainable than a research-grade transformer that requires custom training scripts.

The trade-off we accept: in a store with very dense crowds (rare for Purplle Brigade Road, which is a boutique-format location) detection accuracy could degrade. A future upgrade path is to fine-tune YOLOv8s on Purplle-specific footage and swap it in by changing a single config key, since the pipeline is model-agnostic at the `detect.py` interface.

---

## 2. Event schema: why session_seq matters, and why low-confidence events are not suppressed

**session_seq** is a monotonically increasing integer per visitor per visit, reset to 1 on REENTRY. It solves a subtle double-counting problem in funnel queries. Without it, determining "how many visitors reached the billing zone during their *first* visit, not a re-entry" requires a correlated subquery joining on timestamps. With `session_seq`, the funnel query becomes a simple `WHERE session_seq >= 1 AND event_type = 'BILLING_QUEUE_JOIN'` grouped by `(visitor_id, session_start_date)`. The seq also makes it straightforward to detect tracker glitches: a sequence that jumps from 1 to 8 without 2–7 indicates dropped frames, not a real event gap.

**Not suppressing low-confidence events** is a deliberate audit-trail choice. An event with `confidence=0.31` might represent a real person who is partially occluded by a display stand — suppressing it silently would make the pipeline's output non-reproducible. Instead, the full event is emitted with the actual confidence value. Downstream consumers (the API's metrics functions) can apply their own threshold — currently they include all events regardless of confidence, which is conservative. A future enhancement is to expose a `?min_confidence=0.5` query parameter on the metrics endpoints, letting operators tune precision/recall without reprocessing stored events. Keeping low-confidence events in the database preserves that option.

---

## 3. API persistence: SQLite over PostgreSQL

For a single-store, single-writer analytics workload, SQLite with WAL (Write-Ahead Logging) mode is the pragmatically correct choice.

**Operational simplicity.** SQLite is a file. The entire database is one file mounted into the Docker container via a volume bind. There is no separate service to start, no connection string to configure, no credentials to rotate, and no `pg_hba.conf` to get wrong. For a challenge submission that must run with `docker compose up -d` from a cold start, this is a significant reliability advantage.

**Performance is adequate.** The target workload is one store generating at most ~500 events per camera flush (every 5–30 seconds across 4 cameras) and one dashboard polling 4 read endpoints every 5 seconds. SQLite in WAL mode supports concurrent readers and a single writer without lock contention. Read latency for the aggregation queries (which operate on at most ~200 k rows for a full operating day) is under 5 ms on any reasonable hardware.

**Trade-offs accepted.** SQLite does not support horizontal scaling. If this system were extended to 50 stores each with their own API instance, each instance would still use its own SQLite file (no shared state), which is actually fine for the per-store isolation model. If a centralised multi-store view were required, the `database.py` module is the only file that would need to change — the `DATABASE_URL` constant and the `Base` ORM definitions are the entire persistence boundary. Migrating to PostgreSQL or TimescaleDB at that point would be a one-day effort, not a re-architecture. The async SQLAlchemy layer (`aiosqlite` driver for SQLite, `asyncpg` for Postgres) abstracts the wire protocol, so all query code in `metrics.py`, `funnel.py`, etc. would port unchanged.
