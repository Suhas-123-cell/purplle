import json
import os
import sys
import requests

BATCH_SIZE = 500
API_URL = os.getenv("INGEST_API_URL", "http://localhost:8000")


def main() -> None:
    for path in sys.argv[1:]:
        try:
            with open(path) as fh:
                events = [json.loads(l) for l in fh if l.strip()]
        except Exception as exc:
            print(f"ERROR: {path}: {exc}", file=sys.stderr)
            continue

        total = len(events)
        accepted = 0
        rejected = 0
        duplicate = 0

        for i in range(0, total, BATCH_SIZE):
            batch = events[i: i + BATCH_SIZE]
            try:
                r = requests.post(
                    f"{API_URL}/events/ingest",
                    json={"events": batch},
                    timeout=30,
                )
                r.raise_for_status()
                body = r.json()
                accepted += body.get("accepted", 0)
                rejected += body.get("rejected", 0)
                duplicate += body.get("duplicate", 0)
            except Exception as exc:
                print(f"ERROR: {path}: batch {i // BATCH_SIZE}: {exc}", file=sys.stderr)
                continue

        print(
            f"{path} total={total} accepted={accepted} "
            f"rejected={rejected} duplicate={duplicate}"
        )


if __name__ == "__main__":
    main()
