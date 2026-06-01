import json
import os
import sys
import requests

BATCH_SIZE = 500
API_URL = os.getenv("INGEST_API_URL", "http://localhost:8000")
API_KEY = os.getenv("API_KEY", "")

for path in sys.argv[1:]:
    with open(path) as fh:
        events = [json.loads(l) for l in fh if l.strip()]

    total = len(events)
    accepted = 0
    rejected = 0
    duplicate = 0

    for i in range(0, total, BATCH_SIZE):
        batch = events[i: i + BATCH_SIZE]
        r = requests.post(
            f"{API_URL}/events/ingest",
            json={"events": batch},
            headers={"X-API-Key": API_KEY} if API_KEY else {},
            timeout=30,
        )
        r.raise_for_status()
        body = r.json()
        accepted += body.get("accepted", 0)
        rejected += body.get("rejected", 0)
        duplicate += body.get("duplicate", 0)

    print(
        f"{path} total={total} accepted={accepted} "
        f"rejected={rejected} duplicate={duplicate}"
    )
