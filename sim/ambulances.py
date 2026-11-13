"""Ambulance position simulator (clearly a simulator process). Posts synthetic positions once a second.
Run: docker compose run --rm tools python -m sim.ambulances [--count 300] [--seconds 600]"""
import argparse
import math
import random
import time

from app import chaos
from app.seed import DEMO_POINT


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=300)
    ap.add_argument("--seconds", type=int, default=600)
    a = ap.parse_args()
    rng = random.Random(7)
    amb = [{"id": f"AMB-{i:03d}", "lat": DEMO_POINT[0] + rng.uniform(-0.09, 0.09), "lon": DEMO_POINT[1] + rng.uniform(-0.1, 0.1),
            "h": rng.uniform(0, 2 * math.pi), "status": rng.choice(["EN_ROUTE", "IDLE", "TO_HOSPITAL"])} for i in range(a.count)]
    c = chaos.client(chaos.bases()[0], "dispatcher", "ambulance-simulator")
    t0 = time.time()
    while time.time() - t0 < a.seconds:
        for x in amb:
            x["h"] += rng.uniform(-0.4, 0.4)
            x["lat"] = min(18.66, max(18.42, x["lat"] + 0.0012 * math.sin(x["h"])))
            x["lon"] = min(74.0, max(73.73, x["lon"] + 0.0012 * math.cos(x["h"])))
        try:
            r = c.post("/api/ambulances", json={"items": [{k: x[k] for k in ("id", "lat", "lon", "status")} for x in amb]})
            print(f"posted {len(amb)} positions: {r.status_code}", flush=True)
        except Exception as e:  # noqa: BLE001
            print("post failed:", e, flush=True)
        time.sleep(1.0)


if __name__ == "__main__":
    main()
