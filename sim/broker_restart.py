"""Scenario 5: broker restart during admissions.

Run:  docker compose run --rm tools python -m sim.broker_restart [--seconds 45]
While it runs (it prints when), in another terminal:  docker compose restart rabbitmq

Admissions keep committing to PostgreSQL (outbox) while the broker is down. Afterwards every committed event must
appear exactly once in each bound consumer group's inbox, or be explicitly dead-lettered; nothing unresolved."""
import argparse
import random
import sys
import threading
import time

import httpx

from app import chaos, config, db, mq, ops
from sim.common import report


def node_uptime():
    try:
        return mq.mgmt_get("/api/nodes", timeout=1.0)[0].get("uptime")
    except Exception:  # noqa: BLE001
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=45)
    ap.add_argument("--workers", type=int, default=12)
    a = ap.parse_args()
    start_ts = db.query_one("SELECT extract(epoch FROM clock_timestamp())::float8 AS t")["t"]
    tag = f"{int(start_ts) % 100000}"
    stop = threading.Event()
    stats = {"cycles": 0, "errors": 0, "status": {}}
    lock = threading.Lock()
    urls = chaos.bases()
    h = db.query_one("SELECT lat, lon FROM hospitals WHERE hospital_id='H-10'")

    def worker(i):
        c = chaos.client(urls[i % len(urls)], "operator", f"broker-{tag}-{i}")
        n = 0
        while not stop.is_set():
            n += 1
            try:
                r = c.post("/api/dispatch/reserve", json={
                    "patient_id": f"BR-{tag}-{i}-{n}", "ambulance_id": f"AMB-B{i}", "condition_code": "GENERAL_ACUTE",
                    "bed_type": random.choice(["GENERAL", "ICU"]), "priority": random.choice([1, 2, 3, 4]),
                    "lat": h["lat"] + random.uniform(-0.03, 0.03), "lon": h["lon"] + random.uniform(-0.03, 0.03),
                    "idempotency_key": f"br-{tag}-{i}-{n}"})
                with lock:
                    stats["status"][r.status_code] = stats["status"].get(r.status_code, 0) + 1
                if r.status_code != 201:
                    continue
                rid = r.json()["reservation_id"]
                rc = c.post(f"/api/reservations/{rid}/confirm")
                if rc.status_code == 200:
                    b = rc.json()["bed"]
                    c.post(f"/api/beds/{b['bed_id']}/discharge", json={"expected_version": b["version"]})
                with lock:
                    stats["cycles"] += 1
            except httpx.HTTPError:
                with lock:
                    stats["errors"] += 1
            time.sleep(0.05)

    up0 = node_uptime()
    print(f"broker uptime before: {up0} ms. Load starts now for {a.seconds} s.")
    print(">>> NOW RUN in another terminal:  docker compose restart rabbitmq", flush=True)
    ts = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(a.workers)]
    [t.start() for t in ts]
    seen_down, restarted = False, False
    peak_pending = 0
    t0 = time.time()
    while time.time() - t0 < a.seconds:
        up = node_uptime()
        if up is None:
            seen_down = True
        elif up0 is not None and up < up0:
            restarted = True
        p = db.query_one("SELECT COUNT(*) AS n FROM outbox_events WHERE status='PENDING'")["n"]
        peak_pending = max(peak_pending, p)
        time.sleep(1)
    stop.set()
    [t.join(timeout=15) for t in ts]
    up1 = node_uptime()
    restarted = restarted or (up0 is not None and up1 is not None and up1 < up0)
    print(f"load stopped: {stats}. Waiting for outbox drain and consumers (up to 180 s)...", flush=True)
    t1 = time.time()
    par = None
    while time.time() - t1 < 180:
        par = chaos.parity(since_ts=start_ts)
        if par["pass"]:
            break
        time.sleep(2)
    inv = ops.invariants()
    res = {"seconds": a.seconds, "load": stats, "broker_down_observed": seen_down, "broker_restart_observed": restarted,
           "broker_uptime_before_ms": up0, "broker_uptime_after_ms": up1, "peak_outbox_pending": peak_pending,
           "drain_wait_s": round(time.time() - t1, 1), "parity": par, "invariants_ok": inv["ok"]}
    res["checks"] = {"broker_restart_observed": restarted,
                     "outbox_drained": par["outbox_pending"] == 0,
                     "every_committed_event_processed_or_dlq_per_group": par["pass"],
                     "invariants_ok": inv["ok"]}
    res["pass"] = all(res["checks"].values())
    return report("broker_restart", res)


if __name__ == "__main__":
    sys.exit(main())
