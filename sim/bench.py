"""Latency benchmark: GET search and POST reserve, end to end (client-measured) across both instances, at idle
and under concurrency. Run: docker compose run --rm tools python -m sim.bench [--concurrency 300] [--n 1500]"""
import argparse
import os
import platform
import random
import sys
import threading
import time

from app import beds, chaos
from app.seed import DEMO_POINT
from sim.common import report


def stats(lat, errors, n):
    return {"n": n, "ok": len(lat), "errors": errors, "p50_ms": chaos.pct(lat, 50), "p95_ms": chaos.pct(lat, 95),
            "p99_ms": chaos.pct(lat, 99), "max_ms": round(max(lat), 1) if lat else None}


def run(kind, conc, n, tag, created):
    urls = chaos.bases()
    clients = [chaos.client(urls[i % len(urls)], "dispatcher", f"{tag}-{kind}-{conc}-{i}") for i in range(conc)]
    lat, errors, lock = [], {}, threading.Lock()
    per = max(1, n // conc)
    bar = threading.Barrier(conc)

    def go(i):
        c = clients[i]
        bar.wait()
        for j in range(per):
            lat0, lon0 = DEMO_POINT[0] + random.uniform(-0.08, 0.08), DEMO_POINT[1] + random.uniform(-0.08, 0.08)
            t0 = time.perf_counter()
            try:
                if kind == "search":
                    r = c.get("/api/dispatch/search", params={"lat": lat0, "lon": lon0, "condition_code":
                              random.choice(["CARDIAC_EMERGENCY", "GENERAL_ACUTE", "MAJOR_TRAUMA"]), "bed_type": "ICU"})
                    ok = r.status_code == 200
                else:
                    r = c.post("/api/dispatch/reserve", json={"patient_id": f"{tag}-{i}-{j}-{conc}", "ambulance_id": f"B{i}",
                               "condition_code": "GENERAL_ACUTE", "bed_type": "GENERAL", "priority": 2, "lat": lat0,
                               "lon": lon0, "idempotency_key": f"{tag}-{i}-{j}-{conc}"})
                    ok = r.status_code in (201, 404)
                    if r.status_code == 201:
                        with lock:
                            created.append(r.json()["reservation_id"])
                ms = (time.perf_counter() - t0) * 1000
                with lock:
                    if ok:
                        lat.append(ms)
                    else:
                        errors[r.status_code] = errors.get(r.status_code, 0) + 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1
    ts = [threading.Thread(target=go, args=(i,)) for i in range(conc)]
    t0 = time.time()
    [t.start() for t in ts]
    [t.join() for t in ts]
    s = stats(lat, errors, per * conc)
    s["wall_s"] = round(time.time() - t0, 1)
    s["throughput_rps"] = round(len(lat) / max(0.001, time.time() - t0), 1)
    return s


def run_open(kind, ambulances, per, tag, created, interval=1.0):
    """Open-loop load: each ambulance fires `per` requests spaced `interval` s apart with a random start offset,
    so the arrival rate is ambulances/interval per second regardless of how slow responses are."""
    urls = chaos.bases()
    clients = [chaos.client(urls[i % len(urls)], "dispatcher", f"{tag}-o{kind}-{ambulances}-{i}") for i in range(ambulances)]
    lat, errors, lock = [], {}, threading.Lock()
    t_start = time.time() + 0.5

    def go(i):
        c = clients[i]
        off = random.uniform(0, interval)
        for j in range(per):
            target = t_start + off + j * interval
            d = target - time.time()
            if d > 0:
                time.sleep(d)
            lat0, lon0 = DEMO_POINT[0] + random.uniform(-0.08, 0.08), DEMO_POINT[1] + random.uniform(-0.08, 0.08)
            t0 = time.perf_counter()
            try:
                if kind == "search":
                    r = c.get("/api/dispatch/search", params={"lat": lat0, "lon": lon0, "condition_code": "CARDIAC_EMERGENCY",
                                                              "bed_type": "ICU"})
                    ok = r.status_code == 200
                else:
                    k = f"{tag}-o{i}-{j}-{ambulances}"
                    r = c.post("/api/dispatch/reserve", json={"patient_id": k, "ambulance_id": f"O{i}",
                               "condition_code": "GENERAL_ACUTE", "bed_type": "GENERAL", "priority": 2, "lat": lat0,
                               "lon": lon0, "idempotency_key": k})
                    ok = r.status_code in (201, 404)
                    if r.status_code == 201:
                        with lock:
                            created.append(r.json()["reservation_id"])
                ms = (time.perf_counter() - t0) * 1000
                with lock:
                    if ok:
                        lat.append(ms)
                    else:
                        errors[r.status_code] = errors.get(r.status_code, 0) + 1
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors[type(e).__name__] = errors.get(type(e).__name__, 0) + 1
    ts = [threading.Thread(target=go, args=(i,)) for i in range(ambulances)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    s = stats(lat, errors, ambulances * per)
    s["arrival_rate_per_s"] = round(ambulances / interval, 1)
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--concurrency", type=int, default=300)
    ap.add_argument("--n", type=int, default=1500)
    a = ap.parse_args()
    tag = f"bn{int(time.time()) % 100000}"
    created = []
    res = {"machine": {"platform": platform.platform(), "cpus_visible": os.cpu_count(), "python": platform.python_version()},
           "note": "client-measured through the API (no nginx), alternating api-1/api-2; reserve holds are cancelled afterwards",
           "rate_limit_note": "each simulated user sends at most n/concurrency requests"}
    try:
        res["search_idle"] = run("search", 1, 200, tag, created)
        res["reserve_idle"] = run_open("reserve", 10, 10, tag, created)  # 100 requests, distinct users, low rate
        res[f"search_c{a.concurrency}"] = run("search", a.concurrency, a.n, tag, created)
        res[f"reserve_c{a.concurrency}"] = run("reserve", a.concurrency, a.concurrency * 2, tag, created)
        # open loop: N ambulances, each sends one request every `interval` seconds (arrival rate N/interval per s)
        res[f"search_open_{a.concurrency}amb_every2s"] = run_open("search", a.concurrency, 10, tag, created, interval=2.0)
        res[f"reserve_open_{a.concurrency}amb_every10s"] = run_open("reserve", a.concurrency, 2, tag, created, interval=10.0)
    finally:
        for rid in created:
            try:
                beds.cancel(rid, "bench-cleanup")
            except Exception:  # noqa: BLE001
                pass
    res["checks"] = {"search_p95_under_100ms_idle": (res["search_idle"]["p95_ms"] or 1e9) < 100,
                     f"search_p95_under_100ms_open_{a.concurrency}_ambulances":
                         (res[f"search_open_{a.concurrency}amb_every2s"]["p95_ms"] or 1e9) < 100,
                     f"search_p95_under_100ms_closed_loop_c{a.concurrency}":
                         (res[f"search_c{a.concurrency}"]["p95_ms"] or 1e9) < 100}
    res["pass"] = all(res["checks"].values())
    return report("bench", res)


if __name__ == "__main__":
    sys.exit(main())
