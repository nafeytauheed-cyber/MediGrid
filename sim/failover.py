"""Scenario 6: instance stop, Redis pause, Redis restart, and commit-unknown injections.

Run:  docker compose run --rm tools python -m sim.failover [--phases A,B,C,D]
The script prints each docker command when it is time to run it, then waits for the effect.
  A  docker compose stop <instance serving the stream>   -> SSE must resume on the other instance within 5 s
  B  docker compose pause redis ; (10 s) ; docker compose unpause redis   -> SQL-only, fenced rebuild, 0 duplicates
  C  docker compose restart redis                         -> empty cache rebuilt, 0 duplicates
  D  (no command) crash-after-Redis-hold and ambiguous-commit injections
"""
import argparse
import json
import random
import sys
import threading
import time

import httpx

from app import chaos, config, db, faults, ops
from sim.common import report


def sse_watch(events, stop):
    """Browser-like client: reconnect with the server's retry value; record instance events and breaks."""
    retry = 0.5
    while not stop.is_set():
        try:
            with httpx.stream("GET", f"{config.NGINX_URL}/api/stream", timeout=httpx.Timeout(3.0, read=5.0)) as r:
                ev = None
                for line in r.iter_lines():
                    if stop.is_set():
                        return
                    if line.startswith("retry:"):
                        retry = int(line[6:]) / 1000.0
                    elif line.startswith("event:"):
                        ev = line[6:].strip()
                    elif line.startswith("data:") and ev == "instance":
                        events.append(("instance", time.time(), json.loads(line[5:])["instance"]))
            events.append(("break", time.time(), "eof"))
        except Exception as e:  # noqa: BLE001
            events.append(("break", time.time(), type(e).__name__))
        time.sleep(retry)


def phase_a():
    events, stop = [], threading.Event()
    threading.Thread(target=sse_watch, args=(events, stop), daemon=True).start()
    t0 = time.time()
    while not any(e[0] == "instance" for e in events) and time.time() - t0 < 10:
        time.sleep(0.1)
    first = next((e for e in events if e[0] == "instance"), None)
    if not first:
        stop.set()
        return {"pass": False, "error": "no SSE stream"}
    inst = first[2]
    http = {"ok": 0, "fail": 0, "instances": {}}

    def poll():
        while not stop.is_set():
            try:
                r = httpx.get(f"{config.NGINX_URL}/healthz", timeout=2.0)
                http["ok" if r.status_code == 200 else "fail"] += 1
                i = r.headers.get("X-Instance")
                http["instances"][i] = http["instances"].get(i, 0) + 1
            except httpx.HTTPError:
                http["fail"] += 1
            time.sleep(0.1)
    threading.Thread(target=poll, daemon=True).start()
    print(f">>> SSE stream is on {inst}. NOW RUN:  docker compose stop {inst}", flush=True)
    t1 = time.time()
    brk = None
    while time.time() - t1 < 180:
        brk = next((e for e in events if e[0] == "break" and e[1] > first[1]), None)
        if brk:
            break
        time.sleep(0.1)
    if not brk:
        stop.set()
        return {"pass": False, "error": "stream never broke (instance not stopped?)"}
    t2 = time.time()
    nxt = None
    while time.time() - t2 < 15:
        nxt = next((e for e in events if e[0] == "instance" and e[1] > brk[1]), None)
        if nxt:
            break
        time.sleep(0.05)
    time.sleep(2)
    stop.set()
    gap = (nxt[1] - brk[1]) if nxt else None
    res = {"stopped_instance": inst, "stream_break_at": brk[1], "resumed_on": nxt[2] if nxt else None,
           "sse_resume_gap_s": round(gap, 2) if gap is not None else None, "http_polls": http,
           "note": "gap = stream break to first 'instance' event from the new instance (includes client retry delay)"}
    res["checks"] = {"sse_resumed_on_other_instance_within_5s": bool(nxt and nxt[2] != inst and gap <= 5.0)}
    res["pass"] = all(res["checks"].values())
    print(f">>> Phase A done. Bring it back:  docker compose start {inst}", flush=True)
    return res


def wait_both_up(timeout=120):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if len([b for b in chaos.bases() if b != config.NGINX_URL]) == len(config.API_INTERNAL_URLS):
            return True
        time.sleep(1)
    return False


def state():
    for b in chaos.bases():
        try:
            return httpx.get(f"{b}/api/ops/state", timeout=1.5).json()
        except Exception:  # noqa: BLE001
            continue
    return None


def redis_phase(name, command_text):
    if not wait_both_up():
        print("warning: not both API instances are up; continuing with what answers", flush=True)
    s0 = state()
    gen0 = s0["generation"]
    tag = f"{name}{int(time.time()) % 100000}"
    stop = threading.Event()
    created, lock = [], threading.Lock()
    paths, codes = {}, {}
    seen = []
    urls = chaos.bases()
    cardiac = {"ok": 0, "wrong": 0, "during_degraded": 0}
    from app.seed import DEMO_POINT
    dp = {"lat": DEMO_POINT[0], "lon": DEMO_POINT[1]}

    def load(i):
        c = chaos.client(urls[i % len(urls)], "dispatcher", f"{tag}-u{i}")
        n = 0
        while not stop.is_set():
            n += 1
            cond = "CARDIAC_EMERGENCY" if n % 3 == 0 else "GENERAL_ACUTE"
            try:
                r = c.post("/api/dispatch/reserve", json={
                    "patient_id": f"{tag}-{i}-{n}", "ambulance_id": f"A{i}", "condition_code": cond,
                    "bed_type": "ICU" if cond == "CARDIAC_EMERGENCY" else random.choice(["GENERAL", "ICU"]),
                    "priority": 3, "lat": dp["lat"] + random.uniform(-0.02, 0.02) if cond != "CARDIAC_EMERGENCY" else dp["lat"],
                    "lon": dp["lon"] + random.uniform(-0.02, 0.02) if cond != "CARDIAC_EMERGENCY" else dp["lon"],
                    "idempotency_key": f"{tag}-{i}-{n}"}, timeout=10)
                with lock:
                    codes[r.status_code] = codes.get(r.status_code, 0) + 1
                    if r.status_code == 201:
                        d = r.json()
                        created.append(d["reservation_id"])
                        paths[d["alloc_path"]] = paths.get(d["alloc_path"], 0) + 1
                        if cond == "CARDIAC_EMERGENCY":
                            # H-02 is the nearest capable hospital; it may fill up, but H-01 must never be chosen
                            if d["hospital_id"] == "H-01":
                                cardiac["wrong"] += 1
                            else:
                                cardiac["ok"] += 1
                            if seen and seen[-1][1] != "READY":
                                cardiac["during_degraded"] += 1
            except httpx.HTTPError:
                with lock:
                    codes["net"] = codes.get("net", 0) + 1
            time.sleep(0.15)

    ts = [threading.Thread(target=load, args=(i,), daemon=True) for i in range(16)]
    [t.start() for t in ts]
    print(f">>> Load running against {urls}. NOW RUN:  {command_text}", flush=True)
    t0 = time.time()
    saw_degraded = False
    ready_again = None
    while time.time() - t0 < 180:
        s = state()
        if s and (not seen or seen[-1][1] != s["cache_state"] or seen[-1][2] != s["generation"]):
            seen.append((round(time.time() - t0, 2), s["cache_state"], s["generation"]))
            print(f"  t={seen[-1][0]}s state={s['cache_state']} gen={s['generation']}", flush=True)
        if s and s["cache_state"] != "READY":
            saw_degraded = True
        if saw_degraded and s and s["cache_state"] == "READY" and s["generation"] > gen0:
            ready_again = s
            break
        time.sleep(0.2)
    time.sleep(3)  # keep load running on the fresh generation
    stop.set()
    [t.join(timeout=15) for t in ts]
    dbl = db.query("SELECT bed_id FROM reservations WHERE status IN ('HELD','CONFIRMED') GROUP BY bed_id HAVING COUNT(*) > 1")
    inv = ops.invariants()
    for rid in created:
        try:
            from app import beds
            beds.cancel(rid, "failover-cleanup")
        except Exception:  # noqa: BLE001
            pass
    res = {"command": command_text, "states_seen": seen, "start_generation": gen0,
           "ready_generation": ready_again["generation"] if ready_again else None, "responses": codes, "alloc_paths": paths,
           "cardiac_smartmatch": cardiac, "double_allocation_rows": len(dbl), "invariants_ok": inv["ok"]}
    res["checks"] = {"degraded_observed": saw_degraded, "sql_path_used": paths.get("sql", 0) > 0,
                     "fenced_rebuild_to_new_generation": ready_again is not None,
                     "zero_double_allocations": len(dbl) == 0 and inv["ok"],
                     "smartmatch_never_chose_incapable_H01": cardiac["wrong"] == 0 and cardiac["ok"] > 0}
    res["pass"] = all(res["checks"].values())
    return res


def phase_d():
    urls = chaos.bases()
    c = chaos.client(urls[0], "dispatcher", f"inject-{int(time.time()) % 100000}")
    out = {}
    body = lambda k, p: {"patient_id": p, "ambulance_id": "A-INJ", "condition_code": "GENERAL_ACUTE", "bed_type": "GENERAL",
                         "priority": 2, "lat": 18.52, "lon": 73.856, "idempotency_key": k}
    # (1) API "crashes" after the Redis provisional hold, before PostgreSQL: the hold is an orphan
    faults.set_flag("fault:crash_after_hold", "1")
    time.sleep(1.0)
    k1, p1 = f"inj1-{time.time()}", f"INJ1-{int(time.time())}"
    r1 = c.post("/api/dispatch/reserve", json=body(k1, p1))
    faults.clear("fault:crash_after_hold")
    time.sleep(1.0)
    # retry with the same key after the "crash": must allocate exactly once
    r1b = c.post("/api/dispatch/reserve", json=body(k1, p1))
    n1 = db.query_one("SELECT COUNT(*) AS n FROM reservations WHERE idempotency_key=%s", (k1,))["n"]
    print("  waiting up to 40 s for the orphan scan to settle the abandoned Redis hold...", flush=True)
    from app import cache, rds
    t0 = time.time()
    settled = False
    while time.time() - t0 < 40:
        gen = cache.read_state(force=True)[1]
        try:
            holds = rds.slow_client().hgetall(rds.k_resdata(gen))
            orphan_ids = [rid for rid, v in holds.items()
                          if json.loads(json.loads(v).get("meta") or "{}").get("key") == k1
                          and rid != (r1b.json().get("reservation_id") if r1b.status_code == 201 else None)]
            settled = not orphan_ids
        except Exception:  # noqa: BLE001
            settled = False
        if settled:
            break
        time.sleep(1)
    out["crash_after_hold"] = {"first_status": r1.status_code, "first_error": r1.json().get("error"),
                               "retry_status": r1b.status_code, "reservations_for_key": n1,
                               "orphan_hold_settled_or_adopted": settled}
    # (2) commit outcome unknown: committed, then the connection "dies"; the API must reconcile by key
    faults.set_flag("fault:ambiguous_commit", "1")
    time.sleep(1.0)
    k2 = f"inj2-{time.time()}"
    r2 = c.post("/api/dispatch/reserve", json=body(k2, f"INJ2-{int(time.time())}"))
    faults.clear("fault:ambiguous_commit")
    n2 = db.query_one("SELECT COUNT(*) AS n FROM reservations WHERE idempotency_key=%s", (k2,))["n"]
    out["ambiguous_commit"] = {"status": r2.status_code, "reconciled": r2.json().get("reconciled_after_unknown_commit"),
                               "reservations_for_key": n2}
    for rid in [x.json().get("reservation_id") for x in (r1b, r2)]:
        if rid:
            try:
                from app import beds
                beds.cancel(rid, "inject-cleanup")
            except Exception:  # noqa: BLE001
                pass
    inv = ops.invariants()
    out["checks"] = {"crash_after_hold_returns_retry_safe_503": r1.status_code == 503,
                     "retry_after_crash_allocates_once": r1b.status_code == 201 and n1 == 1,
                     "orphan_hold_settled": settled,
                     "ambiguous_commit_reconciled_once": r2.status_code == 201 and n2 == 1 and bool(out["ambiguous_commit"]["reconciled"]),
                     "invariants_ok": inv["ok"]}
    out["pass"] = all(out["checks"].values())
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phases", default="A,B,C,D")
    a = ap.parse_args()
    phases = a.phases.split(",")
    res = {}
    if "A" in phases:
        res["A_instance_stop"] = phase_a()
    if "B" in phases:
        res["B_redis_pause"] = redis_phase("rp", "docker compose pause redis ; wait 10 s ; docker compose unpause redis")
    if "C" in phases:
        res["C_redis_restart"] = redis_phase("rr", "docker compose restart redis")
    if "D" in phases:
        res["D_injections"] = phase_d()
    res["checks"] = {f"{k}": bool(v.get("pass")) for k, v in res.items()}
    res["pass"] = all(res["checks"].values())
    return report("failover", res)


if __name__ == "__main__":
    sys.exit(main())
