"""Surge simulator: hundreds of concurrent ambulances, duplicate requests, duplicate deliveries, out-of-order patient
events, a slow consumer and (optionally) a crashing consumer.

Run: docker compose run --rm tools python -m sim.surge [--ambulances 300] [--crash]"""
import argparse
import json
import random
import sys
import threading
import time
import uuid

from app import beds, chaos, db, faults, mq, ops
from app.seed import DEMO_POINT
from sim.common import report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ambulances", type=int, default=300)
    ap.add_argument("--crash", action="store_true", help="also send a crash-before-ack event to the audit consumer")
    a = ap.parse_args()
    tag = f"sg{int(time.time()) % 100000}"
    urls = chaos.bases()
    res, created, lock = {}, [], threading.Lock()

    # 1) surge: N ambulances request at once, each from its own user; 10% resend the same request (duplicate)
    clients = [chaos.client(urls[i % len(urls)], "dispatcher", f"{tag}-{i}") for i in range(a.ambulances)]
    out = [None] * a.ambulances
    dup = {}
    bar = threading.Barrier(a.ambulances)
    conds = ["GENERAL_ACUTE", "CARDIAC_EMERGENCY", "MAJOR_TRAUMA", "SUSPECTED_STROKE", "PEDIATRIC_EMERGENCY"]

    def go(i):
        cond = conds[i % len(conds)]
        bt = {"GENERAL_ACUTE": "GENERAL", "MAJOR_TRAUMA": "ICU"}.get(cond, "ICU")
        body = {"patient_id": f"{tag}-{i}", "ambulance_id": f"AMB-{i:03d}", "condition_code": cond, "bed_type": bt,
                "priority": random.choice([1, 2, 3, 4]), "lat": DEMO_POINT[0] + random.uniform(-0.1, 0.1),
                "lon": DEMO_POINT[1] + random.uniform(-0.1, 0.1), "idempotency_key": f"{tag}-{i}"}
        bar.wait()
        t0 = time.perf_counter()
        r = clients[i].post("/api/dispatch/reserve", json=body)
        out[i] = (r.status_code, r.json(), (time.perf_counter() - t0) * 1000)
        if r.status_code == 201:
            with lock:
                created.append(r.json()["reservation_id"])
        if i % 10 == 0:
            r2 = clients[i].post("/api/dispatch/reserve", json=body)
            dup[i] = (r2.status_code, r2.json().get("reservation_id"), r.json().get("reservation_id"))
    ts = [threading.Thread(target=go, args=(i,)) for i in range(a.ambulances)]
    t0 = time.time()
    [t.start() for t in ts]
    [t.join() for t in ts]
    lat = [o[2] for o in out]
    codes = {}
    for o in out:
        codes[o[0]] = codes.get(o[0], 0) + 1
    res["surge"] = {"ambulances": a.ambulances, "wall_s": round(time.time() - t0, 2), "status_counts": codes,
                    "reserve_ms": {"p50": chaos.pct(lat, 50), "p95": chaos.pct(lat, 95), "p99": chaos.pct(lat, 99)},
                    "paths": {p: sum(1 for o in out if o[0] == 201 and o[1].get("alloc_path") == p) for p in ("redis", "sql")}}
    res["duplicate_requests"] = {"sent": len(dup), "replayed_same_reservation": sum(
        1 for s, r2, r1 in dup.values() if s == 200 and r2 == r1)}

    # 2) duplicate deliveries: republish 50 already-processed outbox events with the same event_id
    rows = db.query("SELECT event_id, routing_key, priority, payload FROM outbox_events WHERE status='PUBLISHED' "
                    "ORDER BY created_at DESC LIMIT 50")
    before = db.query_one("SELECT COUNT(*) AS n FROM notifications")["n"]
    pub = mq.ConfirmedPublisher()
    for r in rows:
        pub.publish(mq.EXCHANGE, r["routing_key"], json.dumps(r["payload"]), mq.props(r["event_id"], r["priority"]))

    # 3) out-of-order: synthetic patient stream published 3,2,1 directly to the exchange
    pid = f"{tag}-OOO"
    db.query("INSERT INTO patients (patient_id, state, last_seq_no) VALUES (%s,'IN_TRANSIT',3)", (pid,))
    ooo_ids = []
    for seq in (3, 2, 1):
        eid = str(uuid.uuid4())
        ooo_ids.append(eid)
        pub.publish(mq.EXCHANGE, "bed.sim.ordering", json.dumps({
            "event_id": eid, "type": "bed.sim.ordering", "patient_id": pid, "patient_seq_no": seq, "priority": 2,
            "occurred_at": None, "data": {"seq": seq}, "flags": {}}), mq.props(eid, 2))

    # 4) slow consumer: 20 events that make the notifications handler sleep 300 ms each
    for k in range(20):
        db.run_tx(lambda cur: db.add_event(cur, "sim.slow", {"k": k}, flags={"slow_ms": 300, "slow_group": "notifications"}))
    crash_id = None
    if a.crash:
        faults.set_flag("fault:poison", "1")
        crash_id = db.run_tx(lambda cur: db.add_event(cur, "chaos.poison.crash", {"surge": True},
                                                      flags={"crash_group": "audit"}))
    pub.close()

    # wait for ordering to resolve in every group
    t1 = time.time()
    order = {}
    while time.time() - t1 < 60:
        rows2 = db.query("SELECT consumer_group, seq_no FROM consumer_apply_log WHERE patient_id=%s ORDER BY id", (pid,))
        order = {}
        for r in rows2:
            order.setdefault(r["consumer_group"], []).append(r["seq_no"])
        if len(order) == 4 and all(v == [1, 2, 3] for v in order.values()):
            break
        time.sleep(0.5)
    if a.crash:
        t2 = time.time()
        while time.time() - t2 < 90:
            row = db.query_one("SELECT dead_lettered_at FROM message_attempts WHERE consumer_group='audit' AND event_id=%s",
                               (crash_id,))
            if row and row["dead_lettered_at"]:
                break
            time.sleep(0.5)
        faults.clear("fault:poison")
        chaos.replay_dlq("q.audit.dlq", crash_id)
    time.sleep(3)
    after = db.query_one("SELECT COUNT(*) AS n FROM notifications")["n"]
    dupe_inbox = db.query_one("SELECT COUNT(*) AS n FROM (SELECT consumer_group, event_id FROM inbox_processed_events "
                              "GROUP BY 1,2 HAVING COUNT(*) > 1) x")["n"]
    res["duplicate_deliveries"] = {"republished": len(rows), "notifications_before": before, "notifications_after": after,
                                   "note": "after - before counts only notifications for new surge/slow events, not duplicates",
                                   "duplicate_inbox_rows": dupe_inbox}
    res["out_of_order"] = {"patient_id": pid, "published_order": [3, 2, 1], "applied_order_by_group": order}
    postponed = db.query_one("SELECT value FROM counters WHERE name='postponed'")["value"]
    res["out_of_order"]["postponed_counter_total"] = postponed
    inv = ops.invariants()
    for rid in created:
        try:
            beds.cancel(rid, "surge-cleanup")
        except Exception:  # noqa: BLE001
            pass
    allowed = {201, 404}
    res["checks"] = {
        "no_unexpected_statuses": set(codes) <= allowed,
        "duplicate_requests_replayed": res["duplicate_requests"]["replayed_same_reservation"] == len(dup),
        "no_duplicate_inbox_rows": dupe_inbox == 0,
        "out_of_order_applied_in_order_every_group": len(order) == 4 and all(v == [1, 2, 3] for v in order.values()),
        "invariants_ok": inv["ok"],
    }
    res["pass"] = all(res["checks"].values())
    return report("surge", res)


if __name__ == "__main__":
    sys.exit(main())
