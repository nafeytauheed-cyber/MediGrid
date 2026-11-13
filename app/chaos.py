"""Judge chaos scenarios. The same functions back the in-app buttons (scenarios 1-4) and the sim modules.
Every PASS/FAIL is computed from measurements and PostgreSQL assertions, never assigned."""
import json
import statistics
import threading
import time
import uuid

import httpx
import pika
from psycopg.types.json import Jsonb

from . import beds, config, db, faults, log, mq

IN_APP = ("stampede", "priority", "expiry", "poison")


def start_run(scenario):
    rid = str(uuid.uuid4())
    db.query("INSERT INTO chaos_runs (run_id, scenario, status) VALUES (%s,%s,'RUNNING')", (rid, scenario))
    return rid


def finish_run(run_id, result):
    status = "PASS" if result.get("pass") else "FAIL"
    if result.get("error"):
        status = "ERROR"
    db.query("UPDATE chaos_runs SET status=%s, result=%s, finished_at=NOW() WHERE run_id=%s",
             (status, Jsonb(json.loads(json.dumps(result, default=str))), run_id))
    return status


def execute(scenario, run_id):
    fn = {"stampede": stampede, "priority": priority, "expiry": expiry, "poison": poison}[scenario]
    try:
        result = fn(run_id)
    except Exception as e:  # noqa: BLE001
        log.error("chaos scenario error", scenario=scenario, error=f"{type(e).__name__}: {e}"[:300])
        result = {"pass": False, "error": f"{type(e).__name__}: {e}"[:500]}
    finish_run(run_id, result)
    return result


def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(p / 100.0 * len(s) + 0.5)) - 1))
    return round(s[k], 2)


def client(base, role, user_id, hospital_id=None):
    c = httpx.Client(base_url=base, timeout=15.0)
    r = c.post("/api/identity", json={"role": role, "user_id": user_id, "hospital_id": hospital_id})
    r.raise_for_status()
    return c


def bases():
    """API base URLs that currently answer /healthz."""
    alive = []
    for b in config.API_INTERNAL_URLS:
        try:
            if httpx.get(f"{b}/healthz", timeout=1.0).status_code == 200:
                alive.append(b)
        except httpx.HTTPError:
            pass
    return alive or [config.NGINX_URL]


# ====================================================================== 1. last-bed stampede

def _fill_hospital_type(hospital_id, bed_type, keep, tag):
    """Occupy every FREE bed of a type except `keep` with synthetic placeholder patients (with events)."""
    filled = []

    def tx(cur):
        cur.execute("SELECT bed_id FROM beds WHERE hospital_id=%s AND bed_type=%s AND status='FREE' ORDER BY bed_id "
                    "FOR UPDATE", (hospital_id, bed_type))
        ids = [r["bed_id"] for r in cur.fetchall()]
        for bid in ids[keep:]:
            pid = f"FILL-{tag}-{bid}"[:48]
            rid = str(uuid.uuid4())
            cur.execute("INSERT INTO patients (patient_id, state, current_bed_id, current_hosp_id) VALUES "
                        "(%s,'ADMITTED',%s,%s) ON CONFLICT (patient_id) DO UPDATE SET state='ADMITTED', "
                        "current_bed_id=EXCLUDED.current_bed_id, current_hosp_id=EXCLUDED.current_hosp_id", (pid, bid, hospital_id))
            cur.execute("UPDATE beds SET status='OCCUPIED', reservation_id=%s, patient_id=%s, version=version+1, "
                        "updated_at=NOW() WHERE bed_id=%s RETURNING *", (rid, pid, bid))
            b = cur.fetchone()
            cur.execute("INSERT INTO reservations (reservation_id, actor_id, idempotency_key, request_fingerprint, bed_id, "
                        "hospital_id, bed_type, ambulance_id, patient_id, status, expires_at, alloc_path) VALUES "
                        "(%s,'chaos',%s,'chaos-fill',%s,%s,%s,'FILL',%s,'CONFIRMED',clock_timestamp(),'chaos')",
                        (rid, f"fill-{tag}-{bid}", bid, hospital_id, bed_type, pid))
            db.add_event(cur, "bed.admitted", {"bed_id": bid, "hospital_id": hospital_id, "bed_type": bed_type,
                                               "status": "OCCUPIED", "bed_version": b["version"], "chaos_fill": True},
                         patient_id=pid, priority=1)
            filled.append(bid)
        db.bump_epoch(cur)
        return ids[:keep]
    kept = db.run_tx(tx, statement_timeout="30s")
    from . import cache
    for bid in filled:
        cache.project_bed_taken(bid, hospital_id, bed_type)
    return kept, filled


def _unfill(bed_ids):
    for bid in bed_ids:
        b = db.query_one("SELECT version, status FROM beds WHERE bed_id=%s", (bid,))
        if b and b["status"] == "OCCUPIED":
            try:
                beds.discharge(bid, b["version"], "chaos-cleanup")
            except Exception as e:  # noqa: BLE001
                log.warn("unfill failed", bed=bid, error=str(e)[:120])


def _cancel_all(res_ids):
    for rid in res_ids:
        try:
            beds.cancel(rid, "chaos-cleanup")
        except Exception:  # noqa: BLE001
            pass


def stampede(run_id, n=50):
    tag = run_id[:8]
    h = db.query_one("SELECT lat, lon FROM hospitals WHERE hospital_id='H-01'")
    kept, filled = _fill_hospital_type("H-01", "ICU", 1, tag)
    if not kept:
        _unfill(filled)
        return {"pass": False, "error": "H-01 has no free ICU bed to keep"}
    last_bed = kept[0]
    urls = bases()
    created = []
    try:
        clients = [client(urls[i % len(urls)], "dispatcher", f"stampede-{tag}-{i:02d}") for i in range(n)]
        barrier = threading.Barrier(n)
        results = [None] * n

        def fire(i):
            body = {"patient_id": f"SP-{tag}-{i:02d}", "ambulance_id": f"AMB-S{i:02d}", "condition_code": "GENERAL_ACUTE",
                    "bed_type": "ICU", "priority": 4, "lat": h["lat"], "lon": h["lon"],
                    "idempotency_key": f"stampede-{tag}-{i}"}
            barrier.wait()
            t0 = time.perf_counter()
            try:
                r = clients[i].post("/api/dispatch/reserve", json=body)
                results[i] = (r.status_code, r.json(), (time.perf_counter() - t0) * 1000, r.headers.get("X-Instance"))
            except Exception as e:  # noqa: BLE001
                results[i] = (0, {"error": str(e)}, (time.perf_counter() - t0) * 1000, None)

        ts = [threading.Thread(target=fire, args=(i,)) for i in range(n)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        ok = [r for r in results if r[0] == 201]
        created = [r[1]["reservation_id"] for r in ok]
        got_last = [r for r in ok if r[1]["bed_id"] == last_bed]
        beds_given = [r[1]["bed_id"] for r in ok]
        others_ok = all(r[0] == 201 or r[1].get("result") == "NO_SUITABLE_HOSPITAL_OR_BED" for r in results)
        db_holders = db.query_one("SELECT COUNT(*) AS n FROM reservations WHERE bed_id=%s AND status IN ('HELD','CONFIRMED')",
                                  (last_bed,))["n"]
        dbl = db.query(
            "SELECT bed_id, COUNT(*) FROM reservations WHERE status IN ('HELD','CONFIRMED') GROUP BY bed_id HAVING COUNT(*) > 1")
        instances = sorted({r[3] for r in results if r[3]})
        paths = {}
        for r in ok:
            paths[r[1].get("alloc_path")] = paths.get(r[1].get("alloc_path"), 0) + 1
        reserve_lat = [r[2] for r in results]
        # concurrent search latency (client-side, end to end through the API)
        search_lat = _concurrent_search(clients, h)
        rl = _rate_limit_check(urls[0], tag, h)
        res = {
            "last_bed": last_bed, "requests": n, "instances_used": instances,
            "status_counts": {str(k): sum(1 for r in results if r[0] == k) for k in sorted({r[0] for r in results})},
            "holders_of_last_bed_in_db": db_holders, "responses_with_last_bed": len(got_last),
            "distinct_beds_given": len(set(beds_given)), "successful": len(ok), "alloc_paths": paths,
            "double_allocation_rows": len(dbl), "others_rerouted_or_no_bed": others_ok,
            "reserve_ms": {"p50": pct(reserve_lat, 50), "p95": pct(reserve_lat, 95), "max": round(max(reserve_lat), 1)},
            "search_ms": {"n": len(search_lat), "p50": pct(search_lat, 50), "p95": pct(search_lat, 95)},
            "rate_limit_check": rl,
        }
        res["checks"] = {
            "exactly_one_holder_of_last_bed": db_holders == 1 and len(got_last) == 1,
            "no_bed_given_twice": len(set(beds_given)) == len(beds_given),
            "invariant_query_empty": len(dbl) == 0,
            "others_rerouted_or_no_bed": others_ok,
            "search_p95_under_100ms": (res["search_ms"]["p95"] or 1e9) < 100,
            "rate_limit_429_with_retry_after": rl.get("got_429_with_retry_after", False),
        }
        res["pass"] = all(res["checks"].values())
        return res
    finally:
        _cancel_all(created)
        _unfill(filled)


def _concurrent_search(clients, h, n=50):
    lat = [None] * n
    barrier = threading.Barrier(n)

    def go(i):
        barrier.wait()
        t0 = time.perf_counter()
        r = clients[i % len(clients)].get("/api/dispatch/search", params={
            "lat": h["lat"], "lon": h["lon"], "condition_code": "CARDIAC_EMERGENCY", "bed_type": "ICU"})
        lat[i] = (time.perf_counter() - t0) * 1000 if r.status_code == 200 else None
    ts = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return [x for x in lat if x is not None]


def _rate_limit_check(base, tag, h):
    c = client(base, "dispatcher", f"ratelimit-{tag}")
    created, codes, retry_after = [], [], None
    for i in range(config.RATE_MAX + 3):
        r = c.post("/api/dispatch/reserve", json={
            "patient_id": f"RL-{tag}-{i}", "ambulance_id": "AMB-RL", "condition_code": "GENERAL_ACUTE",
            "bed_type": "GENERAL", "priority": 1, "lat": h["lat"], "lon": h["lon"], "idempotency_key": f"rl-{tag}-{i}"})
        codes.append(r.status_code)
        if r.status_code == 201:
            created.append(r.json()["reservation_id"])
        if r.status_code == 429 and retry_after is None:
            retry_after = r.headers.get("Retry-After")
    _cancel_all(created)
    return {"user": f"ratelimit-{tag}", "requests": len(codes), "accepted": codes.count(201), "rejected_429": codes.count(429),
            "retry_after_header": retry_after, "limit": f"{config.RATE_MAX}/{config.RATE_WINDOW_MS // 1000}s",
            "got_429_with_retry_after": codes.count(429) > 0 and retry_after is not None}


# ====================================================================== 2. flood then critical

def _passive(ch, q):
    r = ch.queue_declare(q, passive=True)
    return r.method.message_count, r.method.consumer_count


def priority(run_id, routine=10000):
    queue = "q.transfer_saga"
    flag = f"pause:{queue}"
    conn = mq.connect()
    ch = conn.channel()
    mq.declare(ch)
    ch.confirm_delivery()
    res = {"queue": queue, "routine": routine}
    try:
        faults.set_flag(flag, "all")
        t0 = time.time()
        while True:
            depth, consumers = _passive(ch, queue)
            if consumers == 0:
                break
            if time.time() - t0 > 15:
                return {"pass": False, "error": f"consumers did not stop ({consumers} still attached)"}
            time.sleep(0.2)
        res["consumers_after_pause"] = consumers
        res["depth_before_fill"] = depth
        t1 = time.time()
        routine_ids = []
        for i in range(routine):
            eid = str(uuid.uuid4())
            body = json.dumps({"event_id": eid, "type": "chaos.priority.routine", "patient_id": None,
                               "patient_seq_no": None, "priority": 1, "data": {"run_id": run_id, "n": i}, "flags": {}})
            mq.publish(ch, "", queue, body, mq.props(eid, 1))
            routine_ids.append(eid)
        crit = str(uuid.uuid4())
        mq.publish(ch, "", queue, json.dumps({"event_id": crit, "type": "chaos.priority.critical", "patient_id": None,
                                              "patient_seq_no": None, "priority": 4,
                                              "data": {"run_id": run_id, "n": routine}, "flags": {}}), mq.props(crit, 4))
        res["fill_seconds"] = round(time.time() - t1, 1)
        depth, consumers = _passive(ch, queue)
        res["depth_after_fill_confirmed"] = depth
        res["consumers_during_fill"] = consumers
        # resume exactly one consumer (prefetch 1)
        target = "worker-1"
        faults.set_flag(flag, f"only:{target}")
        t2 = time.time()
        rows = []
        while time.time() - t2 < 60:
            rows = db.query("SELECT event_id::text, kind, consumer, processed_at FROM chaos_priority_log WHERE run_id=%s "
                            "ORDER BY id LIMIT 20", (run_id,))
            if any(r["kind"] == "critical" for r in rows) and len(rows) >= 5:
                break
            time.sleep(0.3)
        _d, consumers_resumed = _passive(ch, queue)
        res["consumers_after_resume"] = consumers_resumed
        res["resumed_consumer"] = target
        pos = next((i + 1 for i, r in enumerate(rows) if r["kind"] == "critical"), None)
        res["critical_event_id"] = crit
        res["critical_position"] = pos
        res["first_processed"] = [{"pos": i + 1, "kind": r["kind"], "event_id": r["event_id"], "consumer": r["consumer"],
                                   "at": str(r["processed_at"])} for i, r in enumerate(rows[:6])]
        res["checks"] = {"all_consumers_stopped_before_fill": res["consumers_after_pause"] == 0,
                         "backlog_confirmed_in_same_queue": depth >= routine + 1,
                         "critical_processed_first": pos == 1}
        res["pass"] = all(res["checks"].values())
        return res
    finally:
        faults.clear(flag)  # all consumers resume and drain the backlog
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


# ====================================================================== 3. expiry

def expiry(run_id):
    tag = run_id[:8]
    db.query("UPDATE system_state SET value=10 WHERE key='ttl_override_s'")
    try:
        base = bases()[0]
        c = client(base, "dispatcher", f"expiry-{tag}")
        from .seed import DEMO_POINT
        r = c.post("/api/dispatch/reserve", json={"patient_id": f"EXP-{tag}", "ambulance_id": "AMB-EXP",
                                                  "condition_code": "GENERAL_ACUTE", "bed_type": "GENERAL", "priority": 2,
                                                  "lat": DEMO_POINT[0], "lon": DEMO_POINT[1],
                                                  "idempotency_key": f"expiry-{tag}"})
        if r.status_code != 201:
            return {"pass": False, "error": f"reserve returned {r.status_code}: {r.text[:200]}"}
        res = r.json()
        rid, bed, hosp = res["reservation_id"], res["bed_id"], res["hospital_id"]
        ui = {"first_batch_after_free": None}
        stop = threading.Event()
        threading.Thread(target=_watch_sse, args=(hosp, ui, stop), daemon=True).start()
        deadline = time.time() + 40
        observed = None
        while time.time() < deadline:
            row = db.query_one("SELECT r.status AS rs, b.status AS bs, extract(epoch FROM r.expires_at)::float8 AS exp, "
                               "extract(epoch FROM clock_timestamp())::float8 AS now FROM reservations r JOIN beds b "
                               "ON b.bed_id = r.bed_id WHERE r.reservation_id=%s", (rid,))
            if row["rs"] == "EXPIRED" and row["bs"] == "FREE":
                observed = row
                ui["db_free_at"] = row["now"]
                break
            time.sleep(0.1)
        t_wait = time.time() + 5
        while ui["first_batch_after_free"] is None and time.time() < t_wait:
            time.sleep(0.05)
        stop.set()
        if not observed:
            return {"pass": False, "reservation_id": rid, "error": "not released within 40 s"}
        gap = observed["now"] - observed["exp"]
        ui_gap = (ui["first_batch_after_free"] - observed["exp"]) if ui["first_batch_after_free"] else None
        out = {"reservation_id": rid, "bed_id": bed, "hospital_id": hosp, "ttl_s": 10,
               "db_expiry_to_free_s": round(gap, 2), "db_expiry_to_ui_batch_s": round(ui_gap, 2) if ui_gap else None,
               "ui_note": "first SSE batch for that hospital received after the DB transition"}
        out["checks"] = {"released_within_5s_of_expiry": gap <= 5.0,
                         "ui_update_within_5s_of_expiry": ui_gap is not None and ui_gap <= 5.0}
        out["pass"] = all(out["checks"].values())
        return out
    finally:
        db.query("UPDATE system_state SET value=0 WHERE key='ttl_override_s'")


def _watch_sse(hospital_id, ui, stop):
    try:
        with httpx.stream("GET", f"{config.NGINX_URL}/api/stream", timeout=httpx.Timeout(5.0, read=30.0)) as r:
            ev = None
            for line in r.iter_lines():
                if stop.is_set():
                    return
                if line.startswith("event:"):
                    ev = line[6:].strip()
                elif line.startswith("data:") and ev == "batch":
                    d = json.loads(line[5:])
                    if hospital_id in d.get("hospitals", {}) and ui.get("db_free_at"):
                        ui["first_batch_after_free"] = d["server_ts"]
                        return
    except Exception as e:  # noqa: BLE001
        ui["sse_error"] = str(e)[:120]


# ====================================================================== 4. poison and crash

def _insert_event(etype, flags):
    return db.run_tx(lambda cur: db.add_event(cur, etype, {"chaos": True}, priority=2, flags=flags))


def _attempt_row(group, eid):
    return db.query_one("SELECT failures, crashes, dead_lettered_at, updated_at FROM message_attempts "
                        "WHERE consumer_group=%s AND event_id=%s", (group, eid))


def _processed(group, eid):
    return db.query_one("SELECT processed_at FROM inbox_processed_events WHERE consumer_group=%s AND event_id=%s",
                        (group, eid)) is not None


def poison(run_id):
    out = {}
    faults.set_flag("fault:poison", "1")
    try:
        # (A) ordinary handler error -> confirmed 1/3/9 s retries -> DLQ
        group_a = "notifications"
        ea = _insert_event("chaos.poison.handler", {"fail_group": group_a})
        t0 = time.time()
        seen = []
        last_f = 0
        while time.time() - t0 < 40:
            row = _attempt_row(group_a, ea)
            if row and row["failures"] > last_f:
                last_f = row["failures"]
                seen.append(round(time.time() - t0, 2))
            if row and row["dead_lettered_at"]:
                break
            time.sleep(0.1)
        row = _attempt_row(group_a, ea)
        gaps = [round(b - a, 2) for a, b in zip(seen, seen[1:])]
        in_dlq = _dlq_contains("q.notifications", ea)
        out["A_handler_error"] = {"event_id": ea, "group": group_a, "failures": row["failures"] if row else None,
                                  "failure_times_s": seen, "gaps_between_attempts_s": gaps,
                                  "dead_lettered": bool(row and row["dead_lettered_at"]), "visible_in_dlq": in_dlq}
        # (B) crash before ack -> RabbitMQ redelivers -> crash-loop guard -> DLQ
        group_b = "audit"
        eb = _insert_event("chaos.poison.crash", {"crash_group": group_b})
        t0 = time.time()
        while time.time() - t0 < 90:
            row_b = _attempt_row(group_b, eb)
            if row_b and row_b["dead_lettered_at"]:
                break
            time.sleep(0.3)
        row_b = _attempt_row(group_b, eb)
        out["B_crash_before_ack"] = {"event_id": eb, "group": group_b, "crashes": row_b["crashes"] if row_b else None,
                                     "dead_lettered": bool(row_b and row_b["dead_lettered_at"]),
                                     "visible_in_dlq": _dlq_contains("q.audit", eb),
                                     "note": "each crash is os._exit(137) of a whole worker before ack; Docker restarts it"}
    finally:
        faults.clear("fault:poison")
    # replay only after the fault flag is confirmed removed
    flag_removed = faults.get("fault:poison", max_age=0) is None
    out["fault_flag_removed_before_replay"] = flag_removed
    rep_a = replay_dlq("q.notifications.dlq", out["A_handler_error"]["event_id"]) if flag_removed else None
    rep_b = replay_dlq("q.audit.dlq", out["B_crash_before_ack"]["event_id"]) if flag_removed else None
    t0 = time.time()
    while time.time() - t0 < 20:
        if _processed("notifications", out["A_handler_error"]["event_id"]) and _processed("audit", out["B_crash_before_ack"]["event_id"]):
            break
        time.sleep(0.3)
    out["replay"] = {"A": rep_a, "B": rep_b,
                     "A_processed_after_replay": _processed("notifications", out["A_handler_error"]["event_id"]),
                     "B_processed_after_replay": _processed("audit", out["B_crash_before_ack"]["event_id"])}
    a, b = out["A_handler_error"], out["B_crash_before_ack"]
    out["checks"] = {
        "A_three_retries_then_dlq": a["failures"] == 4 and a["dead_lettered"] and a["visible_in_dlq"],
        "A_retry_delays_increase": len(a["gaps_between_attempts_s"]) == 3 and
        a["gaps_between_attempts_s"][0] < a["gaps_between_attempts_s"][1] < a["gaps_between_attempts_s"][2],
        "B_redelivered_after_crash_then_dlq": (b["crashes"] or 0) >= 3 and b["dead_lettered"],
        "replayed_and_processed_once": out["replay"]["A_processed_after_replay"] and out["replay"]["B_processed_after_replay"],
    }
    out["pass"] = all(out["checks"].values())
    return out


def _dlq_contains(queue, eid):
    try:
        for m in mq.peek(mq.dlq(queue), 200):
            if eid in (m.get("payload") or ""):
                return True
    except Exception:  # noqa: BLE001
        return False
    return False


def replay_dlq(dlq_name, event_id=None, max_n=100):
    """Move messages from a DLQ back to their original queue: confirmed publish first, then ack."""
    if not dlq_name.endswith(".dlq") or dlq_name[:-4] not in [q for q, _b, _p in mq.GROUPS.values()]:
        return {"error": "UNKNOWN_DLQ"}
    target = dlq_name[:-4]
    conn = mq.connect()
    ch = conn.channel()
    ch.confirm_delivery()
    replayed, skipped = [], []
    try:
        for _ in range(max_n):
            method, props, body = ch.basic_get(dlq_name, auto_ack=False)
            if method is None:
                break
            try:
                eid = json.loads(body).get("event_id")
            except ValueError:
                eid = None
            if event_id and eid != event_id:
                skipped.append(method.delivery_tag)
                continue
            h = dict(props.headers or {})
            h.update({"x-attempt": 0, "x-replayed": True})
            h.pop("x-postpone", None)
            p = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent, priority=props.priority or 1,
                                     message_id=props.message_id, content_type="application/json", headers=h)
            mq.publish(ch, "", target, body, p)   # confirmed + routed, else raises and the message stays in the DLQ
            ch.basic_ack(method.delivery_tag)
            replayed.append(eid)
        for tag in skipped:
            ch.basic_nack(tag, requeue=True)
    finally:
        conn.close()
    if replayed:
        db.query("UPDATE counters SET value=value+%s WHERE name='dlq_replayed'", (len(replayed),))
    return {"queue": dlq_name, "target": target, "replayed": replayed, "left_in_dlq": len(skipped)}


# ====================================================================== 5. per-group event parity

def parity(since_minutes=30, since_ts=None):
    """For each consumer group: every committed outbox event whose routing key is bound to it must be in that
    group's inbox exactly once (primary key), or be explicitly dead-lettered/blocked. Counts distinct event ids."""
    where = "created_at >= to_timestamp(%s)" if since_ts else "created_at > NOW() - make_interval(mins => %s)"
    arg = since_ts if since_ts else since_minutes
    committed = db.query_one(f"SELECT COUNT(*) AS n, COUNT(*) FILTER (WHERE status='PENDING') AS pending FROM outbox_events "
                             f"WHERE {where}", (arg,))
    out = {"committed_events": committed["n"], "outbox_pending": committed["pending"], "groups": {}}
    bound = {"bed_tracking": "(routing_key LIKE 'bed.%%' OR routing_key LIKE 'transfer.%%')",
             "transfer_saga": "(routing_key LIKE 'bed.%%' OR routing_key LIKE 'transfer.%%')",
             "notifications": "TRUE", "audit": "TRUE"}
    all_ok = committed["pending"] == 0
    for g, pred in bound.items():
        r = db.query_one(f"""
          SELECT COUNT(*) AS expected,
                 COUNT(*) FILTER (WHERE i.event_id IS NOT NULL) AS processed,
                 COUNT(*) FILTER (WHERE i.event_id IS NULL AND m.dead_lettered_at IS NOT NULL) AS dead_lettered,
                 COUNT(*) FILTER (WHERE i.event_id IS NULL AND m.dead_lettered_at IS NULL) AS unresolved
          FROM outbox_events o
          LEFT JOIN inbox_processed_events i ON i.consumer_group=%s AND i.event_id=o.event_id
          LEFT JOIN message_attempts m ON m.consumer_group=%s AND m.event_id=o.event_id
          WHERE {where} AND {pred}""", (g, g, arg))
        sample = db.query(f"""SELECT o.event_id::text, o.routing_key, o.status FROM outbox_events o
          LEFT JOIN inbox_processed_events i ON i.consumer_group=%s AND i.event_id=o.event_id
          LEFT JOIN message_attempts m ON m.consumer_group=%s AND m.event_id=o.event_id
          WHERE {where} AND {pred} AND i.event_id IS NULL AND m.dead_lettered_at IS NULL LIMIT 5""", (g, g, arg))
        r["unresolved_sample"] = sample
        out["groups"][g] = r
        all_ok = all_ok and r["unresolved"] == 0
    out["duplicate_inbox_rows_possible"] = False  # (consumer_group, event_id) is the primary key
    out["pass"] = all_ok
    return out
