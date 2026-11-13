"""Operations metrics and invariant checks (section 3.14). A source that cannot be read is reported as
{"available": false, "reason": ...}; nothing is ever defaulted to 0."""
import time

from . import cache, config, db, metrics, mq, rds

INVARIANTS = {
    "double_allocation": "SELECT bed_id, COUNT(*) AS n FROM reservations WHERE status IN ('HELD','CONFIRMED') "
                         "GROUP BY bed_id HAVING COUNT(*) > 1",
    "bed_reservation_link": """
        SELECT b.bed_id, b.status, b.reservation_id FROM beds b
        LEFT JOIN reservations r ON r.reservation_id = b.reservation_id
        WHERE (b.status = 'FREE' AND b.reservation_id IS NOT NULL)
           OR (b.status = 'RESERVED' AND (r.status IS DISTINCT FROM 'HELD' OR r.bed_id <> b.bed_id))
           OR (b.status = 'OCCUPIED' AND (r.status IS DISTINCT FROM 'CONFIRMED' OR r.bed_id <> b.bed_id))
        UNION ALL
        SELECT r.bed_id, 'ACTIVE_RES_UNLINKED', r.reservation_id FROM reservations r JOIN beds b ON b.bed_id = r.bed_id
        WHERE r.status IN ('HELD','CONFIRMED') AND b.reservation_id IS DISTINCT FROM r.reservation_id""",
    "patient_two_occupied_beds": "SELECT patient_id, COUNT(*) AS n FROM beds WHERE status='OCCUPIED' "
                                 "GROUP BY patient_id HAVING COUNT(*) > 1",
    "patient_two_active_sagas": "SELECT patient_id, COUNT(*) AS n FROM transfers_saga WHERE saga_state IN "
                                "('TARGET_RESERVED','AWAITING_ACCEPTANCE','REJECTED_COMPENSATING','TIMED_OUT_COMPENSATING') "
                                "GROUP BY patient_id HAVING COUNT(*) > 1",
    "held_on_occupied_bed": "SELECT r.reservation_id, r.bed_id FROM reservations r JOIN beds b ON b.bed_id=r.bed_id "
                            "WHERE r.status='HELD' AND b.status='OCCUPIED'",
}


def invariants():
    out = {}
    for name, sql in INVARIANTS.items():
        rows = db.query(sql + " LIMIT 20" if "UNION" not in sql else f"SELECT * FROM ({sql}) x LIMIT 20")
        out[name] = {"violations": len(rows), "sample": [{k: str(v) for k, v in r.items()} for r in rows[:5]]}
    out["ok"] = all(v["violations"] == 0 for k, v in out.items() if isinstance(v, dict))
    out["checked_at"] = time.time()
    return out


def _na(reason):
    return {"available": False, "reason": str(reason)[:200]}


def metrics_snapshot():
    m = {"instance": config.INSTANCE, "ts": time.time()}
    rate = db.query_one("SELECT COALESCE(SUM(events),0) AS n, COUNT(*) AS buckets FROM event_rate "
                        "WHERE bucket > date_trunc('second', NOW()) - interval '10 seconds'")
    series = db.query("SELECT extract(epoch FROM bucket)::bigint AS t, events FROM event_rate "
                      "WHERE bucket > NOW() - interval '60 seconds' ORDER BY bucket")
    m["events_per_second"] = {"available": True, "last_10s_avg": round(float(rate["n"]) / 10.0, 1),
                              "series": [[r["t"], r["events"]] for r in series], "source": "audit consumer"}
    try:
        qs = mq.queue_stats()
        m["queues"] = {"available": True, "items": [
            {"name": n, "messages": q.get("messages"), "ready": q.get("messages_ready"),
             "unacked": q.get("messages_unacknowledged"), "consumers": q.get("consumers")}
            for n, q in sorted(qs.items())]}
        m["dlq_depth"] = {"available": True, "total": sum(q.get("messages") or 0 for n, q in qs.items() if n.endswith(".dlq")),
                          "by_queue": {n: q.get("messages") for n, q in qs.items() if n.endswith(".dlq")}}
    except Exception as e:  # noqa: BLE001
        m["queues"] = _na(f"RabbitMQ management API: {e}")
        m["dlq_depth"] = _na(f"RabbitMQ management API: {e}")
    lag = db.query_one("SELECT COUNT(*) AS n, percentile_cont(0.95) WITHIN GROUP (ORDER BY "
                       "extract(epoch FROM processed_at - occurred_at) * 1000) AS p95_ms, "
                       "MAX(extract(epoch FROM processed_at - occurred_at) * 1000) AS max_ms "
                       "FROM audit_log WHERE processed_at > NOW() - interval '60 seconds' AND occurred_at IS NOT NULL")
    m["consumer_lag"] = {"available": True, "samples": lag["n"],
                         "p95_ms": round(lag["p95_ms"], 1) if lag["p95_ms"] is not None else None,
                         "max_ms": round(lag["max_ms"], 1) if lag["max_ms"] is not None else None,
                         "note": "event age when the audit consumer applied it (last 60 s)"}
    m["counters"] = {r["name"]: r["value"] for r in db.query("SELECT name, value FROM counters")}
    for op in ("search", "reserve"):
        p = metrics.percentiles(op, 60)
        m[f"{op}_latency"] = {"available": True, "window_s": 60, "samples": p["n"],
                              "p50_ms": _r(p["p50"]), "p95_ms": _r(p["p95"]), "p99_ms": _r(p["p99"]),
                              "note": "server-side, both instances"}
    pg = db.query("SELECT bed_type, COUNT(*) FILTER (WHERE status='FREE') AS free, COUNT(*) AS total FROM beds GROUP BY bed_type")
    m["free_beds_pg"] = {"available": True, "source": "postgresql", "by_type": {r["bed_type"]: r["free"] for r in pg},
                         "total_by_type": {r["bed_type"]: r["total"] for r in pg}}
    m["free_beds_redis"] = redis_free_counts()
    ob = db.query_one("SELECT COUNT(*) AS n, extract(epoch FROM NOW() - MIN(created_at)) AS oldest_s FROM outbox_events "
                      "WHERE status='PENDING'")
    m["outbox_pending"] = {"available": True, "count": ob["n"],
                           "oldest_age_s": round(ob["oldest_s"], 1) if ob["oldest_s"] is not None else None}
    st, gen = cache.read_state(force=True)
    m["cache"] = {"state": cache.STATE_NAMES[st], "generation": gen, "breaker_open_here": rds.breaker.is_open,
                  "transitions": [{k: (str(v) if k == "at" else v) for k, v in r.items()} for r in db.query(
                      "SELECT at, from_state, to_state, generation, actor, detail FROM cache_transitions "
                      "ORDER BY id DESC LIMIT 8")]}
    m["blocked_streams"] = [{k: str(v) for k, v in r.items()} for r in db.query(
        "SELECT consumer_group, patient_id, waiting_seq, expected_seq, blocked_at FROM patient_streams_blocked "
        "WHERE resolved_at IS NULL ORDER BY blocked_at DESC LIMIT 20")]
    m["invariants"] = invariants()
    return m


def _r(x):
    return round(float(x), 2) if x is not None else None


def redis_free_counts():
    gen = cache.fast_path_generation()
    if gen is None:
        return _na("cache not READY (degraded or rebuilding)")
    try:
        hosps = [r["hospital_id"] for r in db.query("SELECT hospital_id FROM hospitals")]

        def _do(r):
            p = r.pipeline(transaction=False)
            for h in hosps:
                for t in config.BED_TYPES:
                    p.scard(rds.k_free(gen, h, t))
            return p.execute()
        vals = rds.call(_do)
    except rds.RedisUnavailable as e:
        return _na(f"redis: {e}")
    by = {t: 0 for t in config.BED_TYPES}
    i = 0
    for _h in hosps:
        for t in config.BED_TYPES:
            by[t] += vals[i]
            i += 1
    return {"available": True, "source": f"redis generation {gen}", "by_type": by}
