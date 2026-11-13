"""Worker background loops: outbox publisher, expiry/saga reaper, cache manager."""
import json
import threading
import time

import psycopg

from .. import beds, cache, config, db, log, mq, rds, transfers


class OutboxPublisher(threading.Thread):
    """Authoritative publication path. LISTEN wakes it at commit; a sweep every 250 ms is the fallback.
    Rows are marked PUBLISHED only after a positive, routed confirm (DECISIONS C21)."""

    def __init__(self):
        super().__init__(name="outbox", daemon=True)
        self.pub = mq.ConfirmedPublisher()

    def run(self):
        backoff = 0.5
        listen = None
        while True:
            try:
                if listen is None or listen.closed:
                    listen = psycopg.connect(config.PG_DSN, autocommit=True)
                    listen.execute("LISTEN outbox")
                for _ in listen.notifies(timeout=0.25, stop_after=1):
                    pass
                while self.sweep() >= 100:
                    pass
                backoff = 0.5
            except Exception as e:  # noqa: BLE001
                log.warn("outbox publisher error; backing off", error=str(e)[:200])
                self.pub.close()
                try:
                    if listen is not None:
                        listen.close()
                except Exception:  # noqa: BLE001
                    pass
                listen = None
                time.sleep(backoff)
                backoff = min(backoff * 2, 5)

    def sweep(self):
        sent = []

        def tx(cur):
            cur.execute("SELECT event_id, routing_key, priority, payload FROM outbox_events WHERE status='PENDING' "
                        "ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED")
            rows = cur.fetchall()
            err = None
            for r in rows:
                try:
                    self.pub.publish(mq.EXCHANGE, r["routing_key"], json.dumps(r["payload"]),
                                     mq.props(r["event_id"], r["priority"], {"x-attempt": 0}))
                    sent.append(r["event_id"])
                except Exception as e:  # noqa: BLE001  stop at first uncertain publish; keep the rest PENDING
                    err = e
                    break
            if sent:
                cur.execute("UPDATE outbox_events SET status='PUBLISHED', published_at=NOW() WHERE event_id = ANY(%s)",
                            (sent,))
                cur.execute("UPDATE counters SET value = value + %s WHERE name='outbox_published'", (len(sent),))
            if err:
                # commit what was confirmed, then surface the error so the connection is rebuilt
                return len(rows), err
            return len(rows), None

        n, err = db.run_tx(tx, statement_timeout="30s")
        if err:
            raise err
        return n


class Reaper(threading.Thread):
    """Authoritative expiry: PostgreSQL scan at least once per second (here every 250 ms)."""

    def __init__(self):
        super().__init__(name="reaper", daemon=True)

    def run(self):
        while True:
            try:
                ex = beds.expire_due()
                to = transfers.timeout_due()
                if ex or to:
                    log.info("reaper transitions", expired=len(ex), transfers_timed_out=len(to))
            except Exception as e:  # noqa: BLE001
                log.warn("reaper error", error=str(e)[:200])
            time.sleep(0.25)


class CacheManager(threading.Thread):
    """Health checks Redis, marks DEGRADED on failure, rebuilds when possible, reconciles and finds orphans."""

    def __init__(self):
        super().__init__(name="cache-manager", daemon=True)
        self.last_full = 0.0
        self.last_orphan = 0.0
        self.last_cleanup = 0.0

    def run(self):
        while True:
            try:
                self.tick()
            except Exception as e:  # noqa: BLE001
                log.warn("cache manager error", error=str(e)[:200])
            time.sleep(0.5)

    def tick(self):
        st, gen = cache.read_state(force=True)
        try:
            r = rds.slow_client(timeout=config.REDIS_TIMEOUT_S)
            r.ping()
            pointer = r.get(rds.POINTER)
            healthy = True
        except Exception as e:  # noqa: BLE001
            healthy, pointer = False, None
            if st != cache.DEGRADED:
                cache.mark_degraded(f"health check failed: {str(e)[:100]}")
        if not healthy:
            return
        if st == cache.READY and pointer != str(gen):
            cache.mark_degraded(f"fencing pointer mismatch (pointer={pointer}, generation={gen}); redis restarted?")
            st = cache.DEGRADED
        if st in (cache.DEGRADED, cache.REBUILDING):
            cache.rebuild(config.INSTANCE)
            return
        now = time.monotonic()
        if now - self.last_orphan > 5:
            self.last_orphan = now
            self.orphan_scan(gen)
        if now - self.last_full > 15:
            self.last_full = now
            for h in db.query("SELECT hospital_id FROM hospitals"):
                cache.reconcile_hospital(h["hospital_id"])
        if now - self.last_cleanup > 60:
            self.last_cleanup = now
            db.query("DELETE FROM rate_limit_windows WHERE window_start < NOW() - interval '10 minutes'")
            db.query("DELETE FROM latency_samples WHERE ts < NOW() - interval '2 hours'")

    def orphan_scan(self, gen, grace_ms=10000):
        """Redis holds without a live HELD row are settled against PostgreSQL (release only if FREE+unowned)."""
        try:
            r = rds.slow_client()
            recs = r.hgetall(rds.k_resdata(gen))
            now_ms = int(float(r.time()[0]) * 1000)
        except Exception:  # noqa: BLE001
            return
        old = {rid: json.loads(v) for rid, v in recs.items() if now_ms - int(json.loads(v).get("created_ms", 0)) > grace_ms}
        if not old:
            return
        rows = db.query("SELECT reservation_id::text AS rid, status FROM reservations WHERE reservation_id::text = ANY(%s)",
                        (list(old),))
        status = {x["rid"]: x["status"] for x in rows}
        n = 0
        for rid, rec in old.items():
            if status.get(rid) == "HELD":
                continue
            res = cache.settle_hold(rid, gen, rec["bed_id"])
            n += 1
            log.info("orphan hold settled", correlation_id=rid, pg_status=status.get(rid, "MISSING"),
                     mode=res[0] if res else None, bed=rec["bed_id"])
        return n
