"""Latency samples buffered per instance and flushed to PostgreSQL once a second, so either instance
can report city-wide percentiles."""
import threading
import time

from . import config, db, log

_buf = []
_lock = threading.Lock()
_started = False


def record(op, ms, path=None):
    with _lock:
        _buf.append((config.INSTANCE, op, path, ms))
    _ensure_thread()


def _ensure_thread():
    global _started
    if _started:
        return
    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_flusher, name="latency-flush", daemon=True).start()


def _flusher():
    while True:
        time.sleep(1.0)
        with _lock:
            batch = _buf[:]
            _buf.clear()
        if not batch:
            continue
        try:
            with db.pool().connection() as conn:
                with conn.transaction(), conn.cursor() as cur:
                    cur.executemany("INSERT INTO latency_samples (instance, op, path, ms) VALUES (%s,%s,%s,%s)", batch)
        except Exception as e:  # noqa: BLE001
            log.warn("latency flush failed", error=str(e)[:200], dropped=len(batch))


def percentiles(op, seconds=60):
    row = db.query_one(
        "SELECT COUNT(*) AS n, percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50, "
        "percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95, percentile_cont(0.99) WITHIN GROUP (ORDER BY ms) AS p99 "
        "FROM latency_samples WHERE op=%s AND ts > NOW() - make_interval(secs => %s)", (op, seconds))
    return row
