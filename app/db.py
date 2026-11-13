"""PostgreSQL access: one pool per process, a transaction helper with bounded retry,
and the outbox/epoch helpers that every state transition uses."""
import json
import threading
import time
import uuid

import psycopg
from psycopg import errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from . import config, log

_pool = None
_pool_lock = threading.Lock()

RETRYABLE = (errors.SerializationFailure, errors.DeadlockDetected, errors.LockNotAvailable)


def pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                # autocommit: a single read is one round trip; transactions use explicit conn.transaction().
                # Default timeouts are set once per connection instead of SET LOCAL per transaction.
                # No per-checkout health check: it costs a round trip on every use (about 1 ms here).
                _pool = ConnectionPool(
                    config.PG_DSN, min_size=min(8, config.PG_POOL_MAX), max_size=config.PG_POOL_MAX, timeout=5.0,
                    kwargs={"row_factory": dict_row, "autocommit": True}, open=True, configure=_configure,
                )
    return _pool


def _configure(conn):
    conn.execute("SET lock_timeout = '2s'; SET statement_timeout = '5s'")


class Conflict(Exception):
    """A business conflict that maps to HTTP 409 with the current authoritative state."""

    def __init__(self, message, state=None, code="CONFLICT"):
        super().__init__(message)
        self.state = state
        self.code = code


def run_tx(fn, attempts=4, lock_timeout="2s", statement_timeout="5s"):
    """Run fn(cur) inside one transaction. Retries serialization/deadlock/lock timeout errors
    with a short backoff. Business exceptions propagate after rollback."""
    last = None
    for i in range(attempts):
        try:
            with pool().connection() as conn:
                with conn.transaction():
                    with conn.cursor() as cur:
                        if lock_timeout != "2s" or statement_timeout != "5s":
                            cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'; "
                                        f"SET LOCAL statement_timeout = '{statement_timeout}'")
                        return fn(cur)
        except RETRYABLE as e:
            last = e
            time.sleep(0.01 * (2 ** i))
    raise last


def query(sql, params=None):
    with pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []


def query_one(sql, params=None):
    rows = query(sql, params)
    return rows[0] if rows else None


def get_state(cur, key):
    cur.execute("SELECT value FROM system_state WHERE key=%s", (key,))
    r = cur.fetchone()
    return r["value"] if r else None


EPOCH_FENCE = 4243


def bump_epoch(cur):
    """Last statement of every availability-changing transaction. The shared advisory lock is held until commit,
    so the fenced cache switch (exclusive lock, cache.py) waits for in-flight writers and blocks new ones while it
    re-reads PostgreSQL. Writers do not block each other. The sequence is the change watermark."""
    cur.execute("SELECT pg_advisory_xact_lock_shared(%s), nextval('pg_epoch_seq') AS v", (EPOCH_FENCE,))
    return cur.fetchone()["v"]


def read_epoch():
    return query_one("SELECT last_value AS v FROM pg_epoch_seq")["v"]


def next_patient_seq(cur, patient_id):
    cur.execute("UPDATE patients SET last_seq_no = last_seq_no + 1, updated_at = NOW() "
                "WHERE patient_id=%s RETURNING last_seq_no", (patient_id,))
    return cur.fetchone()["last_seq_no"]


def add_event(cur, etype, data, patient_id=None, priority=2, correlation_id=None, flags=None, patient_set=None):
    """Insert one outbox row in the caller's transaction. Patient events get the next
    per-patient sequence number. No NOTIFY here: NOTIFY takes a global lock across the commit flush and
    serialises all committing transactions (measured, BUILD_LOG); the publisher polls the pending index instead."""
    event_id = str(uuid.uuid4())
    envelope = {
        "event_id": event_id,
        "type": etype,
        "patient_id": patient_id,
        "patient_seq_no": None,
        "correlation_id": correlation_id or event_id,
        "occurred_at": None,
        "priority": priority,
        "data": data,
        "flags": flags or {},
    }
    if patient_id:
        # one round trip: bump the patient's sequence (optionally set state fields) and insert the outbox row
        sets, args = "", []
        for k, v in (patient_set or {}).items():
            assert k in ("state", "current_bed_id", "current_hosp_id")
            sets += f", {k} = %s"
            args.append(v)
        cur.execute(
            f"WITH p AS (UPDATE patients SET last_seq_no = last_seq_no + 1, updated_at = NOW(){sets} "
            "WHERE patient_id = %s RETURNING last_seq_no) "
            "INSERT INTO outbox_events (event_id, routing_key, priority, payload) "
            "SELECT %s, %s, %s, jsonb_set(jsonb_set(%s::jsonb, '{patient_seq_no}', to_jsonb(p.last_seq_no)), "
            "'{occurred_at}', to_jsonb(clock_timestamp())) FROM p RETURNING event_id",
            args + [patient_id, event_id, etype, priority, json.dumps(envelope)])
        if cur.fetchone() is None:
            raise RuntimeError(f"add_event: unknown patient {patient_id}")
    else:
        cur.execute(
            "INSERT INTO outbox_events (event_id, routing_key, priority, payload) "
            "VALUES (%s, %s, %s, jsonb_set(%s::jsonb, '{occurred_at}', to_jsonb(clock_timestamp())))",
            (event_id, etype, priority, json.dumps(envelope)))
    return event_id


def wait_for_db(timeout=60):
    t0 = time.time()
    while True:
        try:
            with psycopg.connect(config.PG_DSN, connect_timeout=3) as c:
                c.execute("SELECT 1")
            return
        except Exception as e:  # noqa: BLE001
            if time.time() - t0 > timeout:
                raise
            log.warn("waiting for postgres", error=str(e))
            time.sleep(1)
