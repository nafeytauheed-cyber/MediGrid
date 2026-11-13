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
                _pool = ConnectionPool(
                    config.PG_DSN, min_size=2, max_size=config.PG_POOL_MAX, timeout=5.0,
                    kwargs={"row_factory": dict_row, "autocommit": False}, open=True,
                    check=ConnectionPool.check_connection,
                )
    return _pool


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
                        cur.execute(f"SET LOCAL lock_timeout = '{lock_timeout}'")
                        cur.execute(f"SET LOCAL statement_timeout = '{statement_timeout}'")
                        return fn(cur)
        except RETRYABLE as e:
            last = e
            time.sleep(0.01 * (2 ** i))
    raise last


def query(sql, params=None):
    with pool().connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall() if cur.description else []
        conn.commit()
        return rows


def query_one(sql, params=None):
    rows = query(sql, params)
    return rows[0] if rows else None


def get_state(cur, key):
    cur.execute("SELECT value FROM system_state WHERE key=%s", (key,))
    r = cur.fetchone()
    return r["value"] if r else None


def bump_epoch(cur):
    """Last statement of every availability-changing transaction: serialises with the
    fenced cache switch (see cache.py) and acts as the change watermark."""
    cur.execute("UPDATE system_state SET value = value + 1 WHERE key='pg_epoch' RETURNING value")
    return cur.fetchone()["value"]


def next_patient_seq(cur, patient_id):
    cur.execute("UPDATE patients SET last_seq_no = last_seq_no + 1, updated_at = NOW() "
                "WHERE patient_id=%s RETURNING last_seq_no", (patient_id,))
    return cur.fetchone()["last_seq_no"]


def add_event(cur, etype, data, patient_id=None, priority=2, correlation_id=None, flags=None):
    """Insert one outbox row in the caller's transaction. Patient events get the next
    per-patient sequence number. pg_notify wakes the outbox publisher at commit."""
    event_id = str(uuid.uuid4())
    seq = next_patient_seq(cur, patient_id) if patient_id else None
    envelope = {
        "event_id": event_id,
        "type": etype,
        "patient_id": patient_id,
        "patient_seq_no": seq,
        "correlation_id": correlation_id or event_id,
        "occurred_at": None,
        "priority": priority,
        "data": data,
        "flags": flags or {},
    }
    cur.execute(
        "INSERT INTO outbox_events (event_id, routing_key, priority, payload) "
        "VALUES (%s, %s, %s, jsonb_set(%s::jsonb, '{occurred_at}', to_jsonb(clock_timestamp())))",
        (event_id, etype, priority, json.dumps(envelope)),
    )
    cur.execute("SELECT pg_notify('outbox', '')")
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
