"""Shared cache lifecycle: DEGRADED / REBUILDING / READY with a monotonic generation in PostgreSQL.

Rules (DECISIONS C15):
- PostgreSQL system_state holds cache_state and cache_generation. Redis only mirrors the READY
  generation in the fencing pointer POINTER. The Lua script compares the pointer with the generation the
  caller read from PostgreSQL, so both must agree before Lua touches any key.
- Any Redis failure seen by any process marks the shared state DEGRADED (conditional update).
- One worker at a time (advisory lock) rebuilds into a NEW generation namespace mg:<gen>:..., then does the
  final reconciliation while holding the pg_epoch row lock. Every availability write bumps pg_epoch as its
  last statement, so no allocation can commit between the final read and the READY switch.
"""
import json
import threading
import time

import psycopg

from . import config, db, log, rds

DEGRADED, REBUILDING, READY = 0, 1, 2
STATE_NAMES = {DEGRADED: "DEGRADED", REBUILDING: "REBUILDING", READY: "READY"}
ADVISORY_REBUILD = 4242

_cached = {"t": 0.0, "state": DEGRADED, "gen": 0}
_cached_lock = threading.Lock()
STATE_TTL_S = 0.25


def read_state(force=False):
    now = time.monotonic()
    if not force and now - _cached["t"] < STATE_TTL_S:
        return _cached["state"], _cached["gen"]
    with _cached_lock:
        if not force and now - _cached["t"] < STATE_TTL_S:
            return _cached["state"], _cached["gen"]
        rows = db.query("SELECT key, value FROM system_state WHERE key IN ('cache_state','cache_generation')")
        d = {r["key"]: r["value"] for r in rows}
        _cached.update(t=time.monotonic(), state=d.get("cache_state", DEGRADED), gen=d.get("cache_generation", 0))
        return _cached["state"], _cached["gen"]


def fast_path_generation():
    """Generation to use for the Lua fast path, or None if the SQL path must be used."""
    try:
        st, gen = read_state()
    except Exception:  # noqa: BLE001
        return None
    if st != READY or not rds.breaker.allow():
        return None
    return gen


def _transition(cur, to_state, actor, detail, gen=None, only_from=None):
    cur.execute("SELECT key, value FROM system_state WHERE key IN ('cache_state','cache_generation') FOR UPDATE")
    d = {r["key"]: r["value"] for r in cur.fetchall()}
    frm = d["cache_state"]
    if only_from is not None and frm not in only_from:
        return False, frm, d["cache_generation"]
    g = gen if gen is not None else d["cache_generation"]
    cur.execute("UPDATE system_state SET value=%s WHERE key='cache_state'", (to_state,))
    cur.execute("UPDATE system_state SET value=%s WHERE key='cache_generation'", (g,))
    cur.execute("INSERT INTO cache_transitions (from_state, to_state, generation, actor, detail) VALUES (%s,%s,%s,%s,%s)",
                (frm, to_state, g, actor, detail[:300]))
    return True, frm, g


def mark_degraded(reason):
    """First detected outage: close the fast path for all instances."""
    try:
        changed = db.run_tx(lambda cur: _transition(cur, DEGRADED, config.INSTANCE, reason,
                                                    only_from=(READY, REBUILDING)))[0]
    except Exception as e:  # noqa: BLE001
        log.warn("mark_degraded failed (postgres)", error=str(e)[:200])
        return
    with _cached_lock:
        _cached["t"] = 0.0
    if changed:
        log.warn("cache state -> DEGRADED", reason=reason)
        try:
            rds.slow_client(timeout=0.2).delete(rds.POINTER)
        except Exception:  # noqa: BLE001
            pass


rds.on_failure(mark_degraded)


# ---------------------------------------------------------------- rebuild (worker only)

def _load_snapshot(r, gen, cur):
    """Load GEO + free sets + HELD reservations into the namespace of `gen` from cursor `cur`."""
    cur.execute("SELECT hospital_id, lat, lon FROM hospitals")
    hosps = cur.fetchall()
    cur.execute("SELECT hospital_id, bed_type, bed_id FROM beds WHERE status='FREE'")
    free = cur.fetchall()
    cur.execute("SELECT reservation_id, bed_id, hospital_id, bed_type, actor_id, idempotency_key, "
                "(EXTRACT(EPOCH FROM expires_at)*1000)::bigint AS exp_ms, "
                "(EXTRACT(EPOCH FROM created_at)*1000)::bigint AS created_ms "
                "FROM reservations WHERE status='HELD'")
    held = cur.fetchall()
    # wipe anything left from a previous attempt at this generation
    keys = list(r.scan_iter(match=f"mg:{gen}:*", count=1000))
    p = r.pipeline(transaction=False)
    for i in range(0, len(keys), 500):
        p.delete(*keys[i:i + 500])
    for h in hosps:
        p.geoadd(rds.k_geo(gen), (h["lon"], h["lat"], h["hospital_id"]))
    groups = {}
    for b in free:
        groups.setdefault((b["hospital_id"], b["bed_type"]), []).append(b["bed_id"])
    for (h, t), ids in groups.items():
        p.sadd(rds.k_free(gen, h, t), *ids)
    for h in held:
        rid = str(h["reservation_id"])
        rec = json.dumps({"reservation_id": rid, "hospital_id": h["hospital_id"], "bed_id": h["bed_id"],
                          "bed_type": h["bed_type"], "expires_at_ms": h["exp_ms"], "created_ms": h["created_ms"],
                          "generation": str(gen),
                          "meta": json.dumps({"actor": h["actor_id"], "key": h["idempotency_key"], "rebuilt": True})})
        p.hset(rds.k_resdata(gen), rid, rec)
        p.zadd(rds.k_expiry(gen), {rid: h["exp_ms"]})
        p.sadd(rds.k_held(gen), h["bed_id"])
    p.execute()
    return len(hosps), len(free), len(held)


def rebuild(actor):
    """Rebuild into a new generation and switch READY. Returns True if READY was reached."""
    lock_conn = psycopg.connect(config.PG_DSN, autocommit=True)
    try:
        if not lock_conn.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_REBUILD,)).fetchone()[0]:
            return False
        t0 = time.monotonic()
        r = rds.slow_client()
        r.ping()
        ok, frm, gen = db.run_tx(lambda cur: _new_generation(cur, actor))
        r.delete(rds.POINTER)  # close the Lua fast path for every instance immediately
        log.info("cache state -> REBUILDING", generation=gen, from_state=STATE_NAMES.get(frm))
        # 1) bulk load from a consistent snapshot (no locks held; writers keep going through SQL)
        with psycopg.connect(config.PG_DSN, row_factory=psycopg.rows.dict_row) as c:
            c.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            with c.cursor() as cur:
                counts = _load_snapshot(r, gen, cur)
            c.commit()
        # 2) fenced switch: hold pg_epoch row lock, re-read authoritative state, reconcile, publish pointer.
        def fenced(cur):
            cur.execute("SELECT value FROM system_state WHERE key='pg_epoch' FOR UPDATE")
            cur.execute("SELECT value FROM system_state WHERE key='cache_generation' FOR UPDATE")
            if cur.fetchone()["value"] != gen:
                raise RuntimeError("generation moved during rebuild")
            _load_snapshot(r, gen, cur)
            # orphan reconciliation of provisional holds from older generations is unnecessary: those
            # generations are no longer read. Their HELD rows were mirrored above from PostgreSQL.
            r.set(rds.POINTER, str(gen))
            _transition(cur, READY, actor, f"rebuilt hospitals={counts[0]} free={counts[1]} held={counts[2]}",
                        gen=gen, only_from=(REBUILDING,))
            return True
        db.run_tx(fenced, lock_timeout="3s", statement_timeout="10s")
        with _cached_lock:
            _cached["t"] = 0.0
        log.info("cache state -> READY", generation=gen, ms=round((time.monotonic() - t0) * 1000, 1),
                 hospitals=counts[0], free_beds=counts[1], held=counts[2])
        _cleanup_old_generations(r, gen)
        return True
    except Exception as e:  # noqa: BLE001
        log.warn("rebuild failed", error=str(e)[:300])
        try:
            rds.slow_client(timeout=0.3).delete(rds.POINTER)
        except Exception:  # noqa: BLE001
            pass
        return False
    finally:
        try:
            lock_conn.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_REBUILD,))
        finally:
            lock_conn.close()


def _new_generation(cur, actor):
    cur.execute("SELECT value FROM system_state WHERE key='cache_generation' FOR UPDATE")
    gen = cur.fetchone()["value"] + 1
    return _transition(cur, REBUILDING, actor, "rebuild start", gen=gen)


def _cleanup_old_generations(r, gen):
    n = 0
    for k in r.scan_iter(match="mg:*", count=2000):
        try:
            g = int(k.split(":", 2)[1])
        except ValueError:
            continue
        if g < gen:
            r.delete(k)
            n += 1
    if n:
        log.info("old generation keys removed", count=n, active_generation=gen)


# ---------------------------------------------------------------- projections (post-commit, best effort)

def project_bed_taken(bed_id, hospital_id, bed_type):
    """After a SQL-path commit took a bed: remove it from whatever generation the pointer names.
    Removing a bed from a free set is always safe, so this does not wait for READY."""
    try:
        def _do(r):
            g = r.get(rds.POINTER)
            if g:
                r.srem(rds.k_free(g, hospital_id, bed_type), bed_id)
        rds.call(_do)
    except rds.RedisUnavailable:
        pass


def project_bed_freed(bed_id):
    """After a commit freed a bed: re-read PostgreSQL; add to the free set only if FREE and unowned."""
    gen = fast_path_generation()
    if gen is None:
        return
    row = db.query_one("SELECT hospital_id, bed_type, status, reservation_id FROM beds WHERE bed_id=%s", (bed_id,))
    if not row or row["status"] != "FREE" or row["reservation_id"] is not None:
        return
    try:
        rds.call(lambda r: r.sadd(rds.k_free(gen, row["hospital_id"], row["bed_type"]), bed_id))
    except rds.RedisUnavailable:
        pass


def settle_hold(reservation_id, generation, bed_id=None):
    """Settle a provisional Redis hold after the PostgreSQL outcome is known.
    'release' only when PostgreSQL proves the bed is FREE and unowned, otherwise 'consume'."""
    if generation is None:
        return None
    mode = "consume"
    if bed_id:
        row = db.query_one("SELECT status, reservation_id FROM beds WHERE bed_id=%s", (bed_id,))
        if row and row["status"] == "FREE" and row["reservation_id"] is None:
            mode = "release"
    try:
        res = rds.call(lambda r: rds.script("release_reservation")(
            keys=[rds.k_expiry(generation)], args=[str(reservation_id), mode, str(generation)]), allow_when_open=False)
        return mode, res[0] if res else None
    except rds.RedisUnavailable:
        return None


def reconcile_hospital(hospital_id, bed_types=None, require_ready=True):
    """Make Redis free sets for a hospital equal to PostgreSQL (DB-verified)."""
    gen = fast_path_generation()
    if gen is None:
        return None
    types = bed_types or config.BED_TYPES
    rows = db.query("SELECT bed_type, bed_id FROM beds WHERE hospital_id=%s AND status='FREE' AND bed_type = ANY(%s)",
                    (hospital_id, list(types)))
    by = {t: [] for t in types}
    for r in rows:
        by[r["bed_type"]].append(r["bed_id"])
    out = {}
    for t in types:
        try:
            res = rds.call(lambda r: rds.script("reconcile_free")(
                keys=[rds.POINTER], args=[str(gen), "1" if require_ready else "0", hospital_id, t] + by[t]))
            out[t] = res
        except rds.RedisUnavailable:
            return None
    return out
