"""Dispatch search and reserve (section 3.2). PostgreSQL decides; Redis proposes."""
import hashlib
import json
import time
import uuid

import psycopg
from psycopg import errors
from psycopg_pool import PoolTimeout

from . import cache, config, db, faults, log, metrics, rds, smartmatch


class ApiError(Exception):
    def __init__(self, status, body, headers=None):
        super().__init__(body.get("error") or body.get("result"))
        self.status = status
        self.body = body
        self.headers = headers or {}


class BedTaken(Exception):
    pass


class Duplicate(Exception):
    def __init__(self, row):
        super().__init__("duplicate")
        self.row = row


FIELDS = ("patient_id", "ambulance_id", "condition_code", "bed_type", "priority", "lat", "lon", "radius_km")


def validate(req, role):
    def need(k):
        v = req.get(k)
        if v in (None, ""):
            raise ApiError(400, {"error": "MISSING_FIELD", "field": k})
        return v

    q = {}
    for k in ("patient_id", "ambulance_id", "condition_code", "bed_type", "idempotency_key"):
        v = str(need(k)).strip()
        if len(v) > (128 if k == "idempotency_key" else 48):
            raise ApiError(400, {"error": "FIELD_TOO_LONG", "field": k})
        q[k] = v
    q["bed_type"] = q["bed_type"].upper()
    if q["bed_type"] not in config.BED_TYPES:
        raise ApiError(400, {"error": "BAD_BED_TYPE", "supported_bed_types": list(config.BED_TYPES)})
    try:
        q["priority"] = int(req.get("priority", 2))
        q["lat"] = float(need("lat"))
        q["lon"] = float(need("lon"))
        q["radius_km"] = float(req.get("radius_km") or config.SEARCH_RADIUS_KM)
    except (TypeError, ValueError):
        raise ApiError(400, {"error": "BAD_NUMBER"})
    if q["priority"] not in config.PRIORITIES:
        raise ApiError(400, {"error": "BAD_PRIORITY", "supported": list(config.PRIORITIES)})
    if not (-90 <= q["lat"] <= 90 and -180 <= q["lon"] <= 180):
        raise ApiError(400, {"error": "BAD_LOCATION"})
    if not smartmatch.radius_ok(q["radius_km"], role):
        raise ApiError(403, {"error": "RADIUS_NOT_PERMITTED",
                             "detail": f"radius above {config.SEARCH_RADIUS_KM:g} km needs operator review"})
    cond, err = smartmatch.validate_condition(q["condition_code"], q["bed_type"])
    if err:
        raise ApiError(400, err)
    q["fingerprint"] = hashlib.sha256(json.dumps({k: q[k] for k in FIELDS}, sort_keys=True).encode()).hexdigest()
    return q, cond


# ------------------------------------------------------------------ search

def search(params, role):
    t0 = time.perf_counter()
    try:
        bed_type = str(params.get("bed_type", "")).upper()
        condition = str(params.get("condition_code", ""))
        lat, lon = float(params["lat"]), float(params["lon"])
        radius = float(params.get("radius_km") or config.SEARCH_RADIUS_KM)
    except (KeyError, TypeError, ValueError):
        raise ApiError(400, {"error": "BAD_QUERY", "required": ["lat", "lon", "condition_code", "bed_type"]})
    if bed_type not in config.BED_TYPES:
        raise ApiError(400, {"error": "BAD_BED_TYPE", "supported_bed_types": list(config.BED_TYPES)})
    cond, err = smartmatch.validate_condition(condition, bed_type)
    if err:
        raise ApiError(400, err)
    if not smartmatch.radius_ok(radius, role):
        raise ApiError(403, {"error": "RADIUS_NOT_PERMITTED"})
    rows, path = None, "sql"
    gen = cache.fast_path_generation()
    if gen is not None:
        try:
            rows = _redis_search_rows(gen, condition, bed_type, lat, lon, radius)
            path = "redis"
        except rds.RedisUnavailable:
            rows = None
    if rows is None:
        rows = smartmatch.sql_search(condition, bed_type, lat, lon, radius)
    out = smartmatch.explain(condition, bed_type, rows, radius)
    ms = (time.perf_counter() - t0) * 1000
    metrics.record("search", ms, path)
    out.update(path=path, server_ms=round(ms, 2), instance=config.INSTANCE)
    return out


def _redis_search_rows(gen, condition, bed_type, lat, lon, radius):
    eligible = smartmatch.eligible_hospitals(condition)
    cat = smartmatch.catalog()

    def _do(r):
        hits = r.geosearch(rds.k_geo(gen), longitude=lon, latitude=lat, radius=radius, unit="km", sort="ASC",
                           withdist=True)
        p = r.pipeline(transaction=False)
        for hid, _d in hits:
            p.scard(rds.k_free(gen, hid, bed_type))
        p.get(rds.POINTER)
        res = p.execute()
        return hits, res[:-1], res[-1]

    hits, counts, pointer = rds.call(_do)
    if pointer != str(gen):
        raise rds.RedisUnavailable("generation changed")
    return [{"hospital_id": hid, "name": cat["hospitals"][hid]["name"], "dist_km": d, "free": c,
             "eligible": hid in eligible and bed_type in cat["conditions"][condition]["bed_types"]}
            for (hid, d), c in zip(hits, counts)]


# ------------------------------------------------------------------ reserve

def lookup(actor, key, lock=False):
    if not lock:
        return db.query_one("SELECT r.*, h.name AS hospital_name FROM reservations r JOIN hospitals h USING (hospital_id) "
                            "WHERE r.actor_id=%s AND r.idempotency_key=%s", (actor, key))

    def _q(cur):
        if lock:
            cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"{actor}:{key}",))
        cur.execute("SELECT r.*, h.name AS hospital_name FROM reservations r JOIN hospitals h USING (hospital_id) "
                    "WHERE r.actor_id=%s AND r.idempotency_key=%s", (actor, key))
        return cur.fetchone()
    return db.run_tx(_q)


def render(row, path=None, extra=None):
    out = {"reservation_id": str(row["reservation_id"]), "status": row["status"], "bed_id": row["bed_id"],
           "hospital_id": row["hospital_id"], "hospital_name": row.get("hospital_name"), "bed_type": row["bed_type"],
           "patient_id": row["patient_id"], "ambulance_id": row["ambulance_id"], "priority": row["priority"],
           "condition_code": row.get("condition_code"), "purpose": row.get("purpose"),
           "expires_at": row["expires_at"].isoformat(), "server_now": time.time(),
           "alloc_path": path or row.get("alloc_path"), "instance": config.INSTANCE}
    if extra:
        out.update(extra)
    return out


def _sql_rate_limit(actor):
    win = config.RATE_WINDOW_MS

    def _do(cur):
        cur.execute(
            "INSERT INTO rate_limit_windows (actor_id, window_start, requests) VALUES "
            "(%s, to_timestamp(floor(extract(epoch FROM clock_timestamp()) * 1000 / %s) * %s / 1000.0), 1) "
            "ON CONFLICT (actor_id, window_start) DO UPDATE SET requests = rate_limit_windows.requests + 1 "
            "RETURNING requests, extract(epoch FROM (window_start + make_interval(secs => %s / 1000.0) "
            "- clock_timestamp())) * 1000 AS remaining_ms", (actor, win, win, win))
        return cur.fetchone()
    r = db.run_tx(_do)
    if r["requests"] > config.RATE_MAX:
        _raise_rate_limited(r["remaining_ms"], "sql")


def _raise_rate_limited(remaining_ms, path):
    secs = max(1, int((float(remaining_ms) + 999) // 1000))
    raise ApiError(429, {"error": "RATE_LIMITED", "retry_after_s": secs, "limiter": path,
                         "limit": f"{config.RATE_MAX} requests per {config.RATE_WINDOW_MS // 1000} s per user"},
                   headers={"Retry-After": str(secs)})


def _commit(actor, q, reservation_id, bed_id=None, path="sql"):
    """The authoritative allocation transaction. Raises Duplicate, BedTaken, db.Conflict or ApiError."""
    params = {"cond": q["condition_code"], "bt": q["bed_type"], "lat": q["lat"], "lon": q["lon"],
              "r": q["radius_km"]}

    def tx(cur):
        # 0. idempotency: lock on (actor, key), then look for a committed row (one round trip)
        cur.execute("SELECT r.* FROM (SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))) l "
                    "LEFT JOIN reservations r ON r.actor_id=%s AND r.idempotency_key=%s",
                    (f"{actor}:{q['idempotency_key']}", actor, q["idempotency_key"]))
        prior = cur.fetchone()
        if prior and prior["reservation_id"]:
            prior["hospital_name"] = smartmatch.catalog()["hospitals"][prior["hospital_id"]]["name"]
            raise Duplicate(prior)
        # 1. patient lock: the upsert takes the row lock (created on first sight) and reports an active hold
        cur.execute("INSERT INTO patients (patient_id, state) VALUES (%s, 'IN_TRANSIT') ON CONFLICT (patient_id) "
                    "DO UPDATE SET updated_at = patients.updated_at RETURNING patient_id, state, EXISTS (SELECT 1 FROM "
                    "reservations WHERE patient_id=%s AND status='HELD' AND purpose='DISPATCH') AS has_hold",
                    (q["patient_id"], q["patient_id"]))
        p = cur.fetchone()
        if p["state"] in ("ADMITTED", "TRANSFERRING"):
            raise db.Conflict(f"patient is {p['state']}", state={"patient": p}, code="PATIENT_NOT_ELIGIBLE")
        if p["has_hold"]:
            raise db.Conflict("patient already has an active dispatch hold", code="PATIENT_HAS_HOLD")
        # 2. bed: the conditional update is the decision; eligibility is verified here against PostgreSQL
        upd = ("UPDATE beds b SET status='RESERVED', reservation_id=%(rid)s, patient_id=%(pid)s, version=version+1, "
               "updated_at=NOW() WHERE b.bed_id=%(bed)s AND b.status='FREE' AND b.bed_type=%(bt)s AND "
               + smartmatch.ELIGIBLE_SQL.format(h="b.hospital_id") + " RETURNING bed_id, hospital_id, bed_type, version")
        args = dict(params, rid=reservation_id, pid=q["patient_id"])
        if bed_id:
            cur.execute(upd, dict(args, bed=bed_id))
            bed = cur.fetchone()
            if not bed:
                raise BedTaken(bed_id)
        else:
            cur.execute(f"""
              SELECT b.bed_id FROM beds b JOIN hospitals h ON h.hospital_id = b.hospital_id
              WHERE b.status='FREE' AND b.bed_type=%(bt)s AND {smartmatch.ELIGIBLE_SQL.format(h='b.hospital_id')}
                AND {smartmatch.HAVERSINE.format(t='h')} <= %(r)s
              ORDER BY {smartmatch.HAVERSINE.format(t='h')}, b.hospital_id, b.bed_id
              LIMIT 1 FOR UPDATE OF b SKIP LOCKED""", params)
            cand = cur.fetchone()
            if not cand:
                raise ApiError(404, {"result": "NO_SUITABLE_HOSPITAL_OR_BED"})
            cur.execute(upd, dict(args, bed=cand["bed_id"]))
            bed = cur.fetchone()
            if not bed:
                raise BedTaken(cand["bed_id"])
        # 3. reservation row; a unique violation aborts the whole transaction (no savepoint needed)
        try:
            cur.execute(
                "INSERT INTO reservations (reservation_id, actor_id, idempotency_key, request_fingerprint, bed_id, "
                "hospital_id, bed_type, ambulance_id, patient_id, priority, status, expires_at, purpose, "
                "condition_code, alloc_path) SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'HELD', clock_timestamp() + "
                "make_interval(secs => CASE WHEN s.value > 0 THEN s.value ELSE %s END), 'DISPATCH', %s, %s "
                "FROM system_state s WHERE s.key='ttl_override_s' RETURNING *",
                (reservation_id, actor, q["idempotency_key"], q["fingerprint"], bed["bed_id"], bed["hospital_id"],
                 bed["bed_type"], q["ambulance_id"], q["patient_id"], q["priority"], config.DEFAULT_TTL_S,
                 q["condition_code"], path))
            res = cur.fetchone()
        except errors.UniqueViolation as e:
            if "uq_res_one_active_per_bed" in str(e):
                raise BedTaken(bed["bed_id"])
            raise db.Conflict("patient already has an active dispatch hold", code="PATIENT_HAS_HOLD")
        # 4. outbox event + patient state in one statement, then the epoch fence
        db.add_event(cur, "bed.reservation.created",
                     {"reservation_id": str(reservation_id), "bed_id": bed["bed_id"], "hospital_id": bed["hospital_id"],
                      "bed_type": bed["bed_type"], "bed_version": bed["version"], "ambulance_id": q["ambulance_id"],
                      "condition_code": q["condition_code"], "expires_at": res["expires_at"].isoformat(),
                      "actor_id": actor, "alloc_path": path},
                     patient_id=q["patient_id"], priority=q["priority"],
                     patient_set={"state": "IN_TRANSIT", "current_bed_id": None, "current_hosp_id": None})
        db.bump_epoch(cur)
        res["hospital_name"] = smartmatch.catalog()["hospitals"][bed["hospital_id"]]["name"]
        return res

    row = db.run_tx(tx)
    if faults.get("fault:ambiguous_commit") == "1":
        # Simulates the connection dying after COMMIT was sent: the caller must reconcile by key.
        raise psycopg.OperationalError("injected: connection lost after COMMIT")
    return row


def reserve(actor, role, req):
    t0 = time.perf_counter()
    q, cond = validate(req, role)
    path = "sql"
    try:
        result, status, path = _reserve(actor, role, q, cond)
        return result, status
    finally:
        metrics.record("reserve", (time.perf_counter() - t0) * 1000, path)


def _reserve(actor, role, q, cond):
    key = q["idempotency_key"]
    prior = lookup(actor, key)
    if prior:
        if prior["request_fingerprint"] != q["fingerprint"]:
            raise ApiError(409, {"error": "IDEMPOTENCY_KEY_REUSED", "detail": "same key, different request"})
        return render(prior, extra={"replay": True}), 200, "replay"

    eligible = smartmatch.eligible_hospitals(q["condition_code"])
    match_info = {"required_capabilities": cond["required"], "eligible_hospitals": len(eligible)}
    if not eligible:
        raise ApiError(404, {"result": "NO_SUITABLE_HOSPITAL_OR_BED", "smartmatch": match_info,
                             "explanation": "No hospital holds every required capability."})

    gen = cache.fast_path_generation()
    lua_consulted = False
    if gen is not None:
        try:
            out = _lua_path(actor, q, gen, eligible, match_info)
            if out is not None:
                return out
            lua_consulted = True
        except rds.RedisUnavailable:
            pass
    if not lua_consulted:
        _sql_rate_limit(actor)
    return _sql_path(actor, q, match_info)


def _lua_path(actor, q, gen, eligible, match_info):
    """Returns (body, status, path) or None to fall back to SQL (after Lua counted the rate limit)."""
    key = q["idempotency_key"]
    hits = rds.call(lambda r: r.geosearch(rds.k_geo(gen), longitude=q["lon"], latitude=q["lat"],
                                          radius=q["radius_km"], unit="km", sort="ASC"))
    candidates = [h for h in hits if h in eligible]
    for attempt in range(config.LUA_ATTEMPTS):
        rid = uuid.uuid4()
        meta = json.dumps({"actor": actor, "key": key, "patient": q["patient_id"], "fp": q["fingerprint"]})
        res = rds.call(lambda r: rds.script("reserve_nearest_bed")(
            keys=[rds.POINTER, rds.k_rate(actor), rds.k_idemp(actor, key), rds.k_expiry(gen)],
            args=[q["bed_type"], str(rid), _ttl_ms_hint(), config.RATE_MAX, config.RATE_WINDOW_MS, str(gen), meta]
            + candidates))
        tag = res[0]
        if tag == "NOT_READY":
            if attempt == 0:
                raise rds.RedisUnavailable("cache generation not ready")  # nothing counted: SQL limiter applies
            return None
        if tag == "RATE_LIMITED":
            _raise_rate_limited(res[1], "redis")
        if tag == "NO_SUITABLE_BED":
            return None  # confirm authoritatively in SQL; a stale cache may be missing freed beds
        if tag == "IDEMPOTENT_HIT":
            return _resolve_idempotent_hit(actor, q, json.loads(res[1]), match_info)
        if tag != "RESERVED":
            return None
        rec = json.loads(res[1])
        if faults.get("fault:crash_after_hold") == "1":
            log.warn("injected crash after Redis hold; hold left for orphan reconciliation",
                     correlation_id=str(rid), bed=rec["bed_id"])
            raise ApiError(503, {"error": "INJECTED_CRASH_AFTER_HOLD", "retry_safe": True, "idempotency_key": key})
        try:
            row = _commit(actor, q, rid, bed_id=rec["bed_id"], path="redis")
            return render(row, "redis", {"smartmatch": _match_extra(row, q, match_info)}), 201, "redis"
        except BedTaken:
            cache.settle_hold(rid, gen, rec["bed_id"])
            _forget_idemp(actor, key)
            cache.reconcile_hospital(rec["hospital_id"], [q["bed_type"]])
            continue
        except Duplicate as d:
            cache.settle_hold(rid, gen, rec["bed_id"])
            return _dup_response(d.row, q)
        except PoolTimeout:
            # no connection was obtained, so nothing was sent: definitely not committed
            cache.settle_hold(rid, gen, rec["bed_id"])
            _forget_idemp(actor, key)
            raise ApiError(503, {"error": "BUSY", "retry_safe": True, "idempotency_key": key}, {"Retry-After": "1"})
        except psycopg.OperationalError as e:
            return _ambiguous(actor, q, rid, gen, rec, e)
        except Exception:
            cache.settle_hold(rid, gen, rec["bed_id"])
            _forget_idemp(actor, key)
            raise
    return None


def _ttl_ms_hint():
    # Redis TTL is only a hint for orphan discovery; PostgreSQL expires_at is authoritative.
    return (config.DEFAULT_TTL_S + 60) * 1000


def _forget_idemp(actor, key):
    try:
        rds.call(lambda r: r.delete(rds.k_idemp(actor, key)))
    except rds.RedisUnavailable:
        pass


def _match_extra(row, q, match_info):
    cat = smartmatch.catalog()
    h = cat["hospitals"][row["hospital_id"]]
    from .seed import km
    d = km((q["lat"], q["lon"]), (h["lat"], h["lon"]))
    cond = cat["conditions"][q["condition_code"]]
    return dict(match_info, distance_km=round(d, 2), hospital_capabilities=cat["caps"].get(row["hospital_id"], []),
                reason=f"{cond['label']}: {', '.join(cond['required']) or 'no specialty required'} verified; "
                       f"{row['bed_type']} bed held; {d:.1f} km")


def _dup_response(row, q):
    if row["request_fingerprint"] != q["fingerprint"]:
        raise ApiError(409, {"error": "IDEMPOTENCY_KEY_REUSED", "detail": "same key, different request"})
    return render(row, extra={"replay": True}), 200, "replay"


def _resolve_idempotent_hit(actor, q, rec, match_info):
    """Redis says this (actor,key) already has a hold, but PostgreSQL had no row at our first read.
    Wait on the database idempotency lock; if the original committed, return it. Otherwise the original
    died: settle its hold against PostgreSQL and allocate through SQL."""
    row = lookup(actor, q["idempotency_key"], lock=True)
    if row:
        return _dup_response(row, q)
    gen = rec.get("generation")
    cache.settle_hold(rec["reservation_id"], gen, rec["bed_id"])
    _forget_idemp(actor, q["idempotency_key"])
    return _sql_path(actor, q, match_info)


def _ambiguous(actor, q, rid, gen, rec, err):
    """Commit outcome unknown. Never release blindly: read back by idempotency key under its lock."""
    log.warn("ambiguous commit; reconciling by idempotency key", correlation_id=str(rid), error=str(err)[:200])
    try:
        row = lookup(actor, q["idempotency_key"], lock=True)
    except Exception:  # noqa: BLE001  PostgreSQL unreachable: leave the hold quarantined
        raise ApiError(503, {"error": "COMMIT_OUTCOME_UNKNOWN", "retry_safe": True,
                             "idempotency_key": q["idempotency_key"]})
    if row:
        if str(row["reservation_id"]) != str(rid) and gen is not None:
            cache.settle_hold(rid, gen, rec["bed_id"] if rec else None)
        return render(row, extra={"reconciled_after_unknown_commit": True}), 201, "reconciled"
    if gen is not None and rec:
        cache.settle_hold(rid, gen, rec["bed_id"])
        _forget_idemp(actor, q["idempotency_key"])
    raise ApiError(503, {"error": "COMMIT_NOT_APPLIED", "retry_safe": True, "idempotency_key": q["idempotency_key"]})


def _sql_path(actor, q, match_info):
    rid = uuid.uuid4()
    try:
        row = None
        for _ in range(3):
            try:
                row = _commit(actor, q, rid, path="sql")
                break
            except BedTaken:
                rid = uuid.uuid4()  # bed changed between select and update: pick again
        if row is None:
            raise ApiError(503, {"error": "CONTENTION", "retry_safe": True, "idempotency_key": q["idempotency_key"]})
    except Duplicate as d:
        return _dup_response(d.row, q)
    except PoolTimeout:
        raise ApiError(503, {"error": "BUSY", "retry_safe": True, "idempotency_key": q["idempotency_key"]},
                       {"Retry-After": "1"})
    except psycopg.OperationalError as e:
        return _ambiguous(actor, q, rid, None, None, e)
    except ApiError as e:
        if e.body.get("result") == "NO_SUITABLE_HOSPITAL_OR_BED":
            s = smartmatch.explain(q["condition_code"], q["bed_type"],
                                   smartmatch.sql_search(q["condition_code"], q["bed_type"], q["lat"], q["lon"],
                                                         q["radius_km"]), q["radius_km"])
            e.body.update(explanation=s.get("explanation"), smartmatch=dict(match_info, excluded=s["excluded"][:8]))
        raise
    cache.project_bed_taken(row["bed_id"], row["hospital_id"], row["bed_type"])
    return render(row, "sql", {"smartmatch": _match_extra(row, q, match_info)}), 201, "sql"
