"""Flask application: pages, JSON API and the SSE stream."""
import json
import threading
import time
import uuid

from flask import Flask, Response, jsonify, redirect, render_template, request, send_from_directory, session

from .. import beds, cache, config, db, dispatch, faults, log, mq, ops, rds, smartmatch, transfers
from ..dispatch import ApiError
from .stream import hub

ROLES = ("dispatcher", "hospital", "operator")


def create_app():
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config.update(SECRET_KEY=config.SECRET_KEY, SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE="Lax",
                      JSON_SORT_KEYS=False)
    hub.start()

    # ------------------------------------------------------------ helpers
    def ident():
        return session.get("ident")

    def need(*roles, hospital=None):
        i = ident()
        if not i:
            raise ApiError(401, {"error": "NO_IDENTITY", "detail": "choose a demo identity first"})
        if i["role"] == "operator":
            return i
        if roles and i["role"] not in roles:
            raise ApiError(403, {"error": "FORBIDDEN", "detail": f"role {i['role']} cannot do this"})
        if hospital and i["role"] == "hospital" and i.get("hospital_id") != hospital:
            raise ApiError(403, {"error": "FORBIDDEN", "detail": f"identity is staff of {i.get('hospital_id')}"})
        return i

    def body():
        return request.get_json(silent=True) or {}

    @app.after_request
    def _hdr(resp):
        resp.headers["X-Instance"] = config.INSTANCE
        return resp

    @app.errorhandler(ApiError)
    def _api_error(e):
        resp = jsonify(dict(e.body, instance=config.INSTANCE))
        resp.status_code = e.status
        for k, v in e.headers.items():
            resp.headers[k] = v
        return resp

    @app.errorhandler(db.Conflict)
    def _conflict(e):
        state = json.loads(json.dumps(e.state or {}, default=str))
        return jsonify({"error": e.code, "detail": str(e), "current": state, "instance": config.INSTANCE}), 409

    @app.errorhandler(Exception)
    def _err(e):
        from werkzeug.exceptions import HTTPException
        if isinstance(e, HTTPException):
            return e
        import psycopg
        if isinstance(e, (psycopg.OperationalError, TimeoutError)) or type(e).__name__ == "PoolTimeout":
            log.error("database unavailable", error=str(e)[:200])
            return jsonify({"error": "DATABASE_UNAVAILABLE", "detail": "PostgreSQL is the allocation authority; "
                            "no allocation without it", "retry_safe": True, "instance": config.INSTANCE}), 503
        log.error("unhandled error", error=f"{type(e).__name__}: {e}"[:300], path=request.path)
        return jsonify({"error": "INTERNAL", "detail": type(e).__name__, "instance": config.INSTANCE}), 500

    # ------------------------------------------------------------ pages
    def page(tpl, **kw):
        return render_template(tpl, ident=ident(), instance=config.INSTANCE, chaos=config.CHAOS_ENABLED, **kw)

    @app.get("/")
    def index():
        return redirect("/control")

    @app.get("/control")
    def p_control():
        return page("control.html", title="Control room")

    @app.get("/dispatch")
    def p_dispatch():
        return page("dispatch.html", title="Dispatch")

    @app.get("/hospital/<hid>")
    def p_hospital(hid):
        return page("hospital.html", title=f"Hospital {hid}", hospital_id=hid)

    @app.get("/hospital")
    def p_hospital_default():
        i = ident()
        return redirect(f"/hospital/{(i or {}).get('hospital_id') or 'H-01'}")

    @app.get("/ops")
    def p_ops():
        return page("ops.html", title="Operations")

    @app.get("/favicon.ico")
    def favicon():
        return send_from_directory(app.static_folder, "favicon.ico", mimetype="image/vnd.microsoft.icon")

    @app.get("/healthz")
    def healthz():
        return jsonify({"ok": True, "instance": config.INSTANCE})

    # ------------------------------------------------------------ identity
    @app.post("/api/identity")
    def set_identity():
        b = body()
        role = b.get("role")
        uid = str(b.get("user_id", "")).strip()
        if role not in ROLES or not uid or len(uid) > 40:
            raise ApiError(400, {"error": "BAD_IDENTITY", "roles": ROLES})
        hid = b.get("hospital_id") if role == "hospital" else None
        if role == "hospital" and hid not in smartmatch.catalog()["hospitals"]:
            raise ApiError(400, {"error": "BAD_HOSPITAL"})
        session["ident"] = {"role": role, "user_id": uid, "hospital_id": hid}
        return jsonify(session["ident"])

    @app.get("/api/me")
    def me():
        return jsonify({"ident": ident(), "instance": config.INSTANCE})

    # ------------------------------------------------------------ reference data / live feed
    @app.get("/api/meta")
    def meta():
        cat = smartmatch.catalog()
        from ..seed import DEMO_POINT
        return jsonify({"conditions": cat["conditions"], "hospitals": [
            dict(h, capabilities=cat["caps"].get(hid, [])) for hid, h in cat["hospitals"].items()],
            "demo_point": {"lat": DEMO_POINT[0], "lon": DEMO_POINT[1]}, "radius_km": config.SEARCH_RADIUS_KM,
            "priorities": config.PRIORITIES, "instance": config.INSTANCE})

    @app.get("/api/snapshot")
    def snapshot():
        return jsonify(hub.snapshot())

    @app.get("/api/stream")
    def stream():
        resp = Response(hub.stream(), mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp

    # ------------------------------------------------------------ dispatch
    @app.get("/api/dispatch/search")
    def d_search():
        i = need("dispatcher", "hospital")
        return jsonify(dispatch.search(request.args, i["role"]))

    @app.post("/api/dispatch/reserve")
    def d_reserve():
        i = need("dispatcher")
        out, status = dispatch.reserve(i["user_id"], i["role"], body())
        return jsonify(out), status

    @app.get("/api/reservations/<rid>")
    def r_get(rid):
        need()
        _uuid(rid)
        row = db.query_one("SELECT r.*, h.name AS hospital_name FROM reservations r JOIN hospitals h USING (hospital_id) "
                           "WHERE reservation_id=%s", (rid,))
        if not row:
            raise ApiError(404, {"error": "NO_SUCH_RESERVATION"})
        return jsonify(dispatch.render(row))

    @app.post("/api/reservations/<rid>/confirm")
    def r_confirm(rid):
        i = need("dispatcher", "hospital")
        _uuid(rid)
        return jsonify(beds.confirm(rid, i["user_id"]))

    @app.post("/api/reservations/<rid>/cancel")
    def r_cancel(rid):
        i = need("dispatcher")
        _uuid(rid)
        return jsonify(beds.cancel(rid, i["user_id"]))

    # ------------------------------------------------------------ hospital console
    @app.get("/api/hospitals/<hid>/beds")
    def h_beds(hid):
        rows = db.query("SELECT b.bed_id, b.bed_type, b.status, b.patient_id, b.reservation_id::text, b.version, "
                        "r.expires_at, r.purpose, extract(epoch FROM r.expires_at)::float8 AS expires_epoch "
                        "FROM beds b LEFT JOIN reservations r ON r.reservation_id = b.reservation_id "
                        "WHERE b.hospital_id=%s ORDER BY b.bed_type, b.bed_id", (hid,))
        now = db.query_one("SELECT extract(epoch FROM clock_timestamp())::float8 AS t")["t"]
        for r in rows:
            r["expires_at"] = r["expires_at"].isoformat() if r["expires_at"] else None
        return jsonify({"hospital_id": hid, "beds": rows, "server_epoch": now, "instance": config.INSTANCE})

    @app.post("/api/beds/<bed_id>/admit")
    def b_admit(bed_id):
        hid = _bed_hospital(bed_id)
        i = need("hospital", hospital=hid)
        b = body()
        return jsonify(beds.admit(bed_id, str(b.get("patient_id", "")).strip(), int(b.get("expected_version", -1)),
                                  i["user_id"], str(b.get("idempotency_key") or uuid.uuid4())))

    @app.post("/api/beds/<bed_id>/discharge")
    def b_discharge(bed_id):
        hid = _bed_hospital(bed_id)
        i = need("hospital", hospital=hid)
        return jsonify(beds.discharge(bed_id, int(body().get("expected_version", -1)), i["user_id"]))

    @app.post("/api/transfers")
    def t_create():
        b = body()
        hid = _bed_hospital(str(b.get("from_bed_id", "")))
        i = need("hospital", hospital=hid)
        return jsonify(transfers.create(i["user_id"], b)), 201

    @app.post("/api/transfers/<tid>/accept")
    def t_accept(tid):
        _uuid(tid)
        i = need("hospital", hospital=_transfer_target(tid))
        v = body().get("expected_version")
        return jsonify(transfers.accept(tid, int(v) if v is not None else None, i["user_id"]))

    @app.post("/api/transfers/<tid>/reject")
    def t_reject(tid):
        _uuid(tid)
        i = need("hospital", hospital=_transfer_target(tid))
        b = body()
        v = b.get("expected_version")
        return jsonify(transfers.reject(tid, int(v) if v is not None else None, i["user_id"],
                                        str(b.get("reason") or "rejected by receiving hospital")))

    @app.get("/api/hospitals/<hid>/transfers")
    def t_list(hid):
        now = db.query_one("SELECT extract(epoch FROM clock_timestamp())::float8 AS t")["t"]
        return jsonify({"items": transfers.incoming(hid), "server_epoch": now})

    @app.get("/api/hospitals/<hid>/notifications")
    def h_notes(hid):
        rows = db.query("SELECT notification_id, kind, message, created_at FROM notifications WHERE hospital_id=%s "
                        "ORDER BY notification_id DESC LIMIT 20", (hid,))
        for r in rows:
            r["created_at"] = r["created_at"].isoformat()
        return jsonify({"items": rows})

    def _bed_hospital(bed_id):
        r = db.query_one("SELECT hospital_id FROM beds WHERE bed_id=%s", (bed_id,))
        if not r:
            raise ApiError(404, {"error": "NO_SUCH_BED"})
        return r["hospital_id"]

    def _transfer_target(tid):
        r = db.query_one("SELECT to_hosp_id FROM transfers_saga WHERE transfer_id=%s", (tid,))
        if not r:
            raise ApiError(404, {"error": "NO_SUCH_TRANSFER"})
        return r["to_hosp_id"]

    # ------------------------------------------------------------ ambulance simulator feed
    @app.post("/api/ambulances")
    def amb_positions():
        need("dispatcher")
        items = body().get("items") or []
        if not isinstance(items, list) or len(items) > 1000:
            raise ApiError(400, {"error": "BAD_ITEMS"})
        now = time.time()
        mapping = {str(a["id"])[:32]: json.dumps({"id": str(a["id"])[:32], "lat": float(a["lat"]), "lon": float(a["lon"]),
                                                  "status": str(a.get("status", ""))[:16], "ts": now}) for a in items}
        try:
            rds.call(lambda r: r.hset(rds.AMB_KEY, mapping=mapping) if mapping else 0)
        except rds.RedisUnavailable:
            raise ApiError(503, {"error": "REDIS_UNAVAILABLE", "detail": "ambulance positions are cache-only"})
        return jsonify({"stored": len(mapping)})

    # ------------------------------------------------------------ operations
    @app.get("/api/ops/metrics")
    def o_metrics():
        need("operator")
        m = ops.metrics_snapshot()
        m["sse"] = dict(hub.stats, clients=len(hub.clients))
        return jsonify(json.loads(json.dumps(m, default=str)))

    @app.get("/api/ops/state")
    def o_state():
        st, gen = cache.read_state(force=True)
        return jsonify({"cache_state": cache.STATE_NAMES[st], "generation": gen, "instance": config.INSTANCE,
                        "breaker_open": rds.breaker.is_open})

    @app.get("/api/ops/invariants")
    def o_inv():
        return jsonify(json.loads(json.dumps(ops.invariants(), default=str)))

    @app.get("/api/ops/dlq")
    def o_dlq():
        need("operator")
        out = {}
        for g, (q, _b, _p) in mq.GROUPS.items():
            try:
                msgs = mq.peek(mq.dlq(q), 20)
                out[mq.dlq(q)] = {"available": True, "items": [_dlq_item(m) for m in msgs]}
            except Exception as e:  # noqa: BLE001
                out[mq.dlq(q)] = {"available": False, "reason": str(e)[:200]}
        return jsonify(out)

    @app.post("/api/ops/dlq/replay")
    def o_replay():
        need("operator")
        b = body()
        from .. import chaos
        return jsonify(chaos.replay_dlq(str(b.get("queue", "")), b.get("event_id"), int(b.get("max", 100))))

    # ------------------------------------------------------------ chaos (scenarios 1-4 run in-app)
    @app.post("/api/chaos/<scenario>/run")
    def c_run(scenario):
        _chaos_guard()
        from .. import chaos
        if scenario not in chaos.IN_APP:
            raise ApiError(404, {"error": "UNKNOWN_SCENARIO", "in_app": list(chaos.IN_APP)})
        run_id = chaos.start_run(scenario)
        threading.Thread(target=chaos.execute, args=(scenario, run_id), daemon=True).start()
        return jsonify({"run_id": run_id, "scenario": scenario}), 202

    @app.post("/api/chaos/verify/<what>")
    def c_verify(what):
        _chaos_guard()
        from .. import chaos
        if what == "parity":
            return jsonify(json.loads(json.dumps(chaos.parity(int(body().get("since_minutes", 30))), default=str)))
        raise ApiError(404, {"error": "UNKNOWN_CHECK"})

    @app.get("/api/chaos/runs")
    def c_runs():
        need("operator")
        rows = db.query("SELECT run_id::text, scenario, status, result, started_at, finished_at FROM chaos_runs "
                        "ORDER BY started_at DESC LIMIT 20")
        return jsonify(json.loads(json.dumps(rows, default=str)))

    @app.post("/api/chaos/ttl")
    def c_ttl():
        _chaos_guard()
        s = int(body().get("seconds", 0))
        if s not in (0, 10):
            raise ApiError(400, {"error": "ONLY_0_OR_10", "detail": "0 resets to 900 s; 10 is the demo override"})
        db.query("UPDATE system_state SET value=%s WHERE key='ttl_override_s'", (s,))
        return jsonify({"ttl_override_s": s})

    @app.get("/api/chaos/flags")
    def c_flags_get():
        need("operator")
        return jsonify({r["key"]: r["value"] for r in db.query("SELECT key, value FROM control_flags")})

    @app.post("/api/chaos/flags")
    def c_flags():
        _chaos_guard()
        b = body()
        key = str(b.get("key", ""))
        allowed = ("fault:poison", "fault:crash_after_hold", "fault:ambiguous_commit") + tuple(
            f"pause:{q}" for q, _b, _p in mq.GROUPS.values())
        if key not in allowed:
            raise ApiError(400, {"error": "UNKNOWN_FLAG", "allowed": allowed})
        if b.get("value") in (None, ""):
            faults.clear(key)
        else:
            faults.set_flag(key, b["value"])
        return jsonify({r["key"]: r["value"] for r in db.query("SELECT key, value FROM control_flags")})

    def _chaos_guard():
        if not config.CHAOS_ENABLED:
            raise ApiError(403, {"error": "CHAOS_DISABLED"})
        need("operator")

    def _uuid(v):
        try:
            uuid.UUID(v)
        except ValueError:
            raise ApiError(400, {"error": "BAD_ID"})

    return app


def _dlq_item(m):
    props = m.get("properties", {})
    h = props.get("headers", {}) or {}
    try:
        p = json.loads(m.get("payload", "{}"))
    except ValueError:
        p = {}
    return {"event_id": p.get("event_id") or props.get("message_id"), "type": p.get("type"), "error": h.get("x-error"),
            "original_queue": h.get("x-original-queue"), "attempt": h.get("x-attempt"), "priority": props.get("priority"),
            "redelivered": m.get("redelivered")}
