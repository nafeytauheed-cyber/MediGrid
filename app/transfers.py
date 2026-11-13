"""Transfer saga between two hospitals (section 3.9). All transitions are single PostgreSQL transactions
with the standard lock order: patient -> saga -> beds sorted -> reservations sorted. Compensation is done
in the same transaction as the reject/timeout decision, so the *_COMPENSATING states are never left behind
(DECISIONS D-SAGA)."""
import hashlib
import json
import uuid

from . import cache, config, db, log, smartmatch
from .beds import _bed_view, _lock_patient
from .dispatch import ApiError

ACTIVE = ("TARGET_RESERVED", "AWAITING_ACCEPTANCE", "REJECTED_COMPENSATING", "TIMED_OUT_COMPENSATING")


def _render(s, cur=None):
    out = {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in s.items()}
    for k in ("deadline_at", "created_at", "updated_at"):
        if out.get(k) is not None and hasattr(out[k], "isoformat"):
            out[k] = out[k].isoformat()
    return out


def create(actor, req):
    try:
        patient_id = str(req["patient_id"]).strip()
        from_bed = str(req["from_bed_id"]).strip()
        to_hosp = str(req["to_hospital_id"]).strip()
        cond_code = str(req["condition_code"]).strip()
        bed_type = str(req["bed_type"]).strip().upper()
        key = str(req["idempotency_key"]).strip()
        priority = int(req.get("priority", 4))
    except (KeyError, TypeError, ValueError):
        raise ApiError(400, {"error": "BAD_REQUEST", "required": ["patient_id", "from_bed_id", "to_hospital_id",
                                                                  "condition_code", "bed_type", "idempotency_key"]})
    if priority not in config.PRIORITIES:
        raise ApiError(400, {"error": "BAD_PRIORITY"})
    cond, err = smartmatch.validate_condition(cond_code, bed_type)
    if err:
        raise ApiError(400, err)
    if to_hosp not in smartmatch.eligible_hospitals(cond_code):
        caps = smartmatch.catalog()["caps"].get(to_hosp)
        if caps is None:
            raise ApiError(404, {"error": "NO_SUCH_HOSPITAL"})
        missing = [c for c in cond["required"] if c not in caps]
        raise ApiError(422, {"error": "DESTINATION_NOT_CAPABLE", "missing_capabilities": missing,
                             "detail": f"{to_hosp} lacks {', '.join(missing)} required for {cond_code}"})
    fp = hashlib.sha256(json.dumps([patient_id, from_bed, to_hosp, cond_code, bed_type, priority]).encode()).hexdigest()
    params = {"cond": cond_code, "bt": bed_type, "h": to_hosp}

    def tx(cur):
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"transfer:{actor}:{key}",))
        cur.execute("SELECT * FROM transfers_saga WHERE actor_id=%s AND idempotency_key=%s", (actor, key))
        prior = cur.fetchone()
        if prior:
            if prior["request_fingerprint"] != fp:
                raise ApiError(409, {"error": "IDEMPOTENCY_KEY_REUSED"})
            return prior, True
        p = _lock_patient(cur, patient_id)
        if not p:
            raise ApiError(404, {"error": "NO_SUCH_PATIENT"})
        cur.execute("SELECT * FROM transfers_saga WHERE patient_id=%s AND saga_state = ANY(%s) FOR UPDATE",
                    (patient_id, list(ACTIVE)))
        active = cur.fetchone()
        if active:
            raise db.Conflict("patient already has an active transfer", code="TRANSFER_ACTIVE",
                              state={"transfer": _render(active)})
        if p["state"] != "ADMITTED" or p["current_bed_id"] != from_bed:
            raise db.Conflict("patient is not admitted in the claimed source bed", code="SOURCE_MISMATCH",
                              state={"patient": dict(p, updated_at=str(p["updated_at"]))})
        if p["current_hosp_id"] == to_hosp:
            raise ApiError(400, {"error": "SAME_HOSPITAL"})
        # target candidate: SKIP LOCKED never waits, so it cannot deadlock with the sorted bed locks below
        cur.execute("SELECT b.bed_id, b.version FROM beds b WHERE b.hospital_id=%(h)s AND b.bed_type=%(bt)s "
                    "AND b.status='FREE' AND " + smartmatch.ELIGIBLE_SQL.format(h="b.hospital_id")
                    + " ORDER BY b.bed_id LIMIT 1 FOR UPDATE SKIP LOCKED", params)
        tgt = cur.fetchone()
        if not tgt:
            raise db.Conflict(f"no free {bed_type} bed at {to_hosp}", code="NO_FREE_BED_AT_DESTINATION")
        cur.execute("SELECT * FROM beds WHERE bed_id=%s FOR UPDATE", (from_bed,))
        src = cur.fetchone()
        if not src or src["status"] != "OCCUPIED" or src["patient_id"] != patient_id:
            raise db.Conflict("source bed not occupied by this patient", code="SOURCE_MISMATCH")
        deadline_s = db.get_state(cur, "transfer_deadline_s") or 60
        rid, tid = str(uuid.uuid4()), str(uuid.uuid4())
        cur.execute("UPDATE beds SET status='RESERVED', reservation_id=%s, patient_id=%s, version=version+1, "
                    "updated_at=NOW() WHERE bed_id=%s AND status='FREE' AND version=%s RETURNING *",
                    (rid, patient_id, tgt["bed_id"], tgt["version"]))
        tb = cur.fetchone()
        if not tb:
            raise db.Conflict("destination bed changed", code="BED_TAKEN")
        # target hold outlives the acceptance deadline by 30 s: the saga deadline, not the hold, decides
        cur.execute("INSERT INTO reservations (reservation_id, actor_id, idempotency_key, request_fingerprint, bed_id, "
                    "hospital_id, bed_type, ambulance_id, patient_id, priority, status, expires_at, purpose, "
                    "condition_code, alloc_path) VALUES (%s,%s,%s,%s,%s,%s,%s,'TRANSFER',%s,%s,'HELD', "
                    "clock_timestamp() + make_interval(secs => %s), 'TRANSFER', %s, 'sql')",
                    (rid, actor, f"transfer:{key}", fp, tb["bed_id"], to_hosp, bed_type, patient_id, priority,
                     deadline_s + 30, cond_code))
        cur.execute("INSERT INTO transfers_saga (transfer_id, patient_id, from_hosp_id, from_bed_id, to_hosp_id, "
                    "to_bed_id, reservation_id, priority, saga_state, deadline_at, actor_id, idempotency_key, "
                    "request_fingerprint, condition_code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'AWAITING_ACCEPTANCE', "
                    "clock_timestamp() + make_interval(secs => %s), %s,%s,%s,%s) RETURNING *",
                    (tid, patient_id, src["hospital_id"], from_bed, to_hosp, tb["bed_id"], rid, priority, deadline_s,
                     actor, key, fp, cond_code))
        saga = cur.fetchone()
        cur.execute("UPDATE patients SET state='TRANSFERRING', updated_at=NOW() WHERE patient_id=%s", (patient_id,))
        db.add_event(cur, "transfer.requested",
                     {"transfer_id": tid, "from_hospital_id": src["hospital_id"], "from_bed_id": from_bed,
                      "to_hospital_id": to_hosp, "to_bed_id": tb["bed_id"], "bed_type": bed_type,
                      "reservation_id": rid, "deadline_at": saga["deadline_at"].isoformat(),
                      "condition_code": cond_code, "hospital_id": to_hosp, "actor_id": actor},
                     patient_id=patient_id, priority=priority)
        db.bump_epoch(cur)
        return saga, False

    saga, replay = db.run_tx(tx)
    if not replay:
        cache.project_bed_taken(saga["to_bed_id"], saga["to_hosp_id"], bed_type)
    return _render(saga) | {"replay": replay}


def _lock_all(cur, transfer_id):
    cur.execute("SELECT patient_id FROM transfers_saga WHERE transfer_id=%s", (transfer_id,))
    h = cur.fetchone()
    if not h:
        raise ApiError(404, {"error": "NO_SUCH_TRANSFER"})
    _lock_patient(cur, h["patient_id"])
    cur.execute("SELECT *, deadline_at > clock_timestamp() AS before_deadline FROM transfers_saga "
                "WHERE transfer_id=%s FOR UPDATE", (transfer_id,))
    saga = cur.fetchone()
    beds = {}
    for b in sorted([saga["from_bed_id"], saga["to_bed_id"]]):
        cur.execute("SELECT * FROM beds WHERE bed_id=%s FOR UPDATE", (b,))
        beds[b] = cur.fetchone()
    src = beds[saga["from_bed_id"]]
    res_ids = sorted({str(saga["reservation_id"])} | ({str(src["reservation_id"])} if src["reservation_id"] else set()))
    resv = {}
    for r in res_ids:
        cur.execute("SELECT *, expires_at > clock_timestamp() AS live FROM reservations WHERE reservation_id=%s FOR UPDATE",
                    (r,))
        resv[r] = cur.fetchone()
    return saga, beds, resv


def accept(transfer_id, expected_version, actor):
    def tx(cur):
        saga, beds, resv = _lock_all(cur, transfer_id)
        if saga["saga_state"] == "COMPLETED":
            return {"replay": True, "transfer": _render(saga)}
        if saga["saga_state"] != "AWAITING_ACCEPTANCE":
            raise db.Conflict(f"transfer is {saga['saga_state']}", code="NOT_AWAITING", state={"transfer": _render(saga)})
        if expected_version is not None and saga["version"] != expected_version:
            raise db.Conflict("transfer changed", code="VERSION_MISMATCH", state={"transfer": _render(saga)})
        if not saga["before_deadline"]:
            raise db.Conflict("acceptance deadline has passed", code="DEADLINE_PASSED", state={"transfer": _render(saga)})
        tres = resv[str(saga["reservation_id"])]
        src, tgt = beds[saga["from_bed_id"]], beds[saga["to_bed_id"]]
        if tres["status"] != "HELD" or not tres["live"]:
            raise db.Conflict("target hold is not live", code="TARGET_NOT_HELD", state={"transfer": _render(saga)})
        if src["status"] != "OCCUPIED" or src["patient_id"] != saga["patient_id"]:
            raise db.Conflict("source bed no longer holds this patient", code="SOURCE_MISMATCH")
        if tgt["status"] != "RESERVED" or str(tgt["reservation_id"]) != str(saga["reservation_id"]):
            raise db.Conflict("target bed link mismatch", code="LINK_MISMATCH")
        cur.execute("UPDATE beds SET status='OCCUPIED', version=version+1, updated_at=NOW() WHERE bed_id=%s RETURNING *",
                    (tgt["bed_id"],))
        ntgt = cur.fetchone()
        cur.execute("UPDATE reservations SET status='CONFIRMED', updated_at=NOW() WHERE reservation_id=%s",
                    (saga["reservation_id"],))
        if src["reservation_id"]:
            cur.execute("UPDATE reservations SET status='RELEASED', updated_at=NOW() WHERE reservation_id=%s "
                        "AND status='CONFIRMED'", (src["reservation_id"],))
        cur.execute("UPDATE beds SET status='FREE', reservation_id=NULL, patient_id=NULL, version=version+1, "
                    "updated_at=NOW() WHERE bed_id=%s RETURNING *", (src["bed_id"],))
        nsrc = cur.fetchone()
        cur.execute("UPDATE patients SET state='ADMITTED', current_bed_id=%s, current_hosp_id=%s, updated_at=NOW() "
                    "WHERE patient_id=%s", (tgt["bed_id"], tgt["hospital_id"], saga["patient_id"]))
        cur.execute("UPDATE transfers_saga SET saga_state='COMPLETED', version=version+1, updated_at=NOW(), "
                    "outcome_reason='accepted' WHERE transfer_id=%s RETURNING *", (transfer_id,))
        ns = cur.fetchone()
        db.add_event(cur, "transfer.completed",
                     {"transfer_id": str(transfer_id), "from_hospital_id": src["hospital_id"], "hospital_id": tgt["hospital_id"],
                      "from_bed_id": src["bed_id"], "to_bed_id": tgt["bed_id"], "actor_id": actor,
                      "beds": [{"bed_id": b["bed_id"], "hospital_id": b["hospital_id"], "status": b["status"]}
                               for b in (nsrc, ntgt)]},
                     patient_id=saga["patient_id"], priority=saga["priority"])
        db.bump_epoch(cur)
        return {"transfer": _render(ns)}

    out = db.run_tx(tx)
    t = out["transfer"]
    gen = cache.fast_path_generation()
    if gen is not None and not out.get("replay"):
        cache.settle_hold(t["reservation_id"], gen, t["to_bed_id"])
        cache.project_bed_freed(t["from_bed_id"])
    return out


def _compensate(cur, saga, beds, resv, state_reason, actor):
    tres = resv[str(saga["reservation_id"])]
    new_res = "EXPIRED" if not tres["live"] else "RELEASED"
    cur.execute("UPDATE reservations SET status=%s, updated_at=NOW() WHERE reservation_id=%s AND status='HELD'",
                (new_res, saga["reservation_id"]))
    # only the matching RESERVED target bed is freed; never an unrelated or reallocated bed
    cur.execute("UPDATE beds SET status='FREE', reservation_id=NULL, patient_id=NULL, version=version+1, updated_at=NOW() "
                "WHERE bed_id=%s AND status='RESERVED' AND reservation_id=%s RETURNING *",
                (saga["to_bed_id"], saga["reservation_id"]))
    freed = cur.fetchone()
    cur.execute("UPDATE patients SET state='ADMITTED', updated_at=NOW() WHERE patient_id=%s AND state='TRANSFERRING'",
                (saga["patient_id"],))
    cur.execute("UPDATE transfers_saga SET saga_state='REVERSED', version=version+1, updated_at=NOW(), outcome_reason=%s "
                "WHERE transfer_id=%s RETURNING *", (state_reason, saga["transfer_id"]))
    ns = cur.fetchone()
    db.add_event(cur, "transfer.reversed",
                 {"transfer_id": str(saga["transfer_id"]), "reason": state_reason, "hospital_id": saga["to_hosp_id"],
                  "from_hospital_id": saga["from_hosp_id"], "to_bed_id": saga["to_bed_id"], "bed_freed": bool(freed),
                  "target_reservation_status": new_res, "actor_id": actor},
                 patient_id=saga["patient_id"], priority=saga["priority"])
    db.bump_epoch(cur)
    return ns


def reject(transfer_id, expected_version, actor, reason="rejected by receiving hospital"):
    def tx(cur):
        saga, beds, resv = _lock_all(cur, transfer_id)
        if saga["saga_state"] == "REVERSED":
            return {"replay": True, "transfer": _render(saga)}
        if saga["saga_state"] != "AWAITING_ACCEPTANCE":
            raise db.Conflict(f"transfer is {saga['saga_state']}", code="NOT_AWAITING", state={"transfer": _render(saga)})
        if expected_version is not None and saga["version"] != expected_version:
            raise db.Conflict("transfer changed", code="VERSION_MISMATCH", state={"transfer": _render(saga)})
        return {"transfer": _render(_compensate(cur, saga, beds, resv, "rejected: " + reason[:200], actor))}

    out = db.run_tx(tx)
    if not out.get("replay"):
        _after_reverse(out["transfer"])
    return out


def _after_reverse(t):
    gen = cache.fast_path_generation()
    if gen is not None:
        cache.settle_hold(t["reservation_id"], gen, t["to_bed_id"])
        cache.project_bed_freed(t["to_bed_id"])


def _timeout_tx(transfer_id):
    def tx(cur):
        cur.execute("SELECT patient_id FROM transfers_saga WHERE transfer_id=%s", (transfer_id,))
        h = cur.fetchone()
        cur.execute("SELECT 1 FROM patients WHERE patient_id=%s FOR UPDATE SKIP LOCKED", (h["patient_id"],))
        if not cur.fetchone():
            return None
        saga, beds, resv = _lock_all(cur, transfer_id)
        if saga["saga_state"] != "AWAITING_ACCEPTANCE":
            return None
        tres = resv[str(saga["reservation_id"])]
        if saga["before_deadline"] and tres["live"]:
            return None
        return _render(_compensate(cur, saga, beds, resv, "timed out: no response before deadline", "reaper"))
    try:
        t = db.run_tx(tx)
    except Exception as e:  # noqa: BLE001
        log.warn("transfer timeout failed", error=str(e)[:200], correlation_id=str(transfer_id))
        return None
    if t:
        _after_reverse(t)
    return t


def timeout_due(batch=50):
    rows = db.query("SELECT transfer_id FROM transfers_saga WHERE saga_state='AWAITING_ACCEPTANCE' "
                    "AND deadline_at <= clock_timestamp() ORDER BY deadline_at LIMIT %s", (batch,))
    return [t for t in (_timeout_tx(r["transfer_id"]) for r in rows) if t]


def timeout_for_reservation(reservation_id):
    r = db.query_one("SELECT transfer_id FROM transfers_saga WHERE reservation_id=%s AND saga_state='AWAITING_ACCEPTANCE'",
                     (reservation_id,))
    if r:
        return _timeout_tx(r["transfer_id"])
    # orphan TRANSFER hold without an active saga: release it like a dispatch hold
    from .beds import _release_hold

    def tx(cur):
        cur.execute("SELECT patient_id, bed_id FROM reservations WHERE reservation_id=%s", (reservation_id,))
        h = cur.fetchone()
        _lock_patient(cur, h["patient_id"])
        cur.execute("SELECT 1 FROM beds WHERE bed_id=%s FOR UPDATE", (h["bed_id"],))
        cur.execute("SELECT * FROM reservations WHERE reservation_id=%s AND status='HELD' AND expires_at <= clock_timestamp() "
                    "FOR UPDATE", (reservation_id,))
        res = cur.fetchone()
        if res:
            _release_hold(cur, res, "EXPIRED", "bed.reservation.expired", "reaper")
    db.run_tx(tx)
    return None


def incoming(hospital_id):
    rows = db.query("SELECT t.*, extract(epoch FROM t.deadline_at)::float8 AS deadline_epoch, extract(epoch FROM clock_timestamp())::float8 "
                    "AS server_epoch FROM transfers_saga t WHERE (to_hosp_id=%s OR from_hosp_id=%s) "
                    "AND (saga_state='AWAITING_ACCEPTANCE' OR updated_at > NOW() - interval '10 minutes') "
                    "ORDER BY created_at DESC LIMIT 30", (hospital_id, hospital_id))
    return [_render(r) for r in rows]
