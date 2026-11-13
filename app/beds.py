"""Bed state transitions other than the initial hold: confirm, cancel, expire, admit, discharge.

Lock order everywhere (DECISIONS C17): patient -> saga -> beds sorted by bed_id -> reservations sorted by id.
Every transition writes its outbox event(s) and bumps pg_epoch in the same transaction."""
import uuid

from psycopg import errors

from . import cache, db, log
from .dispatch import ApiError


def _bed_view(cur, bed_id):
    cur.execute("SELECT bed_id, hospital_id, bed_type, status, patient_id, reservation_id, version FROM beds "
                "WHERE bed_id=%s", (bed_id,))
    b = cur.fetchone()
    if b and b["reservation_id"]:
        b["reservation_id"] = str(b["reservation_id"])
    return b


def _lock_patient(cur, patient_id):
    cur.execute("SELECT * FROM patients WHERE patient_id=%s FOR UPDATE", (patient_id,))
    return cur.fetchone()


def _active_saga(cur, patient_id, lock=True):
    cur.execute("SELECT * FROM transfers_saga WHERE patient_id=%s AND saga_state IN "
                "('TARGET_RESERVED','AWAITING_ACCEPTANCE','REJECTED_COMPENSATING','TIMED_OUT_COMPENSATING')"
                + (" FOR UPDATE" if lock else ""), (patient_id,))
    return cur.fetchone()


def _bed_event(cur, etype, bed, patient_id, priority=2, extra=None):
    data = {"bed_id": bed["bed_id"], "hospital_id": bed["hospital_id"], "bed_type": bed["bed_type"],
            "status": bed["status"], "bed_version": bed["version"]}
    if extra:
        data.update(extra)
    return db.add_event(cur, etype, data, patient_id=patient_id, priority=priority)


# ------------------------------------------------------------------ confirm (ambulance arrived)

def confirm(reservation_id, actor):
    head = db.query_one("SELECT patient_id, bed_id, purpose FROM reservations WHERE reservation_id=%s",
                        (reservation_id,))
    if not head:
        raise ApiError(404, {"error": "NO_SUCH_RESERVATION"})
    if head["purpose"] != "DISPATCH":
        raise ApiError(409, {"error": "TRANSFER_HOLD", "detail": "transfer holds are confirmed by accepting the transfer"})

    def tx(cur):
        _lock_patient(cur, head["patient_id"])
        cur.execute("SELECT * FROM beds WHERE bed_id=%s FOR UPDATE", (head["bed_id"],))
        bed = cur.fetchone()
        cur.execute("SELECT *, expires_at > clock_timestamp() AS live FROM reservations WHERE reservation_id=%s FOR UPDATE",
                    (reservation_id,))
        res = cur.fetchone()
        if res["status"] == "CONFIRMED":
            return {"status": "CONFIRMED", "replay": True, "bed": _bed_view(cur, res["bed_id"])}
        if res["status"] != "HELD" or not res["live"]:
            raise db.Conflict("reservation is no longer held", code="NOT_HELD",
                              state={"reservation_status": res["status"] if res["status"] != "HELD" else "EXPIRING",
                                     "bed": _bed_view(cur, res["bed_id"])})
        if bed["status"] != "RESERVED" or str(bed["reservation_id"]) != str(reservation_id):
            raise db.Conflict("bed is not linked to this reservation", code="LINK_MISMATCH",
                              state={"bed": _bed_view(cur, res["bed_id"])})
        cur.execute("UPDATE beds SET status='OCCUPIED', version=version+1, updated_at=NOW() WHERE bed_id=%s "
                    "AND status='RESERVED' AND reservation_id=%s RETURNING *", (bed["bed_id"], reservation_id))
        nb = cur.fetchone()
        cur.execute("UPDATE reservations SET status='CONFIRMED', updated_at=NOW() WHERE reservation_id=%s "
                    "AND status='HELD'", (reservation_id,))
        cur.execute("UPDATE patients SET state='ADMITTED', current_bed_id=%s, current_hosp_id=%s, updated_at=NOW() "
                    "WHERE patient_id=%s", (nb["bed_id"], nb["hospital_id"], res["patient_id"]))
        db.add_event(cur, "bed.reservation.confirmed", {"reservation_id": str(reservation_id), "bed_id": nb["bed_id"],
                                                        "hospital_id": nb["hospital_id"], "bed_type": nb["bed_type"],
                                                        "actor_id": actor},
                     patient_id=res["patient_id"], priority=res["priority"])
        _bed_event(cur, "bed.admitted", nb, res["patient_id"], res["priority"], {"reservation_id": str(reservation_id)})
        db.bump_epoch(cur)
        return {"status": "CONFIRMED", "bed": _bed_view(cur, nb["bed_id"])}

    out = db.run_tx(tx)
    _settle_redis(reservation_id, head["bed_id"])
    return out


def _settle_redis(reservation_id, bed_id):
    """Post-commit: settle any Redis provisional hold for this reservation in the active generation."""
    gen = cache.fast_path_generation()
    if gen is not None:
        cache.settle_hold(str(reservation_id), gen, bed_id)
        cache.project_bed_freed(bed_id)


# ------------------------------------------------------------------ cancel / expire

def _release_hold(cur, res, new_status, etype, actor):
    """Shared by cancel and expiry. Caller holds patient, bed and reservation locks."""
    cur.execute("UPDATE reservations SET status=%s, updated_at=NOW() WHERE reservation_id=%s AND status='HELD'",
                (new_status, res["reservation_id"]))
    # free only the matching RESERVED bed: never an OCCUPIED bed or one owned by another reservation
    cur.execute("UPDATE beds SET status='FREE', reservation_id=NULL, patient_id=NULL, version=version+1, updated_at=NOW() "
                "WHERE bed_id=%s AND status='RESERVED' AND reservation_id=%s RETURNING *",
                (res["bed_id"], res["reservation_id"]))
    nb = cur.fetchone()
    db.add_event(cur, etype, {"reservation_id": str(res["reservation_id"]), "bed_id": res["bed_id"],
                              "hospital_id": res["hospital_id"], "bed_type": res["bed_type"], "purpose": res["purpose"],
                              "bed_freed": bool(nb), "actor_id": actor},
                 patient_id=res["patient_id"], priority=res["priority"])
    db.bump_epoch(cur)
    return nb


def cancel(reservation_id, actor):
    head = db.query_one("SELECT patient_id, bed_id, purpose FROM reservations WHERE reservation_id=%s", (reservation_id,))
    if not head:
        raise ApiError(404, {"error": "NO_SUCH_RESERVATION"})
    if head["purpose"] != "DISPATCH":
        raise ApiError(409, {"error": "TRANSFER_HOLD", "detail": "reject the transfer instead"})

    def tx(cur):
        _lock_patient(cur, head["patient_id"])
        cur.execute("SELECT * FROM beds WHERE bed_id=%s FOR UPDATE", (head["bed_id"],))
        cur.execute("SELECT * FROM reservations WHERE reservation_id=%s FOR UPDATE", (reservation_id,))
        res = cur.fetchone()
        if res["status"] == "RELEASED":
            return {"status": "RELEASED", "replay": True}
        if res["status"] != "HELD":
            raise db.Conflict("reservation is not held", code="NOT_HELD", state={"reservation_status": res["status"]})
        _release_hold(cur, res, "RELEASED", "bed.reservation.cancelled", actor)
        return {"status": "RELEASED", "bed": _bed_view(cur, res["bed_id"])}

    out = db.run_tx(tx)
    _settle_redis(reservation_id, head["bed_id"])
    return out


def expire_due(batch=50):
    """Authoritative expiry. Candidates are found without locks; each is then locked in the standard order and
    re-checked against database time. Returns the list of expired reservation ids."""
    cands = db.query("SELECT reservation_id, patient_id, bed_id, purpose FROM reservations WHERE status='HELD' "
                     "AND expires_at <= clock_timestamp() ORDER BY expires_at LIMIT %s", (batch,))
    done = []
    for c in cands:
        if c["purpose"] == "TRANSFER":
            from . import transfers
            transfers.timeout_for_reservation(c["reservation_id"])
            continue

        def tx(cur, c=c):
            cur.execute("SELECT 1 FROM patients WHERE patient_id=%s FOR UPDATE SKIP LOCKED", (c["patient_id"],))
            if not cur.fetchone():
                return None  # another transition holds the patient; next tick
            cur.execute("SELECT 1 FROM beds WHERE bed_id=%s FOR UPDATE", (c["bed_id"],))
            cur.execute("SELECT * FROM reservations WHERE reservation_id=%s AND status='HELD' "
                        "AND expires_at <= clock_timestamp() FOR UPDATE", (c["reservation_id"],))
            res = cur.fetchone()
            if not res:
                return None
            nb = _release_hold(cur, res, "EXPIRED", "bed.reservation.expired", "reaper")
            return {"reservation_id": str(res["reservation_id"]), "bed_freed": bool(nb)}
        try:
            r = db.run_tx(tx)
        except Exception as e:  # noqa: BLE001
            log.warn("expiry failed", error=str(e)[:200], correlation_id=str(c["reservation_id"]))
            continue
        if r:
            done.append(r)
            _settle_redis(c["reservation_id"], c["bed_id"])
    return done


# ------------------------------------------------------------------ admit (walk-in) / discharge

def admit(bed_id, patient_id, expected_version, actor, idempotency_key):
    if not patient_id or len(patient_id) > 48:
        raise ApiError(400, {"error": "BAD_PATIENT_ID"})

    def tx(cur):
        cur.execute("INSERT INTO patients (patient_id, state) VALUES (%s, 'IN_TRANSIT') ON CONFLICT DO NOTHING",
                    (patient_id,))
        p = _lock_patient(cur, patient_id)
        cur.execute("SELECT * FROM beds WHERE bed_id=%s FOR UPDATE", (bed_id,))
        bed = cur.fetchone()
        if not bed:
            raise ApiError(404, {"error": "NO_SUCH_BED"})
        cur.execute("SELECT reservation_id FROM reservations WHERE actor_id=%s AND idempotency_key=%s",
                    (actor, idempotency_key))
        if cur.fetchone():
            return {"replay": True, "bed": _bed_view(cur, bed_id)}
        if p["state"] in ("ADMITTED", "TRANSFERRING"):
            raise db.Conflict(f"patient is already {p['state']}", code="PATIENT_NOT_ELIGIBLE",
                              state={"bed": _bed_view(cur, bed_id)})
        cur.execute("SELECT 1 FROM reservations WHERE patient_id=%s AND status='HELD' AND purpose='DISPATCH'",
                    (patient_id,))
        if cur.fetchone():
            raise db.Conflict("patient has an active dispatch hold; confirm it instead", code="PATIENT_HAS_HOLD",
                              state={"bed": _bed_view(cur, bed_id)})
        if bed["version"] != expected_version or bed["status"] != "FREE":
            raise db.Conflict("bed changed", code="VERSION_MISMATCH", state={"bed": _bed_view(cur, bed_id)})
        rid = str(uuid.uuid4())
        cur.execute("UPDATE beds SET status='OCCUPIED', reservation_id=%s, patient_id=%s, version=version+1, "
                    "updated_at=NOW() WHERE bed_id=%s AND version=%s AND status='FREE' RETURNING *",
                    (rid, patient_id, bed_id, expected_version))
        nb = cur.fetchone()
        cur.execute("INSERT INTO reservations (reservation_id, actor_id, idempotency_key, request_fingerprint, bed_id, "
                    "hospital_id, bed_type, ambulance_id, patient_id, priority, status, expires_at, purpose, alloc_path) "
                    "VALUES (%s,%s,%s,'walk-in',%s,%s,%s,'WALK-IN',%s,2,'CONFIRMED',clock_timestamp(),'DISPATCH','console')",
                    (rid, actor, idempotency_key, bed_id, nb["hospital_id"], nb["bed_type"], patient_id))
        cur.execute("UPDATE patients SET state='ADMITTED', current_bed_id=%s, current_hosp_id=%s, updated_at=NOW() "
                    "WHERE patient_id=%s", (bed_id, nb["hospital_id"], patient_id))
        _bed_event(cur, "bed.admitted", nb, patient_id, 2, {"reservation_id": rid, "walk_in": True, "actor_id": actor})
        db.bump_epoch(cur)
        return {"bed": _bed_view(cur, bed_id)}

    try:
        out = db.run_tx(tx)
    except errors.UniqueViolation:
        raise db.Conflict("bed already has an active reservation", code="BED_TAKEN",
                          state={"bed": db.query_one("SELECT * FROM beds WHERE bed_id=%s", (bed_id,))})
    b = out["bed"]
    cache.project_bed_taken(b["bed_id"], b["hospital_id"], b["bed_type"])
    return out


def discharge(bed_id, expected_version, actor):
    head = db.query_one("SELECT patient_id FROM beds WHERE bed_id=%s", (bed_id,))
    if not head:
        raise ApiError(404, {"error": "NO_SUCH_BED"})
    if not head["patient_id"]:
        raise db.Conflict("bed has no patient", code="VERSION_MISMATCH",
                          state={"bed": db.query_one("SELECT * FROM beds WHERE bed_id=%s", (bed_id,))})

    def tx(cur):
        _lock_patient(cur, head["patient_id"])
        saga = _active_saga(cur, head["patient_id"])
        cur.execute("SELECT * FROM beds WHERE bed_id=%s FOR UPDATE", (bed_id,))
        bed = cur.fetchone()
        if saga:
            raise db.Conflict("transfer in progress for this patient", code="TRANSFER_IN_PROGRESS",
                              state={"bed": _bed_view(cur, bed_id), "transfer_id": str(saga["transfer_id"])})
        if bed["version"] != expected_version or bed["status"] != "OCCUPIED" or bed["patient_id"] != head["patient_id"]:
            raise db.Conflict("bed changed", code="VERSION_MISMATCH", state={"bed": _bed_view(cur, bed_id)})
        cur.execute("SELECT * FROM reservations WHERE reservation_id=%s FOR UPDATE", (bed["reservation_id"],))
        cur.execute("UPDATE reservations SET status='RELEASED', updated_at=NOW() WHERE reservation_id=%s "
                    "AND status='CONFIRMED'", (bed["reservation_id"],))
        cur.execute("UPDATE beds SET status='FREE', reservation_id=NULL, patient_id=NULL, version=version+1, "
                    "updated_at=NOW() WHERE bed_id=%s AND version=%s RETURNING *", (bed_id, expected_version))
        nb = cur.fetchone()
        cur.execute("UPDATE patients SET state='DISCHARGED', current_bed_id=NULL, current_hosp_id=NULL, updated_at=NOW() "
                    "WHERE patient_id=%s", (head["patient_id"],))
        _bed_event(cur, "bed.discharged", nb, head["patient_id"], 1, {"actor_id": actor})
        db.bump_epoch(cur)
        return {"bed": _bed_view(cur, bed_id)}

    out = db.run_tx(tx)
    cache.project_bed_freed(bed_id)
    return out
