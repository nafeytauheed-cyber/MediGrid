"""PostgreSQL is the arbiter: last-bed stampede, HELD+CONFIRMED uniqueness, idempotency, ambiguous commit."""
import threading
import time
import uuid

import pytest
from psycopg import errors

from app import db, dispatch, faults
from tests.conftest import invariants_ok, req, uid, validated


def test_stampede_on_one_bed_has_one_winner(free_bed, cleanup):
    bed = free_bed("H-10", "GENERAL")
    n = 30
    results = [None] * n
    bar = threading.Barrier(n)

    def go(i):
        q = validated()
        bar.wait()
        try:
            row = dispatch._commit(f"stamp-{i}", q, uuid.uuid4(), bed_id=bed["bed_id"], path="redis")
            results[i] = ("OK", row["reservation_id"])
        except dispatch.BedTaken:
            results[i] = ("TAKEN", None)
    ts = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    wins = [r for r in results if r[0] == "OK"]
    cleanup.extend(r[1] for r in wins)
    assert len(wins) == 1
    assert sum(1 for r in results if r[0] == "TAKEN") == n - 1
    n_active = db.query_one("SELECT COUNT(*) AS n FROM reservations WHERE bed_id=%s AND status IN ('HELD','CONFIRMED')",
                            (bed["bed_id"],))["n"]
    assert n_active == 1
    assert invariants_ok()[0]


def test_unique_index_covers_held_and_confirmed():
    occ = db.query_one("SELECT bed_id, hospital_id, bed_type FROM beds WHERE status='OCCUPIED' LIMIT 1")
    with pytest.raises(errors.UniqueViolation):
        db.query("INSERT INTO reservations (reservation_id, actor_id, idempotency_key, request_fingerprint, bed_id, "
                 "hospital_id, bed_type, ambulance_id, patient_id, status, expires_at) VALUES "
                 "(%s,'pytest',%s,'x',%s,%s,%s,'A','P','HELD',NOW())",
                 (str(uuid.uuid4()), uid("u"), occ["bed_id"], occ["hospital_id"], occ["bed_type"]))


def test_concurrent_duplicate_requests_create_one_reservation(cleanup):
    body = req()
    n = 10
    out = [None] * n
    bar = threading.Barrier(n)

    def go(i):
        bar.wait()
        try:
            out[i] = dispatch.reserve("idem-actor", "dispatcher", dict(body))
        except dispatch.ApiError as e:
            out[i] = (e.body, e.status)
    ts = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    ids = {o[0].get("reservation_id") for o in out}
    cleanup.extend(i for i in ids if i)
    assert len(ids) == 1 and None not in ids
    assert sorted(o[1] for o in out).count(201) == 1
    cnt = db.query_one("SELECT COUNT(*) AS n FROM reservations WHERE actor_id='idem-actor' AND idempotency_key=%s",
                       (body["idempotency_key"],))["n"]
    assert cnt == 1


def test_same_key_different_payload_is_409(cleanup):
    body = req()
    first, st = dispatch.reserve("fp-actor", "dispatcher", body)
    cleanup.append(first["reservation_id"])
    assert st == 201
    with pytest.raises(dispatch.ApiError) as e:
        dispatch.reserve("fp-actor", "dispatcher", dict(body, priority=4))
    assert e.value.status == 409 and e.value.body["error"] == "IDEMPOTENCY_KEY_REUSED"


def test_ambiguous_commit_is_reconciled_by_key(cleanup):
    faults.set_flag("fault:ambiguous_commit", "1")
    try:
        time.sleep(0.6)
        body = req()
        out, st = dispatch.reserve("amb-actor", "dispatcher", body)
    finally:
        faults.clear("fault:ambiguous_commit")
    cleanup.append(out["reservation_id"])
    assert st == 201 and out.get("reconciled_after_unknown_commit")
    cnt = db.query_one("SELECT COUNT(*) AS n FROM reservations WHERE actor_id='amb-actor' AND idempotency_key=%s",
                       (body["idempotency_key"],))["n"]
    assert cnt == 1


def test_patient_cannot_hold_two_dispatch_beds(cleanup):
    pid = uid("TP")
    a, _ = dispatch.reserve("two-actor", "dispatcher", req(patient_id=pid))
    cleanup.append(a["reservation_id"])
    with pytest.raises(db.Conflict) as e:
        dispatch.reserve("two-actor", "dispatcher", req(patient_id=pid))
    assert e.value.code == "PATIENT_HAS_HOLD"
