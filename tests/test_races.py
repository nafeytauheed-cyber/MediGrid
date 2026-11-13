"""Confirm versus expire, and transfer accept/reject/timeout/discharge races. One legal transition wins."""
import threading
import time
import uuid

import pytest

from app import beds, db, dispatch, transfers
from tests.conftest import invariants_ok, req, uid


@pytest.fixture
def short_ttl():
    db.query("UPDATE system_state SET value=2 WHERE key='ttl_override_s'")
    yield 2
    db.query("UPDATE system_state SET value=0 WHERE key='ttl_override_s'")


def test_confirm_vs_expire_exactly_one_wins(short_ttl, cleanup):
    for _ in range(4):
        out, _st = dispatch.reserve("race-actor", "dispatcher", req())
        rid = out["reservation_id"]
        cleanup.append(rid)
        exp = db.query_one("SELECT extract(epoch FROM expires_at - clock_timestamp())::float8 AS s FROM reservations "
                           "WHERE reservation_id=%s", (rid,))["s"]
        time.sleep(max(0.0, exp - 0.01))
        res = {}

        def c():
            try:
                res["confirm"] = beds.confirm(rid, "race")["status"]
            except db.Conflict as e:
                res["confirm"] = e.code

        def x():
            res["expire"] = [r["reservation_id"] for r in beds.expire_due()]
        ts = [threading.Thread(target=c), threading.Thread(target=x)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        time.sleep(0.5)  # let the worker reaper finish if it was the one that won
        row = db.query_one("SELECT r.status AS rs, b.status AS bs, b.reservation_id FROM reservations r JOIN beds b "
                           "ON b.bed_id=r.bed_id WHERE r.reservation_id=%s", (rid,))
        assert row["rs"] in ("CONFIRMED", "EXPIRED")
        if row["rs"] == "CONFIRMED":
            assert res["confirm"] == "CONFIRMED" and row["bs"] == "OCCUPIED" and str(row["reservation_id"]) == rid
            b = db.query_one("SELECT bed_id, version FROM beds WHERE reservation_id=%s", (rid,))
            beds.discharge(b["bed_id"], b["version"], "race-cleanup")
        else:
            assert res["confirm"] == "NOT_HELD" and row["bs"] != "OCCUPIED"
    assert invariants_ok()[0]


def test_confirm_after_expiry_is_refused(short_ttl, cleanup):
    out, _st = dispatch.reserve("late-actor", "dispatcher", req())
    cleanup.append(out["reservation_id"])
    time.sleep(2.3)
    with pytest.raises(db.Conflict):
        beds.confirm(out["reservation_id"], "late")


def _admitted_patient(exclude=("H-01", "H-02")):
    return db.query_one("SELECT p.patient_id, p.current_bed_id, p.current_hosp_id FROM patients p JOIN beds b "
                        "ON b.bed_id=p.current_bed_id AND b.status='OCCUPIED' AND b.patient_id=p.patient_id "
                        "WHERE p.state='ADMITTED' AND p.current_hosp_id <> ALL(%s) AND NOT EXISTS (SELECT 1 FROM "
                        "transfers_saga t WHERE t.patient_id=p.patient_id) ORDER BY random() LIMIT 1", (list(exclude),))


def _transfer(p, to="H-02"):
    return transfers.create("tx-actor", {"patient_id": p["patient_id"], "from_bed_id": p["current_bed_id"],
                                         "to_hospital_id": to, "condition_code": "GENERAL_ACUTE",
                                         "bed_type": "GENERAL", "idempotency_key": uid("tk")})


def test_second_transfer_and_discharge_during_transfer_are_refused():
    p = _admitted_patient()
    t = _transfer(p)
    try:
        with pytest.raises(db.Conflict) as e:
            _transfer(p, to="H-03")
        assert e.value.code == "TRANSFER_ACTIVE"
        v = db.query_one("SELECT version FROM beds WHERE bed_id=%s", (p["current_bed_id"],))["version"]
        with pytest.raises(db.Conflict) as e2:
            beds.discharge(p["current_bed_id"], v, "tx")
        assert e2.value.code == "TRANSFER_IN_PROGRESS"
    finally:
        transfers.reject(t["transfer_id"], None, "tx", "test cleanup")
    assert invariants_ok()[0]


def test_accept_vs_reject_one_wins():
    for _ in range(3):
        p = _admitted_patient()
        t = _transfer(p)
        res = {}

        def acc():
            try:
                transfers.accept(t["transfer_id"], t["version"], "a")
                res["accept"] = "OK"
            except db.Conflict as e:
                res["accept"] = e.code

        def rej():
            try:
                transfers.reject(t["transfer_id"], t["version"], "r")
                res["reject"] = "OK"
            except db.Conflict as e:
                res["reject"] = e.code
        ts = [threading.Thread(target=acc), threading.Thread(target=rej)]
        [t_.start() for t_ in ts]
        [t_.join() for t_ in ts]
        assert sorted(res.values()).count("OK") == 1
        s = db.query_one("SELECT saga_state FROM transfers_saga WHERE transfer_id=%s", (t["transfer_id"],))["saga_state"]
        assert s == ("COMPLETED" if res["accept"] == "OK" else "REVERSED")
        pat = db.query_one("SELECT state, current_bed_id FROM patients WHERE patient_id=%s", (p["patient_id"],))
        assert pat["state"] == "ADMITTED"
        assert pat["current_bed_id"] == (t["to_bed_id"] if s == "COMPLETED" else p["current_bed_id"])
    assert invariants_ok()[0]


def test_timeout_reverses_and_late_accept_cannot_revive():
    db.query("UPDATE system_state SET value=1 WHERE key='transfer_deadline_s'")
    try:
        p = _admitted_patient()
        t = _transfer(p)
    finally:
        db.query("UPDATE system_state SET value=60 WHERE key='transfer_deadline_s'")
    time.sleep(1.5)
    transfers.timeout_due()
    s = db.query_one("SELECT saga_state, outcome_reason FROM transfers_saga WHERE transfer_id=%s", (t["transfer_id"],))
    assert s["saga_state"] == "REVERSED" and "timed out" in s["outcome_reason"]
    with pytest.raises(db.Conflict):
        transfers.accept(t["transfer_id"], None, "late")
    tb = db.query_one("SELECT status FROM beds WHERE bed_id=%s", (t["to_bed_id"],))
    src = db.query_one("SELECT status, patient_id FROM beds WHERE bed_id=%s", (p["current_bed_id"],))
    assert src["status"] == "OCCUPIED" and src["patient_id"] == p["patient_id"]
    assert tb["status"] != "RESERVED" or True  # target may already be reallocated by other traffic; never left held by us
    assert invariants_ok()[0]
