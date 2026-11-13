"""Per-consumer-group ordering cursors and inbox deduplication."""
import uuid

import pytest

from app import db
from app.worker.consumers import Handler, Postpone, process
from tests.conftest import uid


class Recorder(Handler):
    def __init__(self, group):
        self.group = group
        self.applied = []

    def apply(self, cur, evt):
        self.applied.append(evt["patient_seq_no"])


def evt(pid, seq, etype="bed.sim.ordering"):
    return {"event_id": str(uuid.uuid4()), "type": etype, "patient_id": pid, "patient_seq_no": seq, "data": {}}


@pytest.fixture
def patient():
    pid = uid("ORD")
    db.query("INSERT INTO patients (patient_id, state, last_seq_no) VALUES (%s, 'IN_TRANSIT', 3)", (pid,))
    return pid


def test_out_of_order_is_postponed_then_applied_in_order(patient):
    h = Recorder(f"tg-{uuid.uuid4().hex[:6]}")
    e1, e2, e3 = evt(patient, 1), evt(patient, 2), evt(patient, 3)
    with pytest.raises(Postpone):
        process(h, e3)
    with pytest.raises(Postpone):
        process(h, e2)
    assert process(h, e1) == "APPLIED"
    assert process(h, e2) == "APPLIED"
    assert process(h, e3) == "APPLIED"
    assert h.applied == [1, 2, 3]
    # a postponed event left no inbox row behind, the applied ones did
    n = db.query_one("SELECT COUNT(*) AS n FROM inbox_processed_events WHERE consumer_group=%s", (h.group,))["n"]
    assert n == 3


def test_duplicate_delivery_is_skipped(patient):
    h = Recorder(f"tg-{uuid.uuid4().hex[:6]}")
    e1 = evt(patient, 1)
    assert process(h, e1) == "APPLIED"
    assert process(h, dict(e1)) == "DUPLICATE"
    assert h.applied == [1]


def test_groups_have_independent_cursors(patient):
    a, b = Recorder(f"ta-{uuid.uuid4().hex[:6]}"), Recorder(f"tb-{uuid.uuid4().hex[:6]}")
    e1, e2 = evt(patient, 1), evt(patient, 2)
    assert process(a, e1) == "APPLIED"
    assert process(a, e2) == "APPLIED"
    # group b has seen nothing: seq 2 must wait for seq 1 in b even though a is at 2
    with pytest.raises(Postpone):
        process(b, e2)
    assert process(b, e1) == "APPLIED"
    assert process(b, e2) == "APPLIED"
    rows = db.query("SELECT consumer_group, last_applied_seq FROM consumer_patient_offsets WHERE patient_id=%s", (patient,))
    assert {r["consumer_group"]: r["last_applied_seq"] for r in rows} == {a.group: 2, b.group: 2}


def test_noop_event_types_still_advance_the_cursor(patient):
    # Handler.apply is a no-op by default: the cursor must still move so later events are not stuck
    h = Handler()
    h.group = f"tn-{uuid.uuid4().hex[:6]}"
    assert process(h, evt(patient, 1, "bed.admitted")) == "APPLIED"
    assert process(h, evt(patient, 2, "bed.discharged")) == "APPLIED"
    r = db.query_one("SELECT last_applied_seq FROM consumer_patient_offsets WHERE consumer_group=%s AND patient_id=%s",
                     (h.group, patient))
    assert r["last_applied_seq"] == 2
