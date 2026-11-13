"""Tests run against the live Compose stack (PostgreSQL 16, Redis 7, RabbitMQ), never SQLite.
Run: docker compose run --rm tools python -m pytest -q tests"""
import uuid

import pytest

from app import beds, db, dispatch, smartmatch
from app.seed import DEMO_POINT


def uid(prefix="t"):
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def req(**kw):
    base = {"patient_id": uid("TP"), "ambulance_id": "A-T", "condition_code": "GENERAL_ACUTE", "bed_type": "GENERAL",
            "priority": 2, "lat": DEMO_POINT[0], "lon": DEMO_POINT[1], "idempotency_key": uid("k")}
    base.update(kw)
    return base


def validated(**kw):
    q, cond = dispatch.validate(req(**kw), "dispatcher")
    return q


@pytest.fixture
def cleanup():
    """Collect reservation ids; cancel any still HELD after the test."""
    ids = []
    yield ids
    for rid in ids:
        try:
            beds.cancel(str(rid), "pytest-cleanup")
        except Exception:  # noqa: BLE001
            pass


@pytest.fixture
def free_bed():
    def pick(hospital="H-10", bed_type="GENERAL"):
        r = db.query_one("SELECT bed_id, hospital_id FROM beds WHERE hospital_id=%s AND bed_type=%s AND status='FREE' "
                         "ORDER BY random() LIMIT 1", (hospital, bed_type))
        assert r, "no free bed for the test"
        return r
    return pick


def invariants_ok():
    from app import ops
    inv = ops.invariants()
    return inv["ok"], inv
