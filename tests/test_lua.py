"""Lua reserve/release/reconcile scripts against real Redis, in an isolated test namespace."""
import json
import uuid

import pytest

from app import rds

GEN = "990001"
PTR = "test:cache_ready_generation"


@pytest.fixture
def r():
    c = rds.slow_client()
    for k in c.scan_iter(match=f"mg:{GEN}:*"):
        c.delete(k)
    c.delete(PTR, "ratelimit:lua-actor", "ratelimit:lua-rl")
    c.set(PTR, GEN)
    c.sadd(f"mg:{GEN}:hosp:HT1:beds:ICU:free", "HT1-ICU-1")
    c.sadd(f"mg:{GEN}:hosp:HT2:beds:ICU:free", "HT2-ICU-1")
    yield c
    for k in c.scan_iter(match=f"mg:{GEN}:*"):
        c.delete(k)
    for k in c.scan_iter(match="idemp:res:lua-*"):
        c.delete(k)
    c.delete(PTR, "ratelimit:lua-actor", "ratelimit:lua-rl")


def reserve(actor, key, cands, gen=GEN, rate_max=20, bt="ICU"):
    return rds.script("reserve_nearest_bed")(
        keys=[PTR, rds.k_rate(actor), rds.k_idemp(actor, key), rds.k_expiry(GEN)],
        args=[bt, str(uuid.uuid4()), 60000, rate_max, 10000, gen, "{}"] + cands)


def release(rid, mode):
    return rds.script("release_reservation")(keys=[rds.k_expiry(GEN)], args=[rid, mode, GEN])


def test_reserve_pops_nearest_candidate_first(r):
    res = reserve("lua-actor", "k1", ["HT1", "HT2"])
    assert res[0] == "RESERVED"
    rec = json.loads(res[1])
    assert rec["bed_id"] == "HT1-ICU-1"
    assert r.sismember(f"mg:{GEN}:held_beds", "HT1-ICU-1")
    assert r.scard(f"mg:{GEN}:hosp:HT1:beds:ICU:free") == 0


def test_idempotent_hit_returns_same_record(r):
    first = reserve("lua-actor", "k2", ["HT1", "HT2"])
    second = reserve("lua-actor", "k2", ["HT1", "HT2"])
    assert second[0] == "IDEMPOTENT_HIT"
    assert json.loads(second[1])["bed_id"] == json.loads(first[1])["bed_id"]
    assert r.scard(f"mg:{GEN}:hosp:HT2:beds:ICU:free") == 1  # second call did not pop another bed


def test_rate_limit(r):
    codes = [reserve("lua-rl", f"rk{i}", ["HT9"], rate_max=2)[0] for i in range(3)]
    assert codes == ["NO_SUITABLE_BED", "NO_SUITABLE_BED", "RATE_LIMITED"]


def test_not_ready_on_generation_mismatch(r):
    assert reserve("lua-actor", "k3", ["HT1"], gen="990002")[0] == "NOT_READY"
    assert r.scard(f"mg:{GEN}:hosp:HT1:beds:ICU:free") == 1


def test_release_and_consume_modes(r):
    a = reserve("lua-actor", "k4", ["HT1"])
    b = reserve("lua-actor", "k5", ["HT2"])
    ra, rb = json.loads(a[1])["reservation_id"], json.loads(b[1])["reservation_id"]
    assert release(ra, "release")[0] == "RELEASED"
    assert r.sismember(f"mg:{GEN}:hosp:HT1:beds:ICU:free", "HT1-ICU-1")
    assert release(rb, "consume")[0] == "RELEASED"
    assert not r.sismember(f"mg:{GEN}:hosp:HT2:beds:ICU:free", "HT2-ICU-1")
    assert not r.sismember(f"mg:{GEN}:held_beds", "HT2-ICU-1")
    assert release(ra, "release")[0] == "ALREADY_SETTLED"


def test_bad_input_rejected(r):
    assert reserve("lua-actor", "k6", ["HT1"], bt="SOFA")[0] == "BAD_INPUT"


def test_reconcile_skips_held_beds(r):
    a = reserve("lua-actor", "k7", ["HT1"])
    assert a[0] == "RESERVED"
    # PostgreSQL says HT1-ICU-1 (held) and HT1-ICU-2 are FREE; the held bed must not be re-added
    out = rds.script("reconcile_free")(keys=[PTR], args=[GEN, "1", "HT1", "ICU", "HT1-ICU-1", "HT1-ICU-2"])
    assert out[0] == "OK"
    assert r.smembers(f"mg:{GEN}:hosp:HT1:beds:ICU:free") == {"HT1-ICU-2"}
