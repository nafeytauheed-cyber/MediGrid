"""Redis fencing and hydration, confirmed/mandatory publishing, SSE snapshot and watermark repair, SmartMatch rules."""
import json
import time

import httpx
import pika
import pytest

from app import beds, cache, config, db, dispatch, mq, rds, smartmatch
from app.seed import DEMO_POINT
from tests.conftest import invariants_ok, req


def wait_state(pred, timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st, gen = cache.read_state(force=True)
        if pred(st, gen):
            return st, gen
        time.sleep(0.2)
    raise AssertionError("state not reached")


def test_degraded_closes_fast_path_and_worker_rebuilds_new_generation(cleanup):
    st0, gen0 = wait_state(lambda s, g: s == cache.READY)
    cache.mark_degraded("pytest: simulated outage detection")
    st, _ = cache.read_state(force=True)
    assert st in (cache.DEGRADED, cache.REBUILDING) or cache.read_state(force=True)[1] > gen0
    # while not READY, reservations must take the SQL path
    if cache.read_state(force=True)[0] != cache.READY:
        out, _ = dispatch.reserve("fence-actor", "dispatcher", req())
        cleanup.append(out["reservation_id"])
        assert out["alloc_path"] == "sql"
    st1, gen1 = wait_state(lambda s, g: s == cache.READY and g > gen0)
    r = rds.slow_client()
    assert r.get(rds.POINTER) == str(gen1)
    # hydration: Redis free count equals PostgreSQL free count for a hospital
    pg = db.query_one("SELECT COUNT(*) AS n FROM beds WHERE hospital_id='H-05' AND bed_type='ICU' AND status='FREE'")["n"]
    assert r.scard(rds.k_free(gen1, "H-05", "ICU")) == pg
    # old generation keys are not used by Lua: a stale caller generation gets NOT_READY
    res = rds.script("reserve_nearest_bed")(
        keys=[rds.POINTER, rds.k_rate("stale"), rds.k_idemp("stale", "x"), rds.k_expiry(gen0)],
        args=["ICU", "00000000-0000-0000-0000-000000000000", 1000, 20, 10000, str(gen0), "{}", "H-05"])
    assert res[0] == "NOT_READY"
    assert invariants_ok()[0]


def test_unroutable_publish_is_detected():
    conn = mq.connect()
    try:
        ch = conn.channel()
        ch.confirm_delivery()
        with pytest.raises(mq.PublishFailed):
            mq.publish(ch, "", "q.does-not-exist", b"{}", mq.props("00000000-0000-0000-0000-000000000001", 1))
    finally:
        conn.close()


def test_retry_queues_route_back_to_their_own_service_queue():
    conn = mq.connect()
    try:
        ch = conn.channel()
        q = ch.queue_declare("q.notifications.retry.1s", passive=True)
        assert q.method.queue == "q.notifications.retry.1s"
        info = mq.mgmt_get("/api/queues/%2F/q.notifications.retry.1s")
        args = info["arguments"]
        assert args["x-dead-letter-exchange"] == "" and args["x-dead-letter-routing-key"] == "q.notifications"
        assert args["x-message-ttl"] == 1000
        main = mq.mgmt_get("/api/queues/%2F/q.transfer_saga")
        assert main["arguments"]["x-max-priority"] == 4 and main["durable"] is True
    finally:
        conn.close()


def _read_events(url, seconds):
    events, hb = [], 0
    t0 = time.time()
    with httpx.stream("GET", url, timeout=httpx.Timeout(5.0, read=seconds + 3)) as r:
        ev = None
        for line in r.iter_lines():
            if line.startswith(":"):
                hb += 1
            elif line.startswith("retry:"):
                events.append(("retry", int(line[6:])))
            elif line.startswith("event:"):
                ev = line[6:].strip()
            elif line.startswith("data:"):
                events.append((ev, json.loads(line[5:])))
            if time.time() - t0 > seconds:
                break
    return events, hb, r.headers.get("X-Instance")


def test_sse_snapshot_first_heartbeats_and_jitter():
    events, hb, inst = _read_events(f"{config.NGINX_URL}/api/stream", 4.5)
    kinds = [e[0] for e in events]
    assert kinds[:3] == ["retry", "instance", "snapshot"]
    assert 100 <= events[0][1] <= 600
    snap = events[2][1]
    assert len(snap["hospitals"]) >= 40 and snap["watermark"] is not None
    assert hb >= 1 and inst in ("api-1", "api-2")


def test_change_without_pubsub_reaches_stream_via_watermark(cleanup):
    """Write directly through the domain function (no consumer involved yet) and expect a batch for that hospital.
    The API watermark poller re-reads PostgreSQL, so a lost pub/sub delta cannot leave the UI stale."""
    import threading
    got = {}

    def watch():
        evs, _hb, _i = _read_events(f"{config.API_INTERNAL_URLS[0]}/api/stream", 6)
        got["evs"] = evs
    t = threading.Thread(target=watch)
    t.start()
    time.sleep(1.0)
    out, _ = dispatch.reserve("wm-actor", "dispatcher", req())
    cleanup.append(out["reservation_id"])
    t.join()
    batches = [e[1] for e in got["evs"] if e[0] == "batch"]
    assert any(out["hospital_id"] in b.get("hospitals", {}) for b in batches)


def test_smartmatch_rules():
    rows = smartmatch.sql_search("CARDIAC_EMERGENCY", "ICU", DEMO_POINT[0], DEMO_POINT[1], 50)
    ex = smartmatch.explain("CARDIAC_EMERGENCY", "ICU", rows, 50)
    assert ex["plain_nearest"]["hospital_id"] == "H-01" and not ex["plain_nearest"]["eligible"]
    assert ex["matched"][0]["hospital_id"] == "H-02"
    assert any(e["hospital_id"] == "H-01" and "CARDIAC_EMERGENCY_CARE" in e["reason"] for e in ex["excluded"])
    burns = smartmatch.explain("SEVERE_BURNS", "ICU",
                               smartmatch.sql_search("SEVERE_BURNS", "ICU", DEMO_POINT[0], DEMO_POINT[1], 50), 50)
    assert burns["result"] == "NO_SUITABLE_HOSPITAL_OR_BED" and not burns["matched"]
    _c, err = smartmatch.validate_condition("CARDIAC_EMERGENCY", "VENTILATOR")
    assert err["error"] == "BED_TYPE_NOT_PERMITTED"
    _c, err = smartmatch.validate_condition("NOT_A_CODE", "ICU")
    assert err["error"] == "UNKNOWN_CONDITION"
    assert "H-01" not in smartmatch.eligible_hospitals("CARDIAC_EMERGENCY", max_age=0)
