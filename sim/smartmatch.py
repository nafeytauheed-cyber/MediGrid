"""SmartMatch demonstration and assertions (plan 6.1). Run: docker compose run --rm tools python -m sim.smartmatch
Checks: farther capable H-02 beats nearer incapable H-01; explicit no-match; bed type rejection; destination
capability check; identical choice on the Redis path and the forced SQL-only path; 50 concurrent cardiac requests
never book an incapable hospital and never double-book."""
import sys
import threading
import time

from app import chaos, db, faults, ops
from app.seed import DEMO_POINT
from sim.common import report


def main():
    tag = f"sm{int(time.time()) % 100000}"
    c = chaos.client(chaos.bases()[0], "dispatcher", f"{tag}-d")
    lat, lon = DEMO_POINT
    q = {"lat": lat, "lon": lon, "condition_code": "CARDIAC_EMERGENCY", "bed_type": "ICU"}
    res, created = {}, []
    s = c.get("/api/dispatch/search", params=q).json()
    res["search_redis"] = {"path": s["path"], "first_match": s["matched"][0]["hospital_id"] if s["matched"] else None,
                           "plain_nearest": s["plain_nearest"], "h01_excluded_reason":
                           next((x["reason"] for x in s["excluded"] if x["hospital_id"] == "H-01"), None)}
    faults.set_flag("fault:force_sql", "1")
    time.sleep(1.2)
    s2 = c.get("/api/dispatch/search", params=q).json()
    r_sql = c.post("/api/dispatch/reserve", json=dict(q, patient_id=f"{tag}-sql", ambulance_id="A", priority=4,
                                                      idempotency_key=f"{tag}-sql"))
    faults.clear("fault:force_sql")
    time.sleep(1.2)
    r_redis = c.post("/api/dispatch/reserve", json=dict(q, patient_id=f"{tag}-rds", ambulance_id="A", priority=4,
                                                        idempotency_key=f"{tag}-rds"))
    for r in (r_sql, r_redis):
        if r.status_code == 201:
            created.append(r.json()["reservation_id"])
    res["search_sql"] = {"path": s2["path"], "first_match": s2["matched"][0]["hospital_id"] if s2["matched"] else None}
    res["reserve_sql"] = {k: r_sql.json().get(k) for k in ("alloc_path", "hospital_id", "bed_id")}
    res["reserve_redis"] = {k: r_redis.json().get(k) for k in ("alloc_path", "hospital_id", "bed_id")}
    nm = c.post("/api/dispatch/reserve", json={"lat": lat, "lon": lon, "condition_code": "SEVERE_BURNS", "bed_type": "ICU",
                                               "patient_id": f"{tag}-burn", "ambulance_id": "A", "priority": 4,
                                               "idempotency_key": f"{tag}-burn"})
    res["no_match"] = {"status": nm.status_code, "result": nm.json().get("result"), "explanation": nm.json().get("explanation")}
    bt = c.post("/api/dispatch/reserve", json={"lat": lat, "lon": lon, "condition_code": "CARDIAC_EMERGENCY",
                                               "bed_type": "VENTILATOR", "patient_id": f"{tag}-bt", "ambulance_id": "A",
                                               "priority": 4, "idempotency_key": f"{tag}-bt"})
    res["bed_type_rejected"] = {"status": bt.status_code, "body": bt.json()}
    unk = c.get("/api/dispatch/search", params=dict(q, condition_code="MADE_UP"))
    res["unknown_condition"] = {"status": unk.status_code, "error": unk.json().get("error")}
    # destination capability check for transfers: an admitted seed patient at some hospital, destination H-01 (no cardiac)
    p = db.query_one("SELECT p.patient_id, p.current_bed_id FROM patients p JOIN beds b ON b.bed_id=p.current_bed_id "
                     "WHERE p.state='ADMITTED' AND p.current_hosp_id <> 'H-01' AND b.status='OCCUPIED' LIMIT 1")
    op = chaos.client(chaos.bases()[0], "operator", f"{tag}-op")
    tr = op.post("/api/transfers", json={"patient_id": p["patient_id"], "from_bed_id": p["current_bed_id"],
                                         "to_hospital_id": "H-01", "condition_code": "CARDIAC_EMERGENCY", "bed_type": "ICU",
                                         "idempotency_key": f"{tag}-tr"})
    res["transfer_to_incapable"] = {"status": tr.status_code, "body": tr.json()}
    # 50 concurrent cardiac requests
    n = 50
    out = [None] * n
    bar = threading.Barrier(n)
    cl = [chaos.client(chaos.bases()[i % len(chaos.bases())], "dispatcher", f"{tag}-c{i}") for i in range(n)]

    def go(i):
        bar.wait()
        r = cl[i].post("/api/dispatch/reserve", json=dict(q, patient_id=f"{tag}-c{i}", ambulance_id=f"A{i}", priority=4,
                                                          idempotency_key=f"{tag}-c{i}"))
        out[i] = (r.status_code, r.json())
    ts = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    ok = [o[1] for o in out if o[0] == 201]
    created += [o["reservation_id"] for o in ok]
    capable = {r["hospital_id"] for r in db.query(
        "SELECT hospital_id FROM hospital_capabilities WHERE capability_code='CARDIAC_EMERGENCY_CARE' AND is_active")}
    hosp = {}
    for o in ok:
        hosp[o["hospital_id"]] = hosp.get(o["hospital_id"], 0) + 1
    res["concurrent_50"] = {"created": len(ok), "by_hospital": hosp, "beds_distinct": len({o["bed_id"] for o in ok}),
                            "other_statuses": [o[0] for o in out if o[0] != 201]}
    inv = ops.invariants()
    for rid in created:
        try:
            from app import beds
            beds.cancel(rid, "smartmatch-cleanup")
        except Exception:  # noqa: BLE001
            pass
    res["checks"] = {
        "redis_search_picks_H02_not_H01": res["search_redis"]["first_match"] == "H-02" and res["search_redis"]["plain_nearest"]["hospital_id"] == "H-01",
        "sql_search_identical": res["search_sql"]["path"] == "sql" and res["search_sql"]["first_match"] == "H-02",
        "sql_reserve_H02": res["reserve_sql"]["alloc_path"] == "sql" and res["reserve_sql"]["hospital_id"] == "H-02",
        "redis_reserve_H02": res["reserve_redis"]["alloc_path"] == "redis" and res["reserve_redis"]["hospital_id"] == "H-02",
        "no_match_explicit": nm.status_code == 404 and res["no_match"]["result"] == "NO_SUITABLE_HOSPITAL_OR_BED",
        "incompatible_bed_type_400": bt.status_code == 400 and bt.json().get("error") == "BED_TYPE_NOT_PERMITTED",
        "unknown_condition_400": unk.status_code == 400,
        "transfer_destination_capability_checked": tr.status_code == 422,
        "concurrent_only_capable_hospitals": set(hosp) <= capable and "H-01" not in hosp,
        "concurrent_no_double_booking": len({o["bed_id"] for o in ok}) == len(ok) and inv["ok"],
    }
    res["pass"] = all(res["checks"].values())
    return report("smartmatch", res)


if __name__ == "__main__":
    sys.exit(main())
