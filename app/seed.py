"""Deterministic synthetic seed: 40 fictional hospitals, 4,000+ beds, SmartMatch rules.

Run: python -m app.seed            (skips if already seeded)
     python -m app.seed --reset    (wipes domain data and reseeds; epoch/generation stay monotonic)
"""
import math
import random
import sys
import uuid

import psycopg

from . import config, db, log

SEED = 20260413

# Ambulance demo point used by the SmartMatch demonstration and the dispatch page default.
DEMO_POINT = (18.5200, 73.8560)

CONDITIONS = {
    # code: (label, [required capabilities], [allowed bed types])
    "CARDIAC_EMERGENCY": ("Cardiac emergency", ["CARDIAC_EMERGENCY_CARE"], ["ICU", "GENERAL"]),
    "SUSPECTED_STROKE": ("Suspected stroke", ["STROKE_READY"], ["ICU", "GENERAL"]),
    "MAJOR_TRAUMA": ("Major trauma", ["TRAUMA_CARE"], ["ICU", "VENTILATOR"]),
    "SEVERE_BURNS": ("Severe burns", ["BURN_CARE"], ["ICU", "VENTILATOR"]),
    "OBSTETRIC_EMERGENCY": ("Obstetric emergency", ["EMERGENCY_OBSTETRICS"], ["GENERAL", "ICU"]),
    "PEDIATRIC_EMERGENCY": ("Pediatric emergency", ["PEDIATRIC_EMERGENCY"], ["GENERAL", "ICU"]),
    "GENERAL_ACUTE": ("General acute (no specialty)", [], ["ICU", "VENTILATOR", "GENERAL"]),
}

PREFIXES = ["North", "South", "East", "West", "Central", "Riverside", "Hillview", "Lakeside", "Old Town",
            "Parkside", "Harbour", "Station", "Market", "Garden", "Fort", "Bridge", "Mill", "Canal",
            "Orchard", "Quarry"]
SUFFIXES = ["General Hospital", "Medical Centre", "Community Hospital", "Care Institute"]


def km(a, b):
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def build():
    rng = random.Random(SEED)
    hospitals = []
    # H-01: nearest to the demo point, ICU beds, deliberately NO cardiac capability.
    hospitals.append(("H-01", (18.5290, 73.8600), ["TRAUMA_CARE", "PEDIATRIC_EMERGENCY", "CRITICAL_CARE"]))
    # H-02: farther away (about 7 km), cardiac + stroke capable.
    hospitals.append(("H-02", (18.5780, 73.8880), ["CARDIAC_EMERGENCY_CARE", "STROKE_READY", "CRITICAL_CARE"]))
    d_h01 = km(DEMO_POINT, hospitals[0][1])
    d_h02 = km(DEMO_POINT, hospitals[1][1])
    optional = ["STROKE_READY", "TRAUMA_CARE", "EMERGENCY_OBSTETRICS", "PEDIATRIC_EMERGENCY", "CRITICAL_CARE"]
    n = 3
    while n <= 38:
        p = (round(rng.uniform(18.43, 18.65), 4), round(rng.uniform(73.74, 73.99), 4))
        d = km(DEMO_POINT, p)
        if d <= d_h01 + 0.5:
            continue  # keep H-01 the nearest hospital to the demo point
        caps = sorted(rng.sample(optional, rng.randint(1, 3)))
        # Cardiac capability only for hospitals farther than H-02, so H-02 is the nearest capable one.
        if d > d_h02 + 1.0 and rng.random() < 0.45:
            caps.append("CARDIAC_EMERGENCY_CARE")
        hospitals.append((f"H-{n:02d}", p, sorted(set(caps))))
        n += 1
    # Two satellite hospitals about 70 km north: the only BURN_CARE holders (outside the 50 km radius).
    hospitals.append(("H-39", (19.1530, 73.8710), ["BURN_CARE", "TRAUMA_CARE", "CRITICAL_CARE"]))
    hospitals.append(("H-40", (19.1710, 73.8350), ["BURN_CARE", "CRITICAL_CARE"]))

    rows, caps_rows, beds = [], [], []
    for i, (hid, (lat, lon), caps) in enumerate(hospitals):
        name = f"{PREFIXES[i % len(PREFIXES)]} {SUFFIXES[(i // len(PREFIXES) + i) % len(SUFFIXES)]} ({hid})"
        icu, vent, gen = rng.randint(15, 30), rng.randint(8, 16), rng.randint(60, 100)
        rows.append((hid, name, lat, lon, icu, vent, gen))
        for c in caps:
            caps_rows.append((hid, c))
        for btype, count in (("ICU", icu), ("VENTILATOR", vent), ("GENERAL", gen)):
            short = {"ICU": "ICU", "VENTILATOR": "VENT", "GENERAL": "GEN"}[btype]
            for k in range(1, count + 1):
                beds.append((f"{hid}-{short}-{k:03d}", hid, btype))
    return rows, caps_rows, beds, rng


def seed(conn, rng_rows):
    rows, caps_rows, beds, rng = rng_rows
    cur = conn.cursor()
    cur.executemany("INSERT INTO hospitals VALUES (%s,%s,%s,%s,%s,%s,%s)", rows)
    cur.executemany("INSERT INTO hospital_capabilities (hospital_id, capability_code) VALUES (%s,%s)", caps_rows)
    for code, (label, reqs, btypes) in CONDITIONS.items():
        cur.execute("INSERT INTO condition_catalog VALUES (%s,%s)", (code, label))
        for r in reqs:
            cur.execute("INSERT INTO condition_requirements VALUES (%s,%s)", (code, r))
        for b in btypes:
            cur.execute("INSERT INTO condition_bed_types VALUES (%s,%s)", (code, b))

    bed_rows, patients, reservations = [], [], []
    pn = 0
    for bed_id, hid, btype in beds:
        occupied = rng.random() < 0.35
        # Keep demo beds free: first two ICU beds at H-01 and H-02.
        if hid in ("H-01", "H-02") and btype == "ICU" and bed_id.endswith(("-001", "-002")):
            occupied = False
        if occupied:
            pn += 1
            pid = f"P-{pn:06d}"
            rid = str(uuid.UUID(int=rng.getrandbits(128), version=4))
            bed_rows.append((bed_id, hid, btype, "OCCUPIED", pid, rid))
            patients.append((pid, bed_id, hid, "ADMITTED"))
            reservations.append((rid, "seed", f"seed-{bed_id}", "seed", bed_id, hid, btype, "SEED", pid, 2,
                                 "CONFIRMED"))
        else:
            bed_rows.append((bed_id, hid, btype, "FREE", None, None))
    with cur.copy("COPY beds (bed_id, hospital_id, bed_type, status, patient_id, reservation_id) FROM STDIN") as cp:
        for r in bed_rows:
            cp.write_row(r)
    with cur.copy("COPY patients (patient_id, current_bed_id, current_hosp_id, state) FROM STDIN") as cp:
        for r in patients:
            cp.write_row(r)
    with cur.copy("COPY reservations (reservation_id, actor_id, idempotency_key, request_fingerprint, bed_id, "
                  "hospital_id, bed_type, ambulance_id, patient_id, priority, status, expires_at) FROM STDIN") as cp:
        for r in reservations:
            cp.write_row(r + ("2026-01-01T00:00:00Z",))
    cur.execute("""INSERT INTO hospital_occupancy (hospital_id, bed_type, free, reserved, occupied)
                   SELECT hospital_id, bed_type, COUNT(*) FILTER (WHERE status='FREE'),
                          COUNT(*) FILTER (WHERE status='RESERVED'), COUNT(*) FILTER (WHERE status='OCCUPIED')
                   FROM beds GROUP BY hospital_id, bed_type""")
    cur.execute("SELECT nextval('pg_epoch_seq')")
    cur.execute("UPDATE system_state SET value=0 WHERE key IN ('cache_state','ttl_override_s')")
    return len(rows), len(bed_rows), len(patients)


DOMAIN_TABLES = [
    "consumer_apply_log", "patient_streams_blocked", "message_attempts", "hospital_occupancy", "notifications",
    "audit_log", "event_rate", "saga_log", "latency_samples", "chaos_runs", "chaos_priority_log",
    "inbox_processed_events", "consumer_patient_offsets", "rate_limit_windows", "outbox_events",
    "transfers_saga", "reservations", "patients", "beds", "condition_bed_types", "condition_requirements",
    "condition_catalog", "hospital_capabilities", "hospitals", "control_flags",
]


def main():
    reset = "--reset" in sys.argv
    db.wait_for_db()
    with psycopg.connect(config.PG_DSN) as conn:
        conn.execute("SELECT pg_advisory_xact_lock(7002)")
        n = conn.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
        if n and not reset:
            log.info("seed skipped: already seeded", hospitals=n)
            return
        if reset:
            conn.execute("TRUNCATE " + ", ".join(DOMAIN_TABLES))
            conn.execute("UPDATE counters SET value=0")
        h, b, p = seed(conn, build())
        conn.commit()
    assert h >= 40 and b >= 4000, (h, b)
    log.info("seed complete", hospitals=h, beds=b, occupied_patients=p)


if __name__ == "__main__":
    main()
