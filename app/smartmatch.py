"""MediGrid SmartMatch: deterministic, rule-based filter of hospitals by verified capability and
permitted bed type, applied BEFORE distance ranking. Synthetic rules; not clinical triage.

The same predicate is used in three places so the fast path and SQL path cannot disagree:
- eligible_hospitals(): candidate filter before Lua
- ELIGIBLE_SQL: inside the allocation transaction (final verification)
- sql_search(): degraded-mode search
"""
import threading
import time

from . import config, db

# PostgreSQL predicate: hospital alias h (or a column hospital_id) satisfies every mandatory capability of
# condition %(cond)s, and bed type %(bt)s is permitted for that condition. Unknown condition -> no rows in
# condition_bed_types -> fails closed.
CAPS_SQL = """
  NOT EXISTS (SELECT 1 FROM condition_requirements cr WHERE cr.condition_code = %(cond)s
              AND NOT EXISTS (SELECT 1 FROM hospital_capabilities hc WHERE hc.hospital_id = {h}
                              AND hc.capability_code = cr.required_capability AND hc.is_active))
  AND EXISTS (SELECT 1 FROM condition_catalog cc WHERE cc.condition_code = %(cond)s)
"""
BEDTYPE_SQL = """
  AND EXISTS (SELECT 1 FROM condition_bed_types cbt WHERE cbt.condition_code = %(cond)s AND cbt.bed_type = %(bt)s)
"""
ELIGIBLE_SQL = CAPS_SQL + BEDTYPE_SQL

HAVERSINE = ("6371.0 * 2 * asin(sqrt(power(sin(radians({t}.lat - %(lat)s) / 2), 2) + cos(radians(%(lat)s)) * "
             "cos(radians({t}.lat)) * power(sin(radians({t}.lon - %(lon)s) / 2), 2)))")

_cat = {"t": 0.0, "data": None}
_lock = threading.Lock()


def catalog():
    """Condition catalog (code -> label, required caps, bed types) and hospital capability map.
    Cached 10 s per process for validation and display only; eligibility is re-verified in SQL."""
    if _cat["data"] and time.monotonic() - _cat["t"] < 10:
        return _cat["data"]
    with _lock:
        conds = {r["condition_code"]: {"label": r["label"], "required": [], "bed_types": []}
                 for r in db.query("SELECT condition_code, label FROM condition_catalog ORDER BY condition_code")}
        for r in db.query("SELECT condition_code, required_capability FROM condition_requirements ORDER BY 1,2"):
            conds[r["condition_code"]]["required"].append(r["required_capability"])
        for r in db.query("SELECT condition_code, bed_type FROM condition_bed_types ORDER BY 1,2"):
            conds[r["condition_code"]]["bed_types"].append(r["bed_type"])
        caps = {}
        for r in db.query("SELECT hospital_id, capability_code FROM hospital_capabilities WHERE is_active ORDER BY 1,2"):
            caps.setdefault(r["hospital_id"], []).append(r["capability_code"])
        hosps = {r["hospital_id"]: r for r in db.query("SELECT hospital_id, name, lat, lon, total_icu, total_vent, "
                                                       "total_general FROM hospitals ORDER BY hospital_id")}
        _cat.update(t=time.monotonic(), data={"conditions": conds, "caps": caps, "hospitals": hosps})
        return _cat["data"]


def eligible_hospitals(condition_code):
    """Authoritative read from PostgreSQL of hospitals holding every mandatory active capability."""
    rows = db.query("SELECT h.hospital_id FROM hospitals h WHERE " + CAPS_SQL.format(h="h.hospital_id"),
                    {"cond": condition_code})
    return {r["hospital_id"] for r in rows}


def reason_text(cond, bed_type, dist_km, free):
    label = cond["label"]
    caps = ", ".join(c.replace("_", " ").lower() for c in cond["required"]) or "no specialty required"
    return f"{label}: {caps} verified; {bed_type} bed free ({free}); {dist_km:.1f} km"


def sql_search(condition_code, bed_type, lat, lon, radius_km):
    """Degraded-mode search: every hospital in radius with its free count of bed_type and eligibility."""
    sql = f"""
      SELECT h.hospital_id, h.name, {HAVERSINE.format(t='h')} AS dist_km,
             (SELECT COUNT(*) FROM beds b WHERE b.hospital_id = h.hospital_id AND b.bed_type = %(bt)s
                AND b.status = 'FREE') AS free,
             ({ELIGIBLE_SQL.format(h='h.hospital_id')}) AS eligible
      FROM hospitals h
      WHERE {HAVERSINE.format(t='h')} <= %(r)s
      ORDER BY dist_km, h.hospital_id
    """
    return db.query(sql, {"lat": lat, "lon": lon, "bt": bed_type, "cond": condition_code, "r": radius_km})


def explain(condition_code, bed_type, rows, radius_km, limit=10):
    """Build the SmartMatch rationale from rows of (hospital_id, name, dist_km, free, eligible)."""
    cat = catalog()
    cond = cat["conditions"][condition_code]
    matched, excluded = [], []
    for r in rows:
        hid = r["hospital_id"]
        hcaps = cat["caps"].get(hid, [])
        missing = [c for c in cond["required"] if c not in hcaps]
        item = {"hospital_id": hid, "name": r["name"], "distance_km": round(float(r["dist_km"]), 2),
                "free": int(r["free"]), "capabilities": hcaps}
        if not r["eligible"]:
            item["reason"] = "Excluded: missing " + ", ".join(missing) if missing else "Excluded: bed type not permitted"
            excluded.append(item)
        elif item["free"] <= 0:
            item["reason"] = f"Capable, but no free {bed_type} bed"
            excluded.append(item)
        else:
            item["reason"] = reason_text(cond, bed_type, item["distance_km"], item["free"])
            matched.append(item)
    plain = next((r for r in rows if int(r["free"]) > 0), None)
    plain_nearest = None
    if plain:
        plain_nearest = {"hospital_id": plain["hospital_id"], "name": plain["name"],
                         "distance_km": round(float(plain["dist_km"]), 2), "free": int(plain["free"]),
                         "eligible": bool(plain["eligible"])}
    out = {"condition_code": condition_code, "condition_label": cond["label"], "required_capabilities": cond["required"],
           "allowed_bed_types": cond["bed_types"], "bed_type": bed_type, "radius_km": radius_km,
           "matched": matched[:limit], "matched_total": len(matched), "excluded": excluded[:limit * 2],
           "excluded_total": len(excluded), "plain_nearest": plain_nearest}
    if not matched:
        capable_anywhere = [h for h, caps in cat["caps"].items() if all(c in caps for c in cond["required"])]
        out["result"] = "NO_SUITABLE_HOSPITAL_OR_BED"
        out["explanation"] = (f"No hospital within {radius_km:g} km has all of: "
                              f"{', '.join(cond['required']) or 'no specialty'} and a free {bed_type} bed. "
                              f"{len(capable_anywhere)} hospital(s) city-wide hold the required capabilities. "
                              "An operator may review a wider radius. Follow local clinical escalation protocols.")
    return out


def validate_condition(condition_code, bed_type):
    """Returns (cond, error_dict or None)."""
    cat = catalog()
    if condition_code not in cat["conditions"]:
        return None, {"error": "UNKNOWN_CONDITION", "supported_conditions": sorted(cat["conditions"])}
    cond = cat["conditions"][condition_code]
    if bed_type not in cond["bed_types"]:
        return None, {"error": "BED_TYPE_NOT_PERMITTED", "condition_code": condition_code,
                      "supported_bed_types": cond["bed_types"]}
    return cond, None


def radius_ok(radius_km, role):
    if radius_km <= config.SEARCH_RADIUS_KM:
        return True
    return role == "operator" and radius_km <= config.MAX_RADIUS_KM
