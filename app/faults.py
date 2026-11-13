"""Fault-injection flags stored in PostgreSQL control_flags (shared by every process).
Only honoured when CHAOS_ENABLED=1."""
import time

from . import config, db

_cache = {"t": 0.0, "flags": {}}


def flags(max_age=0.5):
    if not config.CHAOS_ENABLED:
        return {}
    if time.monotonic() - _cache["t"] > max_age:
        try:
            _cache["flags"] = {r["key"]: r["value"] for r in db.query("SELECT key, value FROM control_flags")}
            _cache["t"] = time.monotonic()
        except Exception:  # noqa: BLE001
            pass
    return _cache["flags"]


def get(key, default=None, max_age=0.5):
    return flags(max_age).get(key, default)


def set_flag(key, value):
    db.query("INSERT INTO control_flags (key, value) VALUES (%s,%s) ON CONFLICT (key) DO UPDATE "
             "SET value=EXCLUDED.value, updated_at=NOW()", (key, str(value)))
    _cache["t"] = 0.0


def clear(key):
    db.query("DELETE FROM control_flags WHERE key=%s", (key,))
    _cache["t"] = 0.0
