"""JSON-lines logging to stdout: ts, service, level, event_id, correlation_id, msg."""
import json
import sys
import threading
import time

from . import config

_lock = threading.Lock()


def log(level, msg, event_id=None, correlation_id=None, **extra):
    rec = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int(time.time() * 1000) % 1000:03d}Z",
        "service": config.INSTANCE,
        "level": level,
        "event_id": event_id,
        "correlation_id": correlation_id,
        "msg": msg,
    }
    if extra:
        rec.update(extra)
    line = json.dumps(rec, default=str)
    with _lock:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def info(msg, **kw):
    log("INFO", msg, **kw)


def warn(msg, **kw):
    log("WARN", msg, **kw)


def error(msg, **kw):
    log("ERROR", msg, **kw)
