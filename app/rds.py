"""Redis client with a short timeout and a per-instance circuit breaker.

The breaker is only a local performance optimisation: it avoids paying the 150 ms timeout on every
request while Redis is down. Recovery authority is the shared cache state in PostgreSQL (cache.py)."""
import pathlib
import threading
import time

import redis

from . import config, log

POINTER = "medigrid:cache_ready_generation"
UI_CHANNEL = "medigrid:ui:delta"
AMB_KEY = "medigrid:ambulances"

_LUA_DIR = pathlib.Path(__file__).parent / "lua"


class RedisUnavailable(Exception):
    pass


def k_geo(g):
    return f"mg:{g}:hospitals:geo"


def k_free(g, h, t):
    return f"mg:{g}:hosp:{h}:beds:{t}:free"


def k_resdata(g):
    return f"mg:{g}:res:data"


def k_expiry(g):
    return f"mg:{g}:reservations:expiry"


def k_held(g):
    return f"mg:{g}:held_beds"


def k_rate(actor):
    return f"ratelimit:{actor}"


def k_idemp(actor, key):
    return f"idemp:res:{actor}:{key}"


class Breaker:
    def __init__(self, open_s=2.0):
        self.open_s = open_s
        self.open_until = 0.0
        self.lock = threading.Lock()
        self.failures = 0

    def allow(self):
        return time.monotonic() >= self.open_until

    def fail(self):
        with self.lock:
            self.failures += 1
            self.open_until = time.monotonic() + self.open_s

    def ok(self):
        self.open_until = 0.0

    @property
    def is_open(self):
        return not self.allow()


breaker = Breaker()
_client = None
_scripts = {}
_failure_hooks = []
_init_lock = threading.Lock()


def client():
    global _client
    if _client is None:
        with _init_lock:
            if _client is None:
                c = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, socket_timeout=config.REDIS_TIMEOUT_S,
                                socket_connect_timeout=config.REDIS_TIMEOUT_S, retry_on_timeout=False,
                                decode_responses=True, max_connections=256)
                for name in ("reserve_nearest_bed", "release_reservation", "reconcile_free"):
                    _scripts[name] = c.register_script((_LUA_DIR / f"{name}.lua").read_text(encoding="utf-8"))
                _client = c
    return _client


def slow_client(timeout=5.0):
    """Separate client with a longer timeout for bulk work (rebuild, reconciliation)."""
    return redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, socket_timeout=timeout,
                       socket_connect_timeout=1.0, decode_responses=True)


def on_failure(hook):
    _failure_hooks.append(hook)


def call(fn, allow_when_open=False):
    """Run fn(redis_client). Any connection error or timeout opens the breaker, notifies the
    failure hooks (which mark the shared cache DEGRADED) and raises RedisUnavailable."""
    if not allow_when_open and not breaker.allow():
        raise RedisUnavailable("breaker open")
    try:
        r = fn(client())
        breaker.ok()
        return r
    except (redis.ConnectionError, redis.TimeoutError, OSError) as e:
        breaker.fail()
        log.warn("redis call failed", error=str(e)[:200])
        for h in _failure_hooks:
            try:
                h(str(e)[:200])
            except Exception as he:  # noqa: BLE001
                log.warn("redis failure hook error", error=str(he)[:200])
        raise RedisUnavailable(str(e)) from e


def script(name):
    client()
    return _scripts[name]
