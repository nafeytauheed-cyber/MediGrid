"""Hand-built Server-Sent Events fan-out (section 3.10).

Per instance:
- a Redis pub/sub listener puts deltas from the notification consumer into a dict keyed by hospital id
- a PostgreSQL watermark poller (every 500 ms) re-reads counts when pg_epoch moved, so a lost pub/sub message
  or a Redis outage cannot leave the UI stale (DECISIONS C21)
- a flusher sends one coalesced batch to every client every 100 ms
"""
import json
import queue
import random
import threading
import time

from .. import cache, config, db, log, rds

FLUSH_S = 0.1


class Hub:
    def __init__(self):
        self.clients = set()
        self.lock = threading.Lock()
        self.pending = {}
        self.pending_lock = threading.Lock()
        self.counts = {}
        self.epoch = None
        self.mode = None
        self.ambulances = None
        self.amb_sig = None
        self.snapshot_cache = (0.0, None)
        self.stats = {"pubsub_msgs": 0, "watermark_repairs": 0, "batches": 0}
        self.started = False

    def start(self):
        if self.started:
            return
        self.started = True
        for fn, name in ((self._pubsub, "sse-pubsub"), (self._watermark, "sse-watermark"), (self._flusher, "sse-flush")):
            threading.Thread(target=fn, name=name, daemon=True).start()

    # ------------------------------------------------------------ sources
    def _pubsub(self):
        while True:
            try:
                r = rds.slow_client(timeout=None)
                ps = r.pubsub(ignore_subscribe_messages=True)
                ps.subscribe(rds.UI_CHANNEL)
                while True:
                    m = ps.get_message(timeout=1.0)
                    if m and m["type"] == "message":
                        d = json.loads(m["data"])
                        self.stats["pubsub_msgs"] += 1
                        with self.pending_lock:
                            for h, c in d.get("counts", {}).items():
                                self.pending[h] = c
            except Exception as e:  # noqa: BLE001
                log.warn("sse pubsub disconnected", error=str(e)[:120])
                time.sleep(1.0)

    @staticmethod
    def read_counts():
        rows = db.query("SELECT hospital_id, bed_type, COUNT(*) FILTER (WHERE status='FREE') AS free, "
                        "COUNT(*) FILTER (WHERE status='RESERVED') AS reserved, "
                        "COUNT(*) FILTER (WHERE status='OCCUPIED') AS occupied FROM beds GROUP BY hospital_id, bed_type")
        out = {}
        for r in rows:
            out.setdefault(r["hospital_id"], {})[r["bed_type"]] = {"free": r["free"], "reserved": r["reserved"],
                                                                  "occupied": r["occupied"]}
        return out

    def _watermark(self):
        last_amb = 0.0
        while True:
            try:
                st, gen = cache.read_state()
                mode = cache.STATE_NAMES[st]
                epoch = db.query_one("SELECT value FROM system_state WHERE key='pg_epoch'")["value"]
                if epoch != self.epoch:
                    counts = self.read_counts()
                    changed = {h: c for h, c in counts.items() if self.counts.get(h) != c}
                    self.counts = counts
                    self.epoch = epoch
                    if changed:
                        self.stats["watermark_repairs"] += 1
                        with self.pending_lock:
                            self.pending.update(changed)
                if mode != self.mode:
                    self.mode = mode
                    with self.pending_lock:
                        self.pending["__mode__"] = {"mode": mode, "generation": gen}
                if time.monotonic() - last_amb > 1.0:
                    last_amb = time.monotonic()
                    self._ambulances()
            except Exception as e:  # noqa: BLE001
                log.warn("sse watermark poll failed", error=str(e)[:120])
            time.sleep(0.5)

    def _ambulances(self):
        try:
            raw = rds.call(lambda r: r.hgetall(rds.AMB_KEY))
        except rds.RedisUnavailable:
            raw = None
        if raw is None:
            amb = {"available": False, "reason": "Redis unavailable", "items": []}
        else:
            now = time.time()
            items = [json.loads(v) for v in raw.values()]
            items = [a for a in items if now - a.get("ts", 0) < 10]
            amb = {"available": True, "items": items}
        sig = json.dumps(amb, sort_keys=True)
        if sig != self.amb_sig:
            self.amb_sig = sig
            self.ambulances = amb
            with self.pending_lock:
                self.pending["__amb__"] = amb

    def _flusher(self):
        while True:
            time.sleep(FLUSH_S)
            with self.pending_lock:
                if not self.pending:
                    continue
                batch, self.pending = self.pending, {}
            msg = {"hospitals": {h: c for h, c in batch.items() if not h.startswith("__")},
                   "watermark": self.epoch, "server_ts": time.time()}
            if "__mode__" in batch:
                msg["mode"] = batch["__mode__"]
            if "__amb__" in batch:
                msg["ambulances"] = batch["__amb__"]
            data = json.dumps(msg)
            self.stats["batches"] += 1
            with self.lock:
                clients = list(self.clients)
            for q in clients:
                try:
                    q.put_nowait(("batch", data))
                except queue.Full:
                    pass  # the client will resync from the snapshot after reconnect

    # ------------------------------------------------------------ snapshot + stream
    def snapshot(self):
        t, snap = self.snapshot_cache
        if snap is not None and time.monotonic() - t < 1.0:
            return snap
        st, gen = cache.read_state()
        epoch = db.query_one("SELECT value FROM system_state WHERE key='pg_epoch'")["value"]
        snap = {"hospitals": self.read_counts(), "watermark": epoch, "mode": {"mode": cache.STATE_NAMES[st],
                                                                               "generation": gen},
                "ambulances": self.ambulances, "instance": config.INSTANCE, "server_ts": time.time(),
                "source": "postgresql"}
        self.snapshot_cache = (time.monotonic(), snap)
        return snap

    def stream(self):
        q = queue.Queue(maxsize=200)
        with self.lock:
            self.clients.add(q)

        def gen():
            try:
                yield f"retry: {random.randint(100, 600)}\n\n"
                yield f"event: instance\ndata: {json.dumps({'instance': config.INSTANCE})}\n\n"
                yield f"event: snapshot\ndata: {json.dumps(self.snapshot())}\n\n"
                while True:
                    try:
                        kind, data = q.get(timeout=2.0)
                        yield f"event: {kind}\ndata: {data}\n\n"
                    except queue.Empty:
                        yield ": hb\n\n"
            finally:
                with self.lock:
                    self.clients.discard(q)

        return gen()


hub = Hub()
