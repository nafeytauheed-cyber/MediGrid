"""Four independent consumer groups. Each has its own queue, inbox rows and per-patient ordering cursors.

Message handling (DECISIONS C20):
- success / duplicate: ack after the PostgreSQL transaction (inbox + effect + cursor) committed
- handler error: confirmed publish of a copy to the 1/3/9 s retry queue, THEN ack; after 3 retries confirmed
  publish to the DLQ, then ack. If a publish is not confirmed the original stays unacked and we reconnect.
- missing predecessor (seq > cursor+1): confirmed delayed postpone, then ack; after MAX_POSTPONES -> DLQ and
  the patient stream is marked BLOCKED for an operator.
"""
import json
import os
import threading
import time

import pika
from psycopg.types.json import Jsonb

from .. import cache, config, db, faults, log, mq, rds


class Postpone(Exception):
    def __init__(self, expected):
        super().__init__("missing predecessor")
        self.expected = expected


class Handler:
    group = None

    def apply(self, cur, evt):
        """Domain effect inside the inbox transaction."""

    def after(self, evt):
        """Post-commit, best effort (Redis projection, pub/sub). Never part of the commit."""


def _hospitals_of(evt):
    d = evt.get("data") or {}
    hs = {d.get("hospital_id"), d.get("from_hospital_id"), d.get("to_hospital_id")}
    for b in d.get("beds") or []:
        hs.add(b.get("hospital_id"))
    return sorted(h for h in hs if h)


class BedTracking(Handler):
    group = "bed_tracking"

    def apply(self, cur, evt):
        for h in _hospitals_of(evt):
            cur.execute("""INSERT INTO hospital_occupancy (hospital_id, bed_type, free, reserved, occupied, last_event_id, updated_at)
                           SELECT hospital_id, bed_type, COUNT(*) FILTER (WHERE status='FREE'),
                                  COUNT(*) FILTER (WHERE status='RESERVED'), COUNT(*) FILTER (WHERE status='OCCUPIED'), %s, NOW()
                           FROM beds WHERE hospital_id=%s GROUP BY hospital_id, bed_type
                           ON CONFLICT (hospital_id, bed_type) DO UPDATE SET free=EXCLUDED.free, reserved=EXCLUDED.reserved,
                             occupied=EXCLUDED.occupied, last_event_id=EXCLUDED.last_event_id, updated_at=NOW()""",
                        (evt["event_id"], h))

    def after(self, evt):
        for h in _hospitals_of(evt):
            cache.reconcile_hospital(h)


class TransferSaga(Handler):
    group = "transfer_saga"

    def apply(self, cur, evt):
        t = evt["type"]
        if t.startswith("chaos.priority."):
            cur.execute("INSERT INTO chaos_priority_log (run_id, event_id, kind, consumer) VALUES (%s,%s,%s,%s)",
                        (evt["data"]["run_id"], evt["event_id"], t.rsplit(".", 1)[1], config.INSTANCE))
        elif t.startswith("transfer."):
            cur.execute("INSERT INTO saga_log (transfer_id, event_id, event_type) VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
                        ((evt.get("data") or {}).get("transfer_id"), evt["event_id"], t))
        # other bed.* events: no-op, but the cursor still advances (complete patient stream)

    def after(self, evt):
        d = evt.get("data") or {}
        if evt["type"] == "bed.reservation.expired" and d.get("purpose") == "TRANSFER":
            from .. import transfers
            transfers.timeout_for_reservation(d["reservation_id"])


class Notifications(Handler):
    group = "notifications"
    MESSAGES = {
        "bed.reservation.created": "Bed {bed_id} held for ambulance {ambulance_id}",
        "bed.reservation.confirmed": "Ambulance arrived, bed {bed_id} confirmed",
        "bed.reservation.expired": "Hold on bed {bed_id} expired and was released",
        "bed.reservation.cancelled": "Hold on bed {bed_id} cancelled",
        "bed.admitted": "Patient admitted to bed {bed_id}",
        "bed.discharged": "Patient discharged from bed {bed_id}",
        "transfer.requested": "Incoming transfer request from {from_hospital_id} for bed {to_bed_id}",
        "transfer.completed": "Transfer completed into bed {to_bed_id}",
        "transfer.reversed": "Transfer reversed ({reason})",
    }

    def apply(self, cur, evt):
        tpl = self.MESSAGES.get(evt["type"])
        if not tpl:
            return
        d = evt.get("data") or {}
        try:
            msg = tpl.format(**d)
        except KeyError:
            msg = evt["type"]
        hs = _hospitals_of(evt) or [None]
        # one row per event (unique event_id): a duplicate delivery can never produce a second notification
        cur.execute("INSERT INTO notifications (event_id, hospital_id, kind, message) VALUES (%s,%s,%s,%s) "
                    "ON CONFLICT (event_id) DO NOTHING", (evt["event_id"], hs[0], evt["type"], msg))

    def after(self, evt):
        hs = _hospitals_of(evt)
        if not hs:
            return
        rows = db.query("SELECT hospital_id, bed_type, COUNT(*) FILTER (WHERE status='FREE') AS free, "
                        "COUNT(*) FILTER (WHERE status='RESERVED') AS reserved, "
                        "COUNT(*) FILTER (WHERE status='OCCUPIED') AS occupied FROM beds WHERE hospital_id = ANY(%s) "
                        "GROUP BY hospital_id, bed_type", (hs,))
        counts = {}
        for r in rows:
            counts.setdefault(r["hospital_id"], {})[r["bed_type"]] = {"free": r["free"], "reserved": r["reserved"],
                                                                     "occupied": r["occupied"]}
        msg = json.dumps({"event_id": evt["event_id"], "type": evt["type"], "counts": counts,
                          "occurred_at": evt.get("occurred_at")})
        try:
            rds.call(lambda r: r.publish(rds.UI_CHANNEL, msg))
        except rds.RedisUnavailable:
            pass  # pub/sub is lossy by design; API watermark reconciliation covers it


class Audit(Handler):
    group = "audit"

    def apply(self, cur, evt):
        cur.execute("INSERT INTO audit_log (event_id, event_type, patient_id, priority, occurred_at, payload) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                    (evt["event_id"], evt["type"], evt.get("patient_id"), evt.get("priority"), evt.get("occurred_at"),
                     Jsonb(evt)))
        cur.execute("INSERT INTO event_rate (bucket, events) VALUES (date_trunc('second', clock_timestamp()), 1) "
                    "ON CONFLICT (bucket) DO UPDATE SET events = event_rate.events + 1")


HANDLERS = {h.group: h for h in (BedTracking(), TransferSaga(), Notifications(), Audit())}


def process(handler, evt):
    """Inbox + ordering + effect in one transaction. Returns APPLIED, DUPLICATE or raises Postpone."""
    group = handler.group

    def tx(cur):
        cur.execute("INSERT INTO inbox_processed_events (consumer_group, event_id) VALUES (%s,%s) "
                    "ON CONFLICT DO NOTHING RETURNING event_id", (group, evt["event_id"]))
        if not cur.fetchone():
            return "DUPLICATE"
        pid, seq = evt.get("patient_id"), evt.get("patient_seq_no")
        if pid and seq:
            cur.execute("INSERT INTO consumer_patient_offsets (consumer_group, patient_id) VALUES (%s,%s) "
                        "ON CONFLICT DO NOTHING", (group, pid))
            cur.execute("SELECT last_applied_seq FROM consumer_patient_offsets WHERE consumer_group=%s AND patient_id=%s "
                        "FOR UPDATE", (group, pid))
            cursor = cur.fetchone()["last_applied_seq"]
            if seq <= cursor:
                return "DUPLICATE"
            if seq > cursor + 1:
                raise Postpone(cursor + 1)
            handler.apply(cur, evt)
            cur.execute("UPDATE consumer_patient_offsets SET last_applied_seq=%s WHERE consumer_group=%s AND patient_id=%s",
                        (seq, group, pid))
            cur.execute("INSERT INTO consumer_apply_log (consumer_group, patient_id, seq_no, event_id, event_type) "
                        "VALUES (%s,%s,%s,%s,%s)", (group, pid, seq, evt["event_id"], evt["type"]))
            cur.execute("UPDATE patient_streams_blocked SET resolved_at=NOW() WHERE consumer_group=%s AND patient_id=%s "
                        "AND resolved_at IS NULL AND expected_seq <= %s", (group, pid, seq))
        else:
            handler.apply(cur, evt)
        return "APPLIED"

    return db.run_tx(tx)


def _bump(name, n=1):
    try:
        db.query("UPDATE counters SET value = value + %s WHERE name=%s", (n, name))
    except Exception:  # noqa: BLE001
        pass


def _attempts(group, event_id, field, error=None, dead=False):
    try:
        r = db.query_one(
            f"INSERT INTO message_attempts (consumer_group, event_id, {field}, last_error, dead_lettered_at) "
            f"VALUES (%s,%s,1,%s, CASE WHEN %s THEN NOW() END) ON CONFLICT (consumer_group, event_id) DO UPDATE SET "
            f"{field} = message_attempts.{field} + 1, last_error = COALESCE(EXCLUDED.last_error, message_attempts.last_error), "
            f"dead_lettered_at = COALESCE(EXCLUDED.dead_lettered_at, message_attempts.dead_lettered_at), updated_at=NOW() "
            f"RETURNING {field} AS n", (group, event_id, error, dead))
        return r["n"]
    except Exception as e:  # noqa: BLE001
        log.warn("attempt bookkeeping failed", error=str(e)[:200], event_id=event_id)
        return None


class ConsumerThread(threading.Thread):
    def __init__(self, group):
        super().__init__(name=f"consumer-{group}", daemon=True)
        self.group = group
        self.queue, _b, self.prefetch = mq.GROUPS[group]
        self.handler = HANDLERS[group]
        self.ctag = None
        self.state = "starting"

    # -------------------------------------------------- pause control (scenario 2)
    def _allowed(self):
        v = faults.get(f"pause:{self.queue}", max_age=0.2)
        if v is None:
            return True
        if v == "all":
            return False
        if v.startswith("only:"):
            return v[5:] == config.INSTANCE
        return True

    def run(self):
        backoff = 0.5
        while True:
            try:
                self._session()
                backoff = 0.5
            except Exception as e:  # noqa: BLE001
                self.state = "reconnecting"
                log.warn("consumer connection lost; reconnecting", group=self.group, error=str(e)[:200])
                time.sleep(backoff)
                backoff = min(backoff * 2, 5)

    def _session(self):
        conn = mq.connect()
        try:
            ch = conn.channel()
            mq.declare(ch)
            ch.basic_qos(prefetch_count=self.prefetch)
            pub = conn.channel()
            pub.confirm_delivery()
            self.pub = pub
            self.ctag = None
            log.info("consumer connected", group=self.group, queue=self.queue, prefetch=self.prefetch)
            while True:
                allowed = self._allowed()
                if allowed and self.ctag is None:
                    self.ctag = ch.basic_consume(self.queue, self._on_message, auto_ack=False)
                    self.state = "consuming"
                elif not allowed and self.ctag is not None:
                    ch.basic_cancel(self.ctag)
                    self.ctag = None
                    self.state = "paused"
                    log.info("consumer paused", group=self.group)
                conn.process_data_events(time_limit=0.2)
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    # -------------------------------------------------- message handling
    def _send(self, queue, body, properties, headers):
        h = dict(properties.headers or {})
        h.update(headers)
        p = pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent, priority=properties.priority or 1,
                                 message_id=properties.message_id, content_type="application/json", headers=h)
        mq.publish(self.pub, "", queue, body, p)  # raises PublishFailed -> original stays unacked

    def _on_message(self, ch, method, properties, body):
        headers = properties.headers or {}
        try:
            evt = json.loads(body)
            eid = evt["event_id"]
        except Exception as e:  # noqa: BLE001
            self._send(mq.dlq(self.queue), body, properties, {"x-error": f"malformed: {e}"[:200],
                                                              "x-original-queue": self.queue})
            ch.basic_ack(method.delivery_tag)
            return
        flags = evt.get("flags") or {}
        # (B) crash before ack: the process dies; RabbitMQ redelivers the unacked message. Bounded by a
        # persistent crash counter so a poison message cannot crash-loop forever.
        if flags.get("crash_group") == self.group and faults.get("fault:poison", max_age=0) == "1":
            n = _attempts(self.group, eid, "crashes", "injected crash before ack")
            if n is not None and n > 2:
                self._send(mq.dlq(self.queue), body, properties, {"x-error": "crash loop guard: 3 crashes",
                                                                  "x-original-queue": self.queue})
                _attempts(self.group, eid, "failures", "crash loop guard", dead=True)
                _bump("dlq_published")
                ch.basic_ack(method.delivery_tag)
                log.warn("crash-loop guard sent message to DLQ", event_id=eid, group=self.group)
                return
            log.error("injected crash before ack (SIGKILL-equivalent)", event_id=eid, group=self.group, crashes=n)
            os._exit(137)
        try:
            if flags.get("fail_group") == self.group and faults.get("fault:poison", max_age=0) == "1":
                raise RuntimeError("injected handler error")
            if flags.get("slow_ms") and flags.get("slow_group", self.group) == self.group:
                time.sleep(min(int(flags["slow_ms"]), 5000) / 1000.0)
            outcome = process(self.handler, evt)
        except Postpone as p:
            n = int(headers.get("x-postpone", 0))
            if n >= mq.MAX_POSTPONES:
                self._send(mq.dlq(self.queue), body, properties,
                           {"x-error": f"missing predecessor seq {p.expected}", "x-original-queue": self.queue})
                db.query("INSERT INTO patient_streams_blocked (consumer_group, patient_id, waiting_event_id, waiting_seq, "
                         "expected_seq) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                         (self.group, evt["patient_id"], eid, evt["patient_seq_no"], p.expected))
                _attempts(self.group, eid, "postpones", "BLOCKED: missing predecessor", dead=True)
                _bump("dlq_published")
                log.warn("patient stream BLOCKED", event_id=eid, group=self.group, patient=evt["patient_id"])
            else:
                tier = min(n, len(mq.RETRY_TIERS_MS) - 1)
                self._send(mq.retry_queue(self.queue, tier), body, properties, {"x-postpone": n + 1})
                _attempts(self.group, eid, "postpones")
                _bump("postponed")
            ch.basic_ack(method.delivery_tag)
            return
        except mq.PublishFailed:
            raise
        except Exception as e:  # noqa: BLE001  (A) ordinary handler error
            attempt = int(headers.get("x-attempt", 0))
            err = f"{type(e).__name__}: {e}"[:300]
            if attempt < mq.MAX_ATTEMPTS:
                self._send(mq.retry_queue(self.queue, attempt), body, properties, {"x-attempt": attempt + 1,
                                                                                    "x-last-error": err})
                _attempts(self.group, eid, "failures", err)
                _bump("retry_published")
                log.warn("handler error; retry scheduled", event_id=eid, group=self.group, attempt=attempt + 1,
                         delay_ms=mq.RETRY_TIERS_MS[attempt], error=err)
            else:
                self._send(mq.dlq(self.queue), body, properties, {"x-error": err, "x-original-queue": self.queue,
                                                                  "x-attempt": attempt})
                _attempts(self.group, eid, "failures", err, dead=True)
                _bump("dlq_published")
                log.warn("handler error; dead-lettered", event_id=eid, group=self.group, error=err)
            ch.basic_ack(method.delivery_tag)
            return
        ch.basic_ack(method.delivery_tag)
        if outcome == "APPLIED":
            try:
                self.handler.after(evt)
            except Exception as e:  # noqa: BLE001
                log.warn("post-commit projection failed", event_id=eid, group=self.group, error=str(e)[:200])
