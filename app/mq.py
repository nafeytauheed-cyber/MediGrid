"""RabbitMQ topology and confirmed publishing (section 3.7), using pika BlockingConnection.

A BlockingConnection is never shared between threads: every thread opens its own."""
import time

import httpx
import pika
from pika.exceptions import NackError, UnroutableError

from . import config, log

EXCHANGE = "medigrid.events"
GROUPS = {
    # group: (queue, bindings, prefetch)
    "bed_tracking": ("q.bed_tracking", ["bed.#", "transfer.#"], 1),
    # saga also binds bed.# so it receives the complete patient stream (DECISIONS C16 / D-SAGA-BIND)
    "transfer_saga": ("q.transfer_saga", ["transfer.#", "bed.#"], 1),
    "notifications": ("q.notifications", ["#"], 5),
    "audit": ("q.audit", ["#"], 20),
}
RETRY_TIERS_MS = (1000, 3000, 9000)
MAX_ATTEMPTS = 3      # handler errors: 1 s, 3 s, 9 s, then DLQ
MAX_POSTPONES = 6     # missing predecessor: 1, 3, 9, 9, 9, 9 s, then DLQ + BLOCKED


def retry_queue(queue, tier):
    return f"{queue}.retry.{RETRY_TIERS_MS[tier] // 1000}s"


def dlq(queue):
    return f"{queue}.dlq"


class PublishFailed(Exception):
    pass


def params():
    return pika.ConnectionParameters(host=config.MQ_HOST, credentials=pika.PlainCredentials(config.MQ_USER, config.MQ_PASS),
                                     heartbeat=30, blocked_connection_timeout=30, connection_attempts=1,
                                     socket_timeout=5)


def connect():
    return pika.BlockingConnection(params())


def declare(ch):
    ch.exchange_declare(EXCHANGE, exchange_type="topic", durable=True)
    for group, (q, binds, _p) in GROUPS.items():
        ch.queue_declare(dlq(q), durable=True)
        ch.queue_declare(q, durable=True, arguments={
            "x-queue-type": "classic", "x-max-priority": 4,
            "x-dead-letter-exchange": "", "x-dead-letter-routing-key": dlq(q)})
        for tier, ttl in enumerate(RETRY_TIERS_MS):
            ch.queue_declare(retry_queue(q, tier), durable=True, arguments={
                "x-queue-type": "classic", "x-message-ttl": ttl,
                "x-dead-letter-exchange": "", "x-dead-letter-routing-key": q})
        for b in binds:
            ch.queue_bind(q, EXCHANGE, routing_key=b)


def props(event_id, priority, headers=None):
    return pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent, priority=int(priority), message_id=str(event_id),
                                content_type="application/json", headers=headers or {})


def publish(ch, exchange, routing_key, body, properties):
    """Publish with confirms and mandatory routing. Raises PublishFailed unless the broker confirmed and routed."""
    try:
        ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties, mandatory=True)
    except (UnroutableError, NackError) as e:
        raise PublishFailed(f"{type(e).__name__}: {e}") from e


class ConfirmedPublisher:
    """Thread-local helper used by the outbox publisher and by chaos/DLQ tools."""

    def __init__(self):
        self.conn = None
        self.ch = None

    def ensure(self):
        if self.conn is None or self.conn.is_closed or self.ch is None or self.ch.is_closed:
            self.close()
            self.conn = connect()
            self.ch = self.conn.channel()
            declare(self.ch)
            self.ch.confirm_delivery()
        return self.ch

    def publish(self, exchange, routing_key, body, properties):
        publish(self.ensure(), exchange, routing_key, body, properties)

    def close(self):
        try:
            if self.conn is not None and self.conn.is_open:
                self.conn.close()
        except Exception:  # noqa: BLE001
            pass
        self.conn = self.ch = None


def wait_for_broker(timeout=120):
    t0 = time.time()
    while True:
        try:
            c = connect()
            ch = c.channel()
            declare(ch)
            c.close()
            return
        except Exception as e:  # noqa: BLE001
            if time.time() - t0 > timeout:
                raise
            log.warn("waiting for rabbitmq", error=str(e)[:120])
            time.sleep(2)


# ------------------------------------------------------------------ management HTTP API

def mgmt_get(path, timeout=2.0):
    r = httpx.get(f"{config.MQ_MGMT_URL}{path}", auth=(config.MQ_USER, config.MQ_PASS), timeout=timeout)
    r.raise_for_status()
    return r.json()


def mgmt_post(path, body, timeout=5.0):
    r = httpx.post(f"{config.MQ_MGMT_URL}{path}", auth=(config.MQ_USER, config.MQ_PASS), json=body, timeout=timeout)
    r.raise_for_status()
    return r.json()


def queue_stats():
    rows = mgmt_get("/api/queues/%2F?columns=name,messages,messages_ready,messages_unacknowledged,consumers,"
                    "message_stats.publish_details.rate,message_stats.deliver_get_details.rate")
    return {r["name"]: r for r in rows}


def peek(queue, count=20):
    """Non-destructive inspection: messages are requeued (ackmode ack_requeue_true)."""
    return mgmt_post(f"/api/queues/%2F/{queue}/get", {"count": count, "ackmode": "ack_requeue_true",
                                                      "encoding": "auto", "truncate": 4000})
