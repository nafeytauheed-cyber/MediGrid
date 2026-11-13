# Defence notes

Short, honest answers to the questions we expect. Numbers are from `results/` and the README table.

## Why C and not A or B?
A (PostgreSQL only) is correct but leaves Redis as decoration; we kept it as the degraded path. B (Redis decides)
loses holds on a Redis restart and loses events if the API dies between the Redis pop and the publish. C keeps the
Redis fast path for candidate selection and makes PostgreSQL decide.

## Why does a conditional update back the Lua script?
Lua is atomic inside Redis, but Redis is not the source of truth. After `docker compose pause redis`, the memory is
intact, so the keys look valid while PostgreSQL has moved on. The `UPDATE beds ... WHERE status='FREE'` and the
unique index on HELD or CONFIRMED reservations make a second allocation impossible whatever Redis says. In the
stampede, 50 users across two instances asked for H-01's last ICU bed: one holder, 49 different beds.

## Why is a paused Redis different from a restarted one?
Paused: memory survives, so data may be stale and keys still exist. Restarted (persistence off): everything is gone,
including the fencing pointer. We handle both the same way: any Redis error marks the shared state DEGRADED in
PostgreSQL, all instances use SQL, and one worker rebuilds into a new generation namespace before switching READY.
A stale pre-pause generation is never read again because Lua compares the pointer with the caller's generation.

## What happens while Redis is down?
Search uses SQL Haversine plus the same SmartMatch predicate, allocation uses `FOR UPDATE SKIP LOCKED`, rate limits
use an atomic upsert on `rate_limit_windows`, idempotency uses the unique `(actor_id, idempotency_key)`, and SSE
reads counts from PostgreSQL. The header badge says "Degraded safe mode". If PostgreSQL is down, we return 503:
no allocation without the arbiter.

## Why per-service retry queues?
Dead-lettering back to the topic exchange would redeliver a failing notification to all four services. Each service
has its own 1 s, 3 s and 9 s retry queues that dead-letter to its own queue through the default exchange. The copy is
published with confirms before the original is acked, so a broker failure between the two cannot lose it.

## What the priority queue cannot guarantee
- Messages already delivered to a consumer (prefetch) are not pre-empted; that is why scenario 2 uses prefetch 1.
- Priority only orders messages inside one queue; it says nothing across queues or across consumers.
- Classic queues on a single node: a disk loss or broker crash during a write is outside what we can promise. We test
  restart durability (scenario 5), not hardware failure.
- Priority reorders a patient's events, which is why consumers enforce per-patient order with their own cursor.

## How exactly-once effects work
At-least-once delivery plus an inbox row `(consumer_group, event_id)` committed in the same transaction as the effect.
A duplicate finds the row and is acked with no effect. Notifications have a unique `event_id`, so a duplicate
delivery cannot send a second message. Surge run: 50 republished events, 0 duplicate inbox rows.

## How a transfer stays all-or-nothing
One transaction for each saga step, all locking patient, saga, beds sorted, reservations sorted. Accept checks
state, version and the deadline against `clock_timestamp()`. Reject and timeout free only the matching RESERVED
target bed and return the patient to the source bed. Concurrent accept and reject: exactly one wins (tests).

## Why SSE and not WebSocket?
Updates go one way (server to browser). SSE is plain HTTP through nginx, needs no library, and reconnects with a
server-chosen jittered `retry`. The browser uses a fetch-based reader so the 5 s watchdog also sees heartbeat comments.

## Why is the reserve p95 higher than search p95?
Search reads only. Reserve runs one PostgreSQL transaction of about 8 statements and each API instance is one Python
process, so under hundreds of simultaneous requests the GIL and the 16-request admission limit queue them. The PS
target applies to search; we report reserve separately and do not claim it is under 100 ms under burst load.

## How it would grow (described, not built)
- More API instances behind nginx: they are stateless; the cache state lives in PostgreSQL and Redis.
- Several gunicorn processes per instance would need the SSE coalescer moved to Redis streams or one SSE process.
- Partition `outbox_events`, `audit_log`, `inbox_processed_events` by time and drop old partitions.
- Read replicas for search and dashboards; allocation stays on the primary.
- Shard the Redis GEO and free sets by city zone; route by ambulance location.
- RabbitMQ quorum queues on a three-node cluster for broker failure tolerance (priority would then need separate
  critical and routine queues).
