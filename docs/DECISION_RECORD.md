# Decision record: PostgreSQL-authoritative hybrid allocation

## Decision

Bed allocation is decided only by PostgreSQL. Redis selects a candidate quickly and holds a provisional, non-binding
hold. An allocation exists only when, in one PostgreSQL transaction:

1. `UPDATE beds SET status='RESERVED' ... WHERE bed_id=? AND status='FREE' AND <SmartMatch eligibility>` matched one row,
2. the reservation row inserted under the unique index on `(bed_id) WHERE status IN ('HELD','CONFIRMED')`,
3. the outbox event row inserted,

and the transaction committed. RabbitMQ then delivers the committed event to four independent consumers.

## Context

The problem asks for a bed search under 100 ms, zero double allocations across two instances, and correct behaviour
when Redis is paused or restarted and when an instance or the broker stops. Redis is mandatory and should hold live
availability and short-lived reservations.

## Alternatives considered

**A. PostgreSQL only, Redis as a passive cache.** Correct and simple: `SELECT ... FOR UPDATE SKIP LOCKED` picks a bed.
Rejected as the main path because the problem wants Redis to hold live availability and holds, and every search
would hit PostgreSQL. Kept as the degraded-mode path, so it is built and tested anyway.

**B. Redis decides, then publish directly to RabbitMQ.** Fastest to write. It fails the judges' chaos tests: a Redis
restart loses all holds; an API crash between the Redis pop and the publish loses the event; a missed keyspace
notification leaks a bed; a paused Redis resumes with stale data and can hand out a bed PostgreSQL already gave away.

**C. Hybrid (chosen).** Redis GEOSEARCH plus one Lua script give the fast common case. PostgreSQL's conditional update
makes the decision. The transactional outbox makes "accepted" mean "committed", independent of the broker.

## Why C

- Safety does not depend on Redis being correct. A stale Redis can only propose a bed that the conditional update
  then rejects; the API consumes the hold and tries the next candidate.
- Recovery is fenced: a shared `cache_state`/`cache_generation` in PostgreSQL closes the Lua path for every instance
  while Redis is rebuilt into a new key namespace.
- Events are never lost between the decision and the broker, because they are rows in the same transaction.

## Consequences

- Each allocation costs one PostgreSQL transaction even on the fast path (measured reserve p95 at idle: see README).
- Two places know availability; drift is repaired by DB-verified reconciliation (bed tracker per event, full check
  every 15 s, orphan hold scan every 5 s). Drift can cost a retry, never a double allocation.
- Degraded mode is slower but has the same correctness: verified in the Redis pause and restart runs
  (370 and 139 SQL-path allocations, 0 duplicate rows).
