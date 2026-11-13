# Decisions

Each entry: context, options, choice, reason, consequences. C1 to C22 are the corrections to the architecture
blueprint that the build must not revert. D-entries are decisions made during the build. Evidence refers to
`docs/BUILD_LOG.md` entries and to JSON results under `results/`.

## Pinned versions

| Component | Pin | How verified |
|---|---|---|
| Python image | `python:3.12.8-slim` | `docker pull` succeeded (BUILD_LOG #2) |
| PostgreSQL | `postgres:16.6` | pulled, healthy in Compose |
| Redis | `redis:7.4.1` | pulled; started with `--save "" --appendonly no --notify-keyspace-events Ex` |
| RabbitMQ | `rabbitmq:4.0.5-management` | pulled; classic queues with `x-max-priority` declared and verified through the management API (test_retry_queues_route_back_to_their_own_service_queue) |
| nginx | `nginx:1.27.3-alpine` | pulled; `resolve` upstream parameter accepted by `nginx -t` |
| flask 3.1.0, gunicorn 23.0.0, redis 5.2.1, pika 1.3.2, psycopg[binary] 3.2.3, psycopg_pool 3.2.4, httpx 0.28.1, pytest 8.3.4 | `requirements.txt` | installed in the image and imported (BUILD_LOG #3) |

## Connection pools and concurrency

| Process | PostgreSQL pool | Other |
|---|---|---|
| api-1, api-2 | min 8, max 24 | gunicorn gthread, 1 process, 128 threads; admission semaphore 16 (D-ADMIT); Redis pool max 256, 150 ms timeout |
| worker-1, worker-2 | min 8, max 14 | 4 consumer threads, outbox publisher, reaper, cache manager; each thread has its own pika connection |
| tools | max 24 | simulators and pytest |

Total worst case 2x24 + 2x14 + 24 = 100, under `max_connections=200`.

## C1 Stack: Python, Flask, gunicorn, vanilla JS, SVG map, SSE
- Context: the blueprint used Node, React, Leaflet and WebSocket.
- Options: (a) the blueprint stack; (b) Flask under gunicorn with hand-built SSE and an inline SVG map.
- Choice: (b). Reason: required stack; no internet at the venue (no tiles); SSE needs no library.
- Consequence: one Python process per instance, so CPU-bound request work is limited by the GIL (D-ADMIT).

## C2 PostgreSQL is the final arbiter
- Options: (a) trust a Redis epoch check; (b) conditional `UPDATE beds ... WHERE status='FREE'` plus unique index on
  HELD or CONFIRMED reservations, in the same transaction as the reservation and outbox rows.
- Choice: (b). A paused Redis keeps its memory, so an existence check passes on stale data.
- Evidence: stampede (50 users, one bed) produced one holder; Redis pause and restart runs had 0 duplicate rows.

## C3 Lua returns plain arrays
Tables with `ok`/`err` become status or error replies and drop other fields. All three scripts return arrays such as
`{'RESERVED', rec}`. Evidence: tests/test_lua.py.

## C4 Every hospital in the radius goes to Lua
Passing only the 15 nearest can produce a false "no bed". `GEOSEARCH ... BYRADIUS 50 km ASC` returns all of them;
the SmartMatch filter is applied before the Lua call.

## C5 Retry queues return to the failing service queue through the default exchange
Each `q.<service>.retry.{1s,3s,9s}` has `x-dead-letter-exchange: ""` and `x-dead-letter-routing-key: q.<service>`.
Evidence: test_retry_queues_route_back_to_their_own_service_queue; poison scenario (A) retried only in notifications.

## C6 Bindings `bed.#` and `transfer.#`
`bed.*` would not match `bed.reservation.expired`.

## C7 Four priority levels
`x-max-priority: 4` (routine 1, standard 2, urgent 3, critical 4).

## C8 / C16 Per-consumer-group patient ordering cursors
- `patients.last_seq_no` is only the generator. `consumer_patient_offsets(consumer_group, patient_id)` is the cursor.
- `seq <= cursor`: duplicate for that group. `seq == cursor+1`: apply, advance cursor and inbox in one transaction.
  `seq > cursor+1`: postpone with a confirmed delayed republish, then ack.
- All four groups receive the complete patient stream: notifications and audit bind `#`; the saga queue binds `bed.#`
  in addition to `transfer.#` (D-SAGA-BIND), and ignores bed events as no-ops that still advance its cursor.
- Evidence: tests/test_ordering.py (independent cursors, no-op advancement); surge published seq 3,2,1 and every
  group applied 1,2,3.

## C9 Redis persistence off
Redis is always rebuilt from PostgreSQL; a restart is an empty cache, never a stale snapshot.

## C10 PostgreSQL scan is the expiry authority
The reaper in both workers scans every 250 ms, locks patient, bed, reservation in the standard order and re-checks
`status='HELD' AND expires_at <= clock_timestamp()`. Redis `release` only after PostgreSQL shows the bed FREE and
unowned; otherwise `consume`. Evidence: expiry scenario 0.19 s from DB expiry to FREE.

## C11 Stampede uses 50 user ids; rate limit checked separately
The same scenario sends 23 requests from one user and expects 429 with `Retry-After`.

## C12 Degraded mode covers search, rate limiting, idempotency and the live feed
SQL Haversine search with the same SmartMatch predicate, `rate_limit_windows` atomic upsert, unique
`(actor_id, idempotency_key)`, and SSE fed by a PostgreSQL watermark poller.

## C13 Scenarios 5 and 6 show commands
No Docker socket is mounted. The ops page shows the commands with a copy button; `sim.broker_restart` and
`sim.failover` print when to run them and measure the effect.

## C14 Blueprint numbers are goals
Only measured numbers appear in README and UI.

## C15 Shared, fenced cache recovery
- State `cache_state` (DEGRADED 0 / REBUILDING 1 / READY 2) and `cache_generation` live in PostgreSQL
  `system_state`. Redis holds the pointer `medigrid:cache_ready_generation`. Lua compares the pointer with the
  generation the caller read from PostgreSQL.
- Any Redis error marks DEGRADED (conditional update, logged in `cache_transitions`). A worker with advisory lock
  4242 increments the generation, sets REBUILDING, deletes the pointer, bulk-loads `mg:<gen>:*` from a REPEATABLE
  READ snapshot, then performs the fenced switch (D-EPOCH): re-reads PostgreSQL under an exclusive fence, reconciles
  the namespace, sets the pointer and READY in one short transaction.
- Deviation from the prompt text "recheck the change counter; if it changed, repeat": under continuous load the
  counter always changes, so the loop would never converge. Holding the fence while re-reading gives the same
  guarantee (no committed change is missed) and always terminates. Evidence: Redis pause rebuild 0.32 s and Redis
  restart rebuild 0.30 s while 16 clients were reserving.

## C17 One active transfer per patient, fixed lock order, deadline predicates
`uq_patient_active_saga`; lock order patient, saga, beds sorted, reservations sorted; accept/reject/timeout re-check
state, version and `deadline_at > clock_timestamp()`. Evidence: tests/test_races.py.

## C18 Unique active reservation per bed across HELD and CONFIRMED
`uq_res_one_active_per_bed ON reservations(bed_id) WHERE status IN ('HELD','CONFIRMED')`. Walk-in admissions also
create a CONFIRMED reservation so every OCCUPIED bed is covered. Evidence: test_unique_index_covers_held_and_confirmed.

## C19 Unknown commit outcome reconciled by persisted key
On a connection error after the transaction was sent, the API takes the `(actor, key)` advisory lock and reads the
reservation. Found: return it. Not found: settle the Redis hold against the bed row, 503 retry-safe. PostgreSQL
unreachable: leave the hold for the orphan scan, 503. A `PoolTimeout` (no connection obtained) is classified as
"not sent" (D-POOLTIMEOUT). Evidence: failover phase D, test_ambiguous_commit_is_reconciled_by_key.

## C20 Confirmed retry and DLQ handoff; same-queue priority test
Retry and DLQ publishes use confirms and `mandatory=True`; the original is acked only after the publish returned.
Scenario 2 pauses every consumer of `q.transfer_saga` through a shared flag, verifies 0 consumers with a passive
declare, fills 10,000 routine + 1 critical into that queue, then lets exactly one worker consume with prefetch 1.

## C21 Event parity by distinct event id per consumer group; durable UI reconciliation
`chaos.parity()` joins committed outbox rows with each group's inbox, counting dead-lettered rows separately.
The SSE hub re-reads PostgreSQL counts when the watermark moves and at least every 2 s.

## C22 SmartMatch
Hard filter by verified active capabilities and permitted bed type before distance ranking; the same SQL predicate
(`smartmatch.ELIGIBLE_SQL`) is used in degraded search and inside the allocation transaction. Unknown condition
fails closed. Seed capability data is immutable for the PoC (no capability edit API).

---

## D-EPOCH Change watermark is a sequence; fencing uses an advisory lock
- Context: first design bumped a `system_state` row in every availability transaction; all writers queued on it.
- Options: (a) hot row; (b) `nextval('pg_epoch_seq')` plus `pg_advisory_xact_lock_shared(4243)` in writers and
  `pg_advisory_xact_lock(4243)` in the fenced READY switch.
- Choice: (b). Writers do not block each other; the switch still waits for in-flight writers and blocks new ones.
- Consequence: `nextval` is visible before commit, so the SSE poller also re-reads every 2 s.

## D-NOTIFY No `pg_notify` in allocation transactions
NOTIFY serialises committing transactions on a global queue lock. The outbox publisher polls the partial index
`idx_outbox_pending` every 50 ms instead. Evidence: BUILD_LOG #10 to #12.

## D-RT Fewer round trips per allocation
Measured 1.05 ms per round trip between containers with the per-checkout pool health check, 0.39 ms without it.
The allocation transaction was reduced to about 8 statements: one lock+idempotency query, one patient upsert that
also reports an active hold, the conditional bed update, the reservation insert, one CTE for patient sequence plus
outbox row, and one fence statement. Autocommit pool, timeouts set once per connection.

## D-ADMIT Admission control per API instance
- Context: bench run 1 at 300 concurrent reserves produced pool timeouts; 128 runnable threads made connection
  holders wait for the GIL.
- Options: (a) bigger pool; (b) fewer gunicorn threads (SSE needs threads); (c) semaphore of 16 around `/api/*`
  except `/api/stream`.
- Choice: (c). Excess requests wait without holding a connection; after 10 s they get 503 `Retry-After: 1`.

## D-POOLTIMEOUT PoolTimeout is not an ambiguous commit
`psycopg_pool.PoolTimeout` subclasses `OperationalError`. Treating it as "commit unknown" made the handler ask for
another connection and deepened starvation. It is now handled first as "not sent".

## D-KEEPALIVE gunicorn keep-alive 75 s, nginx upstream keep-alive 50 s
With 5 s on gunicorn, clients reusing an idle connection got resets (surge run 2: 11 of 300). The proxy must close
idle upstream connections before the server does, or POSTs can fail without retry.

## D-NGINX-DNS nginx re-resolves API names every 2 s
After `docker compose up -d` recreated the API containers, nginx kept old IPs and returned 502 for everything
(BUILD_LOG #13). `resolver 127.0.0.11 valid=2s` with `zone` and `server ... resolve` (open-source since 1.27.3).

## D-UI-COUNTS UI counts come from PostgreSQL
Snapshot and watermark counts use one GROUP BY over 4,597 beds (a few ms). The ops page shows Redis counts separately
as the cache view. This avoids flicker from in-flight provisional holds and keeps the UI authoritative.

## D-COUNTS-SCARD Free counts in Redis are set cardinalities
The reference Lua kept a separate counts hash with HINCRBY. Using `SCARD` of the free set removes a second structure
that could drift ("recompute, not blind increments").

## D-SAGA Compensation in one transaction
Reject and timeout run compensation in the same transaction that decides them, so `REJECTED_COMPENSATING` and
`TIMED_OUT_COMPENSATING` are never persisted. The states stay in the CHECK constraint and the unique index.
Transfer target holds expire 30 s after the acceptance deadline so the saga deadline decides first.

## D-SAGA-BIND Saga queue binds `bed.#` too
Gives the saga group a complete patient stream for its cursor (C16). Bed events are no-ops there.

## D-PRIORITY-DIRECT Scenario 2 publishes to the queue through the default exchange
Routing 10,000 test events through the topic exchange would copy them into all four services. Publishing to
`q.transfer_saga` directly (confirmed, mandatory) fills exactly the queue under test.

## D-FAULTS Fault flags in PostgreSQL
`control_flags` holds `fault:poison`, `fault:crash_after_hold`, `fault:ambiguous_commit`, `fault:force_sql` and
`pause:<queue>`. Honoured only with `CHAOS_ENABLED=1`; set only by an operator identity.

## D-SCHEMA Additions to the section 3.5 schema
`condition_catalog` (labels, fail-closed lookup), `reservations.purpose/condition_code/alloc_path/updated_at`,
`transfers_saga` idempotency columns, `outbox_events.published_at`, and operational tables in `db/002_operations.sql`.

## Assumptions
- A1 Hospital staff identity acts only on its own hospital; operator can act everywhere.
- A2 Walk-in admission creates a CONFIRMED reservation with ambulance `WALK-IN`.
- A3 A cancelled or expired dispatch leaves the patient `IN_TRANSIT` (still in the ambulance) so a new dispatch is
  allowed.
- A4 Rate limit 20 reserve requests per 10 s per user id, in Redis when READY and in PostgreSQL otherwise. The two
  counters are separate, so a user can get up to one extra window during a mode switch.
- A5 Search radius 50 km; up to 150 km only for an operator identity ("operator-reviewed expansion").
- A6 The seed has no outbox events (initial state). Every change after seeding produces events.
- A7 Condition-to-capability and condition-to-bed-type rules are illustrative synthetic data, not clinical guidance.
