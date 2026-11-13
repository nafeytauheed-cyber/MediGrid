# PS04 MediGrid: build plan (Approach C)

This file is the order of work and the reasoning. The full specification (stack, architecture, schema, scripts, UI rules, docs rules) is in PS04_PROMPT.md. Both go to the AI together.

The architecture is fixed: Approach C from the architecture blueprint (PostgreSQL-authoritative hybrid fast path, transactional outbox, priority topology). Section 4 lists where I corrected or adapted that blueprint and why.

## 1. What gets marks

Weightage from the PS: Foundation 35, Resilience 30, Production Readiness 25, Design Defence 10.

"Marks are awarded only for behaviour the judges can observe." Every requirement ends in something we can show live: a script, a dashboard number or a screen.

| # | PS target | Pass line (from the PS) | Chaos scenario that shows it |
|---|-----------|-------------------------|------------------------------|
| T1 | Bed change visible city-wide | within 2 s | all scenarios (two browsers, two instances) |
| T2 | Bed search p95 | under 100 ms | scenario 1 |
| T3 | Double allocations | zero | scenarios 1 and 6 |
| T4 | Lost events across a broker restart | zero | scenario 5 |
| T5 | Critical ahead of 10,000 routine | critical first | scenario 2 |
| T6 | Unconfirmed reservation released | within 5 s of expiry | scenario 3 |
| T7 | Instance stopped, users continue | within 5 s | scenario 6 |
| T8 | Redis interrupted | no double allocation | scenario 6 |

The architecture blueprint quotes tighter numbers (for example under 2 ms for the Lua script, under 25 ms p95, release under 2 s). Those are goals. The pass lines are the PS numbers. The README only reports numbers we measured.

## 2. Selected architecture in short

Approach C, with SmartMatch. A deterministic condition-to-specialty rule first excludes hospitals that cannot meet the selected synthetic patient condition; Redis then finds the nearest eligible candidate bed (GEOSEARCH, followed by Lua rate limiting, idempotency and provisional hold). PostgreSQL makes the decision final: a conditional update on the bed row, the reservation row and the outbox row in one transaction. RabbitMQ delivers every event to four independent workers. If Redis is unavailable, the API drops to Degraded Safe Mode and allocates in PostgreSQL with `FOR UPDATE SKIP LOCKED`. On Redis recovery, every instance uses SQL while a shared fenced REBUILDING state blocks Lua reservations; Redis is rebuilt and reconciled before READY. Ambiguous commits are reconciled by PostgreSQL idempotency key, never by blindly returning a bed to Redis.

Everything runs in Docker Compose: `nginx`, `api-1`, `api-2`, `worker-1`, `worker-2`, `postgres`, `redis`, `rabbitmq`, plus one-shot `migrate`, `seed`, and `tools` jobs. One Python image serves the API and worker roles (selected by an environment variable). Stopping a service during the demo is `docker compose stop api-1`.

Who decides what:

- This is **not Redis/PostgreSQL dual authority**: PostgreSQL is the single authority; Redis is a fast, derived selection and hold layer. SmartMatch capabilities and bed eligibility are checked against PostgreSQL before committing.
- PostgreSQL is the final arbiter. No allocation exists unless the conditional `UPDATE beds ... WHERE status = 'FREE'` matched one row, the active HELD/CONFIRMED uniqueness constraint passes, and the reservation and outbox commit atomically.
- Redis is a derived cache and a fast candidate selector. It runs with persistence off, so a restart means empty, and an empty Redis is rebuilt from PostgreSQL.
- RabbitMQ is the delivery mechanism. An event counts as accepted once its outbox row is committed in PostgreSQL.

Three paths for the architecture diagram:

1. Request path: browser, nginx, API instance, Redis (GEOSEARCH plus Lua), PostgreSQL transaction (bed, reservation, outbox), response.
2. Message path: outbox (confirmed publish plus authoritative outbox sweeper), `medigrid.events` topic exchange, four queues, retry queues, DLQ.
3. Update path: notification worker, Redis pub/sub, API instances (100 ms coalescer), SSE, browsers.

## 3. Why C and not A or B (for the defence)

- A (PostgreSQL only, Redis as cache): correct, but the PS asks Redis to hold live availability, and pool contention under a 500-ambulance surge puts p95 at risk. We keep A's mechanism as the outage fallback.
- B (Redis only plus direct publish): fastest to write, fails the chaos tests: a Redis restart loses reservations, an API crash between Redis and publish loses the event, a missed expiry notification leaks a bed.
- C keeps B's speed on the normal path and A's guarantees at the decision point.

## 4. Changes I made to the architecture blueprint

These are in the prompt as "do not revert" rules. Each one gets a DECISIONS.md entry.

| # | Blueprint says | Change | Reason |
|---|----------------|--------|--------|
| 1 | Node, TypeScript, Fastify, React, Vite, Leaflet, WebSocket | Python, Flask under gunicorn, vanilla JS, custom SVG map, SSE | You asked for Flask. The PS forbids ready-made queue and workflow frameworks, and a hand-built SSE fan-out needs no extra library. Leaflet and tiles need internet. The blueprint's own risk 4 says fall back to an inline SVG map. |
| 2 | Epoch check decides whether Redis is safe | Add PostgreSQL as final arbiter: conditional bed update and a unique index on active holds | After `docker pause redis` the memory is intact, so the boot-epoch key still exists and the existence check passes while Redis is stale. The conditional update makes a double allocation impossible even then. |
| 3 | Lua returns `{ ok = ..., data = ... }` | Return plain arrays | Redis turns a table with an `ok` field into a bare status reply and drops `data`. A table with `err` becomes an error reply. |
| 4 | Pass the 15 nearest hospitals to Lua | Pass every hospital in the radius (default 40) | During a surge the 15 nearest can all be full while farther hospitals have beds, which would return a false "no bed". |
| 5 | Retry queues dead-letter to `medigrid.events` | Per-service retry queues that dead-letter to the default exchange with the service queue as routing key | Dead-lettering back to the topic exchange redelivers to all four service queues, not just the failing one. |
| 6 | Bindings `bed.*` and `transfer.*` | `bed.#` and `transfer.#` | `bed.*` matches one word, so `bed.reservation.expired` would never reach the bed tracker. |
| 7 | `x-max-priority: 10` | 4 levels (routine 1, standard 2, urgent 3, critical 4) | The RabbitMQ priority guide recommends a few levels for classic queues. |
| 8 | Ordering by comparing against `last_seq_no` | Add per-consumer-group patient offsets | `last_seq_no` is the generator counter. Each consumer group needs its own "applied up to" value, otherwise nothing is ever out of order. |
| 9 | Redis may restart with a stale snapshot | Redis persistence off | Removes the stale-snapshot case. Redis is always rebuilt from PostgreSQL. |
| 10 | Reaper uses the Redis expiry set | PostgreSQL scan is the expiry authority. The Redis set is used to find orphan holds. Release script uses the already defined `release` and `consume` modes with DB-verified checks. | A hold can be confirmed in PostgreSQL just before a crash. Freeing it from Redis alone would advertise an occupied bed. |
| 11 | 50-ambulance stampede at a 20 requests per 10 s per-user limit | The simulator uses 50 different user ids. A separate check shows one user getting 429. | Otherwise 30 of the 50 requests are rejected by the limiter and the test proves nothing about allocation. |
| 12 | Degraded mode only covers allocation | Degraded mode also covers search (SQL Haversine), rate limiting (atomic shared PostgreSQL fallback), idempotency (unique key in PostgreSQL) and the live feed (SSE polls PostgreSQL) | The blueprint leaves these on Redis, which is down. |
| 13 | One-click chaos buttons for every scenario | In-app buttons for scenarios 1 to 4. Scenarios 5 and 6 show the exact `docker compose` command to copy. | A button that stops containers needs the Docker socket mounted into a container, which gives that container control of the host. |
| 14 | Scorecards and latency figures | Not reproduced | They are estimates. |

### Additional safety corrections (C15–C21; all mandatory)

| ID | Correction | Live evidence required |
|---|---|---|
| C15 | Shared Redis DEGRADED/REBUILDING/READY state, generation fencing and DB-verified hydration; SQL-only while uncertain | Concurrent bookings during pause, restart and rebuild never double-book |
| C16 | Group-specific patient ordering cursors and no-op advancement for fully routed events | Saga and bed-tracker each process the same event exactly once |
| C17 | Unique active saga per patient; deterministic row lock order; deadline/state predicates for accept, reject, expiry and discharge | Races yield one legal transition, never two |
| C18 | Unique active reservation per bed across HELD and CONFIRMED; consistent bed/reservation links | SQL constraint rejects competing state |
| C19 | Unknown PostgreSQL commit outcome reconciled using persisted actor/key/fingerprint | API kill between Redis and SQL response cannot reoffer occupied bed |
| C20 | Retry/DLQ republish only acked after confirm; priority test on the **same queue** with all consumers quiesced | Restart during retries and 10k-backlog priority order logged |
| C21 | Outbox/inbox reconciliation by distinct event IDs and consumer groups; snapshot fallback when SSE pub/sub misses deltas | No falsely passing event parity or stale UI |
| C22 | SmartMatch: hard-filter verified hospital capabilities and compatible bed types before nearest-bed ranking; same SQL rule during Redis outage; PostgreSQL final eligibility validation | Nearby incompatible hospital is skipped; farther capable hospital reserved; no-match is explicit |

## 5. Setup order (before any feature work)

Do not move on until each line works.

1. Machine: 16 GB RAM is comfortable, 8 GB is tight (eight containers plus the simulator). On Windows, Docker Desktop needs WSL2.
2. Install Docker Desktop. Run `docker run hello-world`.
3. Add `.gitattributes` with `* text=auto eol=lf` before the first commit, and use no shell entrypoint scripts. Windows line endings break shell scripts inside Linux containers.
4. `docker-compose.yml` with `postgres`, `redis` (persistence off, keyspace events `Ex`), `rabbitmq` (management image), `api-1`, `api-2`, `worker-1`, `worker-2`, `tools`, `nginx`. Pinned image tags that actually pull. Database ports bound to 127.0.0.1. Nginx published on all interfaces, port 8080.
5. A trivial `/healthz` on the API. Open `http://<laptop-ip>:8080/healthz` from your phone on the same Wi-Fi. If it fails, allow port 8080 in the OS firewall. Some venue Wi-Fi blocks device-to-device traffic, so keep a phone hotspot as the backup.
6. `docker compose stop api-1`, reload the page, confirm nginx serves from `api-2`.
7. `git init`, `docs/` folder, start `BUILD_LOG.md` and `DECISIONS.md`.

## 6. Stages and gates

Time split assumes a 4-hour run. Do not start a stage until the previous gate passes. The blueprint's roadmap assumes four people working in parallel, so treat these as relative effort for one agent.

| Stage | Time | Work | Gate |
|-------|------|------|------|
| 0 | 0:00-0:30 | Setup from section 5, traceability table, empty migrations | Phone reaches the app, nginx fails over, all containers healthy |
| 1 | 0:30-1:10 | DDL incl. active reservation/saga constraints, deterministic seed (40 hospitals, 4,000+ beds) with varied specialty tags, six condition rules, and a near-incompatible/far-compatible SmartMatch fixture, SQL-first specialty-filtered search and reserve/confirm/expiry vertical slice, Redis candidate selection and Lua, shared fenced cache recovery, shared SQL degraded limiter, unknown-commit reconciliation | Measured search (T2); stampede plus SQL invariant (T3); Redis pause/restart/rebuild under concurrent requests (T8) |
| 2 | 1:10-1:50 | Outbox with confirmed publish, four independent consumers, per-group ordered inbox, confirmed retry/DLQ handoff and replay, same-queue priority test, broker reconnect | Per-group event-ID parity (T4); critical ahead of 10,000 in same paused queue (T5) |
| 3 | 1:50-2:20 | Authoritative SQL expiry vs confirmation; transfer saga with unique active transfer, fixed lock order, accept/reject/timeout/discharge conflict tests, safe Redis reconciliation | T6 plus accept-vs-expire, double transfer and reject/timeout compensation tests |
| 4 | 2:20-3:10 | SmartMatch condition selector and matching rationale in dispatch/control UI, API routes, SSE coalescer, PostgreSQL snapshot watermark/reconciliation, reconnect jitter, control map, dispatch and hospital console | T1 across two instances, forced missed pub/sub delta recovery, failover T7 |
| 5 | 3:10-3:40 | SmartMatch demo/assertions (farther capable hospital wins; no-match; Redis outage), ops dashboard, chaos controls, simulator (300+ concurrent ambulances, duplicates, out-of-order, slow consumer), TTL override, measured end-to-end p95 | Six scenarios run; report PASS/FAIL/NOT RUN and raw evidence honestly |
| 6 | 3:40-4:00 | Diagram, decision record, README, defence notes, final audit of the traceability table | Every row marked verified, not run or failed |

If time runs short, cut in this order (last item cut first): UI polish, moving ambulances on the map, keyboard shortcuts beyond tab order and arrows, keyspace-notification accelerator, extra consumers. Never cut SQL correctness, event deduplication, retry confirmations, the priority demonstration, transfer compensation or safety tests. When time runs short, mark incomplete targets NOT RUN rather than pretending success.

Commit at the end of each stage with a plain message saying what changed.

## 6.1 SmartMatch innovation demonstration (additional; does not replace the six mandated chaos tests)

Use deterministic synthetic data: H-01 is closer and has a free ICU bed but **lacks** the `CARDIAC_EMERGENCY_CARE` capability; H-02 is farther away, has that verified capability, and has a free ICU bed. An ambulance reporting the `CARDIAC_EMERGENCY` demo condition must be matched with H-02 even though H-01 is geographically closer. Show the reason for H-01 exclusion and H-02 selection. Re-run in PostgreSQL-only degraded mode and under concurrent ambulance demand; neither mode may book an incompatible hospital. A condition without a capable hospital/free compatible bed must return a clear no-match result, never silently fall back to the closest hospital. These are **synthetic rules for demonstrating system behavior**, not medical diagnosis or validated clinical triage. The six judge chaos scenarios remain the first priority.

## 7. Chaos scenarios (the demo)

| # | Scenario | Trigger | What the judge sees | Pass line |
|---|----------|---------|---------------------|-----------|
| 1 | Last-bed stampede | Button: set H-01 to one free ICU bed, fire 50 requests from 50 users across both instances | Exactly one reservation for that bed, the rest rerouted to other beds or refused; invariant badge "Double allocations: 0" | 0 double allocations, search p95 under 100 ms |
| 2 | Routine flood then critical | Button: stop all consumers of one priority-enabled queue, confirm 10,000 routine events enqueued there, enqueue a critical event in that same queue, resume one consumer with prefetch 1 | Confirmed same-queue backlog and event-ID order prove critical handled ahead of waiting routine messages | Critical processed before the routine backlog |
| 3 | Reservation expiry | Button: TTL override to 10 s, make a hold, do not confirm | Countdown to 0, bed flips from reserved to free | Released within 5 s of expiry |
| 4 | Poison message and consumer crash | Button: send event with the crash flag; or `docker compose kill -s SIGKILL worker-1` | Show separate SIGKILL redelivery and handler-error confirmed 1/3/9 s retry tiers, DLQ and safe replay | 0 lost events |
| 5 | Broker restart | Command shown: `docker compose restart rabbitmq` while the simulator sends admissions | Outbox rows pile up, drain after recovery, distinct per-group inbox IDs match committed bound outbox IDs (or show explicit unresolved IDs) | No unresolved committed events; event-ID parity per consumer group |
| 6 | Instance stop and Redis pause | Commands shown: `docker compose stop api-1` and `docker compose pause redis` for 10 s | Connection badge flips to reconnecting then connected on the other instance; "Degraded safe mode" banner; shared fenced rebuilding, SQL fallback, recovery without double allocation | Failover within 5 s, 0 double allocations through Redis pause/restart and rebuild |

Every pass or fail shown on screen comes from a real query or a real assertion, never a hard-coded label.

## 8. Logs and documents kept during the build

- `docs/BUILD_LOG.md`: time, stage, action, reason, command, real result, status (verified, not run, failed). Failures stay in.
- `docs/DECISIONS.md`: one entry per decision, including C1–C22 from section 4, plus an Assumptions section, pinned versions and pool sizes.
- `docs/TRACEABILITY.md`: every PS requirement, T1 to T8, and the six scenarios, with where it is built and how it is verified.
- `docs/DECISION_RECORD.md`: the one-page record the PS asks for. Subject: dual authority (Redis fast path, PostgreSQL final arbiter).
- `docs/architecture.svg`: the three paths from section 2.
- `docs/DEFENCE_NOTES.md`: why C and not A or B, why a conditional update backs the Lua script, why SSE, what happens when Redis is paused versus restarted, what the priority queue can't guarantee, plus how it would grow.
- Runtime logs: JSON lines per service.
- `README.md`: PS04 and this solution only.

## 9. Where the blueprint's ideas come from

The blueprint cites open-source projects as inspiration: ohcnetwork/care (transfer state names), nobed-ai (capacity and referral screen layout), task-processing-system (outbox, priority queue, DLQ replay, crash toggle), laravel-amqp (per-delay queues), caracal (Lua structure with server time). Use the ideas only. Do not copy code from any of them.

The PS references still apply: RabbitMQ reliability, confirms, priority and dead-letter pages; Saga and Transactional Outbox patterns; Redis Geospatial; WCAG 1.4.1.

## 10. Risks

- **RabbitMQ not ready at start or after restart:** every process needs a reconnect loop with backoff that redeclares exchanges, queues and bindings.
- **Priority queue timing:** prefetch hides priority and already delivered routine messages cannot be preempted. Stop *every* consumer of the same queue for scenario 2; test early.
- **Redis paused versus restarted:** they fail differently (stale memory versus empty memory). Test both in stage 1.
- **Postgres connection limit:** two APIs, two workers and the simulator share 100 connections by default. Keep pools small and log the sizes.
- **Redis and Postgres drifting apart:** a crash or unknown COMMIT outcome after a Redis pop needs read-back and DB-verified reconciliation; a conditional update alone prevents double allocation but not stale free-bed counts. Test by killing an API mid-request and during hydration.
- **SSE reconnect herd:** all browsers reconnect at once when an instance dies. Server-sent jittered retry and a one-second cached snapshot handle it.
- **Venue Wi-Fi blocks LAN access:** use a phone hotspot.
- **One laptop runs everything:** keep the surge sized to the machine and note the specs next to measured numbers.
- **SmartMatch clinical suitability:** synthetic capability tags are illustrative and cannot establish actual clinical suitability; never advertise clinical validation or silently route to an incompatible hospital.
- **Numbers that weren't measured:** none go in the UI or README.

## 10.1 Non-negotiable test matrix

- SmartMatch: farther capable H-02 selected instead of nearer incompatible H-01 for cardiac demo; no-match; capability/bed-type rejection; SQL-only fallback; concurrent specialty-filtered stampede.
- Concurrent actor/key replay with same and different payloads; one committed reservation only.
- 50 requests for the same last ICU bed, including two API instances and forced ambiguous DB commit.
- Confirm versus expire at the cutoff and redis cache expiry; no OCCUPIED bed freed.
- Two simultaneous transfers for one patient; accept versus reject/timeout; discharge during transfer.
- Both bed-tracker and saga process same sequenced patient event without advancing each other’s offset; missing predecessor is visible.
- Consumer restart before ack, confirmed retry publish failure, DLQ replay, RabbitMQ restart with delayed retries present.
- Stop all consumers, fill and confirm 10,000 routine messages in a single priority queue, then enqueue critical event there and verify order.
- Redis PAUSE, UNPAUSE, RESTART, concurrent SQL allocations and shared cache rebuild; recover missed SSE delta from snapshot.
- Each benchmark captures actual latency distribution, failures, environment, time and sample size. Do not invent performance claims.

## 11. Open items

- Confirm SSE instead of WebSocket (all blueprint behaviours carry over).
- Confirm everything runs in Docker Compose, including the Flask instances.
- Confirm Redis with persistence off and the shared cache fencing protocol; if it is too risky to implement within four hours, keep PostgreSQL allocation authoritative and use Redis only as a verified candidate/cache.
- Pin PostgreSQL 16, Redis 7 and RabbitMQ versions and read that RabbitMQ version's priority page.
- Confirm Yuktik '26 allows AI assistance.
