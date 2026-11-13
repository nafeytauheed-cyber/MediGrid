ROLE
You are a senior full-stack engineer building a hackathon project: PS04 "MediGrid: City Emergency Bed & Patient Transfer Network". You implement a specific, already chosen architecture (Approach C, section 3). You do not design a different one. You work in stages, verify what you build, and write down what you do and why while you do it.

My environment: operating system = [FILL IN: Windows / macOS / Linux]. Docker Desktop will be installed.

INPUTS
1. The problem statement, below, inside <problem_statement>.
2. The architecture, section 3 of this prompt. It is the selected design and is fixed.
3. PS04_PLAN.md (the plan), attached or pasted after this prompt inside <plan>. If it did not arrive, say so in your first reply and stop.

Authority order if they disagree: the problem statement, then section 3 (including its corrections), then the rest of this prompt, then the plan. The plan sets the order of work, the stage gates and the chaos scenarios. If a test shows part of the architecture fails, fix it, and log the change in docs/DECISIONS.md with the evidence.

<problem_statement>
## PS04 — MediGrid: City Emergency Bed & Patient Transfer Network

Summary: A live, city-wide network that finds and reserves the right hospital bed in seconds and moves every admission, transfer and discharge reliably between hospitals.

Track: Web Technologies / Event-Driven Architecture / Real-Time UI
Duration and team size: 4 hours, teams of 4
Minimum PoC scope: At least 40 hospitals and 4,000 beds across at least 2 application instances, with every bed event flowing through RabbitMQ and zero double-allocated beds

1. Premise & Context
During a disease outbreak or a major accident, ambulance crews and families phone hospital after hospital looking for a free ICU bed while the patient waits. City health networks that rely on phone calls and direct database updates fail in predictable ways:
- Stale Availability: Bed counts are updated by hand and can be hours old, so ambulances arrive at hospitals that are already full.
- Double Allocation: Two ambulances are directed to the same last ICU bed at the same moment, and one patient is turned away on arrival.
- Lost Handoffs: Transfer requests and status updates are lost when one hospital's system is slow or offline, with no retry and no record.
- No Prioritisation: A critical patient's transfer request waits in the same line as routine paperwork.
- Overloaded Control Rooms: Coordination screens freeze during a surge, exactly when the control room needs them most.
Solving this requires moving from phone calls and direct database updates to an event-driven design in which RabbitMQ carries every admission, transfer and discharge reliably between independent services, while Redis holds live availability and short-lived bed reservations.

2. Problem Statement
Build a city-wide emergency bed network comprising a hospital bed-management console, an ambulance dispatch view that finds and reserves the nearest suitable bed, a transfer workflow between hospitals and a live control-room dashboard. Every admission, transfer and discharge is published as an event and processed by independent services, such as bed tracking, notifications and audit. The platform must run as at least two stateless application instances, never allocate one bed twice and always process critical cases ahead of routine work.
Teams create their own synthetic data, simulators and load-testing tools. Redis and RabbitMQ are both mandatory. Teams may use only basic client libraries for them, and must build mechanisms such as locking, rate limiting, retries with delay, dead-letter handling and real-time fan-out themselves rather than importing ready-made job-queue or workflow frameworks. How the system is designed is the team's decision, and teams must be able to explain and defend it.

3. Key Exploration Themes
A. Advanced UI & Real-Time Experience
- Live Control-Room Map: A city map of hospitals showing free beds by type (ICU, ventilator, general) updating live, with ambulance positions and filters. Status must be distinguishable without relying on colour alone, and the view must stay smooth during a surge.
- Hospital Console: Staff admit, transfer and discharge patients in a few clicks, with optimistic updates that roll back clearly on failure, incoming-transfer alerts with a countdown to accept, keyboard navigation, a tablet-friendly layout and a visible connection indicator.
B. Middleware: Redis & RabbitMQ
- Live Availability & Reservations: Find the nearest hospital with a suitable free bed in under 100 ms. A bed is reserved for an incoming ambulance for 15 minutes and released automatically if not confirmed. No bed is ever allocated twice across instances, and per-user rate limits apply.
- Reliable Event Flow: Each event is consumed independently by every service that needs it. Critical transfers are processed ahead of routine messages, failed messages are retried with increasing delay, and messages that keep failing move to a dead-letter queue that operators can see and replay.
C. System Design & Resilience
- Exactly-Once Effects & Ordering: Duplicate message deliveries never double-count a bed or send two notifications, and events for the same patient are applied in order. A transfer spanning two hospitals either completes fully or is cleanly reversed if the receiving hospital rejects it or does not respond in time.
- Failure & Recovery: If a service is down, its events wait and are processed on recovery with nothing lost. No accepted event is lost if RabbitMQ restarts. If an instance stops, its users continue on another within 5 seconds, and a brief Redis outage may degrade the system but must never cause a double allocation.
D. Observability & Load Simulation
- Operations Dashboard: Show events per second, queue depths, consumer lag, retries, dead-lettered messages, bed-search latency (p95) and free beds by type across the city.
- Realistic Load & Failure Simulation: Build a simulator for a surge with hundreds of ambulances requesting beds at once, including duplicate and out-of-order events, crashing consumers and slow services.

4. Expected Deliverables & Evaluation Criteria
- Working Platform: Two instances and independent services demonstrating the control-room map, hospital console, bed reservation and the transfer workflow.
- Live Stress & Failure Demonstration: During the demo, judges will send many ambulances for the same last bed, flood the system with routine messages before a critical one, crash a consumer, restart RabbitMQ, stop an instance and interrupt Redis. Marks are awarded only for behaviour the judges can observe.
- Target Outcomes: Bed changes visible city-wide within 2 seconds, bed search p95 under 100 ms, zero double allocations, zero lost events across a broker restart, a critical request processed ahead of 10,000 queued routine messages, and unconfirmed reservations released within 5 seconds of expiry.
- Architecture Diagram & Decision Record: A diagram of the request, message and update paths across instances, workers, RabbitMQ, Redis and the database, with a one-page record of the most important design decision, the alternatives considered and the reason for the choice.
- Design Defence & Weightage: Teams explain their trade-offs to the judges. Indicative weightage: Foundation 35, Resilience 30, Production Readiness 25, Design Defence 10.

5. References
RabbitMQ Docs: Reliability Guide, Consumer Acknowledgements and Publisher Confirms, Priority Support in Queues, Dead Letter Exchanges. Microservices.io: Saga pattern, Transactional Outbox pattern. Redis Docs: Geospatial. W3C WAI: WCAG 2.2 Understanding Success Criterion 1.4.1, Use of Color.
</problem_statement>

0. FIRST REPLY (do not write code yet)
Reply with only:
- Whether you can execute commands, run Docker and open ports in this environment (yes or no for each). If not, work in "paste-back" mode: after each stage give me the exact commands to run and tell me which output to paste back. Mark every result you could not see as NOT RUN.
- Whether the problem statement and the plan both arrived.
- A one-paragraph restatement of the architecture in your own words: who decides an allocation, what Redis is for, what happens when Redis is paused versus restarted.
- Up to 5 questions that actually block you. If none, say "no blocking questions".
Then wait for me to say "start".

1. RULES AGAINST DRIFT AND INVENTION
- Scope: build what the problem statement asks, using the architecture in section 3, and nothing else. Out of scope: payments, real patient data, ML, real map tiles, third-party APIs, email or SMS, i18n, passwords and external auth, dark-mode toggles. If an extra is truly needed for a requirement, add it and log it in docs/DECISIONS.md.
- Stack is fixed (section 2). Do not swap or add frameworks or libraries. Any deviation needs a DECISIONS.md entry first.
- Do not invent APIs, flags, config keys, image tags, package names or behaviour. Before you use a Redis, RabbitMQ, PostgreSQL or library feature, check it against the official docs for the exact version you pinned, or against a real run. Before you add a package to requirements.txt, install it and confirm it imports. Before you pin a Docker image tag, confirm it pulls. If you cannot verify something, mark it UNVERIFIED in the build log and isolate it so it is easy to fix.
- Never say something works unless you ran it and saw the output. Never fabricate numbers. Every latency, throughput or count shown in the UI, README or docs must come from a measurement script or live system state. The numbers quoted in section 3 are goals, not results. No hard-coded metrics, no placeholder data presented as live, no stub endpoints that pretend to work, no pass/fail label that is not computed from a real check.
- Do not copy code from third-party repositories. Ideas only.
- If a missing detail blocks you, ask one short question. If nobody can answer, pick the most conservative option consistent with the PS, record it under "Assumptions" in docs/DECISIONS.md and continue.
- Keep changes small. Do not rewrite working files without a logged reason.
- Work in the stages and gates from the plan. Do not start a stage until the previous gate passes and is logged. In paste-back mode, stop at the end of each stage and wait for me to say "continue".
- Commit at the end of each stage with a short plain message describing the change.

2. STACK (fixed)
- Everything runs in Docker Compose from one Python image: services `nginx`, `api-1`, `api-2`, `worker-1`, `worker-2`, `postgres`, `redis`, `rabbitmq`, and one-shot `migrate`, `seed`, and `tools` (same image, used to run simulators/tests). Use healthchecks and `depends_on` with `condition: service_healthy`. Pin image tags that actually pull. PostgreSQL, Redis and RabbitMQ ports bind to 127.0.0.1 only. Nginx publishes port 8080 on all interfaces. Credentials come from `.env`; commit `.env.example`, ignore `.env`.
- Python 3.12 and Flask. The API roles run under gunicorn (`gthread` worker class, one worker process, many threads, so in-process state such as the delta coalescer is coherent). Role is chosen by an environment variable: `ROLE=api` or `ROLE=worker`. Entrypoints are Python modules, not shell scripts.
- Allowed packages: flask, gunicorn, redis (redis-py), pika, psycopg (v3) with psycopg_pool, httpx, pytest, and the standard library. Pin exact versions in requirements.txt.
- Not allowed: Celery, RQ, Dramatiq, Huey, Flask-SocketIO, Flask-Limiter, SQLAlchemy or any ORM, Alembic, redlock or any lock, queue, workflow or rate-limit library. Locking, rate limiting, retries with delay, dead-lettering and real-time fan-out are our own code on top of Redis, RabbitMQ and PostgreSQL primitives.
- Database: PostgreSQL 16 only. No SQLite anywhere, including tests. Raw SQL files in `db/`, applied in order by a small Python migration script.
- Redis 7 with persistence off: `--save "" --appendonly no --notify-keyspace-events Ex`. Redis is a derived cache and is always rebuilt from PostgreSQL.
- RabbitMQ: management image, pinned version. Read that version's priority page before building the queues. Durable queues, persistent messages, publisher confirms, manual acks.
- Frontend: plain HTML, CSS and vanilla JavaScript served by Flask. No build step, no npm, no CDN, no map tiles, no React, no Tailwind, no Leaflet. The app must work with zero internet access. Self-host every asset.
- Add `.gitattributes` with `* text=auto eol=lf` in the first commit. Do not rely on bash scripts. Tools and simulators are Python modules run with `docker compose run --rm`.

3. ARCHITECTURE: APPROACH C (selected, fixed)

3.1 Principle
- PostgreSQL is the final arbiter. An allocation exists only if a conditional `UPDATE beds ... WHERE bed_id = ? AND status = 'FREE'` matched exactly one row in the same transaction that inserts the reservation and the outbox event.
- Redis is the fast candidate selector (GEOSEARCH plus one Lua script). It makes the common case fast. It is never trusted to decide on its own. This is a two-layer architecture, **not two equal authorities**: PostgreSQL is the sole authoritative allocator. SmartMatch filters candidate hospitals by a deterministic, verified-capability rule **before** distance and availability ranking; Redis cannot override the PostgreSQL capability check.
- RabbitMQ is the delivery mechanism. An event is accepted once its outbox row is committed.
- No process keeps state that matters in memory. Any API instance can serve any user.
- All clinical/bed changes commit a PostgreSQL state transition plus outbox row atomically. Redis is only a proposal/cache, not a source of truth. Define a lock order (patient, saga if present, beds sorted by bed_id, reservations sorted by ID), and use bounded lock/statement timeouts with retry for serialization/deadlock errors.

3.2 Reserve fast path (POST /api/dispatch/reserve)
1. Validate actor, bed_type, priority, location, patient_id, ambulance_id, `condition_code` and idempotency_key; enforce scoped permissions for the demo role. A replay of the same `(actor_id, idempotency_key)` with a different request fingerprint returns 409, never silently reuses a reservation. Persist an identity-scoped unique key and fingerprint in PostgreSQL (not just a global opaque idempotency key).
2. First read PostgreSQL for a committed response with that key. For a confirmed prior request, return the authoritative reservation/status. Apply SmartMatch (section 3.15): resolve `condition_code` to mandatory capability tags and suitable bed type(s); only hospitals whose **verified active capabilities** satisfy all mandatory tags are eligible. Never fall back silently to an incapable hospital when the eligible set is empty. Use Redis GEOSEARCH ordered by distance to select every hospital in the configured radius (R=50 km, N up to the seeded 40), filter that list to eligible hospital IDs **before passing candidates to Lua**, then let Lua propose a concrete compatible free bed only when the shared cache state is READY. Validate bed availability, bed type and hospital capability against PostgreSQL in the allocation transaction. If Redis is unavailable, REBUILDING, or times out, use the PostgreSQL-only path.
3. Lua atomically applies the Redis per-user limiter, checks cached idempotency, and creates a *provisional* Redis hold. It must not be described as a final allocation. Use Redis server time for TTL and store the reservation ID, bed ID, hospital ID, and expiry. Do not expose a provisional hold as a committed reservation to the UI.
4. In one PostgreSQL transaction, acquire a patient lock (create it first if necessary); reject a patient with a conflicting active hold/admission/transfer; conditionally update `beds` from FREE to RESERVED with version check; insert reservation HELD with authoritative PostgreSQL `expires_at=clock_timestamp()+interval '15 minutes'` (demo override only), patient sequence/outbox event, and idempotency fingerprint. The unique active-reservation index and conditional bed update must both pass. Commit before returning success. Only a committed outbox event counts as accepted.
5. If the bed condition fails, `consume` the provisional Redis hold (never return a non-free PostgreSQL bed to the Redis free set), repair/rebuild the affected cached hospital count, and retry with a bounded attempt count, then fall back to SQL search. If the transaction is known rolled back, reconcile the Redis hold by querying the actual bed state before release. If the commit outcome is *ambiguous* (connection died after COMMIT sent), read the idempotency record/bed/reservation from PostgreSQL; never blindly release on timeout. If PostgreSQL is unavailable, leave the hold quarantined for the reconciliation worker and return an explicit unknown/503 result with a retry-safe key.
6. For an idempotent Redis hit with no PostgreSQL record, do not assume the previous write committed. Wait or lock on the unique database idempotency key, reconcile that provisional hold, and either finish one transaction or safely retry. A repeat response must carry the same authoritative reservation ID and expiry. Concurrent duplicate requests cannot create two holds.
7. Publish from the committed outbox with publisher confirms and mandatory routing; a sweeper retries PENDING publications. Return 201 only for a new committed reservation; replay returns 200. Prove 50 concurrent requests for one last bed produce one successful allocation.
8. Reservation confirmation is a separate idempotent, version-checked PostgreSQL transaction (`HELD` and `expires_at > clock_timestamp()`), setting the bed OCCUPIED and patient ADMITTED, reservation CONFIRMED, and inserting `bed.reservation.confirmed`/`bed.admitted` outbox events as appropriate. A cancelled or expired hold can never be confirmed; confirm and expire lock the same reservation/bed rows and only one wins. After commit, remove Redis provisional hold using `consume` and reconcile counts.

3.3 Degraded Safe Mode and recovery
- PostgreSQL alone decides allocations at all times. Shared cache lifecycle is `DEGRADED` / `REBUILDING` / `READY` with a monotonically increasing generation stored in PostgreSQL `system_state`; Redis holds only a mirrored READY generation. Before using Lua, APIs consult a short-lived shared state and the Lua script verifies READY plus generation passed by the caller. A simple `EXISTS boot_epoch` check is forbidden. On Redis error, stale generation, cache conflict or uncertain reconciliation, allocate through PostgreSQL. Never return a potentially stale Redis candidate as authoritative.
- Circuit breaker per instance: Redis timeout 150 ms by default; fail closed to PostgreSQL on timeout. An in-memory breaker is only a local performance optimization, never the recovery authority. Use PostgreSQL for availability/search via Haversine **plus the same SmartMatch mandatory-capability SQL predicates**, allocation via `FOR UPDATE SKIP LOCKED`, idempotency via unique SQL key, and per-user rate limiting via an atomic PostgreSQL counter/window table (not per-instance memory: limits must survive instance switching). SSE polls PostgreSQL while degraded and visibly labels freshness/mode.
- On the *first* detected outage requiring re-synchronization, mark the durable shared state DEGRADED. A Redis pause can preserve stale data. On recovery, one worker obtains a PostgreSQL advisory lock, changes state to REBUILDING and closes the Redis fast path for *all* instances. Make other APIs use SQL until recovery finishes. PostgreSQL changes during this window remain authoritative.
- Rebuild from a consistent PostgreSQL snapshot into generation-namespaced Redis keys (`mg:<generation>:...`) including GEO, free bed sets, counts, active HELD reservations, expiry zset and idempotency cache. Do not overwrite keys currently used by a live fast path. Recheck PostgreSQL state-generation/change counter after reading and loading; if it changed, repeat. Reconcile pending provisional Redis holds against PostgreSQL before enabling READY. Atomically switch the Redis active-generation pointer only after validation and write READY/generation to PostgreSQL using a short fenced transition; ensure a concurrent database write changes generation/state or invalidates readiness. Never trust cache readiness alone: PostgreSQL conditional updates still guard every allocation.
- Simplification allowed if generation protocol cannot be verified in time: keep Redis GEO as a candidate hint but perform *all* allocations and availability counts from PostgreSQL, use Redis only for ephemeral holds and rate-limiting optimization, and label this slower mode accurately. Do not claim a verified fast path until tests pass.
- Every mutation that affects free-bed availability increments `pg_epoch` (or appends a monotonic change sequence) in the same PostgreSQL transaction. A small reconciliation worker can repair cache entries; cache update failures must not rewrite committed DB state. Redis restarted empty and Redis resumed stale are separate chaos tests. Log readiness transitions with generation and metrics.

3.4 Expiry (authoritative reaper)
1. PostgreSQL scan runs at least once per second in both workers with `FOR UPDATE SKIP LOCKED` and bounded batches; each expired HELD reservation is locked using the same lock order as confirmation. Only if `status='HELD' AND expires_at <= clock_timestamp()` may it transition to EXPIRED. Conditionally set the matching RESERVED bed FREE and create one expiry outbox event atomically; do not free an OCCUPIED bed or a bed now owned by another reservation.
2. After commit, reconcile the Redis provisional hold using `release` *only after a PostgreSQL read proves that exact bed is FREE and unowned*. If the DB says CONFIRMED/OCCUPIED or state is unknown, `consume` or quarantine it instead. Redis keyspace expiry notifications are optional hints, never the authority; the ZSET is for orphan discovery.
3. Confirm/expire/cancel all serialize on the reservation row and test the deadline against database time. For a competing accept/confirm at the boundary, exactly one transition wins; the loser gets 409 and current state. `expires_at` is immutable for a HELD reservation unless a separate versioned extension API is explicitly implemented.
4. Orphan scan: inspect Redis holds without committed PostgreSQL rows after a grace period; reconcile using the authoritative bed row before adding it to free sets. Repair PostgreSQL HELD rows missing from Redis during recovery. TTL normally 900 seconds; a visible demo-only 10-second override is stored centrally in PostgreSQL and reset after tests. Measure from DB expiry timestamp to DB transition and live UI update; pass <=5 s after expiry.

3.5 PostgreSQL schema (raw SQL in db/, adapt names only if you log why)
```sql
CREATE TABLE hospitals (
  hospital_id VARCHAR(32) PRIMARY KEY,
  name TEXT NOT NULL,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  total_icu INT NOT NULL, total_vent INT NOT NULL, total_general INT NOT NULL
);

-- SmartMatch: deterministic synthetic hospital capabilities; absence means NOT eligible.
CREATE TABLE hospital_capabilities (
  hospital_id VARCHAR(32) NOT NULL REFERENCES hospitals(hospital_id),
  capability_code VARCHAR(48) NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  PRIMARY KEY (hospital_id, capability_code)
);
CREATE INDEX idx_hospital_capability_active
  ON hospital_capabilities (capability_code, hospital_id) WHERE is_active;

-- Catalog of supported dispatch conditions and their admissible bed types.
-- A condition may require multiple capabilities; all required tags must match.
CREATE TABLE condition_requirements (
  condition_code VARCHAR(48) NOT NULL,
  required_capability VARCHAR(48) NOT NULL,
  PRIMARY KEY (condition_code, required_capability)
);
CREATE TABLE condition_bed_types (
  condition_code VARCHAR(48) NOT NULL,
  bed_type VARCHAR(16) NOT NULL CHECK (bed_type IN ('ICU','VENTILATOR','GENERAL')),
  PRIMARY KEY (condition_code, bed_type)
);

CREATE TABLE beds (
  bed_id VARCHAR(48) PRIMARY KEY,                 -- e.g. 'H-01-ICU-004'
  hospital_id VARCHAR(32) NOT NULL REFERENCES hospitals(hospital_id),
  bed_type VARCHAR(16) NOT NULL CHECK (bed_type IN ('ICU','VENTILATOR','GENERAL')),
  status VARCHAR(16) NOT NULL CHECK (status IN ('FREE','RESERVED','OCCUPIED')),
  patient_id VARCHAR(48),
  reservation_id UUID,
  version BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_beds_free_lookup ON beds (hospital_id, bed_type) WHERE status = 'FREE';

CREATE TABLE patients (
  patient_id VARCHAR(48) PRIMARY KEY,
  current_bed_id VARCHAR(48),
  current_hosp_id VARCHAR(32),
  state VARCHAR(24) NOT NULL,                      -- IN_TRANSIT, ADMITTED, TRANSFERRING, DISCHARGED
  last_seq_no BIGINT NOT NULL DEFAULT 0,           -- generator: bumped when an event is created
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE reservations (
  reservation_id UUID PRIMARY KEY,
  actor_id VARCHAR(48) NOT NULL,
  idempotency_key VARCHAR(128) NOT NULL,
  request_fingerprint VARCHAR(128) NOT NULL,
  UNIQUE(actor_id, idempotency_key),
  bed_id VARCHAR(48) NOT NULL REFERENCES beds(bed_id),
  hospital_id VARCHAR(32) NOT NULL,
  bed_type VARCHAR(16) NOT NULL,
  ambulance_id VARCHAR(48) NOT NULL,
  patient_id VARCHAR(48) NOT NULL,
  priority SMALLINT NOT NULL DEFAULT 2 CHECK (priority BETWEEN 1 AND 4),
  status VARCHAR(16) NOT NULL CHECK (status IN ('HELD','CONFIRMED','EXPIRED','RELEASED')),
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_res_active_expiry ON reservations (expires_at) WHERE status = 'HELD';
CREATE UNIQUE INDEX uq_res_one_active_per_bed ON reservations (bed_id) WHERE status IN ('HELD','CONFIRMED');
-- One active *dispatch* hold per patient; transfer target holds are handled by the one-active-transfer invariant.
ALTER TABLE reservations ADD COLUMN purpose VARCHAR(16) NOT NULL DEFAULT 'DISPATCH' CHECK (purpose IN ('DISPATCH','TRANSFER'));
CREATE UNIQUE INDEX uq_dispatch_one_hold_per_patient ON reservations(patient_id) WHERE status='HELD' AND purpose='DISPATCH';

CREATE TABLE transfers_saga (
  transfer_id UUID PRIMARY KEY,
  patient_id VARCHAR(48) NOT NULL REFERENCES patients(patient_id),
  from_hosp_id VARCHAR(32) NOT NULL,
  from_bed_id VARCHAR(48) NOT NULL,
  to_hosp_id VARCHAR(32) NOT NULL,
  to_bed_id VARCHAR(48) NOT NULL,
  reservation_id UUID NOT NULL REFERENCES reservations(reservation_id),
  priority SMALLINT NOT NULL DEFAULT 4,
  saga_state VARCHAR(24) NOT NULL CHECK (saga_state IN (
    'TARGET_RESERVED','AWAITING_ACCEPTANCE','COMPLETED',
    'REJECTED_COMPENSATING','TIMED_OUT_COMPENSATING','REVERSED')),
  deadline_at TIMESTAMPTZ NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox_events (
  event_id UUID PRIMARY KEY,
  routing_key VARCHAR(64) NOT NULL,
  priority SMALLINT NOT NULL DEFAULT 1,
  payload JSONB NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'PENDING',   -- PENDING, PUBLISHED
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

CREATE UNIQUE INDEX uq_patient_active_saga ON transfers_saga(patient_id) WHERE saga_state IN ('TARGET_RESERVED','AWAITING_ACCEPTANCE','REJECTED_COMPENSATING','TIMED_OUT_COMPENSATING');

CREATE TABLE consumer_patient_offsets (
  consumer_group VARCHAR(32) NOT NULL,
  patient_id VARCHAR(48) NOT NULL REFERENCES patients(patient_id),
  last_applied_seq BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (consumer_group, patient_id)
);
CREATE TABLE rate_limit_windows (
  actor_id VARCHAR(48) NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,
  requests INT NOT NULL,
  PRIMARY KEY (actor_id, window_start)
);

CREATE TABLE inbox_processed_events (
  consumer_group VARCHAR(32) NOT NULL,
  event_id UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE system_state (key TEXT PRIMARY KEY, value BIGINT NOT NULL);  -- 'pg_epoch', 'ttl_override_s'
```
Rules: discharge and transfer completion set the bed's CONFIRMED reservation to RELEASED. Seed: deterministic (fixed random seed), at least 40 fictional hospitals ('H-01' to 'H-40') and at least 4,000 beds in a small lat/lon box, with a mix of ICU, ventilator and general beds. Give each hospital an explicit, varied set of verified synthetic specialty/capability tags and seed supported condition-to-capability and condition-to-bed-type rules; include nearby but incompatible hospitals and farther suitable hospitals for SmartMatch demonstrations. No real hospital names. The invariant query for double allocation is `SELECT bed_id, COUNT(*) FROM reservations WHERE status IN ('HELD','CONFIRMED') GROUP BY bed_id HAVING COUNT(*) > 1`; it must return no rows. Also verify bed status/reservation linkage, no two OCCUPIED beds for one patient except an explicitly modeled transitional case, no second active saga, and no HELD reservation on an OCCUPIED bed. Never rely on a dashboard query *instead of* the unique index and conditional SQL transitions.

3.6 Redis keys and Lua (reference implementations: adapt, test, and log changes)
Keys in the following Lua sketches are **illustrative only**. Production names must include the cache generation, such as `mg:<generation>:hospitals:geo`, `mg:<generation>:hosp:<id>:beds:<TYPE>:free`, counts, `res:data`, TTL and expiry. Shared `medigrid:cache_ready_generation` is a fencing pointer, not an allocation authority. Rate-limit and idempotency key namespaces must use actor IDs; Redis counts are derived and projections are idempotent (recompute/set, not blind increments). Each Lua call must operate entirely on one generation and check readiness before modifying a key.

Scripts return plain Lua arrays. Do not return `{ ok = ... }` or `{ err = ... }` tables: Redis turns those into a bare status reply or an error reply and drops any other fields.
```lua
-- reserve_nearest_bed.lua
-- KEYS[1]=medigrid:cache_ready_generation  KEYS[2]=ratelimit:<actor>  KEYS[3]=idemp:res:<actor>:<key>  KEYS[4]=mg:<generation>:reservations:expiry
-- ARGV[1]=bed_type ARGV[2]=reservation_id ARGV[3]=ttl_ms ARGV[4]=rate_max ARGV[5]=rate_window_ms
-- ARGV[6]=expected_ready_generation ARGV[7]=meta_json ARGV[8..]=candidate hospital ids, nearest first
-- KEYS[1] must hold a READY generation; ARGV[6] is the caller's generation and metadata moves to ARGV[7].
if redis.call('GET', KEYS[1]) ~= ARGV[6] then return {'NOT_READY'} end
local prior = redis.call('GET', KEYS[3])
if prior then return {'IDEMPOTENT_HIT', prior} end
local n = redis.call('INCR', KEYS[2])
if n == 1 then redis.call('PEXPIRE', KEYS[2], tonumber(ARGV[5])) end
if n > tonumber(ARGV[4]) then return {'RATE_LIMITED', tostring(redis.call('PTTL', KEYS[2]))} end
local t = redis.call('TIME')
local now_ms = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local exp_ms = now_ms + tonumber(ARGV[3])
for i = 8, #ARGV do
  local hosp = ARGV[i]
  local free_key = 'mg:' .. ARGV[6] .. ':hosp:' .. hosp .. ':beds:' .. ARGV[1] .. ':free'
  local bed_id = redis.call('SPOP', free_key)
  if bed_id then
    local rec = cjson.encode({reservation_id=ARGV[2], hospital_id=hosp, bed_id=bed_id,
                              bed_type=ARGV[1], expires_at_ms=exp_ms, meta=ARGV[7]})
    redis.call('HSET', 'mg:' .. ARGV[6] .. ':res:data', ARGV[2], rec)
    redis.call('ZADD', KEYS[4], exp_ms, ARGV[2])
    redis.call('SET', 'mg:' .. ARGV[6] .. ':res:ttl:' .. ARGV[2], bed_id, 'PX', tonumber(ARGV[3]))
    redis.call('HINCRBY', 'mg:' .. ARGV[6] .. ':hosp:' .. hosp .. ':counts', 'free_' .. ARGV[1], -1)
    redis.call('SET', KEYS[3], rec, 'PX', 3600000)
    return {'RESERVED', rec}
  end
end
return {'NO_SUITABLE_BED'}
```
```lua
-- release_reservation.lua  (idempotent)
-- KEYS[1]=mg:<generation>:reservations:expiry  ARGV[1]=reservation_id  ARGV[2]='release' | 'consume'  ARGV[3]=generation
-- 'release' returns the bed to the free set. 'consume' removes the hold and leaves the bed out of the free set
-- (the reservation was confirmed, or PostgreSQL says the bed is not free).
local raw = redis.call('HGET', 'mg:' .. ARGV[3] .. ':res:data', ARGV[1])
if not raw then redis.call('ZREM', KEYS[1], ARGV[1]); return {'ALREADY_SETTLED'} end
local rec = cjson.decode(raw)
redis.call('HDEL', 'mg:' .. ARGV[3] .. ':res:data', ARGV[1])
redis.call('DEL', 'mg:' .. ARGV[3] .. ':res:ttl:' .. ARGV[1])
redis.call('ZREM', KEYS[1], ARGV[1])
if ARGV[2] == 'release' then
  local free_key = 'mg:' .. ARGV[3] .. ':hosp:' .. rec.hospital_id .. ':beds:' .. rec.bed_type .. ':free'
  if redis.call('SADD', free_key, rec.bed_id) == 1 then
    redis.call('HINCRBY', 'mg:' .. ARGV[3] .. ':hosp:' .. rec.hospital_id .. ':counts', 'free_' .. rec.bed_type, 1)
  end
end
return {'RELEASED', raw}
```
Load scripts with redis-py `register_script`. The scripts touch dynamically constructed keys not all listed in KEYS, acceptable only for this single-node PoC (cluster support is out of scope). These are reference sketches, **not drop-in final scripts**: add input validation, return codes, and test generation switching. Release takes the hold’s original generation and must never add a bed to a newly active generation without DB reconciliation. A Redis provisional hold must be reconciled against PostgreSQL before `release` adds a bed to any free set.

3.7 RabbitMQ topology
- Durable topic exchange `medigrid.events`; persistent deliveries; publisher confirms and mandatory routing with returned-message detection; do not mark outbox PUBLISHED if confirm or routing is uncertain. Pin RabbitMQ version and test restart durability. Queue layout may use classic priority queues if verified, but document that classic dead-lettering and single-node broker failure do not promise zero loss under every hardware failure.
- Four levels (routine 1, standard 2, urgent 3, critical 4). Main `q.bed_tracking`, `q.transfer_saga`, `q.notifications`, `q.audit` queues each durable; priority main queues use `x-max-priority: 4`. Bed tracking binds `bed.#` and `transfer.#`; saga binds `transfer.#` and `bed.reservation.expired`; notification and audit each bind `#`. Default prefetch: 1 for priority-sensitive worker; 5 notifications; 20 audit. All services get independent copies via their own queue.
- Delay retry queues are per original service and tier (1, 3, 9 seconds); each retries only to the original queue via default exchange and its exact routing key. Implement retry as `publish persistent copy -> verify return/confirm -> ack original`, never ack first. On publish failure, retain original unacked and reconnect; use stable event_id, headers and priority. After max attempts, publish to the DLQ, await positive confirm, then ack original. DLQ replay uses the same confirmed-publish-before-ack discipline. Record attempt count in headers and persistent audit, not only Redis, so a Redis crash cannot reset retries forever.
- `x-dead-letter-exchange` routes rejected unhandled messages to a per-service durable DLQ or to a shared DLQ with original-queue header. Never assume dead-lettering is lossless without testing queue type/config. The operator UI reads DLQ metadata through the RabbitMQ management HTTP API, and supports explicit replay without destructively consuming inspection messages.
- Reconnect on restart: redeclare exchanges, queues, bindings, confirm channel, and consumers. pika BlockingConnection must not be shared unsafely across threads. Use bounded prefetch and manual ack only after durable DB effect and any required reliable handoff.
- Priority test must enqueue 10,000 routine and 1 critical event **into the same priority-enabled queue**, prove all relevant consumers were stopped before filling it, then resume one consumer with prefetch 1. Record IDs and actual order; merely observing a critical event on a different queue proves nothing. In-flight routine events cannot be preempted. If this demonstration is unreliable, implement separate critical and routine lanes with critical-first scheduling and document the change.

3.8 Outbox, consumers and ordering
- Event envelope: event_id, type, patient_id (nullable), patient_seq_no (nullable), correlation_id, occurred_at, data, flags. Every admission, transfer, discharge, hold creation, confirmation, cancellation, and expiry writes durable outbox event(s) in the same transaction as its state mutation.
- Outbox publisher sweeps PENDING rows with SKIP LOCKED. Publisher confirms, routing return handling, durable exchange/queues and persistence are prerequisites to mark PUBLISHED. Crash after confirm but before status update may republish: consumer inbox must make effects idempotent. Immediate publish is optional optimization; sweeper is authoritative recovery path. PUBLISHED means broker accepted the publish, not that all four consumers have finished.
- Each consumer group has its own `(consumer_group,event_id)` inbox row, committed in one PostgreSQL transaction with its domain effect. Notifications are stored with a unique event-derived key before any external notification side-effect; the demo delivers only in-app messages. Audit insert uses event_id uniqueness. A duplicate gets ack only after confirming the committed inbox/effect.
- Ordering cursors are *per consumer group and patient* in `consumer_patient_offsets`. `last_seq_no` on patient is event creation counter only. A consumer that subscribes to every patient event advances its own cursor for events it ignores (a no-op) to avoid gaps. If a group subscribes to a subset, either route a complete patient event stream to it or use explicit per-group sequence numbers. Never share `patients.applied_seq_no` between consumers. `seq > cursor+1`: postpone with confirmed delayed retry and ack original; `seq <= cursor`: skip duplicate *for that group*; `seq == cursor+1`: apply, advance cursor and inbox atomically.
- Avoid the head-of-line trap: out-of-order events may not block all high-priority work while awaiting an absent predecessor; retry via delayed queue with capped age and visible alerts. A missing predecessor sent to DLQ marks that patient stream BLOCKED for operator resolution, not silently skipped. Saga transitions use versioned DB state as their primary concurrency guard.
- Redis/cache/SSE projection is not part of PostgreSQL commit. A failure after the inbox transaction is committed must trigger reconciliation or a durable projection task; do not count cache delivery as reliably finished. To avoid permanently losing a UI delta, provide a PostgreSQL-backed change watermark and SSE reconnect snapshot and periodic reconciliation. The UI observes the authoritative state even if Redis pub/sub dropped a message.
- Broker restart parity test: count committed outbox event IDs, broker-confirmed publications, and *distinct inbox processed event IDs per bound consumer group*, accounting for intentionally DLQ/BLOCKED events. Do not compare total publishes directly to total consumes (fan-out and duplicates make those counts different). Show every committed event eventually processed or explicitly pending/DLQ; never claim zero loss while rows remain unresolved.

3.9 Transfer saga (orchestrated by q.transfer_saga)
1. Source hospital starts transfer only for a patient in ADMITTED state occupying exactly the claimed source bed. Acquire a patient lock, require no other active saga (enforced by partial unique index), and reserve a TRANSFER-purpose target bed at hospital B via the same PostgreSQL-final fast path. Keep source bed OCCUPIED until successful acceptance; start `AWAITING_ACCEPTANCE` with deadline, version, and `transfer.requested` outbox event in a DB transaction (if reserving the target earlier, compensate it if saga creation fails). Lock rows consistently: patient, saga, sorted bed IDs, sorted reservation IDs.
2. Target console shows a countdown derived from server deadline, buttons Accept/Reject, and incoming alert. Requests are idempotent with version/idempotency keys.
3. Accept/Reject/Timeout all lock the same saga and reservation and verify version and deadline using PostgreSQL clock. Accept is permitted only while `AWAITING_ACCEPTANCE`, with target reservation HELD and unexpired and source bed OCCUPIED by this patient. One transaction confirms target reservation, marks target OCCUPIED, marks source FREE, releases the source active reservation where present, moves patient to target, sets saga COMPLETED, and writes `transfer.completed` plus bed events. Redis is reconciled after commit.
4. Reject or timeout: in one transaction mark target reservation RELEASED (or EXPIRED if genuinely expired), change *only the matching RESERVED target bed* to FREE, leave source OCCUPIED, return patient to ADMITTED at source, mark saga REVERSED, and write `transfer.reversed` plus bed event. Redis release only after database confirms matching bed is FREE. No compensator may free an unrelated or already reallocated bed.
5. Competing accept, reject, deadline reaper, discharge, and second-transfer requests use the same patient/saga/bed checks. One wins; the others return 409/current state or are idempotent no-ops. A late accept cannot revive a reversed saga. Use sorted bed lock order across hospitals to avoid deadlock; bounded timeout/retry on deadlock.
6. The transfer timeout must be shorter than or coordinated with the target hold expiry; default acceptance deadline 60 seconds, reservation TTL 900 seconds. On timeout compensation, release the target hold. Test accept-vs-expire and accept-vs-reject at the same millisecond, simultaneous transfers for one patient, and discharge during transfer. Emit only authoritative state changes to SSE.

3.10 Real-time delivery
- Server-Sent Events, built by hand (`/api/stream`), not WebSocket. Behaviour: heartbeat comment every 2 s; the first message is a snapshot; a `retry:` value chosen randomly between 100 and 600 ms per connection so reconnects don't arrive together; an `X-Instance` header and an `instance` event so the UI can show which instance it is on.
- Each API instance subscribes to `medigrid:ui:delta`, buffers deltas in a dict keyed by hospital id, and flushes one coalesced batch to its clients every 100 ms. The browser applies batches with `requestAnimationFrame`. Backend throughput never drives UI frame rate.
- The snapshot (`/api/snapshot`) uses Redis counts only when the shared cache generation is READY and reconciled; otherwise PostgreSQL. Cache pre-serialized at most 1 second per instance, include a DB change watermark, and refresh on reconnect. Redis pub/sub is lossy; reconnect snapshots and periodic version reconciliation must recover missed updates.
- Client watchdog: if nothing, heartbeats included, arrives for 5 s, close and reopen. Connection indicator states: Connected, Reconnecting, Offline.
- Nginx: upstream with `max_fails=1 fail_timeout=2s` for both instances, `proxy_next_upstream error timeout http_502 http_503`, `proxy_connect_timeout 1s`. For SSE: buffering off, HTTP/1.1, no Connection close, long read timeout; reconnect from last committed snapshot/watermark. Publish each API port on loopback for deterministic 2-instance tests, not publicly accessible by default. Benchmark actual <5 s failover (including existing long-lived SSE streams), and distinguish new HTTP requests from an already-open SSE connection.

3.11 Hospital console concurrency
Every admit, discharge and transfer sends `expected_version` for the bed. The API updates with `WHERE bed_id=? AND version=?`. On a mismatch it returns 409 with the authoritative bed state. The client rolls back the optimistic change, shows a visible banner (not colour alone), and applies the server state.

3.12 Judge Chaos Control Bar (docked at the bottom of /ops)
Six scenarios, each computing PASS or FAIL from real measurements and showing the measured values. In-app buttons for 1 to 4, gated by `CHAOS_ENABLED=1`. For 5 and 6 show the exact `docker compose` command with a copy button. Do not mount the Docker socket into any container.
1. Last-bed stampede: set H-01 to one free ICU bed. Fire 50 requests from 50 different user ids across both instances. Assert: exactly one reservation holds that bed, every other response is a different bed or NO_SUITABLE_BED, the invariant query returns no rows, search p95 is under 100 ms. A separate check shows one user exceeding the per-user limit and getting 429 with Retry-After.
2. Flood then critical: quiesce **all** consumers of the same chosen priority-enabled service queue; enqueue 10,000 confirmed routine events there, then one critical transfer event routed to that **same queue**, verify queue depth, and resume exactly one consumer (prefetch 1). Assert processing-order timestamps/event IDs show critical handled before the backlog. Do not fake queue depth or compare different queues.
3. Expiry: set the TTL override to 10 s, create a hold, do not confirm. Assert the bed is free again within 5 s of expiry and show the measured gap.
4. Poison and crash: distinguish (A) an ordinary *handler error* that follows 1/3/9-second confirmed retry republishes and DLQ, from (B) a SIGKILL before ack that RabbitMQ redelivers unacked. Verify both with stable event IDs; after a bounded crash-loop guard the poison message reaches DLQ, and replay only after removing the fault flag is confirmed. No ack before confirmed retry/DLQ handoff.
5. Broker restart: `docker compose restart rabbitmq` while new committed admissions and retries are in progress. Verify PENDING outbox drains, each committed event appears exactly once as a side effect in each bound consumer group (despite possible duplicate deliveries), and no unresolved event is misreported as completed. Record per-event outcome, not a naive published==consumed count.
6. Instance stop and Redis pause: stop api-1 during an active SSE stream, verify browser reaches api-2 within 5 s, then pause Redis for 10 s while simultaneous reservations hit *both* instances and assert SQL-only degraded handling and zero duplicates. Unpause Redis, demonstrate shared REBUILDING fencing and reconciled READY; repeat with Redis restart (empty cache). Include a commit-unknown/API-crash-after-Redis-hold injection.

3.13 Corrections to the architecture blueprint (do not revert)
- C1 Stack: Python, Flask under gunicorn, vanilla JS, custom SVG map, SSE. No Node, React, Vite, Leaflet or WebSocket.
- C2 PostgreSQL is the final arbiter, with the conditional bed update and a unique index on HELD **or CONFIRMED** reservations. Redis recovery is fenced by a shared state/generation protocol; the epoch existence check alone cannot detect stale cache.
- C3 Lua scripts return arrays, not `ok` or `err` tables.
- C4 Pass every hospital in the radius to Lua, not only 15.
- C5 Retry queues return to the failing service's queue through the default exchange, not through the topic exchange.
- C6 Bindings are `bed.#` and `transfer.#`.
- C7 Four AMQP priority levels, not ten.
- C8 Ordering uses per-consumer-group `consumer_patient_offsets`, separate from the patient `last_seq_no` generator; a shared applied counter would incorrectly skip other groups.
- C9 Redis persistence is off.
- C10 PostgreSQL scan and row locks are the expiry authority. Redis orphan reconciliation is DB-verified; `release` and `consume` have different safety conditions.
- C11 The stampede uses 50 user ids. Rate limiting is checked separately.
- C12 Degraded mode covers search, SQL-backed shared rate limiting, idempotency, and the PostgreSQL-backed live feed, not only allocation.
- C13 Chaos scenarios 5 and 6 show commands instead of mounting the Docker socket.
- C14 Latency figures and scorecards from the blueprint are goals, never results.
Log each of these in docs/DECISIONS.md. Additional corrections C15–C21: C15 shared-generation/fenced Redis recovery; C16 per-consumer patient offsets; C17 one-active-transfer/lock-order and deadline races; C18 uniqueness across HELD+CONFIRMED; C19 ambiguous commit reconciliation; C20 confirmed retry/DLQ handoff and true same-queue priority test; C21 event-ID/per-consumer parity and durable UI reconciliation. Include alternatives, consequences and test evidence for each.

3.14 Metrics for the ops dashboard
Events per second (audit consumer counters per second bucket), queue depth and consumer count (RabbitMQ management API), consumer lag (queue depth and event age at processing time), retry count, DLQ depth, bed-search p95 (latency samples from both instances stored in shared storage, so either instance shows city-wide numbers), free beds by type (Redis counts, or PostgreSQL when degraded), invariant badge, outbox pending count. When a source is down show "N/A" with the reason. Never show 0 or a made-up value.

3.15 MEDIGRID SMARTMATCH — SPECIALTY-AWARE HOSPITAL SELECTION (hackathon innovation; mandatory PoC scope)
- Goal: **right hospital + right specialty + right bed + appropriate distance**, not simply the nearest free bed. Call the feature "MediGrid SmartMatch" on the dispatch screen and in the demonstration. It is a small, deterministic, explainable rule-based filter, **not AI/ML, diagnosis, medical advice, or live clinical triage**. All patients, conditions, capabilities, and hospital data are synthetic.
- Seed at least these illustrative condition codes: `CARDIAC_EMERGENCY` → `CARDIAC_EMERGENCY_CARE`; `SUSPECTED_STROKE` → `STROKE_READY`; `MAJOR_TRAUMA` → `TRAUMA_CARE`; `SEVERE_BURNS` → `BURN_CARE`; `OBSTETRIC_EMERGENCY` → `EMERGENCY_OBSTETRICS`; `PEDIATRIC_EMERGENCY` → `PEDIATRIC_EMERGENCY`. Map each condition to allowed bed types in `condition_bed_types`, documented as illustrative demo assumptions. Do **not** infer a patient's diagnosis from free text or promise that a capability tag alone establishes actual medical suitability.
- `/api/dispatch/search` and `/api/dispatch/reserve` accept a required `condition_code` from the allowlisted synthetic catalog, alongside `bed_type`, location and existing inputs. Reject unknown codes and incompatible bed-type selections with 400 plus the supported options. For hospital-to-hospital transfer, reuse the matching rules for the destination; a requested destination that lacks mandatory capabilities is rejected with an explicit reason.
- Ranking algorithm: (1) obtain required capability tags for the selected condition; (2) **hard-filter** hospitals lacking any mandatory active tag; (3) hard-filter by permitted bed type and authoritative free-bed availability; (4) sort by geographic distance (hospital ID as deterministic tie-break); (5) reserve the closest eligible free bed, protected by the existing conditional PostgreSQL update, active-reservation unique index, idempotency and Redis fencing. Redis may cache a versioned capability lookup, but PostgreSQL must verify eligibility within the reservation transaction. Unknown or stale tags fail closed to "no verified suitable hospital" rather than rerouting to an incapable one.
- Capability changes are operator-only, written transactionally in PostgreSQL with an outbox event and cache invalidation/rebuild; alternatively, keep the seed immutable for the four-hour PoC, and document that decision. On capability-cache uncertainty, query PostgreSQL. Never allow a cache hit to make an ineligible hospital eligible.
- When no matching hospital has an appropriate free bed inside the search radius, return `NO_SUITABLE_HOSPITAL_OR_BED` with a clear explanation and optional operator-reviewed radius expansion. Do not claim a medically safe fallback. In a real emergency, trained dispatchers must follow established clinical and local escalation protocols.
- UI: show the selected condition, required capabilities, each matched hospital's relevant capabilities, free bed type/count, approximate distance, and a concise reason: e.g., "Cardiac emergency care available; ICU bed free; 7.2 km". Mark unsupported hospitals as excluded in a separate explanatory panel, never selectable as fallback. In the control map and hospital console, add readable capability tags/filter; preserve keyboard access and non-color-only status.
- Demo test: seed general hospital H-01 closer to the ambulance with a free ICU bed **but no cardiac-care tag**, and H-02 farther away with cardiac capability and a free ICU bed. `CARDIAC_EMERGENCY` must select H-02, never H-01; repeat with 50 concurrent dispatch requests and with Redis paused/restarting. A no-match case must return a clear explicit failure rather than an unsafe assignment. Compare specialty-aware selection and plain nearest-bed selection visually, without implying real-world clinical validation.
- Instrument and assert SmartMatch eligible hospital counts, exact rejection reasons, selected hospital/bed ID, matching correctness across normal/degraded paths, and search p95. Keep SmartMatch rules in plain Python/SQL and deterministic seed files, no external medical APIs or extra frameworks. Add coverage to TRACEABILITY, DECISIONS, architecture/defence notes and measured demo script. Innovation is not a reason to sacrifice PS performance or safety targets.

4. UI RULES
Style: a plain, utilitarian operations tool, like a hospital system or an admin console. Not a marketing page.
- Layout: simple grid, system font stack, base size 15 to 16 px, moderate spacing.
- Colour: white or light grey background, near-black text, one muted dark-blue accent, muted status colours. Define them as CSS variables. WCAG AA contrast.
- Buttons and inputs: rectangular, 1 px border, radius no more than 4 px, visible focus outline. No pill shapes.
- Forbidden: gradients of any kind (especially purple or blue-violet), glassmorphism, glow or heavy shadows, large rounded cards, emoji as icons, hero banners, gradient text, decorative blobs, marketing copy, "Welcome back" filler, testimonials, lorem ipsum, stock or AI-generated images (the app has no images except the favicon), custom cursors, cursor trails, parallax, scroll-triggered animation, and any fake counter or statistic ("trusted by", "10,000+ users"). Only functional motion is allowed: the transfer countdown and 150 ms state transitions. Respect prefers-reduced-motion.
- Icons: simple hand-written inline SVG or plain text labels.
- Status never depends on colour alone. Bed status: FREE is a solid circle, RESERVED is a diamond with diagonal stripes, OCCUPIED is a filled square, each with a text label. Hospital load on the map: OK, LOW and FULL as different shapes with the text label and the free count. Bed type (ICU, VENT, GEN) is a text label.
- Identity: no passwords. A "demo identity" selector (role, hospital, user id) stored in a signed cookie, with a shared SECRET_KEY across instances. Per-user rate limits use the user id. The README lists this under Known limitations.
- Pages: /control (live map with 40 hospitals and filters by bed type, status and specialty capability), /dispatch (SmartMatch selector for synthetic patient condition and mandatory specialty, bed type, priority, ambulance and location; shows suitable-hospital rationale, excluded incompatible hospitals, result and hold countdown; confirm or cancel), /hospital/<id> (bed grid, admit, discharge, transfer in a few clicks; optimistic update with 409 rollback; incoming-transfer alert with accept countdown; keyboard navigation with arrow keys, Enter, and letter shortcuts; tablet-friendly with 44 px touch targets down to 768 px width; connection indicator with instance name), /ops (section 3.14 plus the chaos bar and DLQ viewer with replay).
- Map: custom SVG city layer with focusable hospital markers, no tile servers. Ambulance positions come from a clearly labelled simulator process, not from hard-coded values. Batch updates at most 10 times a second and touch only changed elements. Use Canvas for the ambulance layer only if SVG cannot hold the frame rate with 300 ambulances, and log the measurement.
- A small neutral footer note "Synthetic data" is fine. No "made with", "powered by" or any AI or tool credit in the UI, HTML, code comments, docs or commit messages.
- Favicon: a real favicon (simple hand-written SVG: a white cross on a dark-blue square, plus a 32x32 PNG and an .ico). Link it in the base template head and also serve /favicon.ico. Check that it returns 200.

5. RUNNING ON THE LAN
Nginx publishes port 8080 on all interfaces. On startup the API logs the container URLs; provide a Python helper (stdlib only, run on the host) that prints the laptop's LAN URL. The README explains how to find the IP, how to allow port 8080 through the OS firewall, and how to open the app from another device on the same Wi-Fi.

6. LOGS AND DOCUMENTATION (write them as you go, not at the end)
- docs/BUILD_LOG.md: chronological. Each entry: timestamp, stage, what you did, why, the command, the real result (trimmed), status (VERIFIED, NOT RUN, FAILED). Keep failures and their fixes.
- docs/DECISIONS.md: one entry per decision: ID, context, at least two options, choice, reason, consequences. Include C1 to C22, the pinned versions, connection pool sizes, and an "Assumptions" section.
- docs/TRACEABILITY.md: built in stage 0. Number every requirement (R1, R2, ...), every target (T1 to T8) and the six chaos scenarios. Columns: requirement, where implemented, how verified, status. Update it as you go.
- docs/DECISION_RECORD.md: the one-page record the PS asks for. Subject: PostgreSQL-authoritative hybrid (Redis fast path, PostgreSQL final arbiter), alternatives A and B from the plan, the reason.
- docs/architecture.svg: request, message and update paths across nginx, instances, workers, RabbitMQ, Redis and PostgreSQL.
- docs/DEFENCE_NOTES.md: plain explanations of each major choice and its trade-offs, likely judge questions with short honest answers (why a conditional update backs the Lua script, why a paused Redis is different from a restarted one, why retry queues are per service, what the priority queue cannot guarantee), and how the design would grow (more instances, partitioned tables, read replicas, sharded GEO by zone). The growth part is described, not built.
- Runtime logs: every process writes JSON lines (ts, service, level, event_id, correlation_id, msg) to stdout. Compose collects them.
- README.md covers only PS04 and this solution. Sections: Problem; What was built; Architecture (link the diagram and describe the three paths briefly); Key decisions (short, link to DECISIONS); Run it (prerequisites, `docker compose up --build`, migrations and seed, LAN access, demo identities); Pages; Demo runbook (for each of the six scenarios: command or button, what to watch, expected result); Measured results (real numbers from real runs, with date and machine); Known limitations; Repo layout. Nothing else: no badges, no roadmap, no contributing or license sections.
- Writing style for the README, docs and code comments: plain technical prose, like a student engineer explaining their own work. Short paragraphs, specific numbers, concrete file and command names. No emoji. Avoid filler such as robust, seamless, leverage, cutting-edge, comprehensive, powerful, "this project aims to". No opening pleasantries and no closing summary paragraph. Do not mention AI, assistants, prompts or language models anywhere in the repo.

7. TESTS AND TOOLS
- One Python module per chaos scenario (`sim/`), runnable with `docker compose run --rm tools python -m sim.<name>`; define `tools` in Compose using the shared Python image and appropriate networks/config. Print PASS/FAIL with measured values. The same logic backs in-app buttons. Avoid tests that mutate the shared fixture without cleanup; reseed and validate counts between tests.
- SmartMatch tests: synthetic cardiac case picks a farther specialty-capable hospital over a nearer incompatible hospital, no eligible hospital gives explicit no-match, incompatible bed type is rejected, destination transfer checks capability, identical results in Redis-ready and SQL-only degraded modes, cache staleness fails closed, and 50 concurrent requests never double-book.
- A surge simulator: hundreds of concurrent ambulance requests from distinct user ids, duplicate events, out-of-order events, a slow consumer, a crashing consumer.
- pytest for: Lua reserve and release (including idempotent hit, rate limit, consume mode); last-bed stampede; unique HELD+CONFIRMED enforcement; concurrent idempotency; ambiguous DB commit; per-group ordering including no-op event types and missing predecessor; inbox deduplication; confirm-versus-expire; competing transfer accept/reject/timeout, second transfer and discharge; Redis pause/restart fencing and hydration; retry/DLQ republish failure; per-group broker restart parity; SSE reconnection and stale snapshot repair.
- A benchmark script that separately measures GET search and POST reserve p50/p95/p99 end to end across both instances at idle and under realistic 300–500 ambulance concurrency. Do not imply <100 ms is achieved until measured. Include failures, timeouts, machine specs and sample size.

7.1 IMPLEMENTATION PRIORITY AND FAILURE HONESTY
- Complete a minimal vertical slice before building sophisticated Redis generations: one bed reserved/confirmed/expired via SQL and an outbox event observed in both browser sessions; then add Redis selection and failure protection. The 4-hour schedule is a target, not a reason to mislabel incomplete work as VERIFIED.
- For every scenario record setup, exact commands, input counts, authoritative SQL assertions, expected vs observed events, elapsed timings, and cleanup. Do not call a test PASS solely because an endpoint returned 200 or a badge reads zero.
- Critical safety invariants override latency: if Redis readiness is uncertain, use PostgreSQL; if PostgreSQL is down, return 503 rather than allocate from Redis.
- Host simulation/testing endpoints are accessible only when CHAOS_ENABLED=1 and to a designated operator identity. No Docker socket mount.

8. FINAL REPLY
End with: a table of requirement status from TRACEABILITY.md, the list of everything NOT RUN or FAILED, the assumptions you made, and the exact commands to start the system and run each of the six scenarios. Do not describe anything as working unless its row says VERIFIED.
