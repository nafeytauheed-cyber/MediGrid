# Traceability

Status: VERIFIED = ran and passed with evidence; FAILED = ran and did not meet the line; NOT RUN.
Evidence ids refer to `docs/BUILD_LOG.md` rows (#) and pytest names.

## Requirements

| ID | Requirement (PS04) | Where implemented | How verified | Status |
|---|---|---|---|---|
| R1 | At least 40 hospitals, 4,000 beds | `app/seed.py` | seed log: 40 hospitals, 4,597 beds (#4) | VERIFIED |
| R2 | At least 2 stateless application instances | `docker-compose.yml` api-1, api-2; `nginx/nginx.conf` | stampede used both (#12); failover (#20) | VERIFIED |
| R3 | Every bed event through RabbitMQ | `db.add_event` in every transition; `worker/loops.OutboxPublisher` | parity: 3,880 committed events in 4 groups (#19) | VERIFIED |
| R4 | Zero double-allocated beds | conditional UPDATE + `uq_res_one_active_per_bed` (`app/dispatch.py`, `db/001_schema.sql`) | stampede, Redis pause/restart, test_stampede_on_one_bed_has_one_winner | VERIFIED |
| R5 | Find nearest suitable bed, reserve 15 min, auto release | `dispatch.reserve`, `beds.expire_due` | expiry scenario with 10 s override (#16) | VERIFIED |
| R6 | Per-user rate limits | Lua limiter; `rate_limit_windows` fallback | stampede rate check: 20 accepted, 3 x 429, Retry-After 10 | VERIFIED |
| R7 | Each event consumed independently by every service | 4 queues, own inbox per group (`worker/consumers.py`) | parity per group (#19) | VERIFIED |
| R8 | Critical ahead of routine | `x-max-priority 4`, prefetch 1 | scenario 2 (#17) | VERIFIED |
| R9 | Retries with increasing delay | per-service 1/3/9 s retry queues | scenario 4 gaps 1.02/2.92/9.01 s (#18) | VERIFIED |
| R10 | DLQ visible and replayable | `/api/ops/dlq` (management API peek with requeue), `chaos.replay_dlq` | scenario 4 replay (#18) | VERIFIED |
| R11 | Duplicates never double-count or double-notify | inbox PK, `notifications.event_id` unique | surge: 50 republished, 0 duplicate inbox rows (#26); test_duplicate_delivery_is_skipped | VERIFIED |
| R12 | Same-patient events in order | `consumer_patient_offsets` | surge seq 3,2,1 applied 1,2,3 (#26); tests/test_ordering.py | VERIFIED |
| R13 | Transfer completes fully or is reversed | `app/transfers.py` | test_accept_vs_reject_one_wins, test_timeout_reverses_and_late_accept_cannot_revive | VERIFIED |
| R14 | Service down: events wait, processed on recovery | durable queues, manual ack | scenario 4 crash (#18); scenario 2 paused consumers then drained | VERIFIED |
| R15 | No accepted event lost on RabbitMQ restart | outbox + confirms + durable/persistent | scenario 5 (#19) | VERIFIED |
| R16 | Instance stops, users continue within 5 s | nginx upstream, SSE jittered retry, watchdog | scenario 6A: 0.38 s (#20) | VERIFIED |
| R17 | Brief Redis outage never double-allocates | fenced cache state (`app/cache.py`) | scenario 6B/6C (#22, #23) | VERIFIED |
| R18 | Live map with free beds by type, filters, not colour alone | `templates/control.html`, `static/control.js` | page 200, JS syntax (#31); shapes + text labels in code | VERIFIED (load); NOT RUN (interactive) |
| R19 | Hospital console: few clicks, optimistic + rollback, transfer alert countdown, keyboard, tablet, connection indicator | `templates/hospital.html`, `static/hospital.js`, `static/common.js` | page 200, JS syntax (#31); 409 API paths covered by tests | NOT RUN (interactive) |
| R20 | Ops dashboard metrics (events/s, depth, lag, retries, DLQ, search p95, free by type) | `app/ops.py`, `templates/ops.html` | `/api/ops/metrics` returned all sections (#5) | VERIFIED (API); NOT RUN (visual) |
| R21 | Surge simulator with duplicates, out-of-order, slow and crashing consumers | `sim/surge.py` | #26 | VERIFIED |
| R22 | Architecture diagram and one-page decision record | `docs/architecture.svg`, `docs/DECISION_RECORD.md` | files present; SVG parses | VERIFIED |
| R23 | SmartMatch specialty-aware selection | `app/smartmatch.py`, `dispatch.py` | `sim.smartmatch` 10/10 checks (#6); test_smartmatch_rules | VERIFIED |
| R24 | Locking, rate limiting, retries, DLQ, fan-out built by hand | no queue/workflow/limiter libraries in `requirements.txt` | review of requirements.txt | VERIFIED |
| R25 | Redis restarted empty vs paused stale are separate tests | `sim/failover.py` phases B and C | #22, #23 | VERIFIED |
| R26 | LAN access | nginx 0.0.0.0:8080, `tools/lan_url.py` | helper printed LAN URL; phone access | VERIFIED (helper); NOT RUN (second device) |

## Targets

| ID | Target | Pass line | Measured | Evidence | Status |
|---|---|---|---|---|---|
| T1 | Bed change visible city-wide | 2 s | 0.24 s DB expiry to SSE batch via nginx (0.37 s in #16) | #16, #33 | VERIFIED |
| T2 | Bed search p95 | < 100 ms | idle 7.1 ms; 300 ambulances at 150 searches/s 14.1 ms; 50-request burst 76.0 ms; 300 closed loops 1,540 ms | #30, #33 | VERIFIED at realistic load; FAILED at 300 continuous closed loops |
| T3 | Double allocations | 0 | 0 in every run | #12, #22, #23, #26 | VERIFIED |
| T4 | Lost events across broker restart | 0 | 0 unresolved of 3,880 (#19) and of 3,888 (#34) | #19, #34 | VERIFIED |
| T5 | Critical ahead of 10,000 routine | first | position 1 of 10,001 | #17 | VERIFIED |
| T6 | Unconfirmed reservation released | <= 5 s after expiry | 0.19 s (#16), 0.23 s (#33) | #16, #33 | VERIFIED |
| T7 | Instance stopped, users continue | <= 5 s | 0.38 s (#20), 0.62 s (#35) | #20, #35 | VERIFIED |
| T8 | Redis interrupted, no double allocation | 0 | 0 (pause and restart, two runs each) | #22, #23, #35 | VERIFIED |

## Chaos scenarios

| # | Scenario | Run with | Status |
|---|---|---|---|
| 1 | Last-bed stampede | ops button or `python -m sim.stampede` | VERIFIED (#12, #33) |
| 2 | Flood then critical | ops button or `python -m sim.priority` | VERIFIED (#17, #33) |
| 3 | Expiry | ops button or `python -m sim.expiry` | VERIFIED (#16, #33) |
| 4 | Poison and crash | ops button or `python -m sim.poison` | VERIFIED (#18, #33) |
| 5 | Broker restart | `python -m sim.broker_restart` + `docker compose restart rabbitmq` | VERIFIED (#19, #34) |
| 6 | Instance stop, Redis pause/restart, injections | `python -m sim.failover` + printed commands | VERIFIED (#20, #22, #23, #35) |

The in-app buttons call the same functions as the sim modules (`app/chaos.py`); the buttons themselves were not
clicked in a browser (NOT RUN). The simulator runs above exercise the identical code.
