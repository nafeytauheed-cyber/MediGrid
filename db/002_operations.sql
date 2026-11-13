-- Consumer side effects, observability and chaos bookkeeping.

CREATE TABLE control_flags (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());

-- per-group apply log for patient-sequenced events (ordering evidence)
CREATE TABLE consumer_apply_log (
  id BIGSERIAL PRIMARY KEY,
  consumer_group VARCHAR(32) NOT NULL,
  patient_id VARCHAR(48) NOT NULL,
  seq_no BIGINT NOT NULL,
  event_id UUID NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX idx_apply_log_patient ON consumer_apply_log (patient_id, consumer_group, id);

CREATE TABLE patient_streams_blocked (
  consumer_group VARCHAR(32) NOT NULL,
  patient_id VARCHAR(48) NOT NULL,
  waiting_event_id UUID NOT NULL,
  waiting_seq BIGINT NOT NULL,
  expected_seq BIGINT NOT NULL,
  blocked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ,
  PRIMARY KEY (consumer_group, patient_id, waiting_event_id)
);

CREATE TABLE message_attempts (
  consumer_group VARCHAR(32) NOT NULL,
  event_id UUID NOT NULL,
  failures INT NOT NULL DEFAULT 0,
  postpones INT NOT NULL DEFAULT 0,
  crashes INT NOT NULL DEFAULT 0,
  last_error TEXT,
  dead_lettered_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (consumer_group, event_id)
);

CREATE TABLE counters (name TEXT PRIMARY KEY, value BIGINT NOT NULL DEFAULT 0);
INSERT INTO counters VALUES ('retry_published', 0), ('dlq_published', 0), ('postponed', 0),
  ('dlq_replayed', 0), ('outbox_published', 0);

-- bed tracking projection (recomputed, never incremented)
CREATE TABLE hospital_occupancy (
  hospital_id VARCHAR(32) NOT NULL,
  bed_type VARCHAR(16) NOT NULL,
  free INT NOT NULL, reserved INT NOT NULL, occupied INT NOT NULL,
  last_event_id UUID,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (hospital_id, bed_type)
);

CREATE TABLE notifications (
  notification_id BIGSERIAL PRIMARY KEY,
  event_id UUID NOT NULL UNIQUE,
  hospital_id VARCHAR(32),
  kind VARCHAR(64) NOT NULL,
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_notifications_hosp ON notifications (hospital_id, notification_id DESC);

CREATE TABLE audit_log (
  event_id UUID PRIMARY KEY,
  event_type VARCHAR(64) NOT NULL,
  patient_id VARCHAR(48),
  priority SMALLINT,
  occurred_at TIMESTAMPTZ,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  payload JSONB NOT NULL
);
CREATE INDEX idx_audit_processed ON audit_log (processed_at);

CREATE TABLE event_rate (bucket TIMESTAMPTZ PRIMARY KEY, events INT NOT NULL);

CREATE TABLE saga_log (
  id BIGSERIAL PRIMARY KEY,
  transfer_id UUID,
  event_id UUID NOT NULL UNIQUE,
  event_type VARCHAR(64) NOT NULL,
  logged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE latency_samples (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  instance VARCHAR(16) NOT NULL,
  op VARCHAR(32) NOT NULL,
  path VARCHAR(16),
  ms DOUBLE PRECISION NOT NULL
);
CREATE INDEX idx_latency_ts ON latency_samples (op, ts);

CREATE TABLE chaos_runs (
  run_id UUID PRIMARY KEY,
  scenario VARCHAR(32) NOT NULL,
  status VARCHAR(16) NOT NULL,          -- RUNNING, PASS, FAIL, ERROR
  result JSONB,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

CREATE TABLE chaos_priority_log (
  id BIGSERIAL PRIMARY KEY,
  run_id UUID NOT NULL,
  event_id UUID NOT NULL,
  kind VARCHAR(16) NOT NULL,
  consumer VARCHAR(32) NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX idx_chaos_priority_run ON chaos_priority_log (run_id, id);

CREATE TABLE cache_transitions (
  id BIGSERIAL PRIMARY KEY,
  at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  from_state SMALLINT, to_state SMALLINT NOT NULL,
  generation BIGINT NOT NULL,
  actor VARCHAR(32) NOT NULL,
  detail TEXT
);
