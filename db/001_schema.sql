-- Core schema. Section 3.5 of the spec, with the ALTER folded into CREATE TABLE.

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
CREATE TABLE condition_catalog (
  condition_code VARCHAR(48) PRIMARY KEY,
  label TEXT NOT NULL
);

CREATE TABLE beds (
  bed_id VARCHAR(48) PRIMARY KEY,
  hospital_id VARCHAR(32) NOT NULL REFERENCES hospitals(hospital_id),
  bed_type VARCHAR(16) NOT NULL CHECK (bed_type IN ('ICU','VENTILATOR','GENERAL')),
  status VARCHAR(16) NOT NULL CHECK (status IN ('FREE','RESERVED','OCCUPIED')),
  patient_id VARCHAR(48),
  reservation_id UUID,
  version BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_beds_free_lookup ON beds (hospital_id, bed_type) WHERE status = 'FREE';
CREATE INDEX idx_beds_hospital ON beds (hospital_id);

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
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  purpose VARCHAR(16) NOT NULL DEFAULT 'DISPATCH' CHECK (purpose IN ('DISPATCH','TRANSFER')),
  condition_code VARCHAR(48),
  alloc_path VARCHAR(16),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_res_active_expiry ON reservations (expires_at) WHERE status = 'HELD';
CREATE UNIQUE INDEX uq_res_one_active_per_bed ON reservations (bed_id) WHERE status IN ('HELD','CONFIRMED');
CREATE UNIQUE INDEX uq_dispatch_one_hold_per_patient ON reservations(patient_id) WHERE status='HELD' AND purpose='DISPATCH';
CREATE INDEX idx_res_patient ON reservations (patient_id);

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
  actor_id VARCHAR(48),
  idempotency_key VARCHAR(128),
  request_fingerprint VARCHAR(128),
  condition_code VARCHAR(48),
  outcome_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (actor_id, idempotency_key)
);
CREATE UNIQUE INDEX uq_patient_active_saga ON transfers_saga(patient_id) WHERE saga_state IN ('TARGET_RESERVED','AWAITING_ACCEPTANCE','REJECTED_COMPENSATING','TIMED_OUT_COMPENSATING');
CREATE INDEX idx_saga_deadline ON transfers_saga (deadline_at) WHERE saga_state = 'AWAITING_ACCEPTANCE';

CREATE TABLE outbox_events (
  event_id UUID PRIMARY KEY,
  routing_key VARCHAR(64) NOT NULL,
  priority SMALLINT NOT NULL DEFAULT 1,
  payload JSONB NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'PENDING',   -- PENDING, PUBLISHED
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_pending ON outbox_events (created_at) WHERE status = 'PENDING';

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

CREATE TABLE system_state (key TEXT PRIMARY KEY, value BIGINT NOT NULL);
-- pg_epoch: bumped by every availability mutation (last statement of the transaction)
-- ttl_override_s: 0 = default 900 s
-- cache_state: 0 DEGRADED, 1 REBUILDING, 2 READY ; cache_generation: monotonic
INSERT INTO system_state VALUES
  ('pg_epoch', 0), ('ttl_override_s', 0), ('cache_state', 0), ('cache_generation', 0),
  ('transfer_deadline_s', 60);
