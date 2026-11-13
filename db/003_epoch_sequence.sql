-- pg_epoch moves from a hot system_state row to a sequence (DECISIONS D-EPOCH): writers no longer serialise on one
-- row lock. Fencing against the cache rebuild uses advisory lock 4243: writers take it SHARED as the last step of an
-- availability transaction, the fenced READY switch takes it EXCLUSIVE.
CREATE SEQUENCE pg_epoch_seq;
SELECT setval('pg_epoch_seq', GREATEST((SELECT value FROM system_state WHERE key='pg_epoch'), 1));
DELETE FROM system_state WHERE key='pg_epoch';
