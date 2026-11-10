-- Migration 43: Drop the per-row streams NOTIFY trigger installed by migration
-- 39. A notifying commit takes a global lock on the async notification queue,
-- so notifying from inside the write transaction serializes stream writes, for
-- every SDK sharing the database. Stream readers poll, so nothing is lost.
-- Postgres-only, matching migration 39.

DROP TRIGGER IF EXISTS dbos_streams_trigger ON streams;
DROP FUNCTION IF EXISTS streams_function();
