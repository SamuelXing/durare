-- Migration 129: Drop idx_workflow_status_deadline, the index peers build in
-- migration 122, superseded by idx_workflow_status_deadline_v2 (migration 128).
-- A no-op on a database only durare has migrated.

DROP INDEX IF EXISTS "idx_workflow_status_deadline";
