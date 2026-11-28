-- Migration 129: drop idx_workflow_status_deadline, the index peers build in
-- migration 122, superseded by idx_workflow_status_deadline_v2.

DROP INDEX IF EXISTS "idx_workflow_status_deadline";
