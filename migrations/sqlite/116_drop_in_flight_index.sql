-- Migration 116: drop the dequeue index idx_workflow_status_in_flight_v2
-- supersedes.

DROP INDEX IF EXISTS "idx_workflow_status_in_flight";
