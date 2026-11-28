-- Migration 116: Drop the v1 dequeue index (migration 32), superseded by
-- idx_workflow_status_in_flight_v2 (migration 115).

DROP INDEX IF EXISTS "idx_workflow_status_in_flight";
