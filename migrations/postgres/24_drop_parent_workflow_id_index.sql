-- Migration 20: Drop the non-partial index on parent_workflow_id in preparation
-- for recreating it as a partial index (only on non-NULL values).

DROP INDEX IF EXISTS "idx_workflow_status_parent_workflow_id";
