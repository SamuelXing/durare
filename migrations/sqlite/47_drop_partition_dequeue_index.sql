-- Migration 47: drop the index peers build in migration 45, superseded by 46.
DROP INDEX IF EXISTS "idx_workflow_status_partition_dequeue";
