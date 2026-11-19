-- Migration 47: Drop idx_workflow_status_partition_dequeue, the index the Go
-- and Python SDKs build in migration 45, superseded by
-- idx_workflow_status_partition_dequeue_v2 (migration 46). A no-op on a
-- database only durare has migrated.

DROP INDEX IF EXISTS "idx_workflow_status_partition_dequeue";
