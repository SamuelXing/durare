-- Migration 118: drop the index idx_workflow_status_partition_dequeue_v3
-- supersedes.

DROP INDEX IF EXISTS "idx_workflow_status_partition_dequeue_v2";
