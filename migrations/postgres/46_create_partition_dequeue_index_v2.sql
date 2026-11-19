-- Migration 46: Create the partitioned-queue dequeue index: the in-flight
-- index (migration 32) with queue_partition_key ahead of the sort columns and
-- workflow_uuid behind them. Its leading (queue_name, status,
-- queue_partition_key) columns serve the walk over a queue's distinct
-- partition keys, one index seek per key. Covers only in-flight rows with a
-- partition key, so it is built inside the migration transaction, as the
-- in-flight index is.

CREATE INDEX IF NOT EXISTS "idx_workflow_status_partition_dequeue_v2" ON "workflow_status" ("queue_name", "status", "queue_partition_key", "priority", "created_at", "workflow_uuid") WHERE "status" IN ('ENQUEUED', 'PENDING') AND "queue_partition_key" IS NOT NULL;
