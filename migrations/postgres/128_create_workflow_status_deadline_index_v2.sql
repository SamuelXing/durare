-- Migration 128: Partial index for the workflow timeouts monitor, over active
-- workflows with a deadline, SUSPENDED ones included. The reference SDKs build
-- a first version without SUSPENDED in migration 122 and drop it in 129.

CREATE INDEX IF NOT EXISTS "idx_workflow_status_deadline_v2" ON "workflow_status" ("workflow_deadline_epoch_ms") WHERE "status" IN ('ENQUEUED', 'PENDING', 'DELAYED', 'SUSPENDED') AND "workflow_deadline_epoch_ms" IS NOT NULL;
