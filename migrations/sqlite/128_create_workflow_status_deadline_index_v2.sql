-- Migration 128: partial index for the workflow timeouts monitor, over active
-- workflows with a deadline, SUSPENDED ones included.

CREATE INDEX IF NOT EXISTS "idx_workflow_status_deadline_v2" ON "workflow_status" ("workflow_deadline_epoch_ms") WHERE "status" IN ('ENQUEUED', 'PENDING', 'DELAYED', 'SUSPENDED') AND "workflow_deadline_epoch_ms" IS NOT NULL;
