-- Migration 111: Index the step sweep order. Built inside the migration
-- transaction, with the table locked for one scan of it.

CREATE INDEX IF NOT EXISTS "idx_operation_outputs_retention"
    ON "operation_outputs" ("retention_timestamp");
