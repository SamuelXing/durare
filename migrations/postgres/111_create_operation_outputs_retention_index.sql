-- Migration 111: index the step sweep order. Built inside the migration
-- transaction, which holds off step writes for one scan of the table.

CREATE INDEX IF NOT EXISTS "idx_operation_outputs_retention"
    ON "operation_outputs" ("retention_timestamp");
