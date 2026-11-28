-- Migration 112: steps no longer cascade from workflow_status. Deletes remove
-- them by name, and a step write checks its run exists by itself. The first
-- name is the one the constraint has in a schema another SDK created.

ALTER TABLE "operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_foreign";

ALTER TABLE "operation_outputs"
    DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_fkey";
