-- Migration 120: drop the step-completion index migration 119 supersedes
-- where it still exists; migration 112 did not rebuild it.

DROP INDEX IF EXISTS "idx_operation_outputs_completed_at_function_name";
