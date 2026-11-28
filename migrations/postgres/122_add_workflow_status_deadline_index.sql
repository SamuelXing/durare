-- Migration 122: Reserved. The reference SDKs build
-- idx_workflow_status_deadline here, supersede it in migration 128 and drop it
-- in migration 129. Building an index only to drop it in the same transaction
-- would scan the table for nothing, so this step does no work, as migration 45
-- does not; migration 129 still drops the index where a peer built it.

SELECT 1;
