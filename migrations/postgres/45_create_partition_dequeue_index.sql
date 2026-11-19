-- Migration 45: Reserved. The Go and Python SDKs build
-- idx_workflow_status_partition_dequeue here, then supersede it in migration
-- 46 and drop it in migration 47. Building an index only to drop it in the same
-- transaction would scan the table and block its readers for nothing, so this
-- step does no work; migration 47 still drops the index where a peer built it.

SELECT 1;
