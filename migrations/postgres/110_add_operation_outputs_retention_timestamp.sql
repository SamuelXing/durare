-- Migration 110: Stamp each step with the time it was written. A sweep of
-- steps left without a run, which runs outside the SDKs, is ordered by it.

ALTER TABLE "operation_outputs"
    ADD COLUMN IF NOT EXISTS "retention_timestamp" BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint;
