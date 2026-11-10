-- Migration 42: the debounce deadline and marker Postgres migration 42 adds.
ALTER TABLE workflow_status ADD COLUMN "debounce_deadline_epoch_ms" BIGINT DEFAULT NULL;
ALTER TABLE workflow_status ADD COLUMN "is_debounced" BOOLEAN NOT NULL DEFAULT FALSE;
