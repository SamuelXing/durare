-- Migration 126: Topic a SUSPENDED recv waits on.

ALTER TABLE "workflow_status"
    ADD COLUMN IF NOT EXISTS "suspend_key" TEXT DEFAULT NULL;
