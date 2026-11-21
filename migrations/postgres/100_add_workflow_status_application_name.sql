-- Migration 100: Add application_name to workflow_status, the first migration
-- of the history every DBOS SDK defines identically, from version 100 on.
-- NULL means unclaimed: any application may read and claim the row.

ALTER TABLE "workflow_status" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
