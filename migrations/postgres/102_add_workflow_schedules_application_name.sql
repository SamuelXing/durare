-- Migration 102: Add application_name to workflow_schedules, as migration 100.

ALTER TABLE "workflow_schedules" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
