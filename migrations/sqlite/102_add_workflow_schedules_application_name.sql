-- Migration 102: application_name on workflow_schedules, as migration 100.
ALTER TABLE "workflow_schedules" ADD COLUMN "application_name" TEXT DEFAULT NULL;
