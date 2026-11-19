-- Migration 100: the owning application, as on Postgres. NULL means unclaimed.
ALTER TABLE "workflow_status" ADD COLUMN "application_name" TEXT DEFAULT NULL;
