-- Migration 103: application_name on application_versions, as migration 100.
ALTER TABLE "application_versions" ADD COLUMN "application_name" TEXT DEFAULT NULL;
