-- Migration 103: Add application_name to application_versions, as migration 100.

ALTER TABLE "application_versions" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
