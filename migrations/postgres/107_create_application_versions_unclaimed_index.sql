-- Migration 107: The unclaimed half of the key pair started in migration 106.
-- Every pre-upgrade row is unclaimed, so the index covers them all; the table
-- holds one row per application version, so it is built inside the migration
-- transaction.

CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_unclaimed_version" ON "application_versions" ("version_name") WHERE "application_name" IS NULL;
