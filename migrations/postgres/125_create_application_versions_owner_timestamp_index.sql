-- Migration 125: The same index per application, for an owner's latest version.

CREATE INDEX IF NOT EXISTS "idx_application_versions_owner_timestamp" ON "application_versions" ("application_name", "version_timestamp" DESC);
