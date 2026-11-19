-- Migration 106: with 107, the keys replacing version_name's global uniqueness.
CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_owner_version"
    ON "application_versions" ("application_name", "version_name")
    WHERE "application_name" IS NOT NULL;
