-- Migration 107: the unclaimed half of the key pair migration 106 started.
CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_unclaimed_version"
    ON "application_versions" ("version_name")
    WHERE "application_name" IS NULL;
