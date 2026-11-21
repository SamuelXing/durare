-- Migration 109: Workflow inputs and outcomes move to their own tables, keyed
-- by workflow id and without a foreign key. A run's payload is the row here
-- when there is one, and the workflow_status column otherwise.

CREATE TABLE IF NOT EXISTS "workflow_input" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    inputs TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE TABLE IF NOT EXISTS "workflow_output" (
    workflow_uuid TEXT NOT NULL PRIMARY KEY,
    output TEXT,
    error TEXT,
    retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
);

CREATE INDEX IF NOT EXISTS "idx_workflow_input_retention"
    ON "workflow_input" ("retention_timestamp");

CREATE INDEX IF NOT EXISTS "idx_workflow_output_retention"
    ON "workflow_output" ("retention_timestamp");
