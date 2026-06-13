-- Migration 11: Add serialization column to workflow and event tables
-- Stores serialization format/metadata for workflow data.

ALTER TABLE workflow_status ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE notifications ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE workflow_events ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE workflow_events_history ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE operation_outputs ADD COLUMN serialization TEXT DEFAULT NULL;
ALTER TABLE streams ADD COLUMN serialization TEXT DEFAULT NULL;
