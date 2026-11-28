-- Migration 114: drop idx_notifications, a duplicate of idx_workflow_topic.

DROP INDEX IF EXISTS "idx_notifications";
