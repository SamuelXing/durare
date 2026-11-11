-- Migration 44: Drop the per-row workflow_events NOTIFY trigger installed by
-- migration 1, for the same reason as migration 43: notifying from inside the
-- write transaction serializes event writes on the notification queue's
-- global lock. set_event notifications are now coalesced in memory and pushed
-- off the write path by the notifier loop.
--
-- The notifications trigger (send) is deliberately kept: messages can be sent
-- from anywhere, including processes with no notifier loop to flush them.

DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON workflow_events;
DROP FUNCTION IF EXISTS workflow_events_function();
