-- Migration 121: which recv consumed a notification.

ALTER TABLE notifications ADD COLUMN consumed_by_function_id INTEGER;
