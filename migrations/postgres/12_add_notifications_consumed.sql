-- Migration 12: Add consumed column to notifications and index for unconsumed lookups.

ALTER TABLE notifications ADD COLUMN consumed BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX "idx_notifications" ON notifications (destination_uuid, topic);
