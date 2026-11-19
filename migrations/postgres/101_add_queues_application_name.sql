-- Migration 101: Add application_name to queues, as migration 100.

ALTER TABLE "queues" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
