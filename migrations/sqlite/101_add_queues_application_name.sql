-- Migration 101: application_name on queues, as migration 100.
ALTER TABLE "queues" ADD COLUMN "application_name" TEXT DEFAULT NULL;
