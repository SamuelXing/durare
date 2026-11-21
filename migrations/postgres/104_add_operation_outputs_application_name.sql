-- Migration 104: Add application_name to operation_outputs, denormalized from
-- the parent workflow so step observability can filter without a join.

ALTER TABLE "operation_outputs" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
