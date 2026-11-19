-- Migration 104: the parent workflow's application_name, denormalized.
ALTER TABLE "operation_outputs" ADD COLUMN "application_name" TEXT DEFAULT NULL;
