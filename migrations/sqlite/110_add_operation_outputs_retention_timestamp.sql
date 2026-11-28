-- Migration 110: the time each step was written, which orders a sweep of
-- steps left without a run. Nullable: SQLite cannot ADD COLUMN NOT NULL with a
-- non-constant default; migration 112 rebuilds the table.

ALTER TABLE operation_outputs ADD COLUMN retention_timestamp INTEGER;
