-- Migration 111: reserved. Migration 112 rebuilds operation_outputs and
-- creates idx_operation_outputs_retention on the new table, so an index built
-- here would be dropped with the old one.

SELECT 1;
