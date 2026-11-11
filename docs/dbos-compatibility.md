# DBOS compatibility

`durare` is an independently developed Rust durable-execution library. Its
storage formats and tooling integrations are designed for compatibility with
DBOS.

## The system schema

On Postgres the tables live in the `dbos` schema: the same schema, tables, and
columns the DBOS Transact SDKs for Python, Go, TypeScript, and Rust use.
`workflow_status`, `operation_outputs`, `workflow_events`, `notifications`,
`streams`, `workflow_schedules`, `queues`, and the version registry are applied
through embedded per-dialect migrations and recorded in the shared
`dbos_migrations` ledger. [`schema.md`](schema.md) is the column-level
reference: every table, how they relate, and the ledger protocol.

This means:

- Workflow state is inspectable with plain SQL, and rows written by a `durare`
  worker are legible to standard DBOS tooling pointed at the same database.
- Arguments, outputs, and errors use the portable serialization envelope, with
  a structured cross-SDK error format. Custom codecs can be installed through
  `Serializer`.
- The admin server (behind the `admin` feature) exposes the standard DBOS HTTP
  endpoints (`/dbos-healthz`, `/workflows`, cancel/resume/fork, recovery, queue
  metadata), and the Conductor client connects to DBOS Conductor for fleet
  management.

```sql
SELECT workflow_uuid, name, status FROM dbos.workflow_status;
SELECT workflow_uuid, function_id, function_name, output FROM dbos.operation_outputs;
```
