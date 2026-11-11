# DBOS compatibility

`durare` implements the DBOS durable-execution model and stores its state in
the DBOS system schema.

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

## Compared with the official Rust SDK

DBOS publishes an official Rust SDK,
[`dbos-transact-rust`](https://github.com/dbos-inc/dbos-transact-rust).
`durare` is community-maintained. The visible difference is how a workflow
reaches its context: `durare` workflows take a `DurableContext` argument and
pass it to helpers that perform durable work, while the official SDK supplies
context through task-local state, so its workflow functions need no context
parameter.

The context parameter lets `durare` reject step placements that would lose a
checkpoint, such as a step constructed inside a spawned task or inside another
step's body. Construction order still has to be deterministic, and patches
must be awaited sequentially. The
[explicit-context design note](design/explicit-context.md#comparison-with-dboss-rust-sdk)
tabulates the placement rules of both SDKs against a pinned upstream commit and
explains the alternatives considered; the
[`design` guide](https://docs.rs/durare/latest/durare/design/) covers the
checks and caller rules.
