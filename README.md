# durare

[![Crates.io](https://img.shields.io/crates/v/durare.svg)](https://crates.io/crates/durare)
[![Documentation](https://docs.rs/durare/badge.svg)](https://docs.rs/durare)
[![CI](https://github.com/SamuelXing/durare/actions/workflows/ci.yml/badge.svg)](https://github.com/SamuelXing/durare/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

`durare` (Latin *durāre*, "to last") is a durable-execution library for Rust.
Write ordinary async functions; `durare` checkpoints each step to your database
and, after a crash, restart, or redeploy, resumes every unfinished workflow
exactly where it stopped. Completed steps are never re-run.

`durare` is a Rust SDK for [DBOS](https://docs.dbos.dev) durable execution,
aligned by design with the DBOS Transact SDKs for Python, Go, TypeScript, and
Rust: the same programming model, the same semantics, and the same system schema
on the same database. There is no server to operate and no sidecar — the engine
is a library inside your process that talks directly to Postgres or SQLite.
See [DBOS compatibility](#dbos-compatibility). Full API documentation is on
[docs.rs](https://docs.rs/durare).

DBOS also publishes an official Rust SDK,
[`dbos-transact-rust`](https://github.com/dbos-inc/dbos-transact-rust). `durare` is
community-maintained. Both use the DBOS system schema.

## Why the context is explicit

Workflows in `durare` take a `DurableContext` argument and pass it to helpers
that perform durable work. The official Rust SDK supplies context through
task-local state instead, so its workflow functions need no context parameter.

For step calls inline, in helpers, or as prebuilt `join!` arguments, both SDKs
reserve positions at construction. Their placement rules differ:

| Step placement | `durare` | Official Rust SDK |
|---|---|---|
| constructed inside a spawned task | rejected without claiming a position | runs without a checkpoint; repeats if the spawning code runs again |
| constructed inside another step's body | rejected without claiming a position | runs as plain work within the enclosing body |
| constructed in the workflow body, then polled inside a step body | rejected on poll; the reserved position stays spent | rejected on poll |

These are step rules. Other upstream operations, such as `set_event` and
`recv`, reject calls outside a workflow or inside a step.

The context parameter lets `durare` reject step placements that would lose a
checkpoint. Construction order still has to be deterministic, and patches
must be awaited sequentially. The
[`design` guide](https://docs.rs/durare/latest/durare/design/) covers the checks
and caller rules; the
[design note](https://github.com/SamuelXing/durare/blob/main/docs/design/explicit-context.md)
explains the alternatives considered.

```rust
use std::time::Duration;
use durare::{DurableContext, DurableEngine, Result, WorkflowOptions};

#[durare::step]
async fn charge_card(ctx: &DurableContext, order_id: String) -> Result<String> {
    // Any side effect: an HTTP call, an email, a write to another system.
    // It runs once; the result is checkpointed and replayed thereafter.
    Ok(format!("ch_{order_id}"))
}

#[durare::workflow]
async fn process_order(ctx: DurableContext, order_id: String) -> Result<String> {
    let charge_id = charge_card(&ctx, order_id).await?;
    ctx.sleep(Duration::from_secs(24 * 3600)).await?; // durable timer
    Ok(charge_id)
}

#[tokio::main]
async fn main() -> Result<()> {
    let engine = DurableEngine::connect("postgres://localhost/app").await?.build().await?;
    engine.recover().await?; // resume whatever a previous process left unfinished

    let handle = engine
        .start_with(ProcessOrder, "1001".into(), WorkflowOptions::with_id("order-1001"))
        .await?;
    println!("charged: {}", handle.await?);
    Ok(())
}
```

The workflow above sleeps for a day between charging and returning. Kill the
process at any point — mid-sleep included — and the next `recover()` picks it
up with the charge intact and only the remaining sleep to wait. The card is
never charged twice.

## Features

- **Steps.** `#[durare::step]` functions or `ctx.step` closures. Per-step retry
  policy with exponential backoff and a retry predicate (`StepOptions`).
- **Transactions.** `#[durare::transaction]` runs your SQL and records the
  step's completion in the same database transaction, so the write and its
  checkpoint commit or roll back together. This makes the step exactly-once,
  not at-least-once.
- **Timers.** `ctx.sleep` persists its wake instant; a replay waits only the
  remaining time.
- **Queues.** Per-process and global concurrency limits, rate limiting,
  priorities, delayed enqueue, deduplication (reject or return-existing), and
  partitioned queues.
- **Scheduling.** Six-field cron via `#[durare::workflow(schedule = "…")]`,
  plus a managed schedule API: create, pause, resume, trigger, backfill.
- **Messaging, events, streams.** Durable FIFO `send`/`recv` between workflows
  and from the outside, idempotency-key sends, key-value `set_event`/
  `get_event`, and append-only streams that consumers can tail live.
- **Child workflows.** `ctx.start_workflow` with deterministic child ids and
  parent links.
- **Recovery and versioning.** `recover()` resumes by application version, a
  version registry routes work across a fleet, and runaway workflows park
  after a recovery-attempt cap.
- **Management.** List, cancel, resume, and fork (from an arbitrary step)
  workflows; per-workflow timeouts; `ctx.patch` for changing workflow code
  while old runs are still in flight; debouncing for coalescing bursts.
- **Operations.** An admin HTTP server with the standard DBOS endpoints, and a
  client for DBOS Conductor (behind the `conductor` feature).
- **Out-of-process producers.** A registry-free `Client` for services that
  submit and observe workflows but run none of them.

## Quick start

```toml
[dependencies]
durare = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The repository ships eleven self-contained examples, one per primitive plus the
dependency-injection pattern:

| Example | Shows |
| --- | --- |
| `order` | workflow + steps, crash recovery via a fault-injection crash |
| `saga` | compensation on failure |
| `pipeline` | queues: fan-out under a concurrency limit |
| `scheduled` | cron workflows |
| `transfer` | `#[transaction]`: exactly-once money movement, proven by re-run |
| `approval` | human-in-the-loop: durable `recv`, events for observers |
| `client` | out-of-process submit and observe |
| `timer` | durable sleep |
| `subworkflow` | child-workflow fan-out |
| `stream` | live-tailed progress feed |
| `dependencies` | dependency injection: deps in a global, read inside steps |

```bash
cargo run --example saga          # in-memory, no database needed

# Crash recovery, for real:
createdb app
export DATABASE_URL=postgres://localhost:5432/app
FAILPOINTS=after_charge=return cargo run --example order   # charges, then crashes
cargo run --example order                                  # resumes; does not re-charge
```

## Cargo features

Backends are compiled behind features, all on by default; enable just one to
drop the other's driver. **At least one backend is required** — a build with
neither is a compile error. `InMemoryProvider` is always available (no feature).

| Feature | Default | Enables |
| --- | --- | --- |
| `postgres` | ✅ | The `PostgresProvider` backend (sqlx Postgres driver). |
| `sqlite` | ✅ | The `SqliteProvider` backend (sqlx SQLite driver — a bundled C library). |
| `conductor` | — | The DBOS Conductor client (`Conductor`, `ConductorConfig`, `AlertHandler`) — a websocket client for the DBOS control plane; pulls in a TLS websocket stack and gzip framing. |
| `admin` | — | The `AdminServer` HTTP control surface — health, recovery, and workflow-management endpoints for the DBOS console/conductor and health probes; pulls in the axum/hyper/tower stack. |

For a Postgres-only build (no SQLite C compile) that also enables the Conductor
client:

```toml
durare = { version = "0.3", default-features = false, features = ["postgres", "conductor"] }
```

## How it works

Each side-effecting operation a workflow performs — a step, a sleep, a message
receive, a child start — is recorded in the `operation_outputs` table, keyed by
a deterministic per-execution counter and guarded by the operation's name. When
a workflow is re-executed after a failure, operations that already ran return
their recorded results instead of running again, and execution proceeds from
the first checkpoint that is missing.

The consequence, as in every durable-execution system: workflow control flow
must be deterministic. Wall-clock reads, random numbers, and anything else
non-repeatable belong inside a step, where the result is recorded.

What the guarantees actually are:

| Operation | Guarantee |
| --- | --- |
| Workflow state transitions, completion | exactly once |
| Step side effect | at least once — make external calls idempotent |
| `#[transaction]` SQL | exactly once (commits with its own checkpoint) |
| `recv` message consumption | exactly once |
| Cron tick | once per tick, across any number of executors |

The step caveat is the same one Temporal and the DBOS SDKs carry: a crash can
land between an external call and its checkpoint. Transactions close that
window for SQL against the workflow database, which is why they exist as a
separate primitive.

## DBOS compatibility

`durare` implements the DBOS durable-execution model and stores its state in the
DBOS system schema. On Postgres the tables live in the `dbos` schema — the same
schema, tables, and columns the DBOS Transact SDKs for Python, Go, and
TypeScript use: `workflow_status`, `operation_outputs`, `workflow_events`,
`notifications`, `streams`, `workflow_schedules`, `queues`, and the version
registry, applied through embedded per-dialect migrations.

In practice this means:

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

## Backends

| Backend | Intended use | Notes |
| --- | --- | --- |
| `PostgresProvider` | production, multi-executor | `LISTEN`/`NOTIFY` wakes blocked `recv`/`get_event`; queue claims use `FOR UPDATE SKIP LOCKED`; `connect_with_schema` for a custom schema |
| `SqliteProvider` | single-node deployments, local development | full durability on a file database |
| `InMemoryProvider` | tests and examples | no cross-process durability |

All three implement one trait, `StateProvider`, which is also the seam for
adding further backends. `PostgresProvider` and `SqliteProvider` are gated by the
`postgres` and `sqlite` [Cargo features](#cargo-features) (both on by default);
`InMemoryProvider` is always available. Application tests can run workflows
against `InMemoryProvider` with no infrastructure at all; the crate's own test
suite runs against all three backends on every commit, with Postgres against a
live server in CI.

## Benchmarks

[`durare-benchmarks/`](durare-benchmarks/) measures durare on the same
workload the DBOS SDKs benchmark themselves with, plus the metrics where a
compiled, GC-free runtime actually differs (tail latency under load, memory
per in-flight workflow). Durable-execution throughput is dominated by
database round-trips, so read the numbers with that framing — the harness
README documents the methodology, and a nightly CI job keeps the workloads
running.

## Minimum supported Rust version

`durare` builds on Rust **1.88** and later, enforced by a dedicated CI job.
Raising the MSRV is a semver-minor change (it will never happen in a patch
release).

## Contributing

Bug reports and pull requests are welcome — see
[CONTRIBUTING.md](CONTRIBUTING.md) for the development setup and the checks to
run before submitting.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
