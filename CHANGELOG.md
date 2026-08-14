# Changelog

All notable changes to this project are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `TransactionOptions::read_only(true)` now works on Postgres. Every
  transactional path used to insert its durability row inside the read-only
  transaction, which Postgres rejects (`25006`) — so the option failed
  unconditionally there while SQLite silently accepted it. A read-only body
  has no writes to make atomic with a checkpoint, so the read now commits
  first (releasing its snapshot) and the checkpoint is recorded afterwards,
  ordinary-step-style: at-least-once execution is harmless for a pure read,
  and the recorded outcome is durable and replayable. Same semantics on both
  backends and on all three transactional paths.

- Transactional steps now classify a lost checkpoint race the same way plain
  steps do. Before, a `ctx.transaction` body whose checkpoint insert collided
  with an already-recorded row **committed its writes anyway** — applying the
  body a second time — and the datasource paths silently replayed the stored
  outcome and kept executing, doubling every later step's side effects. Now
  the losing transaction rolls back and the stored row is classified: an
  identical write (same name, content, and start instant) converges as a
  replay/retry; a different name is `Error::UnexpectedStep`; anything else is
  `Error::WorkflowConflict` — the losing execution stops and returns the
  recorded workflow outcome instead of its own. Applies to `ctx.transaction`
  on both SQL backends and to `transaction_on`'s single-commit and two-commit
  paths.

- `connect_with_schema` rejects reserved SQL keywords (`user`, `select`, …)
  up front with a clear error. Such names passed shape validation, then
  failed `CREATE SCHEMA` at `init` with a bare syntax error that said
  nothing about the cause. Quoted-identifier paths are unaffected — quoting
  makes any name legal.

## [0.4.1] - 2026-08-13

Durable transactions land end to end: run a step's SQL against your own
application database through your existing `sqlx` pool with exactly-once
semantics, or — when the tables live in the system database — through a
single-commit fast path with no witness table at all. Alongside them:
declarative role-based authorization, and a hardening pass that stops the
Postgres provider from trusting `search_path` — mutable session state that
a transactional step could change out from under the system's own queries.
`durare-macros` is unchanged and stays at `0.1.0`.

Compatibility: additive throughout — code written against 0.4.0 compiles
and behaves unchanged. The one trait addition,
`StateProvider::provider_identity`, ships a functional default, so custom
providers are unaffected.

### Added

- Single-commit fast path for application tables in the system database:
  `PostgresProvider::system_datasource()` / `SqliteProvider::system_datasource()`
  return a data source over the provider's own pool, so `transaction_on`
  commits the body's writes and the step checkpoint in one transaction — the
  same guarantee as `ctx.transaction`, with no witness table — while the body
  keeps the native `sqlx` connection and its full type support (`jsonb`,
  arrays, `uuid`, …) that the portable `Param` set can't express. Sameness is
  established by construction (the provider hands out its own pool), never by
  detection: a user-constructed `PgDataSource` always uses the two-commit
  protocol, which stays correct on any database. A system data source is
  bound to the provider instance that minted it via the new `ProviderIdentity`
  token (`StateProvider::provider_identity`, a defaulted method — custom
  providers are unaffected); used under a different engine it is rejected
  with an error instead of misrouting its checkpoint. The fast path's
  checkpoint insert is schema-qualified, so nothing a body does to
  `search_path` can redirect it, and a duplicate execution that loses the
  checkpoint race is rolled back — its writes discarded — and replays the
  canonical outcome, keeping the step exactly-once even under double
  execution.

- Declarative role-based authorization: `require_roles(name, roles)` on the
  engine and builder declares the roles a caller must hold to invoke a
  workflow. The check runs before the body on every execution path (direct,
  queued, scheduled, child, recovery) against the run's `AuthContext`; the
  first matching required role becomes the run's assumed role, and a denial
  is a typed `Error::NotAuthorized` (`ErrorCode::NotAuthorized`) that
  finalizes the run `ERROR` — terminal by construction, so an unauthorized
  queued run cannot loop through the dispatcher. Portable-mode rows record
  the denial under the cross-SDK `DBOSNotAuthorizedError` envelope name. A
  declaration naming an unregistered workflow is rejected at launch/build.
  Documented in the `security` guide's new Authorization section.
- Durable transactions on a separate application database:
  `ctx.transaction_on(&ds, name, |conn| …)` (and `transaction_on_with` for
  isolation/read-only/retry options) runs a body against your own database
  through a `PgDataSource` or `SqliteDataSource` over your `sqlx` pool. The
  body's writes commit atomically with a `transaction_completion` witness
  row in your database (table shape matches the Go SDK's; created on
  construction, schema configurable on Postgres), then the step checkpoint
  commits to the system database; recovery replays checkpoint-first, then
  the witness row, so the body runs exactly once even across a crash
  between the two commits. The body receives the backend's native `sqlx`
  connection (`&mut PgConnection` / `&mut SqliteConnection`), so existing
  queries, `sqlx` macros, and DAOs work unchanged; on Postgres a raw
  `COMMIT`/`ROLLBACK` inside the body is detected and refused. Permanent
  failures are mirrored into the witness table; conflicts retry on fresh
  transactions without consuming the application retry budget. The
  `DataSource` trait is sealed. Documented in the `transactions` guide.

### Fixed

- Every Postgres provider query now names its system tables with an explicit
  schema (`<schema>.workflow_status`) instead of relying on the connection's
  `search_path`. The search path is mutable session state on pooled
  connections: SQL in a transactional step that changed it could redirect the
  step's own checkpoint insert — running in the same transaction — and any
  later system query on the reused connection into the wrong schema. `init`
  likewise pins `search_path` on the one connection it runs migrations over,
  since their DDL is unqualified by design. `from_pool` providers have no
  configured schema and keep the documented contract: the caller's pool
  decides where unqualified names resolve.

### Documentation

- HTTP triggering recipe: a runnable axum example
  (`examples/http_trigger.rs`) showing the two-line integration — extract
  the `dbos-idempotency-key` header, make it the workflow id — so retried
  requests attach to the same run instead of repeating effects. The same
  shape works in any framework; no adapter crate needed.
- Event-source receiver recipe: the `messaging` guide now documents the
  exactly-once consumption pattern (message coordinates as the workflow
  id; ack only after the start persisted), which turns any at-least-once
  source — Kafka, SQS, `LISTEN` — into exactly-once workflow execution
  without a broker integration crate.

## [0.4.0] - 2026-08-01

The schema catches up to the reference SDKs (migrations 38-40) and the
history-lifecycle story lands end to end: garbage collection with the
cross-SDK semantics, an opt-in client-side retention policy (a durare
extension - no reference SDK trims history for self-hosted deployments),
searchable workflow attributes, and atomic bulk send. Rounding it out:
the security-posture pass (injection sweep, secret-handling guarantees,
`cargo deny` in CI) and the admin server's explicit bind address.
`durare-macros` is unchanged and stays at `0.1.0`.

Compatibility (the 0.x minor lane allows breaking changes): custom
`StateProvider` implementations must add `set_workflow_attributes`
(`garbage_collect` and `insert_notifications` ship functional defaults),
and several public structs gained fields (`WorkflowStatus`,
`WorkflowOptions`, `ListFilter`, `EngineConfig`, `EngineMetrics`) -
breaking only for exhaustive struct literals; `..Default::default()`
construction is unaffected. The schema migrations (38-40) apply
automatically on first connect and are shared with the other DBOS SDKs.

### Added

- Retention policy: `EngineConfig::retention(RetentionPolicy)` makes history
  trimming set-and-forget — `launch` starts a background sweeper that
  periodically garbage-collects per the policy (an age bound, a
  keep-the-newest-N bound, or both; a boundless policy is rejected at
  launch). Sweeps are per-executor with jittered intervals, so a fleet
  needs no coordination, and stop with `shutdown`. A new
  `workflows_collected_total` counter in `EngineMetrics` makes trimming
  observable. Garbage-collection deletes now run in bounded batches
  (10k rows per statement) on all backends, so enabling a policy over a
  large backlog is many short transactions instead of one long
  vacuum-pinning delete.
- Searchable workflow attributes: attach arbitrary key-value metadata at
  creation (`WorkflowOptions::attributes`), replace or clear it later
  (`set_workflow_attributes` on `DurableEngine`, `Client`, and — as one
  durable step — `DurableContext`), and filter `list_workflows` by
  containment (`ListFilter::attributes`): a workflow matches when its
  attributes contain all given pairs, served on Postgres by migration 40's
  GIN index. Cross-SDK semantics throughout: replace-not-merge, no child
  inheritance, and filtering requires Postgres (SQLite stores and reads
  attributes but errors on an attribute filter, matching the reference
  SDKs; the in-memory backend emulates containment for tests). The
  conductor's list requests accept the attributes filter and its responses
  carry each row's attributes, so the DBOS console's attribute views work
  against a Rust process.
- Bulk send: `send_bulk(&[SendMessage])` on `DurableEngine`, `Client`, and
  `DurableContext` fans one call out to many destinations — each message
  with its own destination, topic, and optional at-most-once idempotency
  key (a repeated key within one call is rejected). The SQL backends
  deliver the batch atomically in a single multi-row insert (one missing
  destination rejects the whole batch); from workflow code the batch is
  one recorded step (`DBOS.send_bulk`), so a replay re-delivers nothing.
  Backed by a new `StateProvider::insert_notifications` (sequential
  default for custom providers).
- Garbage collection: `DurableEngine::garbage_collect(cutoff_epoch_ms,
  rows_threshold)` deletes workflow history — every non-in-flight workflow
  created strictly before the cutoff, with its step/event/stream rows — and
  returns the deleted count. The cutoff is the newer of the absolute bound
  and the Nth-newest workflow's `created_at`, matching the other DBOS SDKs;
  `PENDING`/`ENQUEUED`/`DELAYED` work survives regardless of age. Backed by
  a new `StateProvider::garbage_collect` (single-statement overrides on
  Postgres/SQLite; a generic default implementation covers custom
  providers). The conductor's `retention` message now enforces its GC
  bounds — so DBOS Cloud's server-side retention policy actually takes
  effect on a console-connected app — and the admin server's
  `POST /dbos-garbage-collect` runs the real collection instead of the
  reserved no-op.

- `AdminServer::start_on` binds the admin server to an explicit address —
  e.g. loopback, so the unauthenticated control surface is reachable only
  from the machine itself. `start` keeps the cross-SDK all-interfaces
  default (orchestrator probes arrive over the pod network); the admin
  module docs now spell out the exposure model.
- Metrics snapshot: `DurableEngine::metrics()` returns an `EngineMetrics` —
  poll-style, like tokio's runtime metrics, so no metrics-system choice is
  made for you and no dependency is added. Gauges: in-flight workflow runs on
  this process, `ENQUEUED` depth per registered queue (fleet-wide, stable
  keys). Process-lifetime counters: workflows recovered, step retries,
  dead-lettered workflows, failed dequeue polls. Wiring examples in the
  `observability` guide's new Metrics section.
- Readiness probe: `DurableEngine::health()` returns a `HealthReport` with a
  reason per unhealthy axis — the state backend (reachable, dbos schema
  present and migration-current, via the new `StateProvider::ping` method,
  default healthy) and dispatch (launched, not deactivated, not shut down,
  every dispatcher task alive). Never fails; failures are the report's
  content. The admin server serves it as `GET /readyz` (`200`/`503` with the
  per-axis report) — a durare extension alongside the cross-SDK static
  `GET /dbos-healthz` liveness probe, so an orchestrator can drain a
  deactivated process without restarting it.

### Documentation

- Added an `operations` concept guide — the production resource model:
  what durare opens per backend (pool defaults; the Postgres
  `LISTEN`/`NOTIFY` listener holds one connection), who holds a connection
  for how long (transactional steps are the real occupants; blocked
  `recv`/`get_event` hold none), a pool-sizing rule of thumb and the
  executors × pool-size fleet math (with the PgBouncer session-mode
  caveat), statement-cache notes, and SQLite's single-writer shape.
- Added a `security` concept guide — the trust map: the dynamic-SQL
  invariant (no caller-supplied string becomes SQL text; every value is a
  bind parameter — now enforced by an injection sweep in `tests/security.rs`
  across both SQL backends), secret handling (the database URL and conductor
  API key are never logged; `ConductorConfig` has no `Debug` impl, pinned by
  a `compile_fail` doctest), the network-exposure model of the opt-in admin
  and conductor surfaces, and the payloads-live-in-the-database trust
  boundary.

### Security

- CI now runs `cargo deny` (RustSec advisories, a permissive-license
  allow-list, and crates.io-only source pinning, per the new `deny.toml`)
  alongside `cargo audit`, weekly and on dependency changes.

## [0.3.3] - 2026-07-15

Observability and DBOS-console compatibility: the engine now emits `tracing`
spans around every workflow and step, and the conductor client works against
the live DBOS console — connecting a demo app to it surfaced (and fixed) a
dead documented endpoint and a wire-shape incompatibility. `durare-macros` is
unchanged and stays at `0.1.0`.

### Added

- **Tracing spans.** The engine now emits `tracing` spans around every
  workflow execution (direct, queued, scheduled, child, and recovery runs)
  and every durable operation (`step`, `step_with`, `transaction`), carrying
  the DBOS trace attributes (`dbos.operation.workflow_id`,
  `dbos.application.version`, `dbos.executor.id`, `dbos.queue.name`, the
  user identity, and the recorded outcome). Step spans nest under their
  workflow span, child workflows under their parent, and replayed steps are
  marked `dbos.step.replayed = true` — so a post-crash trace shows exactly
  which steps were served from checkpoints. Spans follow the
  `tracing-opentelemetry` conventions (`otel.name`, `otel.status_code`), so
  bridging them to an OTLP exporter needs no engine configuration. See the
  new `observability` module guide.

### Changed

- An empty `ConductorConfig::url` now defaults to the hosted DBOS conductor,
  `wss://cloud.dbos.dev/conductor/v1alpha1` (the domain honors the
  `DBOS_DOMAIN` env var), matching the Go and Python SDKs — previously an
  empty URL was rejected.

### Fixed

- The conductor documentation pointed at `wss://conductor.dbos.dev`, a
  hostname that does not exist — following it produced an endless
  DNS-failure retry loop. The real endpoint is the default above.
- The conductor now tolerates explicit JSON `null` for list-typed request
  fields (`workflow_uuids`, `workflow_ids`, `executor_ids`). The conductor
  service marshals absent lists as `null`, so the console's very first
  workflow-list query failed with `serialization error: invalid type: null,
  expected a sequence` — every list view in the console was broken against
  a Rust process. Found connecting a demo app to the live console.

## [0.3.2] - 2026-07-13

Recovery ergonomics and shutdown correctness: launch can now (opt-in) resume
the work a previous run left pending, and shutdown promptly stops the
background loops and genuinely drains every in-flight run — including recovered
ones. `durare-macros` is unchanged and stays at `0.1.0`.

One compatibility note: `EngineConfig` gained a public field
(`recover_on_launch`), which is technically breaking for code constructing it
as an exhaustive struct literal. The documented construction path —
`EngineConfig::default()` plus setters — and `..Default::default()` literals
are unaffected, and no such literal usage is known. (Marking the config structs
`#[non_exhaustive]` is queued for the pre-1.0 API review, so field additions
stop being breaking at all.)

### Added

- Opt-in recovery on launch: `EngineConfig::recover_on_launch(true)` (or the
  builder's `recover_on_launch(true)`) makes `DurableEngine::launch` recover this
  executor's workflows left pending by a previous run, re-dispatching them on a
  background task — so a crash and restart resumes unfinished work without a
  separate `recover()` call. **Off by default** (no behavior change): it is
  opt-in because it is only sound when each live process has a *unique* executor
  id — recovering "this executor's" pending work assumes the previous owner is
  gone, not running concurrently. Enable it for a single-process app, or when you
  set a distinct `DBOS__VMID` per process; otherwise keep driving recovery
  yourself with `recover()`. (A future release may default it on once recovery
  is liveness-aware.) Recovery honors the graceful-shutdown contract: runs it
  re-dispatches count as in-flight, so `shutdown` drains them, and a shutdown
  that begins mid-recovery stops further dispatch — the run in flight finishes,
  the untouched remainder stays pending for a later recovery.

### Changed

- `shutdown` now stops the background loops promptly: they are signalled through
  a cancellation token they await, instead of a flag they polled between
  iterations — previously a queue dispatcher asleep on its poll interval would
  not notice shutdown until it woke (up to the queue's base polling interval).
  In-flight runs are likewise drained through a task tracker that counts a run
  from the moment it is spawned. Internal modernization (`tokio-util`'s
  `CancellationToken` + `TaskTracker`); no API change.

## [0.3.1] - 2026-07-12

### Added

- `Error::MaxRecoveryAttemptsExceeded` and the matching
  `ErrorCode::MaxRecoveryAttemptsExceeded`: a workflow that exceeds its
  recovery-attempt cap and is parked in the `MAX_RECOVERY_ATTEMPTS_EXCEEDED`
  dead-letter state now surfaces this typed error when its result is awaited, so
  a caller can distinguish a parked workflow from one that ran to completion.
- The queue registry is now persisted to the `queues` table on `launch` — the
  database-backed, fleet-wide registry the DBOS conductor and control plane read —
  and `DurableEngine::list_queues()` reads it back. A queue registered by any
  executor against a shared database is visible to every conductor pointed at it,
  matching the Go and Python SDKs. The write is version-gated and resolved on
  launch: a process self-elects as latest when it first registers its version (so
  its queue config lands on the first launch), and an already-registered
  older-version straggler will not overwrite a newer queue's configuration.
- Durable `ctx.now()`, `ctx.uuid()`, and `ctx.random()`: read the wall clock,
  mint a v4 UUID, or draw an `f64` in `[0, 1)` inside a workflow and have the
  value **checkpointed** — recorded on first execution and replayed identically
  after a recovery, instead of silently breaking determinism the way a bare
  `Utc::now()` / `Uuid::new_v4()` would. Each consumes one step slot, like
  `ctx.sleep`.

### Changed

- The Conductor client's queue views (`list_queues` / `get_queue`) now read the
  database-backed `queues` table (fleet-wide) rather than this process's in-memory
  registry, so a conductor sees queues registered by every executor. The admin
  server's `/dbos-workflow-queues-metadata` still reports the local in-process
  registry (matching Go).

### Fixed

- Awaiting a dead-lettered workflow (`WorkflowHandle::result` /
  `retrieve_workflow` + `await`) no longer falls through to output decoding —
  which for a unit-typed workflow silently returned `Ok(())`, masking the
  failure, and for other output types produced a confusing deserialization
  error. It now returns the typed error above.
- A panic in a workflow or step body is now caught rather than unwinding past the
  terminal-status write, which previously left the row non-terminal (`PENDING`)
  with any polling observer waiting forever. A panic in a **step** becomes a step
  error subject to that step's retry policy (a step that panics once can succeed
  on retry). A panic in the **workflow body** is treated as a recoverable failure,
  like a crash: the row is left non-terminal and a later `recover()` re-runs it
  from its checkpoints (bounded by the recovery-attempt cap — a deterministic
  panic eventually dead-letters), matching the durable-execution model where only
  a returned error terminates a workflow. (Requires the default `panic = "unwind"`;
  under `panic = "abort"` there is nothing to catch.)

### Documentation

- Added a `determinism` concept guide — a `std`-style companion to the
  `durability` guide covering how to write a correct workflow body: the catalog
  of non-determinism foot-guns (wall clock, RNG, `HashMap` iteration order,
  `spawn`/task races, `Drop` side effects, direct env/config/file/network reads)
  and their durable fixes; the durable-safe data rules for values that cross a
  checkpoint-and-replay or cross-SDK boundary (no `NaN`/infinity, string-encoded
  integers past 2⁵³, ordered maps for byte-stable records); and the
  dependency-injection pattern — build a pool/client/config once at startup into
  a process global and read it inside steps, never in durable state — with a note
  on why workflows stay free functions and the trigger that would justify a
  method-based API.
- Added `examples/dependencies.rs`, a runnable companion to that guide's
  dependency-injection section: a `PricingService` (stand-in for an HTTP client
  and its config) wired through a global `OnceLock` and read inside a step, with
  a re-run proving the dependency is invoked exactly once while the replay serves
  the checkpoint.

## [0.3.0] - 2026-07-12

The feature-gating release: optional components you don't use no longer weigh
down your build. The Postgres and SQLite backends are cargo features (both on by
default; at least one required), and the Conductor client and admin HTTP server
are opt-in. `durare-macros` is unchanged and stays at `0.1.0`.

### Changed

- **(breaking)** The DBOS Conductor client — `Conductor`, `ConductorConfig`,
  `AlertHandler` — now lives behind an opt-in `conductor` cargo feature, off by
  default. Enable it with `features = ["conductor"]`. This keeps its
  `tokio-tungstenite` (TLS websocket) and `flate2` (gzip) dependencies out of
  builds that never talk to the DBOS control plane.
- The Postgres and SQLite backends are now cargo features (`postgres`,
  `sqlite`), both enabled by default. Enable a single backend to drop the
  other's driver: a Postgres-only build skips SQLite's bundled C library, and a
  SQLite-only build skips the Postgres network/TLS driver. **At least one backend
  is required** — a build with neither is a compile error. `InMemoryProvider`
  stays available in every build.
- **(breaking)** The admin HTTP server (`AdminServer`) is now behind an opt-in
  `admin` cargo feature, off by default. Enable it with `features = ["admin"]`.
  This keeps the axum/hyper/tower HTTP stack out of builds that don't expose the
  DBOS admin endpoints.

### Documentation

- Added a "Cargo features" section to the crate docs and the README documenting
  the `postgres`, `sqlite`, and opt-in `conductor`/`admin` features, and fixed
  the stale README quick-start version requirement (it had pinned `0.1`, which
  does not resolve to newer releases).

## [0.2.0] - 2026-07-11

This release proves durare's on-the-wire compatibility with the other DBOS
Transact SDKs (Python, Go, TypeScript, Java) and carries one small breaking
change to keep `Result<_, Error>` cheap. `durare-macros` is unchanged and stays
at `0.1.0`.

### Added

- Cross-SDK serialization conformance tests (`tests/interop.rs`) asserting durare
  reproduces the shared DBOS golden `portable_json` strings byte-for-byte
  (encode, decode, both input-envelope orderings, structured errors, round-trip).
- End-to-end cross-SDK conformance test (`tests/interop_db.rs`, SQLite +
  Postgres) mirroring the other SDKs' direct-insert replay: portable rows are
  written to the `dbos` schema via raw SQL (as a Python/Go/TS/Java producer
  would), and durare's engine claims the `ENQUEUED` workflow, runs it (portable
  input → event → stream → consuming a portable message), and writes
  byte-identical output/event/stream.
- Conformance test that durare reads a workflow another SDK ran and *failed*:
  the portable error envelope surfaces as structured `error_info` and
  `result()` reconstructs the typed `Error::Portable`.

### Changed

- **(breaking)** `Error::Portable` now wraps a `Box<PortableWorkflowError>`
  rather than a bare `PortableWorkflowError`, so `Error` (and every
  `Result<_, Error>`) stays small after the `preserve_order` change enlarged
  `serde_json::Value`. Construct it as
  `Error::Portable(Box::new(PortableWorkflowError { … }))` or via the unchanged
  `Error::portable(name, message)` constructor; field access on a matched value
  is unaffected (the `Box` auto-derefs).

### Fixed

- **Portable serialization now preserves object key order** (enabled
  `serde_json`'s `preserve_order`). durare previously sorted object keys
  alphabetically, so its `portable_json` records — though still readable — were
  not byte-identical to those written by the Python, Go, TypeScript, and Java
  SDKs. Cross-SDK portable records are now byte-compatible.

## [0.1.1] - 2026-07-11

Documentation-only release: no library code changed, so `durare-macros` stays at
`0.1.0`. Every improvement below is visible on [docs.rs](https://docs.rs/durare).

### Documentation

- Documented every public API item and enabled `#![warn(missing_docs)]`, now
  enforced in CI so the public surface stays fully documented.
- Rewrote the crate-level docs (the docs.rs landing page) around a tested
  example of the `#[durare::workflow]` + `start_with` path, with a capability
  map linking every major API.
- Converted all `ignore`d doc examples to compiled (most of them runnable)
  doctests, so every example in the docs is checked by `cargo test`.
- Added examples and `# Errors` sections to the hot-path APIs — `step`,
  `step_with`, `sleep`, `send`/`recv`, `set_event`/`get_event`,
  `write_stream`, `start_workflow`, `DurableEngine::start`,
  `WorkflowHandle::result`, and `Client` — plus `#[doc(alias)]`es ("cron",
  "signal", "delay", "timer") for docs.rs search.
- Added crates.io and docs.rs badges, an MSRV policy section, and a
  `CONTRIBUTING.md`.
- Added four `std`-style concept guides as public modules — `durability`
  (checkpoints, replay, and the determinism contract), `queues`, `messaging`,
  and `transactions` — each a module-level essay with tested, mostly runnable
  examples.

## [0.1.0] - 2026-07-10

First release. A DBOS-compatible durable-execution SDK for Rust: write ordinary
async code, checkpoint every step to Postgres or SQLite, and resume unfinished
workflows after a crash.

### Added

- Durable workflows and steps — `#[durare::workflow]`, `#[durare::step]`, and
  `ctx.step` / `ctx.step_with` with exponential-backoff retry policies.
- Transactions — `#[durare::transaction]` commits SQL and its checkpoint in one
  database transaction, making the step exactly-once.
- Durable timers — `ctx.sleep` with a persisted wake instant that does not drift
  across restarts.
- Queues — per-process and global concurrency limits, rate limiting, priorities,
  delayed enqueue, deduplication, and partitioned queues.
- Scheduling — six-field cron via `#[durare::workflow(schedule = "…")]`, plus a
  managed schedule API (create, pause, resume, trigger, backfill).
- Messaging, events, and streams — durable FIFO `send` / `recv`, idempotency-key
  sends, `set_event` / `get_event`, and append-only streams a consumer can tail.
- Child workflows — `ctx.start_workflow` with deterministic ids and parent links.
- Recovery and versioning — `recover()` by application version, a version
  registry for fleet routing, and a recovery-attempt cap.
- Management — list, cancel, resume, and fork (from an arbitrary step) workflows;
  per-workflow timeouts; `ctx.patch` for evolving in-flight workflows; debouncing.
- Operations — an admin HTTP server with the standard DBOS endpoints, and a DBOS
  Conductor client.
- A registry-free `Client` for out-of-process producers.
- Backends — Postgres, SQLite, and in-memory, behind one `StateProvider` trait.
- DBOS compatibility — state is stored in the `dbos` system schema with the same
  tables the DBOS Transact SDKs use, plus a portable cross-SDK serialization
  envelope.

[Unreleased]: https://github.com/SamuelXing/durare/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/SamuelXing/durare/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/SamuelXing/durare/compare/v0.3.3...v0.4.0
[0.3.3]: https://github.com/SamuelXing/durare/compare/v0.3.2...v0.3.3
[0.3.2]: https://github.com/SamuelXing/durare/compare/v0.3.1...v0.3.2
[0.3.1]: https://github.com/SamuelXing/durare/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/SamuelXing/durare/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/SamuelXing/durare/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/SamuelXing/durare/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/SamuelXing/durare/releases/tag/v0.1.0
