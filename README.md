# durust

A **DBOS-style durable execution** library for Rust. Write normal async code;
each step is checkpointed to a database; after a crash the workflow resumes
exactly where it left off — completed steps are **not** re-run.

There is no separate server. The engine is a library that runs inside your
worker and talks directly to the state backend. Storage sits behind one trait
(`StateProvider`), so the Postgres backend shipped here can later be joined by a
DynamoDB / Aurora DSQL backend **without touching the engine** — that decoupling
is the whole design.

> ⚠️ **Not yet compiler-checked.** This v0.1 was written in an environment
> without a Rust toolchain, so it has **not** been run through `cargo check`.
> Run `cargo check` / `cargo test` locally first and expect to fix a small
> number of compile nits (see *Status* below).

## The model

```rust
async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> {
    let charge_id = ctx.step("charge_card", || async {
        Ok::<_, Error>(charge_card(&order).await?)   // side effect, recorded once
    }).await?;

    let shipment_id = ctx.step("create_shipment", || async {
        Ok::<_, Error>(create_shipment(&order).await?)
    }).await?;

    Ok(Receipt { charge_id, shipment_id })
}
```

- **`ctx.step(name, closure)`** — runs the closure once, persists its result.
  On replay, returns the stored result without re-running the closure.
- **`ctx.sleep(duration)`** — durable timer; the wake instant is persisted so it
  doesn't drift across crashes.
- **`engine.recover()`** — call on startup to resume every workflow that was
  left incomplete by a prior crash.

Steps are matched to their checkpoints by a **deterministic per-execution
counter**, so — exactly like Temporal/DBOS — your workflow's control flow must
be deterministic. Non-determinism (wall-clock, RNG, map iteration order) belongs
*inside* a step, where its result is recorded.

## Quick start (no database)

```bash
cargo run --example order      # uses the in-memory backend
cargo test                     # backend-free durability tests
```

## Crash recovery (Postgres)

```bash
createdb durust
export DATABASE_URL=postgres://localhost:5432/durust

# Run 1 — crashes right after charging the card.
CRASH_AFTER_CHARGE=1 cargo run --example order

# Run 2 — recover() resumes the workflow. The card is NOT charged again;
# the charge step is served from its checkpoint.
cargo run --example order
```

Inspect the durable state directly — it's just SQL:

```sql
SELECT id, name, status FROM durust_workflows;
SELECT workflow_id, seq, name, result FROM durust_steps ORDER BY workflow_id, seq;
```

## Architecture

```
your worker process
┌─────────────────────────────────────────────┐
│  DurableEngine  (registry of workflows)       │
│     └─ DurableContext  → ctx.step / ctx.sleep │
│            │                                  │
│            ▼                                  │
│  StateProvider (trait)                        │
│     ├─ PostgresProvider   (v0.1)              │
│     ├─ InMemoryProvider   (tests)             │
│     └─ DynamoDbProvider   (future)            │
└───────────────────────────│──────────────────┘
                            ▼
                  Postgres  (auto-managed by the DB; you
                             never operate a shard map)
```

Tables: `durust_workflows` (instances), `durust_steps` (checkpoints),
`durust_timers` (durable sleeps).

## Exactly-once, honestly

- **Workflow state transitions are exactly-once** (checkpoint per step, idempotent insert).
- **A step's external side effect is at-least-once** in the crash window
  "side-effect committed, checkpoint not yet written." Make external calls
  idempotent (idempotency keys) — same caveat as Temporal and DBOS.

## Status & known v0.1 limitations

- **Compile**: not yet verified — run `cargo check`. Likely touch-up areas:
  the `sqlx` feature set (`runtime-tokio` + `tls-rustls`) and `sqlx::raw_sql`
  exist in current 0.7.x; if your pinned version differs, adjust accordingly.
- **`ctx.sleep` holds a task** for the remaining duration instead of evicting
  the workflow. True **scale-to-zero** (persist the timer, drop the task, let an
  external poller re-invoke the workflow when due) is the next milestone — and
  the reason the timer state is already persisted separately.
- **Recovery is single-process and serial.** No distributed dispatch / queue
  yet; `recover()` re-runs incomplete workflows in the current process.
- **No signals, queries, child workflows, or versioning yet.** Deliberately out
  of scope for v0.1 — the goal here is the durable step + checkpoint + recovery
  core, nothing more.

## Roadmap (the interesting part)

1. **Scale-to-zero timers** — SQS/EventBridge-style poller re-invokes on due.
2. **`DynamoDbProvider`** — conditional writes as the checkpoint primitive; the
   `StateProvider` trait already isolates this.
3. **Ownerless dispatch** — a queue hands work to any worker; the checkpoint's
   idempotent insert is the optimistic-concurrency arbiter on redelivery.

## License

MIT OR Apache-2.0
