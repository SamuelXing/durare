//! Transactional steps: SQL writes and the step checkpoint in **one** commit —
//! exactly-once, even across a crash.
//!
//! # The problem
//!
//! An ordinary [step](crate::DurableContext::step) performs its side effect,
//! *then* commits its checkpoint — two writes, and a crash between them
//! re-runs the step on replay. That is the [at-least-once
//! window](crate::durability#the-at-least-once-window), and for most effects
//! (HTTP calls, emails) idempotency keys are the answer. But when the effect
//! *is a write to the workflow database*, there is a better answer: run the
//! SQL **inside the same database transaction as the checkpoint**. Either both
//! commit or neither does — there is no window. That is what
//! [`DurableContext::transaction`] does.
//!
//! This example runs against a real SQLite database. Note what the second
//! `start` under the same workflow id does — and does not do — to the balance:
//!
//! ```
//! use durare::{DurableContext, DurableEngine, Result, SqliteProvider, WorkflowOptions, params};
//! use std::sync::Arc;
//!
//! #[durare::workflow]
//! async fn transfer(ctx: DurableContext, amount: i64) -> Result<i64> {
//!     ctx.transaction("move_funds", move |tx| Box::pin(async move {
//!         // Demo setup — a real app would have its schema already.
//!         tx.execute("CREATE TABLE IF NOT EXISTS accounts (name TEXT PRIMARY KEY, balance INTEGER)", &params![]).await?;
//!         tx.execute("INSERT OR IGNORE INTO accounts VALUES ('alice', 100), ('bob', 100)", &params![]).await?;
//!
//!         // The writes and this step's checkpoint commit atomically.
//!         tx.execute("UPDATE accounts SET balance = balance - ? WHERE name = 'alice'", &params![amount]).await?;
//!         tx.execute("UPDATE accounts SET balance = balance + ? WHERE name = 'bob'", &params![amount]).await?;
//!         let row = tx.query_one("SELECT balance FROM accounts WHERE name = 'bob'", &params![]).await?;
//!         Ok(row.get::<i64>("balance"))
//!     })).await
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! # let path = std::env::temp_dir().join(format!("durare-tx-guide-{}.db", std::process::id()));
//! # for ext in ["", "-wal", "-shm"] {
//! #     std::fs::remove_file(format!("{}{ext}", path.display())).ok();
//! # }
//! # let url = format!("sqlite://{}", path.display());
//! let engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
//!
//! let handle = engine.start_with(Transfer, 30, WorkflowOptions::with_id("tx-1")).await?;
//! assert_eq!(handle.await?, 130); // bob: 100 + 30
//!
//! // Same workflow id again: the recorded outcome is returned — the SQL
//! // does NOT run twice. Bob has 130, not 160.
//! let replay = engine.start_with(Transfer, 30, WorkflowOptions::with_id("tx-1")).await?;
//! assert_eq!(replay.await?, 130);
//! # for ext in ["", "-wal", "-shm"] {
//! #     std::fs::remove_file(format!("{}{ext}", path.display())).ok();
//! # }
//! # Ok(())
//! # }
//! ```
//!
//! # How it works
//!
//! The body runs on the workflow database's own pool. The engine opens one
//! transaction, hands the body a [`Tx`], and — after the body returns —
//! inserts the step's checkpoint **inside that same transaction**, then
//! commits. On replay, the recorded outcome is read back and returned without
//! running the body at all. One consequence of the single-transaction model:
//! the tables you touch must live in the **same database** as the `dbos`
//! system schema. For tables in a database of their own, see [a separate
//! application database](#a-separate-application-database) below.
//!
//! Transactions require a SQL backend — [`PostgresProvider`] or
//! [`SqliteProvider`]. On [`InMemoryProvider`] they return an error.
//!
//! # Failure semantics
//!
//! If the body returns an error, its SQL **rolls back** — a failed
//! transactional step never leaves partial writes. The *error itself* is still
//! checkpointed (in a separate write, outside the aborted transaction), so the
//! failure is durable too: a replay yields the same error without re-running
//! the body, exactly like an ordinary failed step.
//!
//! # Conflicts and isolation
//!
//! [`TransactionOptions`] selects an [`IsolationLevel`]
//! (`ReadCommitted` — the default — `RepeatableRead`, or `Serializable`) and a
//! read-only hint. Under the stronger levels the database may abort a
//! transaction with a serialization conflict (Postgres `40001`/`40P01`, SQLite
//! `BUSY`/`LOCKED`); the engine **retries the whole transaction on a fresh
//! one** with backoff, which is why the body is `Fn` rather than `FnOnce` —
//! it must be re-runnable. Capture `Copy` values freely; clone anything else
//! inside the closure.
//!
//! Conflict retries are separate from *application* retries: a body **error**
//! is not retried by default, but [`TransactionOptions::max_retries`] (with an
//! optional [`retry_if`](TransactionOptions::retry_if) predicate) re-runs the
//! body on a new transaction with exponential backoff, and only the final
//! outcome is checkpointed.
//!
//! # A separate application database
//!
//! When your tables live in their own database — not the one holding the
//! `dbos` schema — a single commit can no longer cover both the writes and
//! the checkpoint. [`DurableContext::transaction_on`] keeps the exactly-once
//! guarantee anyway, with a **two-commit protocol**: construct a
//! [`PgDataSource`] or [`SqliteDataSource`](crate::SqliteDataSource) over
//! your own `sqlx` pool, and the body's writes commit atomically **with a
//! witness row** in a `transaction_completion` table that durare creates in
//! your database; the ordinary checkpoint follows as a second commit to the
//! system database. Recovery replays in layers — checkpoint first, then the
//! witness row (covering a crash between the two commits) — so the body still
//! runs exactly once.
//!
//! Unlike [`Tx`], the body receives the backend's **native `sqlx`
//! connection** (`&mut sqlx::PgConnection` / `&mut sqlx::SqliteConnection`),
//! so existing queries, compile-time-checked `sqlx` macros, and data-access
//! helpers written against `sqlx` work unchanged:
//!
//! ```no_run
//! # use durare::{DurableContext, PgDataSource, Result};
//! # async fn ex(ctx: DurableContext, ds: PgDataSource) -> Result<()> {
//! let n: i64 = ctx
//!     .transaction_on(&ds, "record-order", |conn| Box::pin(async move {
//!         sqlx::query("INSERT INTO orders(item) VALUES ($1)")
//!             .bind("widget")
//!             .execute(&mut *conn)
//!             .await?;
//!         Ok(1)
//!     }))
//!     .await?;
//! # let _ = n;
//! # Ok(()) }
//! ```
//!
//! The trade for the native connection: the body is committed to one backend
//! at the call site, where a [`Tx`] body runs on either. Failure semantics,
//! isolation, and the retry policy match [`transaction`] — with the failure
//! also mirrored into the witness table, so your database is self-describing.
//! durare owns the transaction: the connection has no commit method, and on
//! Postgres a raw `COMMIT`/`ROLLBACK` smuggled through SQL is detected and
//! fails the step.
//!
//! ## Application tables in the system database
//!
//! When your tables share the database that holds the `dbos` schema, ask the
//! **provider** for the data source instead of building one yourself:
//!
//! ```no_run
//! # use durare::{DurableContext, DurableEngine, PostgresProvider, Result};
//! # use std::sync::Arc;
//! # async fn ex(ctx: DurableContext, url: &str) -> Result<()> {
//! let provider = PostgresProvider::connect(url).await?;
//! let ds = provider.system_datasource(); // provider's own pool — no guessing
//! let engine = DurableEngine::new(Arc::new(provider)).await?;
//!
//! ctx.transaction_on(&ds, "audit", |conn| Box::pin(async move {
//!     // Qualify table names in fast-path bodies: this pool's search_path
//!     // points at the system schema.
//!     sqlx::query("INSERT INTO public.audit_log(entry) VALUES ($1)")
//!         .bind(serde_json::json!({"kind": "transfer"})) // jsonb, natively
//!         .execute(&mut *conn)
//!         .await?;
//!     Ok(())
//! }))
//! .await?;
//! # let _ = engine;
//! # Ok(()) }
//! ```
//!
//! Because that data source is built from the provider's own pool, sameness
//! is true by construction (never detected or guessed), and `transaction_on`
//! takes a **single-commit fast path**: the body's writes and the step
//! checkpoint commit in one transaction — the same guarantee as
//! [`transaction`], with no witness table at all — while the body keeps the
//! native connection and its full type support (`jsonb`, arrays, `uuid`, …)
//! that [`Param`]'s portable set can't express. A user-constructed
//! [`PgDataSource`] never takes the fast path, even if its pool happens to
//! point at the system database: a wrong "same database" guess would break
//! atomicity, so the shortcut is reserved for the case that can't be wrong.
//! For the same reason, a system data source used under a *different* engine
//! is rejected rather than misrouting its checkpoint. This is a durare
//! extension.
//!
//! ## The data source is part of the workflow's contract
//!
//! The engine cannot tell a right database from a wrong one — a body run
//! against the wrong tenant's database **succeeds silently**, and recovery
//! looks for the witness row in whatever database the data source points at
//! *now*. So treat the wiring like the workflow's code: derive the data
//! source deterministically from the workflow's input (a lookup keyed by an
//! input field, not a value captured once at registration), and keep it
//! pointing at the same database for the life of every run — drain in-flight
//! workflows before migrating a database, or move `transaction_completion`
//! along with the data.
//!
//! # Which transaction API?
//!
//! | Your situation | Use | Body receives | Commits |
//! |---|---|---|---|
//! | Simple types, tables in the system database | [`transaction`] | [`Tx`] — portable `?` SQL | 1 |
//! | Rich types or existing sqlx code, tables in the system database | [`transaction_on`] + `system_datasource()` | native connection | 1 |
//! | Tables in a separate database | [`transaction_on`] + [`PgDataSource`] | native connection | 2 |
//!
//! Rule of thumb: start with [`transaction`] — it keeps the body portable
//! across backends. Switch a step to [`transaction_on`] when its types
//! outgrow [`Param`] or it should reuse sqlx-typed helpers.
//!
//! [`transaction`]: crate::DurableContext::transaction
//! [`transaction_on`]: crate::DurableContext::transaction_on
//! [`DurableContext::transaction_on`]: crate::DurableContext::transaction_on
//! [`PgDataSource`]: crate::PgDataSource
//!
//! # Writing the SQL
//!
//! The [`Tx`] API is dialect-agnostic: write `?` placeholders (rewritten to
//! `$1, $2, …` on Postgres), bind with [`params!`](crate::params), and read
//! rows via [`Row::get`] / [`Row::try_get`]. `execute` returns the affected
//! row count; `query_one` / `query_opt` / `query_all` cover the read shapes.
//!
//! Prefer the attribute form for named, reusable transaction functions:
//! `#[durare::transaction]` wraps an
//! `async fn(&DurableContext, &mut Tx, args…) -> Result<T>` so call sites skip
//! the `|tx| Box::pin(…)` scaffolding — see [`transaction`](macro@crate::transaction).

#[doc(no_inline)]
pub use crate::{IsolationLevel, Param, Row, TransactionOptions, Tx, TxBody};

#[allow(unused_imports)]
#[cfg(feature = "postgres")]
use crate::PostgresProvider;
#[allow(unused_imports)]
#[cfg(feature = "sqlite")]
use crate::SqliteProvider;
#[allow(unused_imports)]
use crate::{DurableContext, InMemoryProvider};
