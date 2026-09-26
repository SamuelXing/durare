//! Writing a correct workflow body: determinism, durable-safe data, and
//! dependencies.
//!
//! [The durability guide](crate::durability) gives the model — a workflow is
//! replayed, and every replay must issue the same durable operations in the
//! same order. This guide is the rulebook that follows from it, in three parts:
//!
//! 1. [The deterministic body](#the-deterministic-body) — what may run directly
//!    in a workflow function, and the durable primitives that make the unsafe
//!    things safe.
//! 2. [Durable-safe data](#durable-safe-data) — the types that survive a
//!    checkpoint-and-replay (and cross-SDK) round-trip unchanged.
//! 3. [Dependencies](#dependencies) — where a database pool, HTTP client, or
//!    config lives, given that none of them are durable.
//!
//! # The deterministic body
//!
//! Code *between* steps re-runs on every replay, so it must be a pure function
//! of the workflow input and the recorded step results. Any other source of
//! variation — the clock, randomness, the iteration order of a `HashMap`, a
//! spawned task's timing, a file or an environment variable — makes a replay
//! diverge from the original run: a branch flips, a loop reorders, and the
//! sequence of durable operations no longer lines up with what was recorded.
//!
//! The contract reduces to one habit: pull every such source **into a step**
//! (or a durable primitive), and branch only on the recorded value. The engine
//! catches divergence after the fact — reaching a step position with a
//! *different* operation recorded there is an [`Error::UnexpectedStep`] rather
//! than a silent wrong replay — but that is a backstop, not a substitute for
//! writing a deterministic body.
//!
//! | Source in the body | Why a replay diverges | Use instead |
//! |---|---|---|
//! | `Utc::now()`, `SystemTime::now()`, `Instant::now()` | a later run reads a different time | [`ctx.now()`](DurableContext::now) |
//! | `Uuid::new_v4()`, `rand::random()` | a later run draws a different value | [`ctx.uuid()`](DurableContext::uuid) / [`ctx.random()`](DurableContext::random) |
//! | iterating a `HashMap` / `HashSet` | order is randomized per map, so a loop issues its steps in a different order | a `BTreeMap` / `BTreeSet`, or sort the keys first |
//! | `tokio::spawn` of durable work | a position is claimed where the call is **written**, but a spawned task writes its calls into the same counter from another task, so a replay interleaves them differently | keep durable calls on the workflow's own task; run them concurrently with `join!` / `try_join!` (see the note below it) |
//! | a durable call built *after* an `.await` inside a `join!` branch | `join!` rotates which branch it polls first, so which branch reaches its call first is decided by wake-up order, not by the source | build the durable calls first and join the built calls, so the positions are claimed before anything is polled |
//! | `tokio::select!` over durable calls | positions are fine — every branch is built before any is polled — but only the winner runs, and which one wins turns on real timing, so a replay can pick a different branch and leave the loser's position with nothing recorded at it | race plain async work with [`ctx.select`](DurableContext::select), which records the winner, or give each branch a child workflow |
//! | `FuturesUnordered`, `buffer_unordered`, any "handle them as they finish" loop | the first run observes real I/O latencies; a replay serves every step from its checkpoint at once, so the completion order — and anything derived from it, including which durable call is reached next — differs | collect with `join!` / `try_join!` and process in a fixed order, or give each branch a child workflow |
//! | reading env vars, config, files, or the network | the value can differ between runs | read it inside a [step](DurableContext::step) |
//! | side effects in `Drop` | drop timing and order are not part of the recorded log | put the effect in a step |
//!
//! The first two rows have one-line durable equivalents —
//! [`ctx.now()`](DurableContext::now), [`ctx.uuid()`](DurableContext::uuid), and
//! [`ctx.random()`](DurableContext::random) each record their value at a
//! sequence position and replay it, exactly like a step:
//!
//! ```no_run
//! # use durare::{DurableContext, Result};
//! # async fn workflow(ctx: DurableContext) -> Result<()> {
//! // WRONG: a fresh read each run; a replay sees a different instant, and any
//! // branch on it can flip, diverging the step sequence.
//! let _started = chrono::Utc::now();
//!
//! // RIGHT: recorded once, replayed identically.
//! let _started = ctx.now().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # A durable call belongs to the workflow body
//!
//! Every rule above is one you keep yourself, with `UnexpectedStep` as a
//! backstop. This one the engine checks at the call: a durable operation may not
//! be created inside another durable operation's body, and may not be awaited in
//! a body other than the one it was built in.
//!
//! A step body, a transaction body and a [durable select](DurableContext::select)
//! branch are all bodies. They do not run on a replay — the outer operation is
//! served from its record instead — so a durable call inside one claims a
//! position on the first run that no replay claims again, and every later call
//! shifts onto it. Under a different name that surfaces as an
//! [`Error::UnexpectedStep`] somewhere unrelated, on code that did not change;
//! under the same name it is not detectable at all, and the outer call is served
//! the inner call's result.
//!
//! Both are refused where they happen, before the counter moves, as
//! [`Error::NestedDurableCall`] and [`Error::DurableCallCrossedBody`]. The calls
//! around a refused one keep the positions they would have had.
//!
//! **The check does not reach a spawned task.** It is task-local, so a durable
//! call made from a task the body spawned is outside it and is not refused —
//! the `tokio::spawn` row above still applies, and still has no mechanism
//! behind it.
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # async fn charge(ctx: &DurableContext) -> Result<i64> { Ok(0) }
//! # async fn fetch() -> i64 { 0 }
//! # async fn workflow(ctx: DurableContext) -> Result<()> {
//! // WRONG: `inner` is created while `outer`'s body is being polled.
//! let inner_ctx = ctx.clone();
//! ctx.step("outer", || async move {
//!     inner_ctx.step("inner", || async { Ok::<_, Error>(1) }).await
//! }).await?;
//!
//! // RIGHT: the body does plain work; the durable calls are siblings.
//! let a = ctx.step("a", || async { Ok::<_, Error>(fetch().await) }).await?;
//! let b = ctx.step("b", || async { Ok::<_, Error>(fetch().await) }).await?;
//! # let _ = (a, b);
//! # Ok(())
//! # }
//! ```
//!
//! What a body may do is call ordinary functions, as deeply as it likes. The
//! rule is about durable calls, not about nesting code.
//!
//! # Durable-safe data
//!
//! Every value that crosses a durable boundary — a workflow's input and output,
//! a step's return value, a [message](crate::messaging), an event, a stream
//! item — is serialized to JSON and stored. So *durable-safe* means "round-trips
//! through JSON unchanged," and for interop with other DBOS SDKs on a shared
//! database it means "serializes to the same bytes." Four rules cover it:
//!
//! - **No `NaN` or infinity.** JSON has no representation for them, so a
//!   non-finite float cannot be stored faithfully. Validate at the edge, or
//!   carry a sentinel.
//! - **String-encode integers that can exceed 2⁵³.** Many JSON readers decode
//!   numbers as IEEE-754 doubles and lose integer precision above 2⁵³. Store
//!   large ids, counters, and amounts as strings.
//! - **Prefer ordered maps.** A `HashMap` serializes its entries in a
//!   per-map-random order; a `BTreeMap` serializes them sorted. Where two runs —
//!   or two SDKs — must produce byte-identical records, reach for `BTreeMap`.
//! - **Keep data self-describing and owned.** A value whose meaning depends on
//!   live process state — a file descriptor, a raw pointer, an `Instant` — means
//!   nothing after a restart. Don't put it in durable data.
//!
//! ```
//! use std::collections::BTreeMap;
//! // A BTreeMap serializes in key order — identical bytes on every run, which
//! // the cross-SDK portable format relies on. (A HashMap would vary.)
//! let mut m = BTreeMap::new();
//! m.insert("b", 2);
//! m.insert("a", 1);
//! assert_eq!(serde_json::to_string(&m).unwrap(), r#"{"a":1,"b":2}"#);
//! ```
//!
//! These rules bite hardest under the [portable](Serializer::Portable)
//! serializer, whose whole purpose is byte-identical cross-SDK records. The
//! default JSON format is more forgiving on the wire, but the replay rules — no
//! non-finite floats, no runtime-bound types — apply to it just the same.
//!
//! # Dependencies
//!
//! A dependency — a database pool, an HTTP client, a message producer, loaded
//! config — is **not durable**. A live connection cannot be checkpointed and
//! cannot be replayed; on restart it has to be built anew. So a dependency must
//! never live in durable state: not as a workflow parameter (parameters are
//! serialized), not in a step's return value, not held across an await that
//! might replay.
//!
//! The pattern is to build dependencies once at startup into a process global,
//! and read them **inside steps**:
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # use std::sync::OnceLock;
//! # struct Deps { api_base: String }
//! # static DEPS: OnceLock<Deps> = OnceLock::new();
//! # async fn charge(ctx: DurableContext, amount: u64) -> Result<()> {
//! // Set once at startup, in `main`, before the engine launches:
//! //     DEPS.set(Deps { api_base: "https://api.example.com".into() }).ok();
//! //
//! // Read inside a step — where side effects belong:
//! let _receipt = ctx.step("charge", || async {
//!     let deps = DEPS.get().expect("deps set at startup");
//!     // ... use deps.api_base to make the call ...
//!     Ok::<String, Error>(format!("{}/charge/{amount}", deps.api_base))
//! }).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Why a global rather than a parameter: workflow inputs are serialized, and a
//! pool is not serializable — nor should it be, since its identity is
//! meaningless after a restart. Why read it inside a step: acquiring and using a
//! connection is a side effect, and side effects belong in steps, which do not
//! re-run on replay.
//!
//! Workflows stay free functions. durare deliberately has no object/method layer
//! that binds dependencies to a workflow instance: a global covers the common
//! case — one configuration per process — with no ceremony. The case that would
//! justify a method-based API is a single workflow that must run as several
//! named instances with *different* configuration in one process; until that
//! need is concrete, a global is the whole story.
//!
//! [`examples/dependencies.rs`](https://github.com/SamuelXing/durare/blob/main/examples/dependencies.rs)
//! runs this pattern end to end: a dependency wired through a global, read inside
//! a step, and — because the step's result is checkpointed — invoked exactly once
//! even across a replay.

#[allow(unused_imports)]
use crate::{DurableContext, Error, Serializer};
