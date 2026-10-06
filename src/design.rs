//! Why the API has the shape it does: the principle behind it, the contract
//! that principle produces, and what enforces each line of it.
//!
//! The other guides explain how to use durare. This one explains the choices —
//! why a workflow takes an explicit [`DurableContext`], why a durable call is
//! placed where it is written, why some mistakes are compile errors, some are
//! runtime errors and some are rules you keep yourself. Read it when a rule in
//! [`determinism`](crate::determinism) seems arbitrary; the reason is here.
//!
//! # Three questions
//!
//! Take any durable call in a workflow:
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # async fn workflow(ctx: DurableContext) -> Result<String> {
//! let receipt = ctx.step("charge", || async { Ok::<_, Error>(String::new()) }).await?;
//! # Ok(receipt) }
//! ```
//!
//! The programmer has to be able to answer three questions about it, and the
//! answers have to stay the same as the code around it changes:
//!
//! 1. **On recovery, does this re-run or return its record?** If the process
//!    died after the charge was made, a replay must find the receipt, not
//!    charge again.
//! 2. **If the effect happened but the record did not commit, can it repeat?**
//!    There is a window between the charge and its checkpoint. What happens if
//!    the crash lands in it, and what can the programmer do about it?
//! 3. **If this line moves — into a helper, a branch, a concurrent block, a
//!    spawned task — do the answers to 1 and 2 still hold?**
//!
//! The third question is the one that decides the shape of the API. Code
//! moves: it is refactored into functions, run under `join!`, raced with a
//! timeout, pushed onto a task. An API where the first two answers quietly
//! change when that happens is one where every refactor is a correctness
//! review. So the principle durare is built on is:
//!
//! > **Durable semantics survive code motion, or the SDK refuses early and
//! > loudly.**
//!
//! Everything below is an application of that sentence.
//!
//! # Why the context is explicit
//!
//! A durable call needs to know which workflow execution it belongs to. That
//! identity decides its position in the history, which record it is served on
//! replay, and which database row its checkpoint goes to. There are two ways
//! to supply it: as an argument the call takes, or as ambient state the call
//! looks up.
//!
//! durare takes it as an argument, [`DurableContext`], because the argument
//! moves with the code. A step inside a helper function takes the context the
//! helper was given; a step inside a `join!` branch takes the context the
//! branch captured; a step inside a spawned task takes the context the task
//! was handed. In each case the call can see where it is, and in the one case
//! where it has been handed somewhere it cannot work — another task, another
//! execution — it says so, before anything is recorded:
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # async fn workflow(ctx: DurableContext) -> Result<()> {
//! // Moved into a helper: same answers as inline.
//! async fn lookup(ctx: &DurableContext) -> Result<u64> {
//!     ctx.step("lookup", || async { Ok::<_, Error>(7) }).await
//! }
//! let n = lookup(&ctx).await?;
//!
//! // Moved into a spawned task: refused at the call, before a position is
//! // claimed, with `Error::DurableCallOutsideExecution`. A spawned task is
//! // not part of this execution and its timing is not part of the history.
//! let spawned = ctx.clone();
//! let refused = tokio::spawn(async move {
//!     spawned.step("detached", || async { Ok::<_, Error>(0) }).await
//! });
//! assert!(matches!(refused.await.unwrap(), Err(Error::DurableCallOutsideExecution { .. })));
//! # let _ = n;
//! # Ok(()) }
//! ```
//!
//! An ambient context — one found through task-local state rather than passed —
//! answers the same questions differently in the spawned case: the lookup
//! finds nothing, so the call runs as a plain function, unrecorded, and re-runs
//! on every replay. That is a defensible choice; it is not the one this crate
//! makes, because it is the quiet kind of change the principle rules out. The
//! [design note](https://github.com/SamuelXing/durare/blob/main/docs/design/explicit-context.md)
//! sets the two models side by side with their consequences.
//!
//! # The contract
//!
//! Each row is one thing the programmer may rely on. The middle column says
//! what enforces it — the compiler, a check at the call, or nothing but the
//! guide — and the last names the test that pins it. A row enforced by the
//! guide alone is a rule the programmer keeps; the table says so rather than
//! implying a mechanism that is not there.
//!
//! | Guarantee | Enforced by | Pinned by |
//! |---|---|---|
//! | A durable call's position is the one its source order gives it, however it is polled. | At the call: the position is claimed when the call is built. | `step_position::a_randomised_poll_order_does_not_move_a_position`, `native_transaction_position::native_transactions_keep_positions_when_awaited_in_reverse_order` |
//! | A call that is built and never awaited has still spent its position. | At the call. `PendingStep` is `#[must_use]`. | `step_position::a_built_call_that_is_dropped_has_spent_its_position` |
//! | A call made inside another operation's body is refused and spends no position. | At the call: [`Error::NestedDurableCall`]. | `durable_call_nesting::a_step_inside_a_step_body_is_refused_and_claims_no_position`, `…::a_call_created_in_a_transaction_body_is_refused`, `…::a_step_inside_a_select_branch_is_refused` |
//! | A call built outside a body and awaited inside one is refused. | On every poll: [`Error::DurableCallCrossedBody`]. | `durable_call_nesting::a_call_built_outside_and_polled_inside_a_body_is_refused`, `execution::scope_tests::move_async_call_after_first_poll` |
//! | A sibling call built while another body is in flight is allowed. | The scope covers a body's *poll*, not its lifetime. | `durable_call_nesting::a_sibling_built_while_a_body_is_in_flight_is_allowed` |
//! | A call from a context moved into another task or execution is refused and spends no position. | At the call and on every poll: [`Error::DurableCallOutsideExecution`]. | `context_scope::every_durable_constructor_refuses_a_spawned_context_without_spending_positions`, `…::a_context_from_another_execution_is_refused` |
//! | A refused placement is a programming error: recordable, never retried, never a recovery signal. | At the call. | `context_scope::a_placement_error_is_a_recordable_programming_error_not_recovery`, `…::body_boundary_errors_do_not_retry_the_enclosing_step` |
//! | A failure that was recorded replays as the same error, with the same classification. | At the record: the versioned envelope in [`durability`](crate::durability#recorded-errors). | `error_replay::timeout_does_not_change_the_recovery_path`, `…::database_error_does_not_change_the_recovery_path` |
//! | A checkpoint that cannot be written stops the execution; nothing is recorded as a business outcome. | At the write: [`Error::RecoveryRequired`]. | `checkpoint_recovery_tests::sqlite_checkpoint_faults_remain_recoverable`, `…::encoding_a_step_result_must_not_repeat_its_effect_on_recovery` |
//! | A step body can read its own position and attempt, and derive a key stable across retry and recovery. | [`StepCtx`]; the key is `(workflow_id, step_id)`, never the attempt. | `step_context::step_body_sees_its_claimed_position_even_when_polled_out_of_order`, `step_idempotency_key::key_is_stable_across_step_retries_and_distinct_for_effects`, `…::fork_gets_new_key_for_rerun_step` |
//! | An edited workflow body can be checked against a recorded history without running or writing anything. | [`DurableEngine::verify_replay`]. | `replay_verifier::swapped_steps_are_a_mismatch`, `…::verification_writes_nothing_and_runs_nothing` |
//! | A plain step's effect is at-least-once: it can repeat if the crash lands between the effect and its checkpoint. | The guide. Close the window with [`StepCtx::idempotency_key_for`] or a [transaction](crate::transactions). | `crash_consistency::sqlite_crash_sweep_at_every_boundary`, `…::pg_crash_sweep_at_every_boundary` |
//! | Two durable calls whose construction order depends on an await between them are numbered by that order. | **The guide only.** No mechanism sees it. | none |
//!
//! The last row is the honest ceiling. `let a = ctx.step(..); x.await; let b =
//! ctx.step(..)` numbers `a` before `b` on every run, because the counter
//! moves at construction and both constructions are in source order. But
//! `join!(async { x.await; ctx.step("a", ..).await }, async { ctx.step("b", ..).await })`
//! builds `a` only after `x` resolves, and `x`'s latency is not part of the
//! history — the first run may build `b` first. Nothing in either model
//! catches this; the [determinism guide](crate::determinism#concurrent-work-and-recorded-choices)
//! gives the rule: build every durable call before the first await of the
//! block it lives in.
//!
//! # Composition
//!
//! What the contract means for the three ways concurrent work is written.
//!
//! | Shape | What is recorded | Deterministic when |
//! |---|---|---|
//! | `join!` / `try_join!` over durable calls | Every branch, at the position its source order gave it. | The calls are built before the first poll — which is what writing them as the arguments does. |
//! | A race: [`ctx.select`](DurableContext::select) over plain futures | One checkpoint: the winner's index and value. The race *is* a step whose body happens to be a race. | Always, as to position. Which branch *wins* is recorded, not reproduced; the losers' effects before they were dropped are not undone. |
//! | Completion order: `FuturesUnordered`, `buffer_unordered`, "handle them as they finish" | Each call at its source position; the *order the body sees them in* is not recorded. | Never. The first run observes real latencies; a replay serves every record at once. Collect with `join!` and process in a fixed order. |
//!
//! A durable call inside a `select` branch is a nested call and is refused
//! (third row of the contract). The branches are plain work; the race is the
//! durable operation. A durable race over durable calls would be a different
//! operation with a different contract, and does not exist.
//!
//! # Effects
//!
//! What each operation's side effect is, under the contract:
//!
//! - A **step**'s effect is at-least-once. The window is the one between the
//!   effect and its checkpoint; [`StepCtx::idempotency_key_for`] gives the
//!   downstream system what it needs to recognise the repeat.
//! - A **transaction**'s writes to the workflow database are exactly-once: the
//!   SQL and the checkpoint commit together.
//! - A **send** may be delivered more than once across a crash; a **recv**
//!   consumes exactly once.
//! - A **select** winner's effect is at-least-once, as a step's is. A loser may
//!   have had effects before it was dropped; dropping it stops nothing it
//!   spawned.
//! - An effect inside a **refused** call never happens: refusal is at the
//!   call, before the body runs.
//!
//! # What this costs
//!
//! Explicit context is a parameter on every workflow and helper, and a
//! `tokio::spawn` that wants to do durable work cannot — it does plain work
//! inside a step, or the work is a child workflow. Those are the prices of
//! the third question having a stable answer. The design note records the
//! alternatives that were considered and why each was set aside, including a
//! borrowed context that would turn the spawn case into a compile error; that
//! one is deferred, not rejected, because the runtime check already makes the
//! mistake loud and the compile-time version costs every registration closure
//! its shape.

#[allow(unused_imports)]
use crate::{DurableContext, DurableEngine, Error, StepCtx};
