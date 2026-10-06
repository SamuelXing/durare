//! Why workflows take an explicit context, and how durable calls are placed
//! in their recorded history.
//!
//! # Replay and call positions
//!
//! Recovery re-runs the workflow function. Each durable operation looks up
//! its position in the history and returns the recorded result if one exists.
//! A step whose result was not committed can run again, so external effects
//! still need idempotency keys. See [`durability`](crate::durability).
//!
//! For example, this charge is replayed from its recorded receipt:
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # async fn workflow(ctx: DurableContext) -> Result<String> {
//! let receipt = ctx.step("charge", || async { Ok::<_, Error>(String::new()) }).await?;
//! # Ok(receipt) }
//! ```
//!
//! Most durable calls return [`PendingStep`]. They reserve a position when
//! constructed, so changing their poll order does not change which records
//! they read. `patch` and `deprecate_patch` are exceptions: they inspect the
//! history before deciding whether to consume a position and must be awaited
//! sequentially.
//!
//! Construction order still has to be deterministic. Moving a call past an
//! await in a concurrent branch can change that order; the SDK cannot detect
//! every such change. The [determinism guide](crate::determinism) gives the
//! rules for workflow code.
//!
//! # Why the context is explicit
//!
//! A [`DurableContext`] identifies one workflow execution and owns its
//! position counter. Workflows pass it to helpers that need durable work.
//! Calls check both that execution identity and whether they are inside an
//! operation's body.
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # async fn workflow(ctx: DurableContext) -> Result<()> {
//! async fn lookup(ctx: &DurableContext) -> Result<u64> {
//!     ctx.step("lookup", || async { Ok::<_, Error>(7) }).await
//! }
//! let n = lookup(&ctx).await?;
//!
//! // A spawned task cannot allocate positions in this execution.
//! let spawned = ctx.clone();
//! let refused = tokio::spawn(async move {
//!     spawned.step("detached", || async { Ok::<_, Error>(0) }).await
//! });
//! assert!(matches!(refused.await.unwrap(), Err(Error::DurableCallOutsideExecution { .. })));
//! # let _ = n;
//! # Ok(()) }
//! ```
//!
//! A step body is skipped when its result is replayed, so durable calls inside
//! it would leave positions that replay never visits. durare rejects those
//! calls. It also rejects calls made with a context moved to a spawned task,
//! where allocation order would depend on task scheduling.
//!
//! DBOS's Rust SDK supplies context through task-local state. Its `step` calls
//! run without a checkpoint outside a workflow or inside another step. In
//! durare, those placements return errors. This comparison is specific to
//! steps; other upstream operations have their own placement checks. The
//! [design note](https://github.com/SamuelXing/durare/blob/main/docs/design/explicit-context.md)
//! covers the comparison and the alternatives considered.
//!
//! # Guarantees and limits
//!
//! The table distinguishes runtime checks from rules the caller must follow.
//!
//! | Guarantee | Enforced by | Tests |
//! |---|---|---|
//! | A successfully built `PendingStep` reserves its position in construction order, regardless of poll order. | At the call: the position is claimed when the call is built. | `step_position::a_randomised_poll_order_does_not_move_a_position`, `native_transaction_position::native_transactions_keep_positions_when_awaited_in_reverse_order` |
//! | Dropping a successfully built `PendingStep` leaves its position spent. | At the call. `PendingStep` is `#[must_use]`. | `step_position::a_built_call_that_is_dropped_has_spent_its_position` |
//! | A `PendingStep` constructed inside another operation's body is refused without spending a position. | At construction: [`Error::NestedDurableCall`]. | `durable_call_nesting::a_step_inside_a_step_body_is_refused_and_claims_no_position`, `…::a_call_created_in_a_transaction_body_is_refused`, `…::a_step_inside_a_select_branch_is_refused` |
//! | A prebuilt `PendingStep` polled inside a body is refused; its position remains spent. | On every poll: [`Error::DurableCallCrossedBody`]. | `durable_call_nesting::a_call_built_outside_and_polled_inside_a_body_is_refused` |
//! | A sibling call built while another body is in flight is allowed. | The scope covers a body's *poll*, not its lifetime. | `durable_call_nesting::a_sibling_built_while_a_body_is_in_flight_is_allowed` |
//! | Constructing a `PendingStep` outside its execution spends no position; moving a prebuilt call there is also refused, with its position still spent. | At construction and on every poll: [`Error::DurableCallOutsideExecution`]. | `context_scope::every_durable_constructor_refuses_a_spawned_context_without_spending_positions`, `…::a_context_from_another_execution_is_refused` |
//! | A placement error can be recorded and bypasses retry; it does not request recovery. | Error classification and retry checks. | `context_scope::a_placement_error_is_a_recordable_programming_error_not_recovery`, `…::body_boundary_errors_do_not_retry_the_enclosing_step` |
//! | A failure that was recorded replays as the same error, with the same classification. | At the record: the versioned envelope in [`durability`](crate::durability#recorded-errors). | `error_replay::timeout_does_not_change_the_recovery_path`, `…::database_error_does_not_change_the_recovery_path` |
//! | A checkpoint that cannot be written stops the execution; nothing is recorded as a business outcome. | At the write: [`Error::RecoveryRequired`]. | `checkpoint_recovery_tests::sqlite_checkpoint_faults_remain_recoverable`, `…::encoding_a_step_result_must_not_repeat_its_effect_on_recovery` |
//! | A step body can read its own position and attempt, and derive a key stable across retry and recovery. | [`StepCtx`]; the key includes workflow id, step id, and effect label, but not the attempt. | `step_context::step_body_sees_its_claimed_position_even_when_polled_out_of_order`, `step_idempotency_key::key_is_stable_across_step_retries_and_distinct_for_effects`, `…::fork_gets_new_key_for_rerun_step` |
//! | An edited workflow body can be checked against a recorded history without running operation bodies or writing records. | [`DurableEngine::verify_replay`]. | `replay_verifier::swapped_steps_are_a_mismatch`, `…::verification_writes_nothing_and_runs_nothing` |
//! | A plain step's effect is at-least-once: it can repeat if the crash lands between the effect and its checkpoint. | The guide. Deduplicate external effects with [`StepCtx::idempotency_key_for`] or use a [transaction](crate::transactions) for SQL writes. | `crash_consistency::sqlite_crash_sweep_at_every_boundary`, `…::pg_crash_sweep_at_every_boundary` |
//! | `patch` and `deprecate_patch` consume a position only when the stored history requires it. Await them sequentially. | At poll time; sequential use is the caller's responsibility. | `patch::patch_new_workflow_takes_new_path`, `patch::patch_pre_patch_workflow_takes_old_path`, `sqlite::sqlite_deprecate_patch_keeps_alignment` |
//! | Two durable calls whose construction order depends on an await between them are numbered by that order. | Caller discipline; no ordering check. | none |
//!
//! Position reservation does not make concurrent workflow branches
//! deterministic. In
//! `join!(async { x.await; ctx.step("a", ..).await }, async { ctx.step("b", ..).await })`,
//! the order in which the calls are constructed can depend on `x`'s latency.
//! Build independent `PendingStep` calls before joining them. For branches
//! that need several durable calls, use child workflows with separate histories.
//!
//! # Concurrent work
//!
//! | Shape | Recorded result | Caller responsibility |
//! |---|---|---|
//! | `join!` / `try_join!` over prebuilt `PendingStep` calls | Each completed operation has its own checkpoint. | Build calls in a fixed order and process results in input order. `try_join!` may drop unfinished calls after an error. |
//! | [`ctx.select`](DurableContext::select) over plain futures | One checkpoint with the winner's index and value. | Keep durable calls out of the branches. Losers' effects are not undone. |
//! | `FuturesUnordered` / `buffer_unordered` | Each operation has its own checkpoint; completion order is not recorded. | Do not use completion order to decide later durable work unless that choice is recorded separately. |
//!
//! `select` runs its branches inside an operation body. It rejects nested
//! durable calls and prebuilt calls carried into those branches. A race over
//! separately checkpointed durable calls would need a different replay contract.
//!
//! # Effects
//!
//! - A step's external effect can repeat after a crash between the effect and
//!   its checkpoint. Use [`StepCtx::idempotency_key_for`] with a downstream
//!   system that supports deduplication.
//! - A transaction's SQL writes to the workflow database commit atomically
//!   with its checkpoint.
//! - A workflow `send` can repeat across a crash; `recv` consumes each message
//!   exactly once.
//! - A `select` winner has the same crash window as a step. Losing branches
//!   may already have made effects, and dropping them does not stop spawned work.
//! - Placement checks run before the refused call's body or provider work.
//!   A call moved after its first poll may already have performed work before
//!   the later poll is refused.
//!
//! # Tradeoffs
//!
//! Workflows and helpers that do durable work need a context parameter. For
//! concurrent durable work, use prebuilt calls or child workflows. Spawned
//! tasks can do plain work inside a step.
//!
//! A borrowed context could reject some spawned-task mistakes at compile time.
//! That change is deferred because it would also change registration closures
//! and transaction callbacks. The design note records the prototype's costs.

#[allow(unused_imports)]
use crate::{DurableContext, DurableEngine, Error, PendingStep, StepCtx};
