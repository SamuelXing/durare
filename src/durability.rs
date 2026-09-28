//! How durable execution works: checkpoints, replay, and the determinism
//! contract.
//!
//! Read this guide first — everything else in the crate builds on the model
//! described here. It has no API of its own; it explains the machinery behind
//! [`DurableContext`] and [`DurableEngine`].
//!
//! # The execution model
//!
//! A workflow is an ordinary async function. What makes it durable is that it
//! is allowed to be **executed more than once** — and must reach the same
//! result each time. The first execution does the real work. If the process
//! crashes, restarts, or the workflow is resumed or
//! [forked](DurableEngine::fork_workflow), a later execution **replays** the
//! function from the beginning.
//!
//! Replay does not repeat side effects, because of one rule: every side effect
//! lives inside a **step**, and every step's outcome is **checkpointed** to
//! the database before the workflow moves past it. Each workflow accumulates
//! an ordered log of its durable operations — steps, [sleeps], [sends],
//! [child starts] — one row per operation, keyed by sequence position. When
//! the function is replayed, the engine walks it again; at each durable
//! operation it finds the recorded outcome and returns it *without running
//! anything*, until it reaches the first operation with no checkpoint. That is
//! exactly where the previous execution stopped, and execution is live from
//! there on.
//!
//! Three consequences worth internalizing:
//!
//! - **A step that succeeded never re-runs.** Its recorded output is served on
//!   every subsequent execution.
//! - **A step that failed stays failed.** The error is checkpointed too, and a
//!   replay returns the same error rather than giving a flaky step a second
//!   chance to succeed and send the workflow down a different path.
//! - **Code *between* steps re-runs on every replay.** It must be a pure
//!   function of the workflow input and prior step results.
//!
//! # The determinism contract
//!
//! Replay is only sound if the workflow function — given the same input and
//! the same recorded step results — performs the **same durable operations in
//! the same order**. The engine enforces this as it goes: when a replay
//! reaches step position *n* and finds a *different* operation recorded there,
//! it fails with [`Error::UnexpectedStep`] instead of silently returning the
//! wrong checkpoint.
//!
//! In practice the contract reduces to one habit: capture every source of
//! non-determinism **inside a step**, and only branch on the recorded value.
//!
//! ```no_run
//! # use durare::{DurableContext, Error, Result};
//! # async fn workflow(ctx: DurableContext) -> Result<()> {
//! // WRONG: fresh randomness outside a step. A replay draws a different
//! // value, the branch flips, and the step sequence diverges.
//! let lucky = uuid::Uuid::new_v4().as_u128() % 2 == 0;
//!
//! // RIGHT: record it once; every replay sees the same value.
//! let lucky = ctx.step("draw_lottery", || async {
//!     Ok::<_, Error>(uuid::Uuid::new_v4().as_u128() % 2 == 0)
//! }).await?;
//!
//! if lucky {
//!     ctx.step("apply_discount", || async { Ok::<_, Error>(()) }).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The same reasoning applies to clocks, environment variables, config reads,
//! and anything fetched over the network. Durable primitives are already safe:
//! [`sleep`][sleeps] records its wake instant, `recv`/`get_event` record both
//! the value observed and the timeout deadline, and a [durable
//! select](DurableContext::select) records which branch won.
//!
//! The [determinism guide](crate::determinism) is the full rulebook this
//! section sketches: the catalog of non-determinism foot-guns and their durable
//! fixes, the types that are safe to store and send, and where dependencies
//! live.
//!
//! # Recorded errors
//!
//! A failed step returns the error reconstructed from its checkpoint on the
//! initial execution too. Built-in errors such as [`Error::Timeout`] keep their
//! variant and fields. Errors containing live driver, migration, or JSON source
//! objects become [`Error::Recorded`]: their message, [`Error::code`], and `is_*`
//! classifications are saved, but the original object is not. Live step retry
//! predicates still inspect the original error before final recording.
//!
//! An [`Error::app_source`] loses its source when it crosses that durable
//! boundary, on the initial return as well as replay. Use the saved fields for
//! workflow decisions, not source downcasts. In portable mode an ordinary
//! application message returns the generic [`Error::Portable`] envelope on
//! both executions; structured application envelopes keep their data in either
//! serializer. Recorded `ERROR` workflow outcomes and polling handles use the
//! same decoder. Cancellation and workflow-deadline control outcomes are separate.
//!
//! **Storage and upgrades.** New built-in failures use a version-1
//! `durare.RecordedError` envelope. Its `data` holds `version` and a tagged
//! `error`. Portable rows store that JSON directly; other formats store it after
//! the reserved `__DURARE_ERROR__:` prefix. Ordinary application messages retain
//! their old format. New messages beginning with the prefix, and application
//! envelopes using the reserved class name, are escaped in an outer record.
//!
//! Old bare errors are still application errors: missing types cannot be
//! recovered by guessing from their messages. Foreign portable envelopes remain
//! readable. The fieldless `DBOSNotAuthorizedError` envelope is recognized as
//! [`Error::NotAuthorized`]; user-supplied portable errors with that exact shape
//! are escaped so their application meaning is preserved.
//!
//! Older durare versions cannot reconstruct new typed records. Foreign SDKs can
//! read a portable record's message/data but do not acquire Rust's classifications.
//! Upgrade all readers/recovery workers for the affected application before
//! recording new failures; do not replay those runs on older binaries. Before
//! upgrading legacy histories, check for application text starting with the
//! reserved prefix or portable errors named `durare.RecordedError`: they predate
//! escaping and cannot be distinguished from SDK records. Recognized records
//! with malformed or unknown payloads return a serialization error instead of
//! rerunning the failed body. Unknown error codes are rejected too: silently
//! mapping one to an unknown/application category could change the workflow's
//! next branch. Adding a code requires compatible readers before new writes;
//! `non_exhaustive` supports source evolution, not wire forward compatibility.
//! No schema migration is required.
//!
//! # Storage failures during execution
//!
//! A checkpoint read/write failure or unreadable stored envelope is not a
//! business outcome.
//! The execution stops with [`Error::RecoveryRequired`], retaining the original
//! cause. The engine does not write `ERROR` or `SUCCESS` for that execution,
//! even if workflow code catches the error. Further durable calls on the same
//! context (including its clones and already-built calls) refuse to run.
//! An in-flight database commit or an external effect can still finish; stopping
//! the execution does not undo either one.
//!
//! Recovery reads durable state again. If a checkpoint committed but its reply
//! was lost, recovery serves that record. If no checkpoint committed, the body
//! can run again: use an idempotency key or an atomic transaction for side
//! effects. A failed terminal-status write is read back too; an existing
//! terminal outcome wins, otherwise the caller receives `RecoveryRequired`.
//! Concurrent cancellation/completion may already have changed the stored status.
//!
//! Storage interruptions at all execution entries share recovery after backoff: direct starts,
//! children, schedules, recovery dispatch and queues. An atomic ownership check
//! uses the executor and recovery generation captured when that run started.
//! Queued runs return to their queue; direct runs are dispatched after a won
//! claim. Exceeding `max_recovery_attempts` parks the workflow and releases its
//! queue slot. Recorded business failures do not schedule recovery claims.
//!
//! Storage failures while claiming retry the same generation at most eight
//! times, without running a body. If these attempts fail, an error event reports
//! that explicit recovery is required after repair; a database refusing writes
//! cannot reliably store a parking transition. If a claim's response is lost,
//! the stale generation cannot be claimed twice. A lost claim is never permission
//! to dispatch: another executor may already be running it. An ambiguous direct
//! claim may therefore need explicit recovery after the previous owner is known
//! to have stopped. Deactivation stops automatic dispatch and direct recovery
//! claims that would restart a body. Once an execution has stopped, its bounded
//! settlement can still return a queued run to its queue or park a panicked or
//! exhausted run. Requeued work may run on another active executor. Shutdown
//! stops all automatic settlement, including these database-only transitions.
//! A direct claim already committed when dispatch stops may need explicit
//! recovery. Explicit operator recovery remains available on a deactivated engine.
//!
//! The cap counts successful recovery claims, including restarts after storage
//! interruptions, not only process crashes. Progress does not reset
//! the count: flapping storage can exhaust a healthy workflow's budget. Persistent
//! decoding/configuration problems use this same bounded policy, because their
//! error category cannot establish when an operator or deployment will repair
//! them. Repair the cause and explicitly resume a parked workflow to reset the
//! budget; increasing the cap alone does not make a persistent cause recoverable.
//!
//! Explicit recovery remains available for unfinished executions whose owner has
//! stopped, including after exhausted claim retries. `PENDING` alone does not
//! prove the old execution stopped. [`DurableEngine::shutdown`] returns `Ok(())`
//! even when its drain timeout expires; running bodies can still finish and
//! commit afterward. Deactivation and dropping the engine do not prove process
//! death either. Before explicit recovery or resume, establish that the selected
//! previous executions cannot still run. Prefer
//! [`DurableEngine::recover_pending_for`] with confirmed-stopped executor ids;
//! an unfiltered recovery call is not a safe periodic sweep over a live fleet.
//! The claim CAS chooses between recoverers but does not fence the old body.
//! Resume resets the recovery counter, so it must not overlap an old recovery
//! claim that could match the reset generation. Stop those claim tasks as part
//! of the handoff too; later checkpoint conflicts cannot undo duplicate effects.
//!
//! Every execution path emits an error event with `workflow_id`, `workflow`, `error`,
//! and `recovery_required=true`. Persistent decoding failures require a compatible
//! reader or repaired data before recovery can succeed.
//!
//! Stopping drops the workflow future: code after a caught storage failure,
//! including asynchronous compensation, is not guaranteed to run. Keep necessary
//! compensation durable and perform it from a separate, healthy execution.
//! This also drops sibling durable-call futures whose bodies may have finished
//! while their checkpoint writes are in flight. Recovery reuses a committed row;
//! if that write did not commit, the sibling's effect may repeat. Cancellation
//! does not strengthen the plain-step at-least-once guarantee. A running step
//! can opt in to [`StepCtx::cancelled`], which reads the persisted `CANCELLED`
//! status even when another process requested it. The body must stop its own
//! work and return [`Error::Cancelled`]; that control error is not checkpointed
//! or retried, so an explicit resume can re-run the unfinished step. The signal
//! cannot undo a side effect already completed.
//!
//! Workflow-body panics have zero automatic body retries. Their ownership CAS
//! parks the row in `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, releasing queue capacity;
//! it increments the ownership generation but does not exhaust the configured
//! storage-recovery budget by rerunning the body. Repair the code and explicitly
//! resume the parked workflow. Step-body panics retain the step retry policy.
//! A local handle reports `RecoveryRequired` for an interrupted execution; a
//! polling handle observes either its recovered outcome or its parked status.
//! Externally aborting the entire owning task bypasses this settlement path and
//! can still require explicit recovery. Parking requires writable storage; failed
//! parking claims follow the same bounded claim retries described above.
//!
//! A child started with [`DurableContext::start_workflow`] returns a polling
//! handle. If the child parks, the handle returns
//! [`Error::MaxRecoveryAttemptsExceeded`]. A parent that propagates that error
//! becomes `ERROR`, releasing its own queue slot; it does not pause alongside
//! the child. Parent code may handle the error explicitly instead. Resuming the
//! child later resumes only the child: it does not reopen the parent's recorded
//! failure, and resume is a no-op for an `ERROR` or `SUCCESS` parent.
//!
//! Queue progress requires readable/writable storage and a running dispatcher.
//! A waiting parent is still running and occupies capacity. If a child's ownership
//! is ambiguous, or its recovery cannot write a parking/requeue transition, the
//! parent may keep waiting until safe operator recovery, cancellation or a deadline.
//! The recovery budgets above do not bound every wait: handle polling and existing
//! live transaction-conflict retries have separate semantics.
//!
//! Creation, retrieval and polling infrastructure failures instead return
//! [`Error::ObservationFailed`]. This says the operation could not be observed,
//! not that its target stopped. Retry a read; retry ambiguous creation with the
//! same workflow id. A retry reconciles existence, not execution ownership. If
//! creation committed but its reply was lost, an existing direct run (including
//! a child or scheduled tick) may have no task. It needs explicit recovery after
//! the previous creator is known to have stopped. An existing child is never
//! redispatched just because its parent's relationship checkpoint is absent:
//! that child could still be running. Queued creation is claimed by a dispatcher.
//! Do not recover the target based on an observation error alone. A failed
//! terminal readback after another execution committed is an observation failure,
//! not permission to recover that execution. Stream reads and handle status reads
//! follow the same rule; conversion to the requested Rust type stays catchable.
//! Its `code()` and diagnostic predicates describe the underlying cause.
//! Neither signal can be checkpointed as a business outcome or sent through
//! business retry policy. When propagated through a parent durable body, it
//! interrupts that parent's execution; catching the durable call's returned
//! error cannot authorize the parent's finalization.
//! Other errors returned by user step/transaction bodies remain business outcomes,
//! including database errors. Failure to encode a body's return value is saved
//! as a step failure. Transaction output conversion uses
//! [`Error::OutputSerialization`] to bypass body retry policy, while an error
//! explicitly returned by user code retains that policy. Once the failure
//! checkpoint commits, recovery will not repeat the body for the encoding error.
//! Rollback or failure-checkpoint storage faults can still require recovery.
//! Provider semantic rejections (for example a missing message destination or
//! closed stream) and conversion of a recorded value to an incompatible requested
//! Rust type remain catchable API errors. The provider's storage error contract
//! is defined by [`StateProvider`](crate::StateProvider); error variants alone
//! do not classify errors returned by arbitrary user code.
//!
//! # Crash recovery
//!
//! On startup, call [`DurableEngine::recover`]: it finds every workflow this
//! application version left unfinished on this executor and re-dispatches it —
//! each one replays to its frontier and continues. [`DurableEngine::launch`] can
//! do this for you if you opt in with
//! [`recover_on_launch(true)`](crate::EngineConfig::recover_on_launch); it is
//! off by default because it is only sound when each live process has a *unique*
//! executor id (recovering "this executor's" pending work assumes the previous
//! owner is gone, not running concurrently) — enable it for a single-process app
//! or when you set a distinct `DBOS__VMID` per process. Queued workflows need
//! nothing special: an `ENQUEUED` row survives the crash and is simply claimed
//! again by the next dispatcher. A workflow that keeps crashing is not retried
//! forever — after a bounded number of recovery attempts it is parked as
//! `MAX_RECOVERY_ATTEMPTS_EXCEEDED` for an operator to inspect and resume.
//!
//! For a live demonstration — a process killed mid-workflow, restarted, and
//! finishing without repeating completed work — run
//! [`examples/order.rs`](https://github.com/SamuelXing/durare/blob/main/examples/order.rs).
//!
//! # The at-least-once window
//!
//! A step performs its effect, *then* its checkpoint commits — two writes to
//! two systems, and a crash can land between them. On replay the step re-runs.
//! So a step's side effect is **at-least-once**: exactly-once except when a
//! crash splits that window. Where it matters, make the effect idempotent —
//! pass [`StepCtx::idempotency_key_for`] to a downstream API that atomically
//! deduplicates by that key, so a retry is recognized. A step name alone is
//! insufficient: the same name can occur more than once in one workflow.
//! Use a stable effect label for each external operation inside the step.
//!
//! Two cases are already closed for you:
//!
//! - **Writes to the workflow database**: use a
//!   [transaction](crate::transactions) — the SQL and the checkpoint commit
//!   atomically, making the step genuinely exactly-once.
//! - **Messages between workflows**: [`send`][sends] may re-deliver across a
//!   crash, but [`recv`](DurableContext::recv) consumes exactly once, and
//!   producers outside a workflow can use
//!   [`send_with_idempotency_key`](DurableEngine::send_with_idempotency_key).
//!
//! # Evolving workflow code
//!
//! Replay pins each workflow to the code shape it started under, so changing a
//! workflow function while runs are in flight needs care. Three tools, from
//! coarse to fine:
//!
//! - **Version gating.** Every run is stamped with an application version, and
//!   [`recover`](DurableEngine::recover) only re-dispatches rows of its own
//!   version — old executors drain old runs while new code takes new ones.
//! - **Patching.** [`DurableContext::patch`] forks behavior *inside* one
//!   workflow function: runs that already passed the patch point keep the old
//!   path, everything else takes the new one, and checkpoints stay aligned.
//! - **Forking.** [`DurableEngine::fork_workflow`] clones a workflow's
//!   checkpoints up to a chosen step and re-executes from there — useful to
//!   re-run a fixed version of a failed workflow without repeating its
//!   completed work.
//!
//! Before any of them: [`DurableEngine::verify_replay`] re-runs a recorded
//! workflow against the new code and reports the first durable operation the two
//! disagree about, without executing or writing anything — the pre-deploy check
//! for exactly this change. The [determinism
//! guide](crate::determinism#checking-a-change-before-you-ship-it) covers what it
//! does and does not see.
//!
//! [sleeps]: DurableContext::sleep
//! [sends]: DurableContext::send
//! [child starts]: DurableContext::start_workflow

#[allow(unused_imports)]
use crate::{DurableContext, DurableEngine, Error, StepCtx};
