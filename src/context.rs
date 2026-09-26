use crate::engine::{Runtime, WorkflowOptions};
use crate::error::{panic_message, Error, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{ChangeWait, StateProvider, StepOutcome, WorkflowStatus, STATUS_CANCELLED};
use crate::replay::{Divergence, Verification};
use crate::tx::{TransactionOptions, Tx, TxBody};
use futures_util::FutureExt;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::future::{poll_fn, Future};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::task::futures::TaskLocalFuture;
use tracing::Instrument;

tokio::task_local! {
    /// The durable body whose poll is in progress on this task.
    ///
    /// Set by [`in_body`] around a step closure, a transaction body and the
    /// polling of `select`'s branches — and around nothing else. It does not
    /// reach a task spawned from inside a body: a task-local belongs to the
    /// task, so durable calls made from `tokio::spawn` are outside every scope
    /// and are not refused. It is a *scope*, so it is set when a body's poll
    /// begins and
    /// restored when the poll returns, however it returns: ready, pending, an
    /// early `?`, a drop mid-poll, or a panic unwinding through it. Nothing has
    /// to clear it, which is the point: a flag that outlives one poll cannot
    /// tell a body that is *running* from a body that is merely *in flight*,
    /// and would refuse a sibling call the workflow body makes in between.
    static CURRENT_BODY: ();
}

/// Whether a durable body is being polled on this task.
///
/// The scope carries no value because there is nothing to tell apart: a call
/// can never be *built* inside a body — that is refused before it claims a
/// position — so "which body" has no valid answer to distinguish. Presence is
/// the whole question.
fn in_a_body() -> bool {
    CURRENT_BODY.try_with(|()| ()).is_ok()
}

/// Runs a durable body in its own scope: the closure's own work *and* the
/// future it returns.
///
/// **Not an `async fn`.** A closure does its synchronous work where it is
/// *called*, not where the future it returns is polled, so a body written
/// `|| { let p = ctx.step(..); async move { p.await } }` reaches `ctx.step`
/// before anything is awaited. Calling the closure inside a synchronous scope
/// here is what puts that work inside the body too; an `async fn` would enter
/// the scope one step too late and let the call through.
///
/// The return type is named rather than `impl Future` so that the result can be
/// boxed with a borrow in it, which the transaction path needs.
///
/// The operation's own machinery — the replay lookup, the checkpoint write —
/// stays outside the scope, so an operation never trips its own check.
fn in_body<F, Fut>(body: F) -> TaskLocalFuture<(), Fut>
where
    F: FnOnce() -> Fut,
    Fut: Future,
{
    // `sync_scope` restores the previous value on the way out, including when
    // the closure panics, so a body that fails synchronously leaves nothing set.
    let running = CURRENT_BODY.sync_scope((), body);
    CURRENT_BODY.scope((), running)
}

/// A position the nesting guard has already cleared.
///
/// Its own module, so that its fields cannot be filled in from anywhere else in
/// this file: [`Position::claim`] is the only way to make one, and it runs the
/// check. Since [`PendingStep`] cannot be built holding a position without one,
/// a durable operation that skips the guard cannot be written by *omission* —
/// there is nothing to pass. It also carries the operation's name, so each call
/// site spells it once rather than two or three times.
mod position {
    use super::{DurableContext, Result};

    #[derive(Clone, Copy)]
    pub(super) struct Position {
        seq: i32,
        operation: &'static str,
    }

    impl Position {
        /// Claim the next position for `operation`, refusing to claim one
        /// inside another durable operation's body.
        ///
        /// The refusal happens before the counter moves, so the calls around a
        /// refused one keep the positions they would have had — the failure is
        /// as deterministic as the code that caused it, and a replay refuses in
        /// the same place.
        pub(super) fn claim(ctx: &DurableContext, operation: &'static str) -> Result<Self> {
            ctx.refuse_inside_body(operation)?;
            Ok(Self {
                seq: ctx.next_seq(),
                operation,
            })
        }

        pub(super) fn seq(self) -> i32 {
            self.seq
        }

        pub(super) fn operation(self) -> &'static str {
            self.operation
        }
    }
}

use position::Position;

/// Claim the position for a durable operation, or hand the refusal straight
/// back to the caller as an already-failed call.
///
/// A macro rather than a function because the refusal is an early return of the
/// caller's own `PendingStep<T>`, whose `T` the helper would have to name.
macro_rules! claim {
    ($self:expr, $operation:literal) => {
        match $self.claim_position($operation) {
            Ok(position) => position,
            Err(e) => return PendingStep::failed(e),
        }
    };
}

/// Predicate deciding whether a step error is retryable — see
/// [`StepOptions::retry_if`]. Returning `false` stops retries at once.
pub type RetryPredicate = Arc<dyn Fn(&Error) -> bool + Send + Sync>;

/// Retry policy for a durable step.
///
/// Defaults: no retries, factor 2.0, 100ms base, 5s cap.
#[derive(Clone)]
pub struct StepOptions {
    /// Step name recorded with the checkpoint.
    pub name: String,
    /// Additional attempts after the first failure (0 = run once, no retry).
    pub max_retries: u32,
    /// Exponential backoff multiplier between attempts.
    pub backoff_factor: f64,
    /// Delay before the first retry.
    pub base_interval: Duration,
    /// Upper bound on any single backoff delay.
    pub max_interval: Duration,
    /// Optional predicate deciding whether a given step error is retryable. When
    /// it returns `false` the step is *not* retried — the error propagates
    /// immediately even if `max_retries` attempts remain, so a permanent failure
    /// fails fast. `None` (the default) retries every error up to `max_retries`.
    pub retry_if: Option<RetryPredicate>,
}

impl StepOptions {
    /// Default policy (no retries) for a step named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            max_retries: 0,
            backoff_factor: 2.0,
            base_interval: Duration::from_millis(100),
            max_interval: Duration::from_secs(5),
            retry_if: None,
        }
    }

    /// Set the number of retries (attempts after the first).
    pub fn max_retries(mut self, n: u32) -> Self {
        self.max_retries = n;
        self
    }

    /// Set the backoff multiplier.
    pub fn backoff_factor(mut self, f: f64) -> Self {
        self.backoff_factor = f;
        self
    }

    /// Set the initial retry delay.
    pub fn base_interval(mut self, d: Duration) -> Self {
        self.base_interval = d;
        self
    }

    /// Set the maximum retry delay.
    pub fn max_interval(mut self, d: Duration) -> Self {
        self.max_interval = d;
        self
    }

    /// Set a predicate that decides whether a step error is retryable. It is
    /// consulted on every failure before backoff; returning `false` stops retries
    /// at once (the error propagates), so permanent errors don't burn attempts:
    ///
    /// ```
    /// use durare::{Error, StepOptions};
    ///
    /// let opts = StepOptions::new("fetch")
    ///     .max_retries(5)
    ///     .retry_if(|e: &Error| e.is_retryable());
    /// ```
    pub fn retry_if<P>(mut self, predicate: P) -> Self
    where
        P: Fn(&Error) -> bool + Send + Sync + 'static,
    {
        self.retry_if = Some(Arc::new(predicate));
        self
    }
}

/// The identity a workflow runs under: the user it was started on behalf of,
/// the role assumed for this run, and the full set of roles available to that
/// user. It is persisted with the workflow and flows into any work the workflow
/// starts, so an audit trail and authorization decisions stay consistent across
/// a workflow tree and across recovery.
///
/// All fields are optional — a workflow started without an identity carries an
/// empty `AuthContext`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AuthContext {
    /// User on whose behalf the workflow was started.
    pub authenticated_user: Option<String>,
    /// Role assumed for this run.
    pub assumed_role: Option<String>,
    /// Roles available to the authenticated user.
    pub authenticated_roles: Vec<String>,
}

impl AuthContext {
    /// Lift the identity recorded on a persisted workflow row.
    pub(crate) fn from_status(s: &WorkflowStatus) -> Self {
        Self {
            authenticated_user: s.authenticated_user.clone(),
            assumed_role: s.assumed_role.clone(),
            authenticated_roles: s.authenticated_roles.clone(),
        }
    }

    /// `true` when no identity was attached.
    pub fn is_empty(&self) -> bool {
        self.authenticated_user.is_none()
            && self.assumed_role.is_none()
            && self.authenticated_roles.is_empty()
    }
}

/// Handle passed into every workflow function. It carries the workflow id, the
/// state backend, the identity the workflow runs under, and a deterministic
/// per-execution step counter.
///
/// All durable operations a workflow performs go through this context:
/// [`DurableContext::step`] / [`DurableContext::step_with`] for checkpointed work
/// and [`DurableContext::sleep`] for durable timers.
///
/// A context is a **borrowed capability**: the engine owns it for the run and
/// lends it to the workflow body as `&DurableContext`. It is deliberately not
/// `Clone`, and every durable call borrows it for as long as the call lives, so
/// durable work cannot be moved into a `tokio::spawn`ed task — a task the
/// engine could not replay in order. Concurrency *within* the body is
/// ordinary: `tokio::join!` over several calls, helpers taking
/// `&DurableContext`, and child workflows all work as written.
pub struct DurableContext {
    workflow_id: String,
    provider: Arc<dyn StateProvider>,
    /// Shared execution core, so a workflow can start child workflows.
    runtime: Arc<Runtime>,
    auth: AuthContext,
    // Monotonic step index. Because the workflow's control flow is
    // deterministic, the same code path yields the same seq on every replay,
    // which is how we match a step call to its stored checkpoint.
    seq: AtomicI32,
    // Set while a transaction is running. Guards overlapping transactions on
    // this context, which could otherwise wait on the same write lock.
    in_transaction: AtomicBool,
    // `Some` in a replay verification run (`DurableEngine::verify_replay`): the
    // run serves recorded outcomes, refuses to execute anything live, and books
    // what it saw here.
    //
    // One field rather than a flag beside a cell, because they are one fact:
    // there is no such thing as a verification run without somewhere to report
    // to, and no report to fill outside one. Whether this is a verification is
    // read off the `Option`; the report is shared with the verifier.
    verify: Option<Arc<Verification>>,
}

impl DurableContext {
    pub(crate) fn new(workflow_id: String, runtime: Arc<Runtime>, auth: AuthContext) -> Self {
        Self {
            workflow_id,
            provider: runtime.provider().clone(),
            runtime,
            auth,
            seq: AtomicI32::new(0),
            in_transaction: AtomicBool::new(false),
            verify: None,
        }
    }

    /// A context for a **replay verification** run
    /// ([`DurableEngine::verify_replay`](crate::DurableEngine::verify_replay)):
    /// every durable operation is served from its record, and the first one with
    /// nothing recorded at its position stops the run instead of executing.
    ///
    /// Nothing else about the run differs — same counter, same identity, same
    /// provider — because the point is to reach the same positions in the same
    /// order as a real replay would.
    pub(crate) fn new_verifying(
        workflow_id: String,
        runtime: Arc<Runtime>,
        auth: AuthContext,
        verification: Arc<Verification>,
    ) -> Self {
        Self {
            verify: Some(verification),
            ..Self::new(workflow_id, runtime, auth)
        }
    }

    /// The id of the workflow this context belongs to.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    /// The identity this workflow runs under (see [`AuthContext`]).
    pub fn auth(&self) -> &AuthContext {
        &self.auth
    }

    /// The user this workflow was started on behalf of, if any.
    pub fn authenticated_user(&self) -> Option<&str> {
        self.auth.authenticated_user.as_deref()
    }

    /// The role assumed for this run, if any.
    pub fn assumed_role(&self) -> Option<&str> {
        self.auth.assumed_role.as_deref()
    }

    /// The roles available to the authenticated user.
    pub fn authenticated_roles(&self) -> &[String] {
        &self.auth.authenticated_roles
    }

    /// Take the next position, with no check.
    ///
    /// Not the way to claim one: [`Position::claim`] is, and going through it is
    /// what keeps a durable call from claiming a position inside another
    /// operation's body. The four callers left here are all already past that
    /// check — [`patch`](Self::patch) and
    /// [`deprecate_patch`](Self::deprecate_patch), whose position depends on a
    /// database read and so cannot be claimed when they are built, and the
    /// second slot [`recv`](Self::recv) and [`get_event`](Self::get_event) take
    /// for their deadline.
    fn next_seq(&self) -> i32 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Claim the next position for `operation`, refusing to claim one inside
    /// another durable operation's body. See [`Position::claim`].
    fn claim_position(&self, operation: &'static str) -> Result<Position> {
        Position::claim(self, operation)
    }

    /// The check itself, for the two durable calls that take their position at
    /// poll time rather than at construction ([`patch`](Self::patch) and
    /// [`deprecate_patch`](Self::deprecate_patch)) and so cannot go through
    /// [`claim_position`](Self::claim_position).
    pub(super) fn refuse_inside_body(&self, operation: &'static str) -> Result<()> {
        if in_a_body() {
            return Err(Error::NestedDurableCall {
                workflow_id: self.workflow_id.clone(),
                operation: operation.to_owned(),
            });
        }
        Ok(())
    }

    /// Check the operation now executing at `seq` against the name recorded
    /// there — the comparison every path that serves a record makes.
    ///
    /// A different name is the classic non-deterministic replay: the recorded
    /// outcome belongs to another operation, and returning it would be a wrong
    /// replay rather than a failed one. A verification run books the same
    /// comparison as its verdict — a match is one more recorded operation
    /// reached, a mismatch is the divergence it was looking for — and then
    /// returns the error every run returns.
    fn check_recorded(&self, seq: i32, expected: &str, recorded: &str) -> Result<()> {
        if recorded != expected {
            if let Some(verify) = &self.verify {
                verify.saw(Divergence::Mismatch {
                    position: seq,
                    expected: expected.to_owned(),
                    recorded: recorded.to_owned(),
                });
            }
            return Err(Error::unexpected_step(
                &self.workflow_id,
                seq,
                expected,
                recorded,
            ));
        }
        self.served_record(seq);
        Ok(())
    }

    /// Whether this is a [replay
    /// verification](crate::DurableEngine::verify_replay) run rather than an
    /// execution.
    fn verifying(&self) -> bool {
        self.verify.is_some()
    }

    /// Book one recorded operation served to a verification run, where the name
    /// is already known to match and there is nothing to compare — the
    /// [`patch`](Self::patch) marker paths. A no-op in a normal run.
    fn served_record(&self, seq: i32) {
        if let Some(verify) = &self.verify {
            verify.served_record(seq);
        }
    }

    /// Book position `seq` as claimed for an `operation` record this call may
    /// never ask for — a wait's deadline. A no-op in a normal run.
    fn reserved_record(&self, seq: i32, operation: &'static str) {
        if let Some(verify) = &self.verify {
            verify.reserved_record(seq, operation);
        }
    }

    /// The point where a run that found nothing recorded at `seq` would start
    /// doing the work itself.
    ///
    /// Nothing to refuse in a normal run: that position is the replay frontier,
    /// and reaching it is how a replay becomes a live execution again. A
    /// verification run may not execute, write, or emit anything, so this is
    /// where it stops. Against a complete history that is a
    /// [`Divergence::Extra`]; against one still being written it is the end of
    /// what there is to check, booked as a fact rather than a divergence.
    ///
    /// The returned error is a courtesy, not the channel — a body may swallow it
    /// (see [`Verification`]) — so what was seen is booked before it is handed
    /// back.
    fn refuse_live_work(&self, seq: i32, operation: &str) -> Result<()> {
        let Some(verify) = &self.verify else {
            return Ok(());
        };
        if verify.complete() {
            let divergence = Divergence::Extra {
                position: seq,
                operation: operation.to_owned(),
            };
            verify.saw(divergence.clone());
            return Err(Error::ReplayDiverged {
                workflow_id: self.workflow_id.clone(),
                divergence,
            });
        }
        verify.stopped(seq, operation);
        Err(Error::app(format!(
            "replay verification of workflow `{}` reached the end of the recorded history at step {seq}",
            self.workflow_id
        )))
    }

    /// What the in-transaction flag reports.
    const CONCURRENT_TRANSACTION: &'static str =
        "a transaction is already running in this workflow";

    /// Take the in-transaction flag, refusing a second transaction while one is
    /// already running — it would deadlock on the outer's write lock.
    ///
    /// This is a *concurrency* guard, not a nesting one. Nesting is refused by
    /// [`claim_position`](Self::claim_position) before the counter moves, with
    /// [`Error::NestedDurableCall`]. What is left for the flag is two
    /// transactions running at once on one workflow — through `join!` or a
    /// spawned task — which the task-local scope cannot observe, since it is
    /// unset whenever a body is between polls. The guard clears it on drop.
    fn begin_transaction(&self) -> Result<TxFlagGuard<'_>> {
        if self.in_transaction.swap(true, Ordering::SeqCst) {
            return Err(Error::app(Self::CONCURRENT_TRANSACTION));
        }
        Ok(TxFlagGuard(&self.in_transaction))
    }

    /// The span covering one durable operation (a step or a transaction),
    /// carrying the DBOS trace attributes (see the
    /// [`observability`](crate::observability) guide). Created inside the
    /// workflow's instrumented future, so it parents under the workflow span
    /// contextually.
    fn op_span(&self, op: &'static str, name: &str, seq: i32) -> tracing::Span {
        tracing::info_span!(
            "step",
            otel.name = %name,
            dbos.operation.type = op,
            dbos.operation.workflow_id = %self.workflow_id,
            dbos.step.id = seq,
            dbos.step.replayed = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        )
    }

    /// The current step index — the `seq` the next durable operation will use,
    /// i.e. how many durable operations (steps, sleeps, sends, child workflows)
    /// this execution has performed so far.
    pub fn current_step_id(&self) -> i32 {
        self.seq.load(Ordering::Relaxed)
    }

    /// Decide whether this workflow should run the **patched** (new) code at this
    /// point: returns `true` for new code, `false` for old.
    ///
    /// **Await this where it is written.** Unlike every other durable call, a
    /// patch cannot claim its position when it is built: whether it occupies one
    /// at all depends on what is already recorded there, which is a read of the
    /// database. It therefore takes its position when it is first polled, and a
    /// patch built alongside other durable calls and awaited out of order would
    /// collide with them. Nothing races a patch in practice — it is a branch
    /// point in the workflow's own control flow — so the rule costs nothing to
    /// keep.
    ///
    /// This lets you change a workflow's body while long-lived workflows are
    /// still running. Wrap the changed region in a patch:
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// if ctx.patch("use-v2-pricing").await? {
    ///     // new code
    /// } else {
    ///     // old code
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A workflow that reaches this point for the first time (new, or one that
    /// started but hadn't got here yet) records a marker and takes the new path.
    /// A workflow that already executed past this point before the patch existed
    /// takes the old path, and its existing checkpoints stay aligned because the
    /// marker only consumes a step slot on the new path.
    pub async fn patch(&self, name: &str) -> Result<bool> {
        self.refuse_inside_body("patch")?;
        let seq = self.current_step_id();
        let marker = format!("{PATCH_PREFIX}{name}");
        let patched = match self.provider.get_step_name(&self.workflow_id, seq).await? {
            // Not seen before: record the marker and take the new path. The
            // marker is a write, so a verification run stops here instead.
            None => {
                self.refuse_live_work(seq, &marker)?;
                self.provider
                    .record_patch(&self.workflow_id, seq, &marker)
                    .await?;
                true
            }
            // Our own marker (a replay/recovery of a patched run): new path.
            Some(recorded) if recorded == marker => {
                self.served_record(seq);
                true
            }
            // A different step already occupies this slot (a pre-patch run): old path.
            Some(_) => false,
        };
        if patched {
            // The marker takes its own step slot, so new-path steps that follow
            // are numbered after it. Old-path runs don't consume it.
            self.next_seq();
        }
        Ok(patched)
    }

    /// Remove a patch once every workflow that recorded it has finished migrating
    /// — the counterpart to [`patch`](Self::patch). Call it where the `patch`
    /// call used to be, then keep only the new code.
    ///
    /// For a run that recorded this patch, it consumes the marker's step slot so
    /// the following checkpoints still line up; for any other run it does
    /// nothing. Once no running workflow carries the marker, the call can be
    /// deleted entirely.
    pub async fn deprecate_patch(&self, name: &str) -> Result<()> {
        self.refuse_inside_body("deprecate_patch")?;
        let seq = self.current_step_id();
        let marker = format!("{PATCH_PREFIX}{name}");
        if self
            .provider
            .get_step_name(&self.workflow_id, seq)
            .await?
            .as_deref()
            == Some(marker.as_str())
        {
            // The marker's slot is consumed, not re-recorded: read-only on every
            // run, verification included.
            self.served_record(seq);
            self.next_seq();
        }
        Ok(())
    }

    /// Start a **child workflow** from within this workflow and return a handle
    /// to it. Await its result with [`WorkflowHandle::result`].
    ///
    /// The child runs durably and independently of the parent. It is keyed to
    /// this call's step position: unless `opts.workflow_id` is set, it gets the
    /// deterministic id `{parent_id}-{seq}`, and the parent→child link is
    /// checkpointed. On replay the same child is re-attached instead of being
    /// started again, so the child runs at most once per logical call.
    ///
    /// The child inherits this workflow's identity ([`AuthContext`]) field by
    /// field — each auth field set on `opts` overrides just that field — and
    /// records its `parent_workflow_id`. Pass
    /// `opts.queue` to route the child through a queue instead of running it inline.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result, WorkflowOptions};
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// // Fan out durable children, then gather their results.
    /// let mut handles = Vec::new();
    /// for region in ["us", "eu", "ap"] {
    ///     let h = ctx
    ///         .start_workflow::<_, u64>("count_orders", region.to_string(), WorkflowOptions::default())
    ///         .await?;
    ///     handles.push(h);
    /// }
    /// let mut total = 0;
    /// for h in handles {
    ///     total += h.result().await?;
    /// }
    /// # let _ = total;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// [`Error::UnknownWorkflow`] if `name` is not registered on this engine,
    /// or [`Error::UnexpectedStep`] if a replay finds a different child (or
    /// operation) recorded at this position.
    pub fn start_workflow<'a, I, O>(
        &'a self,
        name: &str,
        input: I,
        mut opts: WorkflowOptions,
    ) -> PendingStep<'a, WorkflowHandle<O>>
    where
        I: Serialize,
        O: Send + 'a,
    {
        // Encoded before the position is claimed, so an input that will not
        // serialize fails without moving the counter.
        let input_json = match serde_json::to_value(input) {
            Ok(input_json) => input_json,
            Err(e) => return PendingStep::failed(e.into()),
        };
        let name = name.to_owned();
        let position = claim!(self, "start_workflow");
        let seq = position.seq();
        PendingStep::new(position, async move {
            // Replay: re-attach to the child already started at this step. A
            // different workflow name recorded here means the parent is
            // non-deterministic — re-attaching would hand back the wrong child.
            if let Some((child_id, recorded)) = self
                .provider
                .check_child_workflow(&self.workflow_id, seq)
                .await?
            {
                self.check_recorded(seq, &name, &recorded)?;
                return Ok(WorkflowHandle::polling(child_id, self.provider.clone()));
            }
            // Starting a child is a side effect; a verification run stops here
            // rather than launching one.
            self.refuse_live_work(seq, &name)?;

            let child_id = opts
                .workflow_id
                .clone()
                // An explicit empty id means "assign one for me": fall through to the
                // deterministic `{parent}-{seq}` so an empty id is never persisted.
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("{}-{}", self.workflow_id, seq));
            opts.workflow_id = Some(child_id.clone());

            // The child inherits this workflow's identity **per field**: each auth
            // field set on `opts` overrides just that field, and every unset field
            // falls back to the parent's — so overriding only the assumed role still
            // carries the parent's user and roles (matching the reference SDKs).
            let child_auth = AuthContext {
                authenticated_user: opts
                    .authenticated_user
                    .clone()
                    .or_else(|| self.auth.authenticated_user.clone()),
                assumed_role: opts
                    .assumed_role
                    .clone()
                    .or_else(|| self.auth.assumed_role.clone()),
                authenticated_roles: if opts.authenticated_roles.is_empty() {
                    self.auth.authenticated_roles.clone()
                } else {
                    opts.authenticated_roles.clone()
                },
            };

            self.runtime
                .spawn_child(
                    &child_id,
                    &name,
                    input_json,
                    opts,
                    &self.workflow_id,
                    child_auth,
                )
                .await?;
            self.provider
                .record_child_workflow(&self.workflow_id, seq, &name, &child_id)
                .await?;

            Ok(WorkflowHandle::polling(child_id, self.provider.clone()))
        })
    }

    /// Run a durable step with the default policy (no retries).
    ///
    /// On the first execution, `f` runs and its result is checkpointed to the
    /// state backend. On any later replay (e.g. after a crash) the stored result
    /// is returned and `f` is **not** run again — so side effects inside `f`
    /// execute at most once per logical step under normal operation.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Error, Result};
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// let charge_id = ctx
    ///     .step("charge_card", |_step| async {
    ///         // Any side effect: an HTTP call, an email, a write to another system.
    ///         Ok::<_, Error>("ch_123".to_string())
    ///     })
    ///     .await?;
    /// # let _ = charge_id;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// `f` is `FnOnce`: it is invoked at most once per call, and receives a
    /// [`StepCtx`] describing the attempt. For automatic retries, use
    /// [`step_with`](Self::step_with).
    ///
    /// # Errors
    ///
    /// Returns the error `f` failed with — checkpointed, so a replay yields the
    /// same error without re-running `f`. Also [`Error::Cancelled`] if the
    /// workflow was cancelled, and [`Error::UnexpectedStep`] if a replay finds a
    /// different operation recorded at this step position (a non-deterministic
    /// workflow function).
    pub fn step<'a, T, F, Fut>(&'a self, name: &str, f: F) -> PendingStep<'a, T>
    where
        T: Serialize + DeserializeOwned + Send + 'a,
        F: FnOnce(StepCtx) -> Fut + Send + 'a,
        Fut: Future<Output = Result<T>> + Send + 'a,
    {
        let position = claim!(self, "step");
        let seq = position.seq();
        let span = self.op_span("step", name, seq);
        let name = name.to_owned();
        PendingStep::new(position, async move {
            let out = async {
                if let Some(stored) = self.replay_or_guard::<T>(seq, &name).await? {
                    return Ok(stored);
                }
                let started = chrono::Utc::now().timestamp_millis();
                match run_step_catching(
                    &name,
                    in_body(|| f(StepCtx::new(&self.workflow_id, seq, 0, 1))),
                )
                .await
                {
                    Ok(v) => self.checkpoint(seq, &name, v, Some(started)).await,
                    Err(e) => self.record_failure(seq, &name, e, Some(started)).await,
                }
            }
            .instrument(span.clone())
            .await;
            span.record("otel.status_code", if out.is_ok() { "OK" } else { "ERROR" });
            out
        })
    }

    /// Run a durable step with an explicit retry [`StepOptions`] policy.
    ///
    /// If the closure errors, it is retried with exponential backoff up to
    /// `max_retries` times. Only the **final** outcome is checkpointed, so a
    /// replay never re-runs a step that previously succeeded. Before running a
    /// fresh (non-replayed) attempt, the workflow's status is checked: a
    /// `CANCELLED` workflow refuses to run the step and returns
    /// [`Error::Cancelled`].
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Error, Result, StepOptions};
    /// # async fn fetch_quote() -> Result<f64> { Ok(1.0) }
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// let quote = ctx
    ///     .step_with(
    ///         StepOptions::new("fetch_quote").max_retries(5),
    ///         |step| async move {
    ///             tracing::debug!(attempt = step.attempt, "fetching quote");
    ///             fetch_quote().await
    ///         },
    ///     )
    ///     .await?;
    /// # let _ = quote;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns the **final** error once retries are exhausted (or immediately,
    /// if a [`retry_if`](StepOptions::retry_if) predicate rejects it) —
    /// checkpointed, so a replay yields the same error without re-running.
    /// Also [`Error::Cancelled`] if the workflow was cancelled, and
    /// [`Error::UnexpectedStep`] on a divergent replay.
    pub fn step_with<'a, T, F, Fut>(&'a self, opts: StepOptions, mut f: F) -> PendingStep<'a, T>
    where
        T: Serialize + DeserializeOwned + Send + 'a,
        F: FnMut(StepCtx) -> Fut + Send + 'a,
        Fut: Future<Output = Result<T>> + Send + 'a,
    {
        let position = claim!(self, "step");
        let seq = position.seq();
        let span = self.op_span("step", &opts.name, seq);
        PendingStep::new(position, async move {
            let out = async {
                if let Some(stored) = self.replay_or_guard::<T>(seq, &opts.name).await? {
                    return Ok(stored);
                }
                // Run with retries; only the final result/error is observed, then
                // checkpointed — a success as its output, a failure as its error.
                let started = chrono::Utc::now().timestamp_millis();
                match self.run_with_retries(seq, &opts, &mut f).await {
                    Ok(v) => self.checkpoint(seq, &opts.name, v, Some(started)).await,
                    Err(e) => self.record_failure(seq, &opts.name, e, Some(started)).await,
                }
            }
            .instrument(span.clone())
            .await;
            span.record("otel.status_code", if out.is_ok() { "OK" } else { "ERROR" });
            out
        })
    }

    /// Run a **transactional step**: the closure's SQL writes and this step's
    /// checkpoint commit in **one** database transaction, so the writes happen
    /// exactly once. On replay the recorded output is returned without
    /// re-running the body; on a body error the transaction rolls back (nothing
    /// the body wrote persists) and the step re-runs on replay, like an ordinary
    /// step. Requires a SQL backend (Postgres or SQLite); on the in-memory
    /// backend it returns an error.
    ///
    /// This is the default transactional step: the body stays portable across
    /// backends. When a step's types outgrow [`Param`](crate::Param) (`jsonb`,
    /// arrays, `uuid`, …) or it should reuse sqlx-typed helpers, switch that
    /// step to [`transaction_on`](Self::transaction_on) — see the
    /// [`transactions`](crate::transactions) guide's "Which transaction API?"
    /// table.
    ///
    /// The body receives a [`Tx`] and returns a boxed future — `Box::pin(async
    /// move { … })`, mirroring sqlx's own transaction closures. Native
    /// [`transaction_on`](Self::transaction_on) uses the same boxed-future form.
    /// Use [`#[transaction]`](macro@crate::transaction) to skip the scaffolding.
    /// SQL is written with `?` placeholders (rewritten to `$1, $2, …` for
    /// Postgres) and bound via [`params!`](crate::params):
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result, params};
    /// # async fn ex(ctx: &DurableContext) -> Result<()> {
    /// let bal: i64 = ctx
    ///     .transaction("debit", |tx| Box::pin(async move {
    ///         tx.execute("UPDATE acct SET bal = bal - ? WHERE id = ?",
    ///                    &params![10_i64, 1_i64]).await?;
    ///         let row = tx.query_one("SELECT bal FROM acct WHERE id = ?",
    ///                                &params![1_i64]).await?;
    ///         Ok(row.get::<i64>("bal"))
    ///     }))
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn transaction<'a, T, F>(&'a self, name: &str, f: F) -> PendingStep<'a, T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: for<'t, 'c> Fn(&'t mut Tx<'c>) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 't>>
            + Send
            + Sync
            + 'static,
    {
        self.transaction_with(TransactionOptions::new(name), f)
    }

    /// Like [`transaction`](Self::transaction) but with explicit
    /// [`TransactionOptions`] — isolation level and read-only.
    ///
    /// Under `RepeatableRead`/`Serializable` a serialization conflict restarts
    /// the whole transaction on a fresh one, so the body may run more than once;
    /// it must therefore be `Fn` (re-runnable). Capture `Copy` data freely;
    /// clone other captures inside the closure.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, IsolationLevel, Result, TransactionOptions, params};
    /// # async fn ex(ctx: &DurableContext) -> Result<()> {
    /// let opts = TransactionOptions::new("transfer").isolation(IsolationLevel::Serializable);
    /// ctx.transaction_with::<(), _>(opts, |tx| Box::pin(async move {
    ///     tx.execute("UPDATE acct SET bal = bal - ? WHERE id = ?", &params![10_i64, 1_i64]).await?;
    ///     Ok(())
    /// })).await?;
    /// # Ok(()) }
    /// ```
    pub fn transaction_with<'a, T, F>(
        &'a self,
        opts: TransactionOptions,
        f: F,
    ) -> PendingStep<'a, T>
    where
        T: Serialize + DeserializeOwned + Send + 'static,
        F: for<'t, 'c> Fn(&'t mut Tx<'c>) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 't>>
            + Send
            + Sync
            + 'static,
    {
        // The nesting guard first, so a transaction opened inside a body reports
        // the same error as any other durable call made there. The flag below
        // cannot tell that case apart from a concurrent one: it is held for as
        // long as a transaction runs, and a body that is parked is still holding
        // it. What it uniquely catches is two transactions running at once in
        // one workflow, which the guard cannot see, because a task-local scope
        // is not set while a body is between polls.
        let position = claim!(self, "transaction");
        let seq = position.seq();
        let span = self.op_span("transaction", &opts.name, seq);
        PendingStep::new(position, async move {
            let out = async {
                // A transaction's own replay check runs inside the provider, in
                // the database transaction it is about to open — too late for a
                // verification run, which must not open one. Consult the record
                // here first: a recorded outcome is served, and nothing recorded
                // at this position stops the run before the body can execute.
                if self.verifying() {
                    if let Some(stored) = self.replay_or_guard::<T>(seq, &opts.name).await? {
                        return Ok(stored);
                    }
                }
                let _guard = self.begin_transaction()?;
                let started = chrono::Utc::now().timestamp_millis();
                // Separate the call from the `async move`: `f(tx)` borrows `f`
                // and yields a future that we move in, so the wrapper stays `Fn`
                // (re-runnable).
                let body: TxBody = Box::new(move |tx| {
                    // Entered here rather than inside the `async move`: `f`'s
                    // own work happens at this call, and it is part of the body
                    // too.
                    let running = in_body(|| f(tx));
                    Box::pin(async move {
                        let out = running.await?;
                        Ok::<_, Error>(serde_json::to_value(out)?)
                    })
                });
                let value = self
                    .provider
                    .run_transaction_step(&self.workflow_id, seq, started, &opts, body)
                    .await?;
                Ok(serde_json::from_value(value)?)
            }
            .instrument(span.clone())
            .await;
            span.record("otel.status_code", if out.is_ok() { "OK" } else { "ERROR" });
            out
        })
    }

    /// Run a durable transaction on a **separate application database**.
    ///
    /// Claims its position at construction and borrows the workflow context.
    /// The returned future can be combined with other durable operations in scope.
    ///
    /// [`transaction`](Self::transaction) commits the body's SQL and the step
    /// checkpoint together — but only in the *system* database. This runs the
    /// body against your own database through a
    /// [`PgDataSource`](crate::PgDataSource) or
    /// [`SqliteDataSource`](crate::SqliteDataSource), keeping the same
    /// exactly-once guarantee with a two-commit protocol: the body's writes
    /// and a `transaction_completion` witness row commit atomically on the
    /// application database, then the ordinary checkpoint is written to the
    /// system database. Recovery replays in layers — checkpoint first, then
    /// the completion row (a crash between the two commits) — and re-runs the
    /// body only when neither exists.
    ///
    /// The body receives the backend's **native `sqlx` connection**
    /// (`&mut sqlx::PgConnection` / `&mut sqlx::SqliteConnection`), so
    /// existing queries, `sqlx` macros, and data-access helpers work
    /// unchanged. durare owns the transaction: there is no commit method on a
    /// plain connection, and on Postgres a raw `COMMIT`/`ROLLBACK` statement
    /// smuggled through SQL is detected and fails the step.
    ///
    /// The callback returns a boxed `Send` future. It must be repeatable (`Fn`):
    /// conflicts and retries start a fresh transaction. Clone owned captures in
    /// the callback before returning `Box::pin(async move { ... })`.
    ///
    /// If your application tables live **in the system database**, get the
    /// data source from the provider instead —
    /// `PostgresProvider::system_datasource` /
    /// `SqliteProvider::system_datasource`. Sameness is then known by
    /// construction, and this call takes a **single-commit fast path**: the
    /// body's writes and the checkpoint commit in one transaction (no witness
    /// row, no crash window) while the body keeps the native connection —
    /// unlike [`transaction`](Self::transaction), whose
    /// [`Param`](crate::Param) bindings cover only a small portable type set.
    /// A system data source is bound to the provider that minted it; used
    /// under a different engine it is rejected rather than misrouting its
    /// checkpoint.
    ///
    /// # The data source is part of the workflow's contract
    ///
    /// Which database `ds` points at is invisible to the engine — it cannot
    /// tell a right database from a wrong one, and running against the wrong
    /// one **succeeds silently**. Two rules keep that from biting:
    ///
    /// - **Derive `ds` from the workflow's input**, deterministically (e.g.
    ///   look the tenant up in a map keyed by an input field) — never from
    ///   ambient state that can disagree with the input, and never captured
    ///   once at registration for all runs.
    /// - **Keep the wiring stable across executions**, exactly like the
    ///   workflow's code: recovery looks for the witness row in whatever
    ///   database `ds` points at *now*, so repointing it while runs are
    ///   in flight (e.g. migrating a tenant's data mid-run) strands the
    ///   witness and re-runs the body. Drain in-flight workflows before
    ///   moving a database, or move the `transaction_completion` table with
    ///   the data.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, PgDataSource, Result};
    /// # async fn ex(ctx: &DurableContext, ds: PgDataSource) -> Result<()> {
    /// let total: i64 = ctx
    ///     .transaction_on(&ds, "record-order", |conn| Box::pin(async move {
    ///         sqlx::query("INSERT INTO orders(item) VALUES ($1)")
    ///             .bind("widget")
    ///             .execute(&mut *conn)
    ///             .await?;
    ///         let n = sqlx::query_scalar("SELECT count(*) FROM orders")
    ///             .fetch_one(&mut *conn)
    ///             .await?;
    ///         Ok(n)
    ///     }))
    ///     .await?;
    /// # let _ = total;
    /// # Ok(()) }
    /// ```
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub fn transaction_on<'a, DS, T, F>(
        &'a self,
        ds: &'a DS,
        name: &str,
        f: F,
    ) -> PendingStep<'a, T>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + Send + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> crate::BoxFuture<'c, Result<T>> + Send + Sync + 'a,
    {
        self.transaction_on_with(ds, TransactionOptions::new(name), f)
    }

    /// Like [`transaction_on`](Self::transaction_on), with an explicit retry policy.
    /// The callback is repeatable: clone owned dependencies before each returned future.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub fn transaction_on_with<'a, DS, T, F>(
        &'a self,
        ds: &'a DS,
        opts: TransactionOptions,
        f: F,
    ) -> PendingStep<'a, T>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + Send + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> crate::BoxFuture<'c, Result<T>> + Send + Sync + 'a,
    {
        let position = claim!(self, "transaction");
        let seq = position.seq();
        let span = self.op_span("transaction", &opts.name, seq);
        PendingStep::new(position, async move {
            let _guard = self.begin_transaction()?;
            let out = self
                .run_datasource_transaction(ds, &opts, &f, seq)
                .instrument(span.clone())
                .await;
            span.record("otel.status_code", if out.is_ok() { "OK" } else { "ERROR" });
            out
        })
    }

    /// The two-commit protocol behind [`transaction_on`](Self::transaction_on):
    /// layered replay, then fresh execution under the same two-loop retry
    /// structure as the single-database transactional step.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    async fn run_datasource_transaction<DS, T, F>(
        &self,
        ds: &DS,
        opts: &TransactionOptions,
        f: &F,
        seq: i32,
    ) -> Result<T>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> crate::BoxFuture<'c, Result<T>> + Send + Sync,
    {
        // Layer 1: the system-database checkpoint — a completed run.
        if let Some(stored) = self.replay_or_guard::<T>(seq, &opts.name).await? {
            return Ok(stored);
        }
        let started = chrono::Utc::now().timestamp_millis();
        let ser = self.provider.serializer();

        // A system data source runs on the system database's own pool, so one
        // commit can cover the body's writes and the checkpoint — no witness
        // row, no crash window, no layer 2. But only under the engine whose
        // provider minted it: the fast path writes the checkpoint through the
        // data source's pool, which is only this workflow's system database if
        // the identities match. A mismatch is a wiring bug — fail loudly
        // rather than splitting the checkpoint from the status row.
        //
        // This check deliberately sits AFTER the layer-1 replay above: a step
        // that already completed replays from its checkpoint even when the
        // wiring is now foreign, so recovering finished work is never hostage
        // to a configuration change — only a fresh execution is rejected. Do
        // not hoist the (cheaper) match above the replay read.
        match ds.kind() {
            crate::datasource::DataSourceKind::System(identity)
                if self
                    .provider
                    .provider_identity()
                    .is_some_and(|own| identity.matches(own)) =>
            {
                return self
                    .run_system_datasource_transaction(ds, opts, f, seq, &ser, started)
                    .await;
            }
            crate::datasource::DataSourceKind::System(_) => {
                return Err(Error::app(
                    "this system data source was minted by a different provider than the \
                     one this workflow runs on; use system_datasource() from this \
                     engine's own provider (or an external data source)",
                ));
            }
            crate::datasource::DataSourceKind::External => {}
        }

        // Layer 2: a completion row without a checkpoint — the application
        // transaction committed but the run crashed before the system commit.
        // Replay the stored outcome without re-running the body.
        if let Some(row) = ds.fetch_completion(&self.workflow_id, seq).await? {
            return self
                .replay_completion_row(seq, &opts.name, row, started)
                .await;
        }

        // OUTER loop: the user-facing retry policy for application errors,
        // mirroring the single-database transactional step. Conflicts are
        // handled by the inner loop and don't count against this budget.
        let mut user_attempt: u32 = 0;
        let body_err = loop {
            // INNER loop: one committed attempt, or an application error
            // surfaced to the outer loop. A serialization/deadlock conflict or
            // transient DB error rolls back and retries on a fresh transaction
            // — unbounded (until it clears or the workflow is cancelled).
            let mut conflict_attempt: u32 = 0;
            let outcome = loop {
                match self.datasource_attempt(ds, opts, f, seq, &ser).await {
                    Ok(DsAttempt::Committed(value)) => break Ok(value),
                    // Another execution committed this step first. An identical
                    // stored row is a replay/retry of this same logical write —
                    // converge on it. A divergent one is a live rival execution:
                    // stop, and let the engine adopt the recorded workflow
                    // outcome rather than doubling every remaining step.
                    Ok(DsAttempt::AlreadyCompleted { value }) => {
                        let row = ds
                            .fetch_completion(&self.workflow_id, seq)
                            .await?
                            .ok_or_else(|| {
                                Error::app(
                                    "transaction_completion row vanished after a duplicate insert",
                                )
                            })?;
                        if !completion_row_matches(&ser, &row, &value)? {
                            return Err(Error::WorkflowConflict(self.workflow_id.clone()));
                        }
                        return self
                            .replay_completion_row(seq, &opts.name, row, started)
                            .await;
                    }
                    Err(e) if e.is_tx_conflict() || e.is_retryable() => {
                        self.datasource_conflict_wait(conflict_attempt).await?;
                        conflict_attempt = conflict_attempt.saturating_add(1);
                    }
                    Err(e) => break Err(e),
                }
            };
            match outcome {
                Ok(value) => {
                    // Second commit: checkpoint into the system database. The
                    // application transaction is already durable, so a racing
                    // writer's canonical outcome wins if there is one.
                    let stored = self
                        .provider
                        .record_step_result(
                            &self.workflow_id,
                            seq,
                            &opts.name,
                            value,
                            None,
                            Some(started),
                            Some(self.runtime.executor_id()),
                        )
                        .await?;
                    return outcome_value(stored);
                }
                Err(e) if opts.should_user_retry(&e, user_attempt) => {
                    let delay = opts.user_retry_backoff(user_attempt);
                    tracing::warn!(
                        step = %opts.name,
                        attempt = user_attempt + 1,
                        error = %e,
                        "transaction failed; retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                    user_attempt += 1;
                }
                Err(e) => break e,
            }
        };

        // Mirror the permanent failure into the application database (the
        // body's transaction rolled back, so this is a standalone insert),
        // written before the system-database record to keep the
        // layer-1-then-layer-2 recovery order. Best-effort: the system
        // database remains the source of truth.
        let encoded = crate::serialize::encode_error(&ser, &body_err);
        if let Err(mirror_err) = ds
            .insert_failure(&self.workflow_id, seq, &encoded, ser.name())
            .await
        {
            tracing::warn!(
                step = %opts.name,
                error = %mirror_err,
                "failed to mirror the transaction failure into the application database"
            );
        }
        self.record_failure(seq, &opts.name, body_err, Some(started))
            .await
    }

    /// One fresh application-database attempt: begin, run the body, write the
    /// completion row, commit — all atomic. Begins a fresh transaction on
    /// every call so a closed/aborted one never leaks into a retry.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    async fn datasource_attempt<DS, T, F>(
        &self,
        ds: &DS,
        opts: &TransactionOptions,
        f: &F,
        seq: i32,
        ser: &crate::serialize::Serializer,
    ) -> Result<DsAttempt>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> crate::BoxFuture<'c, Result<T>> + Send + Sync,
    {
        let mut tx = ds.begin(opts.isolation, opts.read_only).await?;
        let fingerprint = ds.tx_fingerprint(&mut *tx).await?;
        match in_body(|| f(&mut *tx)).await {
            Ok(v) => {
                let value = serde_json::to_value(v)?;
                // A body that ended our transaction via raw SQL would make the
                // completion row commit separately from the writes it
                // witnesses — detect and refuse instead of breaking atomicity.
                if let Some(expected) = &fingerprint {
                    if ds.tx_fingerprint(&mut *tx).await?.as_ref() != Some(expected) {
                        let _ = ds.rollback(tx).await;
                        return Err(Error::app(TX_TERMINATED_MSG));
                    }
                }
                // A read-only body writes nothing, so there is nothing for
                // the witness row to witness — and Postgres rejects any write
                // inside a read-only transaction. Commit the read; the caller
                // checkpoints to the system database ordinary-step-style
                // (at-least-once is harmless for a pure read).
                if opts.read_only {
                    ds.commit(tx).await?;
                    return Ok(DsAttempt::Committed(value));
                }
                let encoded = ser.encode(&value)?;
                if !ds
                    .insert_completion(
                        &mut *tx,
                        &self.workflow_id,
                        seq,
                        Some(&encoded),
                        None,
                        ser.name(),
                    )
                    .await?
                {
                    let _ = ds.rollback(tx).await;
                    return Ok(DsAttempt::AlreadyCompleted { value });
                }
                ds.commit(tx).await?;
                Ok(DsAttempt::Committed(value))
            }
            Err(e) => {
                let _ = ds.rollback(tx).await;
                Err(e)
            }
        }
    }

    /// The single-commit fast path behind [`transaction_on`](Self::transaction_on)
    /// for a **system** data source (one built by a provider's
    /// `system_datasource`): the pool is the system database's own, so the
    /// step checkpoint commits inside the body's transaction — same guarantee
    /// as [`transaction`](Self::transaction), no witness row, no crash window.
    /// Same two-loop retry structure as the two-commit path.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    async fn run_system_datasource_transaction<DS, T, F>(
        &self,
        ds: &DS,
        opts: &TransactionOptions,
        f: &F,
        seq: i32,
        ser: &crate::serialize::Serializer,
        started: i64,
    ) -> Result<T>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> crate::BoxFuture<'c, Result<T>> + Send + Sync,
    {
        let mut user_attempt: u32 = 0;
        let body_err = loop {
            let mut conflict_attempt: u32 = 0;
            let outcome = loop {
                match self
                    .system_datasource_attempt(ds, opts, f, seq, ser, started)
                    .await
                {
                    Ok(DsAttempt::Committed(value)) => break Ok(value),
                    // Another execution checkpointed this step first. An
                    // identical recorded output is a replay/retry of this same
                    // logical write — converge on it. Anything else is a live
                    // rival execution: stop, and let the engine adopt the
                    // recorded workflow outcome rather than doubling every
                    // remaining step.
                    Ok(DsAttempt::AlreadyCompleted { value }) => {
                        let rec = self
                            .provider
                            .get_step_result(&self.workflow_id, seq)
                            .await?
                            .ok_or_else(|| {
                                Error::app("checkpoint row vanished after a duplicate insert")
                            })?;
                        self.check_recorded(seq, &opts.name, &rec.name)?;
                        if !matches!(&rec.outcome, StepOutcome::Output(stored) if *stored == value)
                        {
                            return Err(Error::WorkflowConflict(self.workflow_id.clone()));
                        }
                        tracing::Span::current().record("dbos.step.replayed", true);
                        return Ok(serde_json::from_value(value)?);
                    }
                    Err(e) if e.is_tx_conflict() || e.is_retryable() => {
                        self.datasource_conflict_wait(conflict_attempt).await?;
                        conflict_attempt = conflict_attempt.saturating_add(1);
                    }
                    Err(e) => break Err(e),
                }
            };
            match outcome {
                Ok(value) => {
                    // The read-only fast path skipped the in-transaction
                    // checkpoint (a read-only transaction cannot write one);
                    // record it now, ordinary-step-style — the canonical
                    // stored outcome wins if a rival got there first.
                    if opts.read_only {
                        let stored = self
                            .provider
                            .record_step_result(
                                &self.workflow_id,
                                seq,
                                &opts.name,
                                value,
                                None,
                                Some(started),
                                Some(self.runtime.executor_id()),
                            )
                            .await?;
                        return outcome_value(stored);
                    }
                    return Ok(serde_json::from_value(value)?);
                }
                Err(e) if opts.should_user_retry(&e, user_attempt) => {
                    let delay = opts.user_retry_backoff(user_attempt);
                    tracing::warn!(
                        step = %opts.name,
                        attempt = user_attempt + 1,
                        error = %e,
                        "transaction failed; retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                    user_attempt += 1;
                }
                Err(e) => break e,
            }
        };
        // No witness table on the fast path: the failure is recorded in the
        // system database only, like the single-database transactional step.
        self.record_failure(seq, &opts.name, body_err, Some(started))
            .await
    }

    /// One fast-path attempt: begin on the system pool, run the body, insert
    /// the `operation_outputs` checkpoint in the same transaction, commit.
    /// `AlreadyCompleted` means another execution checkpointed this step
    /// first: this attempt rolled back — its writes discarded — and the
    /// caller replays the canonical outcome.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    async fn system_datasource_attempt<DS, T, F>(
        &self,
        ds: &DS,
        opts: &TransactionOptions,
        f: &F,
        seq: i32,
        ser: &crate::serialize::Serializer,
        started: i64,
    ) -> Result<DsAttempt>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> crate::BoxFuture<'c, Result<T>> + Send + Sync,
    {
        let mut tx = ds.begin(opts.isolation, opts.read_only).await?;
        let fingerprint = ds.tx_fingerprint(&mut *tx).await?;
        match in_body(|| f(&mut *tx)).await {
            Ok(v) => {
                let value = serde_json::to_value(v)?;
                // Ending our transaction via raw SQL would split the writes
                // from their checkpoint — detect and refuse.
                if let Some(expected) = &fingerprint {
                    if ds.tx_fingerprint(&mut *tx).await?.as_ref() != Some(expected) {
                        let _ = ds.rollback(tx).await;
                        return Err(Error::app(TX_TERMINATED_MSG));
                    }
                }
                // A read-only body has no writes to make atomic with the
                // checkpoint — and Postgres rejects the insert in a read-only
                // transaction. Commit the read; the caller checkpoints
                // afterwards, ordinary-step-style.
                if opts.read_only {
                    ds.commit(tx).await?;
                    return Ok(DsAttempt::Committed(value));
                }
                let encoded = ser.encode(&value)?;
                if !ds
                    .insert_checkpoint(
                        &mut *tx,
                        &self.workflow_id,
                        seq,
                        &opts.name,
                        &encoded,
                        ser.name(),
                        started,
                    )
                    .await?
                {
                    // Another execution already checkpointed this step. Roll
                    // back — discarding this attempt's writes keeps the step
                    // exactly-once even under duplicate execution — and let
                    // the caller classify the stored row.
                    let _ = ds.rollback(tx).await;
                    return Ok(DsAttempt::AlreadyCompleted { value });
                }
                ds.commit(tx).await?;
                Ok(DsAttempt::Committed(value))
            }
            Err(e) => {
                let _ = ds.rollback(tx).await;
                Err(e)
            }
        }
    }

    /// Replay a layer-2 completion row: backfill the system-database
    /// checkpoint from it, then surface the stored outcome — the recorded
    /// output, or the recorded failure as its reconstructed error.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    async fn replay_completion_row<T: DeserializeOwned>(
        &self,
        seq: i32,
        name: &str,
        row: crate::datasource::CompletionRow,
        started: i64,
    ) -> Result<T> {
        tracing::Span::current().record("dbos.step.replayed", true);
        if let Some(err_text) = row.error.as_deref() {
            let stored = self
                .provider
                .record_step_result(
                    &self.workflow_id,
                    seq,
                    name,
                    Value::Null,
                    Some(err_text),
                    Some(started),
                    Some(self.runtime.executor_id()),
                )
                .await?;
            return outcome_value(stored);
        }
        let output = row
            .output
            .as_deref()
            .ok_or_else(|| Error::app("transaction completion row has neither output nor error"))?;
        let ser = self.provider.serializer();
        let value = crate::serialize::decode(&ser, row.serialization.as_deref(), output)?;
        let stored = self
            .provider
            .record_step_result(
                &self.workflow_id,
                seq,
                name,
                value,
                None,
                Some(started),
                Some(self.runtime.executor_id()),
            )
            .await?;
        outcome_value(stored)
    }

    /// Back off after an application-database conflict, bailing out if the
    /// workflow has been cancelled — so a transaction stuck on contention or a
    /// transient outage keeps retrying until it clears or the workflow is
    /// actually cancelled.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    async fn datasource_conflict_wait(&self, attempt: u32) -> Result<()> {
        if let Some(status) = self.provider.get_workflow_status(&self.workflow_id).await? {
            if status.status == STATUS_CANCELLED {
                return Err(Error::Cancelled(self.workflow_id.clone()));
            }
        }
        let ms = (1u64 << attempt.min(10)).min(1000);
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        Ok(())
    }

    /// Race several async `branches` and return the `(index, value)` of the first
    /// to complete — a **durable** select.
    ///
    /// The winning index and value are recorded as a single step, so a replay
    /// returns the same winner without re-running anything. On a tie the lowest
    /// index wins.
    ///
    /// The branches are **plain async work**: a branch is a durable body, so a
    /// durable operation created in one is refused with
    /// [`Error::NestedDurableCall`] and one built outside and awaited in one with
    /// [`Error::DurableCallCrossedBody`]. On a replay the branches are not polled
    /// at all — the recorded winner is returned — so a durable call inside a
    /// branch would claim a position on the first run that no replay claims
    /// again, and every later operation would shift onto it. Race the plain work
    /// here and put the durable operations around the race; for a race between
    /// recorded operations, give each branch a child workflow.
    ///
    /// This is a step whose body is a race, and the step contract applies to it
    /// in full. The winner's own side effect is [at-least-once]: if the process
    /// stops after a branch completed but before this operation's row commits,
    /// the whole race runs again on recovery and a different branch may win.
    /// Losing branches are dropped, which cancels their futures but does not
    /// undo whatever they did before that, and does not stop tasks they spawned.
    ///
    /// [at-least-once]: crate::durability#the-at-least-once-window
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn fetch_primary() -> String { String::new() }
    /// # async fn fetch_fallback() -> String { String::new() }
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// let (winner, value) = ctx
    ///     .select(vec![
    ///         Box::pin(async { fetch_primary().await }),
    ///         Box::pin(async { fetch_fallback().await }),
    ///     ])
    ///     .await?;
    /// # let _ = (winner, value);
    /// # Ok(())
    /// # }
    /// ```
    pub fn select<'a, T>(
        &'a self,
        mut branches: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>>,
    ) -> PendingStep<'a, (usize, T)>
    where
        T: Serialize + DeserializeOwned + Send + 'a,
    {
        // Refused before the position is claimed, so a race that cannot run
        // moves no counter and the calls around it keep their positions.
        if branches.is_empty() {
            return PendingStep::failed(Error::app("select requires at least one branch"));
        }
        let position = claim!(self, "select");
        let seq = position.seq();
        PendingStep::new(position, async move {
            if let Some(stored) = self
                .replay_or_guard::<(usize, T)>(seq, "DBOS.select")
                .await?
            {
                return Ok(stored);
            }
            let started = chrono::Utc::now().timestamp_millis();

            // Poll the branches in index order on this one task; the first ready wins
            // (lowest index on a tie). The losers are dropped — and so cancelled —
            // when `branches` goes out of scope.
            let race = poll_fn(|cx| {
                for (i, branch) in branches.iter_mut().enumerate() {
                    if let Poll::Ready(value) = branch.as_mut().poll(cx) {
                        return Poll::Ready((i, value));
                    }
                }
                Poll::Pending
            });
            let (index, value) = in_body(|| race).await;

            self.checkpoint(seq, "DBOS.select", (index, value), Some(started))
                .await
        })
    }

    /// Consult the record at `seq` for the operation `expected`, now executing:
    /// the one gate every durable call passes through before doing work.
    /// `Ok(Some(v))` means "return `v`"; `Ok(None)` means "nothing recorded,
    /// proceed". A replay that finds a *different* operation recorded at this
    /// position fails with [`Error::UnexpectedStep`] — the workflow is
    /// non-deterministic, and the stored checkpoint would be the wrong step's
    /// result — and a verification run stops at the first position with nothing
    /// recorded ([`refuse_live_work`](Self::refuse_live_work)).
    async fn serve_recorded<T: DeserializeOwned>(
        &self,
        seq: i32,
        expected: &str,
    ) -> Result<Option<T>> {
        if let Some(rec) = self
            .provider
            .get_step_result(&self.workflow_id, seq)
            .await?
        {
            self.check_recorded(seq, expected, &rec.name)?;
            // Mark the enclosing operation span; a no-op for callers without
            // one (the field is not declared on any other span).
            tracing::Span::current().record("dbos.step.replayed", true);
            // A recorded failure replays as its error, so a failed step is not
            // re-run (and a non-deterministic step cannot succeed on replay).
            return Ok(Some(outcome_value(rec.outcome)?));
        }
        // Nothing recorded here: a live run proceeds from this position, a
        // verification run stops at it.
        self.refuse_live_work(seq, expected)?;
        Ok(None)
    }

    /// Shared step preamble: [`serve_recorded`](Self::serve_recorded), then
    /// refuse to start fresh work on a `CANCELLED` workflow. `Ok(Some(v))` means
    /// "return `v`"; `Ok(None)` means "proceed to run the closure".
    async fn replay_or_guard<T: DeserializeOwned>(
        &self,
        seq: i32,
        expected: &str,
    ) -> Result<Option<T>> {
        if let Some(stored) = self.serve_recorded(seq, expected).await? {
            return Ok(Some(stored));
        }
        if let Some(status) = self.provider.get_workflow_status(&self.workflow_id).await? {
            if status.status == STATUS_CANCELLED {
                return Err(Error::Cancelled(self.workflow_id.clone()));
            }
        }
        Ok(None)
    }

    /// Durably record a successful `result` under `(workflow_id, seq)` and return
    /// the canonical stored value (a racing writer's outcome wins if there is one
    /// — including a recorded failure, which is then surfaced as an error).
    /// `started_at_ms` is when the step's work began, for duration introspection.
    async fn checkpoint<T: Serialize + DeserializeOwned>(
        &self,
        seq: i32,
        name: &str,
        result: T,
        started_at_ms: Option<i64>,
    ) -> Result<T> {
        let json = serde_json::to_value(&result)?;
        let outcome = self
            .provider
            .record_step_result(
                &self.workflow_id,
                seq,
                name,
                json,
                None,
                started_at_ms,
                Some(self.runtime.executor_id()),
            )
            .await?;
        outcome_value(outcome)
    }

    /// Durably record a failed step's error under `(workflow_id, seq)`. Returns
    /// the original `err` once recorded (preserving its concrete type on this
    /// first execution); if a concurrent execution recorded a *success* first,
    /// that canonical output is returned instead. On any later replay the recorded
    /// failure is reconstructed by [`replay_or_guard`], so the step never re-runs.
    async fn record_failure<T: DeserializeOwned>(
        &self,
        seq: i32,
        name: &str,
        err: Error,
        started_at_ms: Option<i64>,
    ) -> Result<T> {
        let encoded = crate::serialize::encode_error(&self.provider.serializer(), &err);
        let outcome = self
            .provider
            .record_step_result(
                &self.workflow_id,
                seq,
                name,
                Value::Null,
                Some(&encoded),
                started_at_ms,
                Some(self.runtime.executor_id()),
            )
            .await?;
        match outcome {
            StepOutcome::Failure { .. } => Err(err),
            StepOutcome::Output(v) => Ok(serde_json::from_value(v)?),
        }
    }

    /// Drive `f` to success, retrying on error per `opts` with exponential
    /// backoff. Returns the last error if all attempts are exhausted.
    async fn run_with_retries<T, F, Fut>(
        &self,
        seq: i32,
        opts: &StepOptions,
        f: &mut F,
    ) -> Result<T>
    where
        F: FnMut(StepCtx) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt: u32 = 0;
        let max_attempts = u64::from(opts.max_retries) + 1;
        loop {
            let step = StepCtx::new(&self.workflow_id, seq, attempt, max_attempts);
            match run_step_catching(&opts.name, in_body(|| f(step))).await {
                Ok(v) => return Ok(v),
                Err(e) => {
                    // A predicate that rejects the error stops retries immediately,
                    // regardless of remaining attempts (fail fast on permanent errors).
                    let retryable = opts.retry_if.as_ref().is_none_or(|p| p(&e));
                    if !retryable || attempt >= opts.max_retries {
                        return Err(e);
                    }
                    let backoff =
                        opts.base_interval.as_secs_f64() * opts.backoff_factor.powi(attempt as i32);
                    let delay = Duration::from_secs_f64(backoff).min(opts.max_interval);
                    self.runtime
                        .counters
                        .step_retries
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        step = %opts.name,
                        attempt = attempt + 1,
                        error = %e,
                        "step failed; retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Durably sleep for `dur`.
    ///
    /// The absolute wake time is fixed and persisted on the first call as an
    /// ordinary `DBOS.sleep` step in `operation_outputs`, so the timer does not
    /// drift if the workflow crashes and is replayed: a replay reads the same
    /// wake instant and only waits the *remaining* time. A workflow can safely
    /// sleep for days:
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # use std::time::Duration;
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// ctx.sleep(Duration::from_secs(7 * 24 * 3600)).await?; // a restart doesn't reset it
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails only on a storage error, or [`Error::UnexpectedStep`] on a
    /// divergent replay.
    #[doc(alias = "timer")]
    #[doc(alias = "delay")]
    pub fn sleep(&self, dur: Duration) -> PendingStep<'_, ()> {
        let position = claim!(self, "sleep");
        let seq = position.seq();
        PendingStep::new(position, async move {
            let wake_at = self
                .durable_value_at(seq, "DBOS.sleep", || wake_instant(dur))
                .await?;
            let now = chrono::Utc::now();
            // A verification run reads the recorded wake instant and moves on.
            // Waiting it out would stall the check for however long the timer
            // has left — and there is nothing to wait for: no later operation
            // runs, so none can depend on the delay having elapsed.
            if wake_at > now && !self.verifying() {
                let remaining = (wake_at - now).to_std().unwrap_or(Duration::ZERO);
                tokio::time::sleep(remaining).await;
            }
            Ok(())
        })
    }

    /// A durable wall-clock read. Records the current instant on first execution
    /// and replays that same instant thereafter, so a timestamp taken inside a
    /// workflow is stable across recovery — where a bare `Utc::now()` would
    /// silently return a different value and break determinism.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn ex(ctx: &DurableContext) -> Result<()> {
    /// let started = ctx.now().await?; // same value on every replay
    /// # Ok(()) }
    /// ```
    pub fn now(&self) -> PendingStep<'_, chrono::DateTime<chrono::Utc>> {
        self.durable_value("DBOS.now", chrono::Utc::now)
    }

    /// A durable random UUID (v4): minted on first execution and replayed
    /// thereafter. The safe way to generate an id inside a workflow — a bare
    /// `Uuid::new_v4()` would differ on recovery. Returned as a string.
    pub fn uuid(&self) -> PendingStep<'_, String> {
        self.durable_value("DBOS.uuid", || uuid::Uuid::new_v4().to_string())
    }

    /// A durable random `f64` in `[0, 1)`: drawn on first execution and replayed
    /// thereafter. For any randomness a workflow's control flow depends on.
    pub fn random(&self) -> PendingStep<'_, f64> {
        self.durable_value("DBOS.random", || {
            // 48 fully-random bits from a v4 UUID (OS-entropy-backed via
            // getrandom). Bytes 0..6 precede the version/variant nibbles, so
            // they are uniformly random; 48 bits are exactly representable in an
            // f64 mantissa, giving a uniform value in [0, 1).
            let b = uuid::Uuid::new_v4().into_bytes();
            let n = (0..6).fold(0u64, |acc, i| (acc << 8) | b[i] as u64);
            n as f64 / (1u64 << 48) as f64
        })
    }

    /// Record (first execution) or replay (thereafter) a non-deterministic value
    /// under a reserved `DBOS.*` op at the next seq, so a clock/RNG/UUID read
    /// returns the same value on every replay. The shared machinery behind
    /// [`now`](Self::now), [`uuid`](Self::uuid), and [`random`](Self::random).
    fn durable_value<'a, T, P>(&'a self, name: &'static str, produce: P) -> PendingStep<'a, T>
    where
        T: Serialize + DeserializeOwned + Send + 'a,
        P: FnOnce() -> T + Send + 'a,
    {
        let position = match self.claim_position(name) {
            Ok(position) => position,
            Err(e) => return PendingStep::failed(e),
        };
        PendingStep::new(
            position,
            self.durable_value_at(position.seq(), name, produce),
        )
    }

    /// [`durable_value`](Self::durable_value)'s run, once the position is
    /// claimed: serve the recorded value, or produce and checkpoint one. Also the
    /// wake instant behind [`sleep`](Self::sleep) and the `recv`/`get_event`
    /// timeouts, recorded as a `DBOS.sleep` step so a timer never extends across
    /// a crash.
    ///
    /// No cancellation check, unlike [`replay_or_guard`](Self::replay_or_guard):
    /// reading the clock or minting an id is not work a cancelled workflow is
    /// refused.
    async fn durable_value_at<T, P>(&self, seq: i32, name: &str, produce: P) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        P: FnOnce() -> T,
    {
        if let Some(value) = self.serve_recorded(seq, name).await? {
            return Ok(value);
        }
        self.checkpoint(seq, name, produce(), None).await
    }

    /// Durably send a message to another workflow on `topic`. Recorded as a
    /// `DBOS.send` step, so a replay does not re-send.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// ctx.send("order-1001", "approved".to_string(), "review").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Like any step side effect, the send commits before its checkpoint: a
    /// crash in that window re-sends on replay (at-least-once). The receiving
    /// side ([`recv`](Self::recv)) consumes exactly once.
    ///
    /// # Errors
    ///
    /// [`Error::NonExistentWorkflow`] if the destination workflow does not
    /// exist; otherwise storage errors, [`Error::Cancelled`], or
    /// [`Error::UnexpectedStep`] on a divergent replay.
    #[doc(alias = "signal")]
    pub fn send<T: Serialize>(
        &self,
        destination_id: &str,
        message: T,
        topic: &str,
    ) -> PendingStep<'_, ()> {
        // Encoded before the position is claimed, so a message that will not
        // serialize fails without moving the counter.
        let encoded = match serde_json::to_value(message) {
            Ok(encoded) => encoded,
            Err(e) => return PendingStep::failed(e.into()),
        };
        let destination_id = destination_id.to_owned();
        let topic = topic.to_owned();
        let position = claim!(self, "send");
        let seq = position.seq();
        PendingStep::new(position, async move {
            if let Some(_done) = self.replay_or_guard::<Value>(seq, "DBOS.send").await? {
                return Ok(());
            }
            self.provider
                .insert_notification(&destination_id, &topic, encoded, None)
                .await?;
            self.provider
                .record_step_result(
                    &self.workflow_id,
                    seq,
                    "DBOS.send",
                    Value::Null,
                    None,
                    None,
                    Some(self.runtime.executor_id()),
                )
                .await?;
            Ok(())
        })
    }

    /// **Replace** the custom attributes attached to workflow `id` (commonly
    /// this workflow's own — [`workflow_id`](Self::workflow_id)); `None` or an
    /// empty map clears them. Recorded as one durable step
    /// (`DBOS.updateWorkflowAttributes`, the cross-SDK name), so under
    /// recovery the replacement happens exactly once and a replay does not
    /// re-run it. Replace, not merge.
    ///
    /// # Errors
    ///
    /// [`Error::NonExistentWorkflow`] if the target workflow does not exist;
    /// otherwise storage errors, [`Error::Cancelled`], or
    /// [`Error::UnexpectedStep`] on a divergent replay.
    pub fn set_workflow_attributes(
        &self,
        id: &str,
        attributes: Option<serde_json::Map<String, Value>>,
    ) -> PendingStep<'_, ()> {
        let id = id.to_owned();
        let position = claim!(self, "set_workflow_attributes");
        let seq = position.seq();
        PendingStep::new(position, async move {
            if let Some(_done) = self
                .replay_or_guard::<Value>(seq, "DBOS.updateWorkflowAttributes")
                .await?
            {
                return Ok(());
            }
            self.provider
                .set_workflow_attributes(&id, attributes.as_ref())
                .await?;
            self.provider
                .record_step_result(
                    &self.workflow_id,
                    seq,
                    "DBOS.updateWorkflowAttributes",
                    Value::Null,
                    None,
                    None,
                    Some(self.runtime.executor_id()),
                )
                .await?;
            Ok(())
        })
    }

    /// Send many messages in one durable operation — the fan-out counterpart
    /// of [`send`](Self::send). The whole batch is one recorded step
    /// (`DBOS.send_bulk`): on replay nothing is re-delivered, and on the SQL
    /// backends the messages land atomically (all or none — see
    /// [`SendMessage`](crate::SendMessage) for per-message fields).
    ///
    /// # Errors
    ///
    /// [`Error::NonExistentWorkflow`] if any destination does not exist;
    /// otherwise storage errors, [`Error::Cancelled`], or
    /// [`Error::UnexpectedStep`] on a divergent replay.
    pub fn send_bulk<T: Serialize>(
        &self,
        messages: &[crate::SendMessage<T>],
    ) -> PendingStep<'_, ()> {
        // Validate + serialize before claiming the seq, so a bad batch fails
        // without consuming a checkpoint slot.
        let rows = match crate::engine::prepare_bulk(messages) {
            Ok(rows) => rows,
            Err(e) => return PendingStep::failed(e),
        };
        let position = claim!(self, "send_bulk");
        let seq = position.seq();
        PendingStep::new(position, async move {
            if let Some(_done) = self.replay_or_guard::<Value>(seq, "DBOS.send_bulk").await? {
                return Ok(());
            }
            self.provider.insert_notifications(&rows).await?;
            self.provider
                .record_step_result(
                    &self.workflow_id,
                    seq,
                    "DBOS.send_bulk",
                    Value::Null,
                    None,
                    None,
                    Some(self.runtime.executor_id()),
                )
                .await?;
            Ok(())
        })
    }

    /// Receive the oldest unconsumed message sent to this workflow on `topic`,
    /// waiting up to `timeout`. Messages are consumed FIFO, exactly once: the
    /// claim and the step checkpoint commit
    /// atomically, and a replay returns the recorded message without consuming
    /// another. Returns `None` on timeout (also recorded, so a replay does not
    /// wait again). The timeout deadline itself is durable: a crash mid-wait
    /// resumes with the *remaining* time, not a fresh timeout.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # use std::time::Duration;
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// // Block this workflow until an approval message arrives (or a day passes).
    /// match ctx.recv::<String>("review", Duration::from_secs(24 * 3600)).await? {
    ///     Some(decision) => println!("decision: {decision}"),
    ///     None => println!("timed out waiting for review"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A timeout is **not** an error — it is `Ok(None)`. Fails on storage or
    /// decode errors, [`Error::Cancelled`], or [`Error::UnexpectedStep`] on a
    /// divergent replay.
    #[doc(alias = "signal")]
    pub fn recv<'a, T: DeserializeOwned + Send + 'a>(
        &'a self,
        topic: &str,
        timeout: Duration,
    ) -> PendingStep<'a, Option<T>> {
        let topic = topic.to_owned();
        let position = claim!(self, "recv");
        let seq = position.seq();
        let deadline_seq = self.next_seq();
        PendingStep::new(position, async move {
            // Construction reserves positions; only a polled wait accounts for
            // its deadline during verification. Do this before the replay gate,
            // which may stop at an in-flight wait with only a deadline recorded.
            self.reserved_record(deadline_seq, "DBOS.sleep");
            if let Some(stored) = self.replay_or_guard::<Option<T>>(seq, "DBOS.recv").await? {
                return Ok(stored);
            }

            let mut deadline: Option<chrono::DateTime<chrono::Utc>> = None;
            loop {
                if let Some(msg) = self
                    .provider
                    .consume_notification(&self.workflow_id, &topic, seq, "DBOS.recv")
                    .await?
                {
                    return Ok(Some(serde_json::from_value(msg)?));
                }

                // Mailbox empty: fix the durable deadline (first miss only), then
                // poll until a message arrives or the deadline passes.
                let deadline = match deadline {
                    Some(d) => d,
                    None => *deadline.insert(
                        self.durable_value_at(deadline_seq, "DBOS.sleep", || wake_instant(timeout))
                            .await?,
                    ),
                };
                let now = chrono::Utc::now();
                if now >= deadline {
                    self.provider
                        .record_step_result(
                            &self.workflow_id,
                            seq,
                            "DBOS.recv",
                            Value::Null,
                            None,
                            None,
                            Some(self.runtime.executor_id()),
                        )
                        .await?;
                    return Ok(None);
                }
                let remaining = (deadline - now).to_std().unwrap_or(Duration::ZERO);
                self.provider
                    .await_change(
                        ChangeWait::Notification {
                            workflow_id: &self.workflow_id,
                            topic: &topic,
                        },
                        remaining.min(self.wait_interval()),
                    )
                    .await;
            }
        })
    }

    /// Publish (or overwrite) the value of event `key` on this workflow.
    /// Recorded as a `DBOS.setEvent` step; other workflows and external code
    /// read it with `get_event` — the natural way to expose progress or a
    /// result to observers:
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// ctx.set_event("status", "shipped".to_string()).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails on a storage error, [`Error::Cancelled`], or
    /// [`Error::UnexpectedStep`] on a divergent replay.
    pub fn set_event<T: Serialize>(&self, key: &str, value: T) -> PendingStep<'_, ()> {
        // Encoded before the position is claimed, so a value that will not
        // serialize fails without moving the counter and leaving every later
        // call one slot along.
        let encoded = match serde_json::to_value(value) {
            Ok(encoded) => encoded,
            Err(e) => return PendingStep::failed(e.into()),
        };
        let key = key.to_owned();
        let position = claim!(self, "set_event");
        let seq = position.seq();
        PendingStep::new(position, async move {
            if let Some(_done) = self.replay_or_guard::<Value>(seq, "DBOS.setEvent").await? {
                return Ok(());
            }
            self.provider
                .upsert_event(&self.workflow_id, &key, encoded)
                .await?;
            self.provider
                .record_step_result(
                    &self.workflow_id,
                    seq,
                    "DBOS.setEvent",
                    Value::Null,
                    None,
                    None,
                    Some(self.runtime.executor_id()),
                )
                .await?;
            Ok(())
        })
    }

    /// Read event `key` of another workflow, waiting up to `timeout` for it to
    /// be set. The value observed is recorded as a `DBOS.getEvent` step, so
    /// replays see the same value even if the event is overwritten later.
    /// Returns `None` on timeout.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # use std::time::Duration;
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// let status: Option<String> = ctx
    ///     .get_event("order-1001", "status", Duration::from_secs(60))
    ///     .await?;
    /// # let _ = status;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// A timeout is **not** an error — it is `Ok(None)`. Fails on storage or
    /// decode errors, [`Error::Cancelled`], or [`Error::UnexpectedStep`] on a
    /// divergent replay.
    pub fn get_event<'a, T: DeserializeOwned + Send + 'a>(
        &'a self,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> PendingStep<'a, Option<T>> {
        let target_workflow_id = target_workflow_id.to_owned();
        let key = key.to_owned();
        let position = claim!(self, "get_event");
        let seq = position.seq();
        let deadline_seq = self.next_seq();
        PendingStep::new(position, async move {
            // Construction reserves positions; only a polled wait accounts for
            // its deadline during verification. Do this before the replay gate,
            // which may stop at an in-flight wait with only a deadline recorded.
            self.reserved_record(deadline_seq, "DBOS.sleep");
            if let Some(stored) = self
                .replay_or_guard::<Option<T>>(seq, "DBOS.getEvent")
                .await?
            {
                return Ok(stored);
            }

            let mut deadline: Option<chrono::DateTime<chrono::Utc>> = None;
            loop {
                if let Some(value) = self
                    .provider
                    .get_event_value(&target_workflow_id, &key)
                    .await?
                {
                    let outcome = self
                        .provider
                        .record_step_result(
                            &self.workflow_id,
                            seq,
                            "DBOS.getEvent",
                            value,
                            None,
                            None,
                            Some(self.runtime.executor_id()),
                        )
                        .await?;
                    return Ok(Some(outcome_value(outcome)?));
                }

                let deadline = match deadline {
                    Some(d) => d,
                    None => *deadline.insert(
                        self.durable_value_at(deadline_seq, "DBOS.sleep", || wake_instant(timeout))
                            .await?,
                    ),
                };
                let now = chrono::Utc::now();
                if now >= deadline {
                    self.provider
                        .record_step_result(
                            &self.workflow_id,
                            seq,
                            "DBOS.getEvent",
                            Value::Null,
                            None,
                            None,
                            Some(self.runtime.executor_id()),
                        )
                        .await?;
                    return Ok(None);
                }
                let remaining = (deadline - now).to_std().unwrap_or(Duration::ZERO);
                self.provider
                    .await_change(
                        ChangeWait::Event {
                            workflow_id: &target_workflow_id,
                            key: &key,
                        },
                        remaining.min(self.wait_interval()),
                    )
                    .await;
            }
        })
    }

    /// Append `value` to the append-only durable stream `key` on this workflow.
    /// Recorded as a `DBOS.writeStream` step, so a replay does not re-append.
    /// Each write lands at the next offset; readers drain values in order with
    /// [`DurableEngine::read_stream`](crate::DurableEngine::read_stream).
    ///
    /// Like any step side effect, the append commits before its checkpoint: a
    /// crash in that window re-appends on replay (at-least-once).
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: &DurableContext) -> Result<()> {
    /// for i in 0..3 {
    ///     ctx.write_stream("progress", format!("chunk {i}")).await?;
    /// }
    /// ctx.close_stream("progress").await?; // seal it; readers stop cleanly
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails if the stream was already closed by
    /// [`close_stream`](Self::close_stream); otherwise storage errors,
    /// [`Error::Cancelled`], or [`Error::UnexpectedStep`] on a divergent replay.
    pub fn write_stream<T: Serialize>(&self, key: &str, value: T) -> PendingStep<'_, ()> {
        let encoded = match serde_json::to_value(value) {
            Ok(encoded) => encoded,
            Err(e) => return PendingStep::failed(e.into()),
        };
        let key = key.to_owned();
        let position = claim!(self, "write_stream");
        let seq = position.seq();
        PendingStep::new(position, async move {
            if self
                .replay_or_guard::<Value>(seq, "DBOS.writeStream")
                .await?
                .is_some()
            {
                return Ok(());
            }
            self.provider
                .write_stream(&self.workflow_id, &key, Some(encoded), seq)
                .await?;
            self.provider
                .record_step_result(
                    &self.workflow_id,
                    seq,
                    "DBOS.writeStream",
                    Value::Null,
                    None,
                    None,
                    Some(self.runtime.executor_id()),
                )
                .await?;
            Ok(())
        })
    }

    /// Close the durable stream `key` on this workflow, sealing it against
    /// further writes. Recorded as a `DBOS.closeStream` step. A reader draining
    /// the stream observes the close and stops. Writing to a closed stream
    /// errors.
    pub fn close_stream(&self, key: &str) -> PendingStep<'_, ()> {
        let key = key.to_owned();
        let position = claim!(self, "close_stream");
        let seq = position.seq();
        PendingStep::new(position, async move {
            if self
                .replay_or_guard::<Value>(seq, "DBOS.closeStream")
                .await?
                .is_some()
            {
                return Ok(());
            }
            self.provider
                .write_stream(&self.workflow_id, &key, None, seq)
                .await?;
            self.provider
                .record_step_result(
                    &self.workflow_id,
                    seq,
                    "DBOS.closeStream",
                    Value::Null,
                    None,
                    None,
                    Some(self.runtime.executor_id()),
                )
                .await?;
            Ok(())
        })
    }

    /// Read the durable stream `key` produced by `workflow_id` (another workflow,
    /// or this one), blocking until the stream is closed or its producer goes
    /// inactive. Returns every value in order and whether the stream is closed —
    /// the consumer side of [`write_stream`](Self::write_stream).
    ///
    /// Unlike the write side, this is a **live read, not a durable step**: it is
    /// not checkpointed, so on replay it re-reads from the start (matching the
    /// other SDKs, where the producer's writes are durable but a reader is not).
    pub async fn read_stream<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> Result<(Vec<T>, bool)> {
        crate::provider::drain_stream(self.provider.as_ref(), workflow_id, key).await
    }

    /// Read the currently-available values of stream `key` on `workflow_id` from
    /// `from_offset`, without blocking — the non-blocking counterpart to
    /// [`read_stream`](Self::read_stream). Returns the values in order and whether
    /// the close sentinel has been reached; pass the count read so far as the next
    /// `from_offset` to poll incrementally. Also a live read (not checkpointed).
    pub async fn read_stream_snapshot<T: DeserializeOwned>(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<T>, bool)> {
        crate::provider::snapshot_stream(self.provider.as_ref(), workflow_id, key, from_offset)
            .await
    }

    /// Read the durable stream `key` on `workflow_id` as an asynchronous
    /// [`Stream`](futures_util::Stream), yielding each value in order as it is
    /// committed — the incremental counterpart to [`read_stream`](Self::read_stream),
    /// which instead blocks and returns the whole stream at once. The stream ends
    /// when the producer closes it or goes inactive; a decode or backend failure is
    /// the final `Err` item. Also a live read (not checkpointed). Consume it with
    /// [`StreamExt::next`](futures_util::StreamExt::next):
    ///
    /// ```no_run
    /// use durare::StreamExt;
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: &DurableContext, id: &str) -> Result<()> {
    /// let mut values = ctx.read_stream_values::<String>(id, "events");
    /// while let Some(v) = values.next().await {
    ///     println!("{}", v?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_stream_values<T: DeserializeOwned + 'static>(
        &self,
        workflow_id: &str,
        key: &str,
    ) -> impl futures_util::Stream<Item = Result<T>> + '_ {
        crate::provider::stream_values(self.provider.as_ref(), workflow_id, key)
    }

    /// Escape hatch for building application errors inside steps.
    pub fn err(&self, msg: impl Into<String>) -> Error {
        Error::app(msg)
    }

    /// How long a blocked `recv`/`get_event` waits before re-checking the
    /// database. On a backend with push wake-ups (Postgres `LISTEN`/`NOTIFY`)
    /// this is just a long backstop — [`StateProvider::await_change`] returns as
    /// soon as the awaited row is written — so we poll rarely; otherwise it is
    /// the short polling interval.
    fn wait_interval(&self) -> Duration {
        if self.provider.supports_listen_notify() {
            LISTEN_NOTIFY_BACKSTOP
        } else {
            NOTIFICATION_POLL_INTERVAL
        }
    }
}

/// How often blocked `recv`/`get_event` calls re-check the database on a backend
/// that only polls (in-memory, SQLite). Short, for responsiveness.
const NOTIFICATION_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Backstop re-check interval on a backend with push wake-ups (Postgres
/// `LISTEN`/`NOTIFY`): the await returns promptly when the row is written, so we
/// only fall back to a database re-check this often (covering a missed signal).
const LISTEN_NOTIFY_BACKSTOP: Duration = Duration::from_secs(5);

/// Prefix on the `function_name` of a patch marker recorded in `operation_outputs`.
/// A shared identifier, so a patch decision a worker in any language recorded is
/// read back consistently.
const PATCH_PREFIX: &str = "DBOS.patch-";

/// Error for a `transaction_on` body that ended durare's database transaction
/// via raw SQL, which would split the writes from their durability record.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
const TX_TERMINATED_MSG: &str =
    "the transaction body terminated the surrounding database transaction (a raw \
     COMMIT or ROLLBACK?), so its writes cannot be committed atomically with the \
     durability record";

/// Clears the in-transaction flag on drop (see
/// [`DurableContext::begin_transaction`]).
struct TxFlagGuard<'a>(&'a AtomicBool);

impl Drop for TxFlagGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

/// Outcome of one application-database attempt in the two-commit protocol
/// behind [`DurableContext::transaction_on`].
#[cfg(any(feature = "postgres", feature = "sqlite"))]
enum DsAttempt {
    /// The body ran and its transaction committed; carries the JSON output.
    Committed(Value),
    /// A completion row already existed — another execution committed this
    /// step first, and this attempt rolled back. Carries the output this
    /// attempt computed, so the caller can classify the stored row: identical
    /// content is a replay/retry converging on the canonical outcome, anything
    /// else is a live rival execution ([`Error::WorkflowConflict`]).
    AlreadyCompleted {
        /// The rolled-back attempt's computed output.
        value: Value,
    },
}

/// Whether a stored completion row records the same successful outcome this
/// attempt computed: a recorded success whose decoded output equals `value`.
/// A recorded failure, or a divergent output, is another live execution's
/// write — the caller reports [`Error::WorkflowConflict`]. (The witness table
/// carries no step name or start instant, so content is the whole comparison.)
#[cfg(any(feature = "postgres", feature = "sqlite"))]
fn completion_row_matches(
    ser: &crate::serialize::Serializer,
    row: &crate::datasource::CompletionRow,
    value: &Value,
) -> Result<bool> {
    let Some(output) = row.output.as_deref() else {
        return Ok(false);
    };
    if row.error.is_some() {
        return Ok(false);
    }
    let stored = crate::serialize::decode(ser, row.serialization.as_deref(), output)?;
    Ok(stored == *value)
}

/// Turn a recorded step outcome into the typed value a step returns: a recorded
/// output is deserialized; a recorded failure is surfaced as its reconstructed
/// error (so a replayed failed step returns the same error without re-running).
fn outcome_value<T: DeserializeOwned>(outcome: StepOutcome) -> Result<T> {
    Ok(serde_json::from_value(outcome.into_value_result()?)?)
}

/// The absolute instant a durable timer started now would fire at. A duration
/// too large for `chrono` counts as zero, so an absurd timeout fires at once
/// rather than failing the workflow.
fn wake_instant(dur: Duration) -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now()
        + chrono::Duration::from_std(dur).unwrap_or_else(|_| chrono::Duration::zero())
}

/// Await a step's future, converting a panic in the step body into an error so
/// it flows through the normal failure path — retry (per [`StepOptions`]), then
/// checkpoint the failure — instead of unwinding the whole workflow. A step that
/// panics is treated as a failed step, subject to its retry policy.
async fn run_step_catching<T>(name: &str, body: impl Future<Output = Result<T>>) -> Result<T> {
    match AssertUnwindSafe(body).catch_unwind().await {
        Ok(result) => result,
        Err(payload) => Err(Error::app(format!(
            "step `{name}` panicked: {}",
            panic_message(&*payload)
        ))),
    }
}

/// What a step body is told about the attempt it is running as.
///
/// Handed by value to the closure passed to [`DurableContext::step`] and
/// [`DurableContext::step_with`], one per attempt. It is small and `Clone`, and
/// it deliberately carries **no** durable capability: a step body cannot start
/// nested steps, child workflows, or transactions through it, which is what
/// keeps a step a leaf of the workflow.
#[derive(Clone, Debug)]
pub struct StepCtx {
    /// The position this step occupies in the workflow.
    pub step_id: i32,
    /// Which attempt this is, counting from zero.
    pub attempt: u32,
    /// How many attempts the step's retry policy allows in total.
    pub max_attempts: u64,
    workflow_id: String,
}

impl StepCtx {
    fn new(workflow_id: &str, step_id: i32, attempt: u32, max_attempts: u64) -> Self {
        Self {
            step_id,
            attempt,
            max_attempts,
            workflow_id: workflow_id.to_owned(),
        }
    }

    /// Stable across retries and recovery of this workflow and position.
    ///
    /// This is a versioned JSON pair, without the attempt number. A new workflow
    /// ID (including a fork) produces a different key. Reusing an ID and position
    /// for different work reuses the key; code compatibility remains required.
    /// The receiver must actually enforce deduplication. This is not exactly-once IO.
    pub fn idempotency_key(&self) -> String {
        format!(
            "durare:v1:{}",
            serde_json::to_string(&(&self.workflow_id, self.step_id))
                .expect("string and integer serialize")
        )
    }

    /// Whether this is the final attempt allowed by the retry policy.
    pub fn is_last_attempt(&self) -> bool {
        u64::from(self.attempt) + 1 >= self.max_attempts
    }
}

/// A durable operation that has claimed its position in the workflow but has
/// not run yet.
///
/// Every durable call on a [`DurableContext`] hands one of these back instead of
/// being an `async fn`, and the difference is where the call's **position** is
/// decided. A position is the `(workflow_id, seq)` key its checkpoint is written
/// under, and a replay finds the recorded result only by asking for the same
/// position the first run asked for.
///
/// An `async fn` body does not begin until something polls it, so a position
/// taken inside one follows *poll* order — which the combinator driving the
/// futures decides, not the code. `tokio::select!` polls in a randomised order;
/// awaiting two calls in the opposite order to the one they were written in
/// reverses them. Either way a replay is free to number the same calls
/// differently, and two calls that share a name then replay each other's
/// results with nothing to notice it.
///
/// Building one of these takes the position immediately, in the order the calls
/// appear in the source, so what fixes a position is a rule the language
/// guarantees rather than one the runtime happens to follow. Awaiting is then
/// only *running* something that already knows where it stands, and
/// `tokio::join!` over several is ordinary code.
///
/// # A built call has spent its position
///
/// The position is claimed at the call, so building one and dropping it without
/// awaiting still moves the counter. That is deterministic — a replay runs the
/// same code and skips the same position — but it is no longer the no-op it
/// would be for a plain future, which is why this is `#[must_use]`.
#[must_use = "this durable call has already claimed its position; awaiting it is what runs it"]
pub struct PendingStep<'a, T> {
    running: Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
    /// The operation whose position this holds, or `None` for a call refused
    /// before it claimed one. A refusal must report why it was refused wherever
    /// it is awaited, rather than being refused a second time for being there.
    claimed: Option<&'static str>,
}

impl<'a, T> PendingStep<'a, T> {
    /// Wraps the run of a call whose position has already been claimed.
    fn new(position: Position, running: impl Future<Output = Result<T>> + Send + 'a) -> Self {
        Self {
            running: Box::pin(running),
            claimed: Some(position.operation()),
        }
    }

    /// A call that failed before it could run, and so never claimed a position.
    /// Returned in place of a claim, never after one.
    fn failed(e: Error) -> Self {
        Self {
            running: Box::pin(async move { Err(e) }),
            claimed: None,
        }
    }
}

impl<T> Future for PendingStep<'_, T> {
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // Checked on *every* poll, not just the first: this type is `Unpin`, so
        // a call polled once in the workflow body can still be moved into a
        // step body afterwards.
        //
        // Only a call that holds a position is checked. A refusal carries none,
        // and reports what it was refused for wherever it is awaited. Asking
        // whether this call holds a position, rather than remembering which body
        // it was built in, is also what makes this a backstop: a durable
        // operation that skipped the check at construction is still refused.
        if let Some(operation) = self.claimed {
            if in_a_body() {
                return Poll::Ready(Err(Error::DurableCallCrossedBody(operation.to_owned())));
            }
        }
        self.running.as_mut().poll(cx)
    }
}

impl<T> std::fmt::Debug for PendingStep<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingStep").finish_non_exhaustive()
    }
}
