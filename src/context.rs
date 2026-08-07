use crate::engine::{Runtime, WorkflowOptions};
use crate::error::{panic_message, Error, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{ChangeWait, StateProvider, StepOutcome, WorkflowStatus, STATUS_CANCELLED};
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
use tracing::Instrument;

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
#[derive(Clone)]
pub struct DurableContext {
    workflow_id: String,
    provider: Arc<dyn StateProvider>,
    /// Shared execution core, so a workflow can start child workflows.
    runtime: Arc<Runtime>,
    auth: AuthContext,
    // Monotonic step index. Because the workflow's control flow is
    // deterministic, the same code path yields the same seq on every replay,
    // which is how we match a step call to its stored checkpoint.
    seq: Arc<AtomicI32>,
    // Set while a transaction body is running (shared across context clones, so a
    // clone captured inside a body sees it). Guards against nesting a transaction
    // inside another — which would deadlock on the outer's write lock.
    in_transaction: Arc<AtomicBool>,
}

impl DurableContext {
    pub(crate) fn new(workflow_id: String, runtime: Arc<Runtime>, auth: AuthContext) -> Self {
        Self {
            workflow_id,
            provider: runtime.provider().clone(),
            runtime,
            auth,
            seq: Arc::new(AtomicI32::new(0)),
            in_transaction: Arc::new(AtomicBool::new(false)),
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

    fn next_seq(&self) -> i32 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Set the in-transaction flag, refusing a nested transaction (it would
    /// deadlock on the outer's write lock). The guard clears the flag on drop.
    fn begin_transaction(&self) -> Result<TxFlagGuard<'_>> {
        if self.in_transaction.swap(true, Ordering::SeqCst) {
            return Err(Error::app(
                "cannot start a transaction inside another transaction",
            ));
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
    /// This lets you change a workflow's body while long-lived workflows are
    /// still running. Wrap the changed region in a patch:
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
        let seq = self.current_step_id();
        let marker = format!("{PATCH_PREFIX}{name}");
        let patched = match self.provider.get_step_name(&self.workflow_id, seq).await? {
            // Not seen before: record the marker and take the new path.
            None => {
                self.provider
                    .record_patch(&self.workflow_id, seq, &marker)
                    .await?;
                true
            }
            // Our own marker (a replay/recovery of a patched run): new path.
            Some(recorded) if recorded == marker => true,
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
        let seq = self.current_step_id();
        let marker = format!("{PATCH_PREFIX}{name}");
        if self
            .provider
            .get_step_name(&self.workflow_id, seq)
            .await?
            .as_deref()
            == Some(marker.as_str())
        {
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
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
    pub async fn start_workflow<I, O>(
        &self,
        name: &str,
        input: I,
        opts: WorkflowOptions,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
    {
        let seq = self.next_seq();

        // Replay: re-attach to the child already started at this step. A
        // different workflow name recorded here means the parent is
        // non-deterministic — re-attaching would hand back the wrong child.
        if let Some((child_id, recorded)) = self
            .provider
            .check_child_workflow(&self.workflow_id, seq)
            .await?
        {
            if recorded != name {
                return Err(Error::unexpected_step(
                    &self.workflow_id,
                    seq,
                    name,
                    recorded,
                ));
            }
            return Ok(WorkflowHandle::polling(child_id, self.provider.clone()));
        }

        let child_id = opts
            .workflow_id
            .clone()
            // An explicit empty id means "assign one for me": fall through to the
            // deterministic `{parent}-{seq}` so an empty id is never persisted.
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("{}-{}", self.workflow_id, seq));
        let mut opts = opts;
        opts.workflow_id = Some(child_id.clone());
        let input_json = serde_json::to_value(input)?;

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
                name,
                input_json,
                opts,
                &self.workflow_id,
                child_auth,
            )
            .await?;
        self.provider
            .record_child_workflow(&self.workflow_id, seq, name, &child_id)
            .await?;

        Ok(WorkflowHandle::polling(child_id, self.provider.clone()))
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
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
    /// let charge_id = ctx
    ///     .step("charge_card", || async {
    ///         // Any side effect: an HTTP call, an email, a write to another system.
    ///         Ok::<_, Error>("ch_123".to_string())
    ///     })
    ///     .await?;
    /// # let _ = charge_id;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// `f` is `FnOnce`: it is invoked at most once per call. For automatic
    /// retries, use [`step_with`](Self::step_with).
    ///
    /// # Errors
    ///
    /// Returns the error `f` failed with — checkpointed, so a replay yields the
    /// same error without re-running `f`. Also [`Error::Cancelled`] if the
    /// workflow was cancelled, and [`Error::UnexpectedStep`] if a replay finds a
    /// different operation recorded at this step position (a non-deterministic
    /// workflow function).
    pub async fn step<T, F, Fut>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let seq = self.next_seq();
        let span = self.op_span("step", name, seq);
        let out = async {
            if let Some(stored) = self.replay_or_guard::<T>(seq, name).await? {
                return Ok(stored);
            }
            let started = chrono::Utc::now().timestamp_millis();
            match run_step_catching(name, f()).await {
                Ok(v) => self.checkpoint(seq, name, v, Some(started)).await,
                Err(e) => self.record_failure(seq, name, e, Some(started)).await,
            }
        }
        .instrument(span.clone())
        .await;
        span.record("otel.status_code", if out.is_ok() { "OK" } else { "ERROR" });
        out
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
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
    /// let quote = ctx
    ///     .step_with(
    ///         StepOptions::new("fetch_quote").max_retries(5),
    ///         || async { fetch_quote().await },
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
    pub async fn step_with<T, F, Fut>(&self, opts: StepOptions, mut f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let seq = self.next_seq();
        let span = self.op_span("step", &opts.name, seq);
        let out = async {
            if let Some(stored) = self.replay_or_guard::<T>(seq, &opts.name).await? {
                return Ok(stored);
            }
            // Run with retries; only the final result/error is observed, then
            // checkpointed — a success as its output, a failure as its error.
            let started = chrono::Utc::now().timestamp_millis();
            match self.run_with_retries(&opts, &mut f).await {
                Ok(v) => self.checkpoint(seq, &opts.name, v, Some(started)).await,
                Err(e) => self.record_failure(seq, &opts.name, e, Some(started)).await,
            }
        }
        .instrument(span.clone())
        .await;
        span.record("otel.status_code", if out.is_ok() { "OK" } else { "ERROR" });
        out
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
    /// move { … })`, mirroring sqlx's own transaction closures. SQL is written
    /// with `?` placeholders (rewritten to `$1, $2, …` for Postgres) and bound
    /// via [`params!`](crate::params):
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result, params};
    /// # async fn ex(ctx: DurableContext) -> Result<()> {
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
    pub async fn transaction<T, F>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + 'static,
        F: for<'t, 'c> Fn(&'t mut Tx<'c>) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 't>>
            + Send
            + Sync
            + 'static,
    {
        self.transaction_with(TransactionOptions::new(name), f)
            .await
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
    /// # async fn ex(ctx: DurableContext) -> Result<()> {
    /// let opts = TransactionOptions::new("transfer").isolation(IsolationLevel::Serializable);
    /// ctx.transaction_with::<(), _>(opts, |tx| Box::pin(async move {
    ///     tx.execute("UPDATE acct SET bal = bal - ? WHERE id = ?", &params![10_i64, 1_i64]).await?;
    ///     Ok(())
    /// })).await?;
    /// # Ok(()) }
    /// ```
    pub async fn transaction_with<T, F>(&self, opts: TransactionOptions, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned + 'static,
        F: for<'t, 'c> Fn(&'t mut Tx<'c>) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 't>>
            + Send
            + Sync
            + 'static,
    {
        let _guard = self.begin_transaction()?;

        let seq = self.next_seq();
        let span = self.op_span("transaction", &opts.name, seq);
        let started = chrono::Utc::now().timestamp_millis();
        // Separate the call from the `async move`: `f(tx)` borrows `f` and yields
        // a future that we move in, so the wrapper stays `Fn` (re-runnable).
        let body: TxBody = Box::new(move |tx| {
            let fut = f(tx);
            Box::pin(async move {
                let out = fut.await?;
                Ok::<_, Error>(serde_json::to_value(out)?)
            })
        });
        let out = async {
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
    }

    /// Run a durable transaction on a **separate application database**.
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
    /// smuggled through SQL is detected and fails the step. Like
    /// [`transaction`](Self::transaction), the body must be re-runnable
    /// (`Fn`): a serialization conflict or deadlock restarts it on a fresh
    /// transaction.
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
    /// # async fn ex(ctx: DurableContext, ds: PgDataSource) -> Result<()> {
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
    pub async fn transaction_on<DS, T, F>(&self, ds: &DS, name: &str, f: F) -> Result<T>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>
            + Send
            + Sync
            + 'static,
    {
        self.transaction_on_with(ds, TransactionOptions::new(name), f)
            .await
    }

    /// Like [`transaction_on`](Self::transaction_on) but with explicit
    /// [`TransactionOptions`] — isolation level (advisory on SQLite),
    /// read-only, and the application-error retry policy. Conflicts and
    /// transient database errors are retried on a fresh transaction
    /// regardless, without consuming the `max_retries` budget; once that
    /// budget is exhausted the failure is recorded in **both** databases, so a
    /// replay returns the same error without re-running the body.
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    pub async fn transaction_on_with<DS, T, F>(
        &self,
        ds: &DS,
        opts: TransactionOptions,
        f: F,
    ) -> Result<T>
    where
        DS: crate::datasource::DataSource,
        T: Serialize + DeserializeOwned + 'static,
        F: for<'c> Fn(&'c mut DS::Conn) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>
            + Send
            + Sync
            + 'static,
    {
        let _guard = self.begin_transaction()?;

        let seq = self.next_seq();
        let span = self.op_span("transaction", &opts.name, seq);
        let out = self
            .run_datasource_transaction(ds, &opts, &f, seq)
            .instrument(span.clone())
            .await;
        span.record("otel.status_code", if out.is_ok() { "OK" } else { "ERROR" });
        out
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
        F: for<'c> Fn(&'c mut DS::Conn) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>
            + Send
            + Sync
            + 'static,
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
                    // Another execution committed this step first: its row is
                    // the canonical outcome — replay it.
                    Ok(DsAttempt::AlreadyCompleted) => {
                        let row = ds
                            .fetch_completion(&self.workflow_id, seq)
                            .await?
                            .ok_or_else(|| {
                                Error::app(
                                    "transaction_completion row vanished after a duplicate insert",
                                )
                            })?;
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
        F: for<'c> Fn(&'c mut DS::Conn) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>
            + Send
            + Sync
            + 'static,
    {
        let mut tx = ds.begin(opts.isolation, opts.read_only).await?;
        let fingerprint = ds.tx_fingerprint(&mut *tx).await?;
        match f(&mut *tx).await {
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
                    return Ok(DsAttempt::AlreadyCompleted);
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
        F: for<'c> Fn(&'c mut DS::Conn) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>
            + Send
            + Sync
            + 'static,
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
                    // Another execution checkpointed this step first; its
                    // recorded outcome is canonical — replay it.
                    Ok(DsAttempt::AlreadyCompleted) => {
                        return self
                            .replay_or_guard::<T>(seq, &opts.name)
                            .await?
                            .ok_or_else(|| {
                                Error::app("checkpoint row vanished after a duplicate insert")
                            });
                    }
                    Err(e) if e.is_tx_conflict() || e.is_retryable() => {
                        self.datasource_conflict_wait(conflict_attempt).await?;
                        conflict_attempt = conflict_attempt.saturating_add(1);
                    }
                    Err(e) => break Err(e),
                }
            };
            match outcome {
                Ok(value) => return Ok(serde_json::from_value(value)?),
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
        F: for<'c> Fn(&'c mut DS::Conn) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'c>>
            + Send
            + Sync
            + 'static,
    {
        let mut tx = ds.begin(opts.isolation, opts.read_only).await?;
        let fingerprint = ds.tx_fingerprint(&mut *tx).await?;
        match f(&mut *tx).await {
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
                    // the caller replay the canonical outcome.
                    let _ = ds.rollback(tx).await;
                    return Ok(DsAttempt::AlreadyCompleted);
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
            .record_step_result(&self.workflow_id, seq, name, value, None, Some(started))
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
    /// The branches must be **plain async work** — do not call
    /// [`step`](Self::step), [`start_workflow`](Self::start_workflow), or other
    /// durable operations inside them. The whole race is checkpointed as one
    /// operation and, on replay, the branches are not polled at all, so any
    /// durable calls nested inside would desynchronize the step sequence.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn fetch_primary() -> String { String::new() }
    /// # async fn fetch_fallback() -> String { String::new() }
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
    pub async fn select<T>(
        &self,
        branches: Vec<Pin<Box<dyn Future<Output = T> + Send + '_>>>,
    ) -> Result<(usize, T)>
    where
        T: Serialize + DeserializeOwned,
    {
        if branches.is_empty() {
            return Err(Error::app("select requires at least one branch"));
        }
        let seq = self.next_seq();
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
        let mut branches = branches;
        let (index, value) = poll_fn(|cx| {
            for (i, branch) in branches.iter_mut().enumerate() {
                if let Poll::Ready(value) = branch.as_mut().poll(cx) {
                    return Poll::Ready((i, value));
                }
            }
            Poll::Pending
        })
        .await;

        self.checkpoint(seq, "DBOS.select", (index, value), Some(started))
            .await
    }

    /// Shared step preamble: serve a replayed checkpoint if present, otherwise
    /// refuse to start fresh work on a `CANCELLED` workflow. `Ok(Some(v))` means
    /// "return `v`"; `Ok(None)` means "proceed to run the closure". `expected`
    /// is the operation now executing: a replay that finds a *different*
    /// operation recorded at this position fails with
    /// [`Error::UnexpectedStep`] — the workflow is non-deterministic, and the
    /// stored checkpoint would be the wrong step's result.
    async fn replay_or_guard<T: DeserializeOwned>(
        &self,
        seq: i32,
        expected: &str,
    ) -> Result<Option<T>> {
        if let Some(rec) = self
            .provider
            .get_step_result(&self.workflow_id, seq)
            .await?
        {
            if rec.name != expected {
                return Err(Error::unexpected_step(
                    &self.workflow_id,
                    seq,
                    expected,
                    rec.name,
                ));
            }
            // Mark the enclosing operation span; a no-op for callers without
            // one (the field is not declared on any other span).
            tracing::Span::current().record("dbos.step.replayed", true);
            // A recorded failure replays as its error, so a failed step is not
            // re-run (and a non-deterministic step cannot succeed on replay).
            return Ok(Some(outcome_value(rec.outcome)?));
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
            .record_step_result(&self.workflow_id, seq, name, json, None, started_at_ms)
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
            )
            .await?;
        match outcome {
            StepOutcome::Failure { .. } => Err(err),
            StepOutcome::Output(v) => Ok(serde_json::from_value(v)?),
        }
    }

    /// Drive `f` to success, retrying on error per `opts` with exponential
    /// backoff. Returns the last error if all attempts are exhausted.
    async fn run_with_retries<T, F, Fut>(&self, opts: &StepOptions, f: &mut F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempt: u32 = 0;
        loop {
            match run_step_catching(&opts.name, f()).await {
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
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
    pub async fn sleep(&self, dur: Duration) -> Result<()> {
        let seq = self.next_seq();
        let wake_at = self.durable_wake_at(seq, dur).await?;
        let now = chrono::Utc::now();
        if wake_at > now {
            let remaining = (wake_at - now).to_std().unwrap_or(Duration::ZERO);
            tokio::time::sleep(remaining).await;
        }
        Ok(())
    }

    /// Resolve the absolute wake instant for a durable timer at `seq`: the
    /// first call records `now + dur` as a `DBOS.sleep` step; replays read the
    /// stored instant back, so timers (and recv/get_event timeouts built on
    /// them) never extend across crashes.
    async fn durable_wake_at(
        &self,
        seq: i32,
        dur: Duration,
    ) -> Result<chrono::DateTime<chrono::Utc>> {
        match self
            .provider
            .get_step_result(&self.workflow_id, seq)
            .await?
        {
            Some(rec) => {
                if rec.name != "DBOS.sleep" {
                    return Err(Error::unexpected_step(
                        &self.workflow_id,
                        seq,
                        "DBOS.sleep",
                        rec.name,
                    ));
                }
                outcome_value(rec.outcome)
            }
            None => {
                let proposed = chrono::Utc::now()
                    + chrono::Duration::from_std(dur).unwrap_or_else(|_| chrono::Duration::zero());
                let outcome = self
                    .provider
                    .record_step_result(
                        &self.workflow_id,
                        seq,
                        "DBOS.sleep",
                        serde_json::to_value(proposed)?,
                        None,
                        None,
                    )
                    .await?;
                outcome_value(outcome)
            }
        }
    }

    /// A durable wall-clock read. Records the current instant on first execution
    /// and replays that same instant thereafter, so a timestamp taken inside a
    /// workflow is stable across recovery — where a bare `Utc::now()` would
    /// silently return a different value and break determinism.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn ex(ctx: DurableContext) -> Result<()> {
    /// let started = ctx.now().await?; // same value on every replay
    /// # Ok(()) }
    /// ```
    pub async fn now(&self) -> Result<chrono::DateTime<chrono::Utc>> {
        self.durable_value("DBOS.now", chrono::Utc::now).await
    }

    /// A durable random UUID (v4): minted on first execution and replayed
    /// thereafter. The safe way to generate an id inside a workflow — a bare
    /// `Uuid::new_v4()` would differ on recovery. Returned as a string.
    pub async fn uuid(&self) -> Result<String> {
        self.durable_value("DBOS.uuid", || uuid::Uuid::new_v4().to_string())
            .await
    }

    /// A durable random `f64` in `[0, 1)`: drawn on first execution and replayed
    /// thereafter. For any randomness a workflow's control flow depends on.
    pub async fn random(&self) -> Result<f64> {
        self.durable_value("DBOS.random", || {
            // 48 fully-random bits from a v4 UUID (OS-entropy-backed via
            // getrandom). Bytes 0..6 precede the version/variant nibbles, so
            // they are uniformly random; 48 bits are exactly representable in an
            // f64 mantissa, giving a uniform value in [0, 1).
            let b = uuid::Uuid::new_v4().into_bytes();
            let n = (0..6).fold(0u64, |acc, i| (acc << 8) | b[i] as u64);
            n as f64 / (1u64 << 48) as f64
        })
        .await
    }

    /// Record (first execution) or replay (thereafter) a non-deterministic value
    /// under a reserved `DBOS.*` op at the next seq, so a clock/RNG/UUID read
    /// returns the same value on every replay. The shared machinery behind
    /// [`now`](Self::now), [`uuid`](Self::uuid), and [`random`](Self::random) —
    /// the same record-or-replay shape as [`sleep`](Self::sleep)'s wake instant.
    async fn durable_value<T, P>(&self, name: &str, produce: P) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        P: FnOnce() -> T,
    {
        let seq = self.next_seq();
        match self
            .provider
            .get_step_result(&self.workflow_id, seq)
            .await?
        {
            Some(rec) => {
                if rec.name != name {
                    return Err(Error::unexpected_step(
                        &self.workflow_id,
                        seq,
                        name,
                        rec.name,
                    ));
                }
                outcome_value(rec.outcome)
            }
            None => {
                let value = produce();
                let outcome = self
                    .provider
                    .record_step_result(
                        &self.workflow_id,
                        seq,
                        name,
                        serde_json::to_value(&value)?,
                        None,
                        None,
                    )
                    .await?;
                outcome_value(outcome)
            }
        }
    }

    /// Durably send a message to another workflow on `topic`. Recorded as a
    /// `DBOS.send` step, so a replay does not re-send.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
    pub async fn send<T: Serialize>(
        &self,
        destination_id: &str,
        message: T,
        topic: &str,
    ) -> Result<()> {
        let seq = self.next_seq();
        if let Some(_done) = self.replay_or_guard::<Value>(seq, "DBOS.send").await? {
            return Ok(());
        }
        self.provider
            .insert_notification(destination_id, topic, serde_json::to_value(message)?, None)
            .await?;
        self.provider
            .record_step_result(&self.workflow_id, seq, "DBOS.send", Value::Null, None, None)
            .await?;
        Ok(())
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
    pub async fn set_workflow_attributes(
        &self,
        id: &str,
        attributes: Option<serde_json::Map<String, Value>>,
    ) -> Result<()> {
        let seq = self.next_seq();
        if let Some(_done) = self
            .replay_or_guard::<Value>(seq, "DBOS.updateWorkflowAttributes")
            .await?
        {
            return Ok(());
        }
        self.provider
            .set_workflow_attributes(id, attributes.as_ref())
            .await?;
        self.provider
            .record_step_result(
                &self.workflow_id,
                seq,
                "DBOS.updateWorkflowAttributes",
                Value::Null,
                None,
                None,
            )
            .await?;
        Ok(())
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
    pub async fn send_bulk<T: Serialize>(&self, messages: &[crate::SendMessage<T>]) -> Result<()> {
        // Validate + serialize before claiming the seq, so a bad batch fails
        // without consuming a checkpoint slot.
        let rows = crate::engine::prepare_bulk(messages)?;
        let seq = self.next_seq();
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
            )
            .await?;
        Ok(())
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
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
    pub async fn recv<T: DeserializeOwned>(
        &self,
        topic: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let seq = self.next_seq();
        let deadline_seq = self.next_seq();

        if let Some(stored) = self.replay_or_guard::<Option<T>>(seq, "DBOS.recv").await? {
            return Ok(stored);
        }

        let mut deadline: Option<chrono::DateTime<chrono::Utc>> = None;
        loop {
            if let Some(msg) = self
                .provider
                .consume_notification(&self.workflow_id, topic, seq, "DBOS.recv")
                .await?
            {
                return Ok(Some(serde_json::from_value(msg)?));
            }

            // Mailbox empty: fix the durable deadline (first miss only), then
            // poll until a message arrives or the deadline passes.
            let deadline = match deadline {
                Some(d) => d,
                None => *deadline.insert(self.durable_wake_at(deadline_seq, timeout).await?),
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
                    )
                    .await?;
                return Ok(None);
            }
            let remaining = (deadline - now).to_std().unwrap_or(Duration::ZERO);
            self.provider
                .await_change(
                    ChangeWait::Notification {
                        workflow_id: &self.workflow_id,
                        topic,
                    },
                    remaining.min(self.wait_interval()),
                )
                .await;
        }
    }

    /// Publish (or overwrite) the value of event `key` on this workflow.
    /// Recorded as a `DBOS.setEvent` step; other workflows and external code
    /// read it with `get_event` — the natural way to expose progress or a
    /// result to observers:
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
    /// ctx.set_event("status", "shipped".to_string()).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails on a storage error, [`Error::Cancelled`], or
    /// [`Error::UnexpectedStep`] on a divergent replay.
    pub async fn set_event<T: Serialize>(&self, key: &str, value: T) -> Result<()> {
        let seq = self.next_seq();
        if let Some(_done) = self.replay_or_guard::<Value>(seq, "DBOS.setEvent").await? {
            return Ok(());
        }
        self.provider
            .upsert_event(&self.workflow_id, key, serde_json::to_value(value)?)
            .await?;
        self.provider
            .record_step_result(
                &self.workflow_id,
                seq,
                "DBOS.setEvent",
                Value::Null,
                None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Read event `key` of another workflow, waiting up to `timeout` for it to
    /// be set. The value observed is recorded as a `DBOS.getEvent` step, so
    /// replays see the same value even if the event is overwritten later.
    /// Returns `None` on timeout.
    ///
    /// ```no_run
    /// # use durare::{DurableContext, Result};
    /// # use std::time::Duration;
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
    pub async fn get_event<T: DeserializeOwned>(
        &self,
        target_workflow_id: &str,
        key: &str,
        timeout: Duration,
    ) -> Result<Option<T>> {
        let seq = self.next_seq();
        let deadline_seq = self.next_seq();

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
                .get_event_value(target_workflow_id, key)
                .await?
            {
                let outcome = self
                    .provider
                    .record_step_result(&self.workflow_id, seq, "DBOS.getEvent", value, None, None)
                    .await?;
                return Ok(Some(outcome_value(outcome)?));
            }

            let deadline = match deadline {
                Some(d) => d,
                None => *deadline.insert(self.durable_wake_at(deadline_seq, timeout).await?),
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
                    )
                    .await?;
                return Ok(None);
            }
            let remaining = (deadline - now).to_std().unwrap_or(Duration::ZERO);
            self.provider
                .await_change(
                    ChangeWait::Event {
                        workflow_id: target_workflow_id,
                        key,
                    },
                    remaining.min(self.wait_interval()),
                )
                .await;
        }
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
    /// # async fn demo(ctx: DurableContext) -> Result<()> {
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
    pub async fn write_stream<T: Serialize>(&self, key: &str, value: T) -> Result<()> {
        let seq = self.next_seq();
        if self
            .replay_or_guard::<Value>(seq, "DBOS.writeStream")
            .await?
            .is_some()
        {
            return Ok(());
        }
        self.provider
            .write_stream(
                &self.workflow_id,
                key,
                Some(serde_json::to_value(value)?),
                seq,
            )
            .await?;
        self.provider
            .record_step_result(
                &self.workflow_id,
                seq,
                "DBOS.writeStream",
                Value::Null,
                None,
                None,
            )
            .await?;
        Ok(())
    }

    /// Close the durable stream `key` on this workflow, sealing it against
    /// further writes. Recorded as a `DBOS.closeStream` step. A reader draining
    /// the stream observes the close and stops. Writing to a closed stream
    /// errors.
    pub async fn close_stream(&self, key: &str) -> Result<()> {
        let seq = self.next_seq();
        if self
            .replay_or_guard::<Value>(seq, "DBOS.closeStream")
            .await?
            .is_some()
        {
            return Ok(());
        }
        self.provider
            .write_stream(&self.workflow_id, key, None, seq)
            .await?;
        self.provider
            .record_step_result(
                &self.workflow_id,
                seq,
                "DBOS.closeStream",
                Value::Null,
                None,
                None,
            )
            .await?;
        Ok(())
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
    /// # async fn demo(ctx: DurableContext, id: &str) -> Result<()> {
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
    /// step first, and its row is the canonical outcome.
    AlreadyCompleted,
}

/// Turn a recorded step outcome into the typed value a step returns: a recorded
/// output is deserialized; a recorded failure is surfaced as its reconstructed
/// error (so a replayed failed step returns the same error without re-running).
fn outcome_value<T: DeserializeOwned>(outcome: StepOutcome) -> Result<T> {
    Ok(serde_json::from_value(outcome.into_value_result()?)?)
}

/// Await a step's future, converting a panic in the step body into an error so
/// it flows through the normal failure path — retry (per [`StepOptions`]), then
/// checkpoint the failure — instead of unwinding the whole workflow. A step that
/// panics is treated as a failed step, subject to its retry policy.
async fn run_step_catching<T>(name: &str, fut: impl Future<Output = Result<T>>) -> Result<T> {
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(result) => result,
        Err(payload) => Err(Error::app(format!(
            "step `{name}` panicked: {}",
            panic_message(&*payload)
        ))),
    }
}
