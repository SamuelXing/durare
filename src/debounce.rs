//! Workflow **debouncing**: coalesce rapid repeated triggers of a workflow into
//! a single delayed run.
//!
//! Each [`Debouncer::debounce`] call (re)schedules the target workflow `delay`
//! into the future and replaces its input with the latest one. As long as new
//! calls keep arriving within `delay` of each other, the run keeps getting
//! pushed back, up to an optional `timeout` from the first call, and only the
//! final input runs. Calls are grouped by `key`, so independent groups debounce
//! independently.
//!
//! The debounced run is an ordinary queued workflow waiting `DELAYED`. It holds
//! its debounce key, `{workflow}-{key}`, as its queue deduplication id and is
//! marked `is_debounced`. Configured instances use `{workflow}/{config}-{key}`
//! so separate handlers never coalesce. A bounce atomically checks the target
//! identity, pushes its wake instant out (never past the deadline), and replaces
//! its input. When the
//! dispatcher promotes the run to `ENQUEUED` it releases the key, so a later
//! call under the same key starts a fresh run. The Go and Python SDKs use the
//! same rows, so a durare debounce and a peer's debounce of the same workflow
//! and key coalesce into one run.

use crate::client::Client;
use crate::context::DurableContext;
use crate::engine::{DeduplicationPolicy, DurableEngine, WorkflowOptions, INTERNAL_QUEUE};
use crate::error::{Error, ErrorCode, Result};
use crate::handle::WorkflowHandle;
use crate::provider::{DebounceOutcome, DebounceRequest, StateProvider, WorkflowStatus};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

/// How long a bounce waits before trying again when the key's holder is a
/// debounced run of the same workflow that left `DELAYED` between the bounce
/// and the lookup: the promotion that releases the key is a moment away.
const RETRY_INTERVAL: Duration = Duration::from_millis(100);

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// Options a debounce owns or cannot honour are refused rather than silently
/// overridden, as the Go and Python SDKs refuse them.
fn reject_conflicting_options(opts: &WorkflowOptions) -> Result<()> {
    if opts.dedup_id.is_some() {
        return Err(Error::app(
            "cannot debounce a workflow with a deduplication id set: the debounce key is \
             the workflow's deduplication id",
        ));
    }
    if opts.delay.is_some() {
        return Err(Error::app(
            "cannot debounce a workflow with a delay set: the debounce period controls \
             the workflow's delay",
        ));
    }
    if opts.partition_key.is_some() {
        return Err(Error::app(
            "cannot debounce a workflow with a queue partition key set: partitioned \
             queues do not support deduplication, which debouncing requires",
        ));
    }
    if opts.dedup_policy != DeduplicationPolicy::Reject {
        return Err(Error::app(
            "cannot debounce a workflow with a deduplication policy set: a debounce owns \
             the deduplication behaviour",
        ));
    }
    Ok(())
}

/// What marks a fresh run as a debounce: the flag, and the furthest instant a
/// bounce may push its wake to.
fn mark_debounced(deadline_ms: Option<i64>) -> impl FnOnce(&mut WorkflowStatus) {
    move |row| {
        row.is_debounced = true;
        row.debounce_deadline_ms = deadline_ms;
    }
}

/// The producer side of a debounce, implemented by both [`DurableEngine`]
/// (in-process) and [`Client`] (out-of-process): a debounce is a bounce on the
/// provider plus, when nothing holds the key, one enqueue, so a producer with
/// no executor of its own debounces just as well as an engine.
#[allow(async_fn_in_trait)] // crate-internal, only ever used with a concrete backend
pub(crate) trait DebounceBackend {
    fn provider(&self) -> &Arc<dyn StateProvider>;
    /// Enqueue a fresh debounced run of `target`. `opts` carries the queue, the
    /// debounce key as its deduplication id, and the delay; the row is marked
    /// debounced with `deadline_ms` as the furthest a bounce may push it.
    /// Returns the run's id.
    async fn enqueue_debounced(
        &self,
        target: &str,
        input: Value,
        opts: WorkflowOptions,
        deadline_ms: Option<i64>,
    ) -> Result<String>;
}

impl DebounceBackend for DurableEngine {
    fn provider(&self) -> &Arc<dyn StateProvider> {
        DurableEngine::provider(self)
    }
    async fn enqueue_debounced(
        &self,
        target: &str,
        input: Value,
        opts: WorkflowOptions,
        deadline_ms: Option<i64>,
    ) -> Result<String> {
        self.enqueue_stamped(target, input, &opts, mark_debounced(deadline_ms))
            .await
    }
}

impl DebounceBackend for Client {
    fn provider(&self) -> &Arc<dyn StateProvider> {
        Client::provider(self)
    }
    async fn enqueue_debounced(
        &self,
        target: &str,
        input: Value,
        opts: WorkflowOptions,
        deadline_ms: Option<i64>,
    ) -> Result<String> {
        let queue = opts
            .queue
            .clone()
            .expect("the debounce loop sets the queue");
        let handle: WorkflowHandle<Value> = self
            .enqueue_with(&queue, target, input, opts, mark_debounced(deadline_ms))
            .await?;
        Ok(handle.id().to_string())
    }
}

/// Schedule (or push back) the target's run for `key`, over any
/// [`DebounceBackend`]. Returns the id of the run every producer for this
/// active `key` is handed.
pub(crate) async fn run_debounce<B: DebounceBackend>(
    backend: &B,
    target: &str,
    timeout: Duration,
    opts: &WorkflowOptions,
    key: &str,
    delay: Duration,
    input: Value,
) -> Result<String> {
    reject_conflicting_options(opts)?;
    let queue = opts
        .queue
        .clone()
        .unwrap_or_else(|| INTERNAL_QUEUE.to_string());
    let prefix = match opts.config_name.as_deref().filter(|s| !s.is_empty()) {
        Some(config) => format!("{target}/{config}"),
        None => target.to_string(),
    };
    let dedup_id = format!("{prefix}-{key}");
    let delay_ms = delay.as_millis() as i64;
    // The first wait never outlasts the cap either.
    let first_wait = if timeout.is_zero() {
        delay
    } else {
        delay.min(timeout)
    };
    let timeout_ms = (!timeout.is_zero()).then_some(timeout.as_millis() as i64);

    loop {
        let now = now_ms();
        let outcome = backend
            .provider()
            .debounce_delayed_workflow(&DebounceRequest {
                workflow_name: target,
                class_name: opts.class_name.as_deref().filter(|s| !s.is_empty()),
                config_name: opts.config_name.as_deref().filter(|s| !s.is_empty()),
                queue_name: &queue,
                dedup_id: &dedup_id,
                delay_until_ms: now + delay_ms,
                input: &input,
            })
            .await?;
        match outcome {
            DebounceOutcome::Bounced(id) => return Ok(id),
            // A run that is not debounced holds the key, or a debounce of
            // another workflow collides with ours (`"a"` + `"b-c"` and
            // `"a-b"` + `"c"` share a key): the caller's deduplication conflict.
            DebounceOutcome::Held {
                workflow_name,
                is_debounced,
                class_name,
                config_name,
            } if !is_debounced
                || workflow_name != target
                || class_name.as_deref().filter(|s| !s.is_empty())
                    != opts.class_name.as_deref().filter(|s| !s.is_empty())
                || config_name.as_deref().filter(|s| !s.is_empty())
                    != opts.config_name.as_deref().filter(|s| !s.is_empty()) =>
            {
                return Err(Error::queue_deduplicated(queue, dedup_id));
            }
            // Our own debounced run left DELAYED between the bounce and the
            // lookup; its promotion releases the key.
            DebounceOutcome::Held { .. } => tokio::time::sleep(RETRY_INTERVAL).await,
            DebounceOutcome::Unheld => {
                let mut run = opts.clone();
                run.queue = Some(queue.clone());
                run.dedup_id = Some(dedup_id.clone());
                run.delay = Some(first_wait);
                let deadline_ms = timeout_ms.map(|t| now + t);
                match backend
                    .enqueue_debounced(target, input.clone(), run, deadline_ms)
                    .await
                {
                    Ok(id) => return Ok(id),
                    // Another producer took the key first: bounce its run.
                    Err(e) if e.code() == ErrorCode::QueueDeduplicated => continue,
                    Err(e) => return Err(e),
                }
            }
        }
    }
}

/// Debounces a target workflow. Create one with
/// [`DurableEngine::debouncer`](crate::DurableEngine::debouncer).
pub struct Debouncer<'e> {
    engine: &'e DurableEngine,
    target: String,
    timeout: Duration,
    options: WorkflowOptions,
}

impl<'e> Debouncer<'e> {
    pub(crate) fn new(engine: &'e DurableEngine, target: impl Into<String>) -> Self {
        Debouncer {
            engine,
            target: target.into(),
            timeout: Duration::ZERO,
            options: WorkflowOptions::default(),
        }
    }

    /// Cap how far the start time can be pushed back from the first call.
    /// `Duration::ZERO` (the default) means no cap.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Run the debounced target with these [`WorkflowOptions`]: its queue (the
    /// internal queue when unset), priority, application version, timeout,
    /// attributes, class and config name, authenticated identity, and the id
    /// of a fresh run. The debounce owns the run's deduplication id and delay,
    /// so `dedup_id`, `delay`, a partition key or a deduplication policy are
    /// refused by [`debounce`](Self::debounce). The run is queued, so an
    /// application version override routes it to executors on that version, as
    /// it does any enqueue.
    pub fn options(mut self, opts: WorkflowOptions) -> Self {
        self.options = opts;
        self
    }

    /// Schedule (or push back) the target's run for `key`, with the latest
    /// `input`. Returns a handle to the single eventual run; every call for the
    /// same active `key` returns a handle to the *same* run.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownWorkflow`] if the target is not registered on this
    /// engine, [`Error::UnknownQueue`] for an unknown queue in the options,
    /// [`Error::QueueDeduplicated`] when the debounce key is held by a run
    /// that is not a debounce of this workflow, and an application error for
    /// an option the debounce owns.
    pub async fn debounce<I, O>(
        &self,
        key: &str,
        delay: Duration,
        input: I,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        self.engine.check_run_target(&self.target, &self.options)?;
        let input_val = serde_json::to_value(input)?;
        let target_id = run_debounce(
            self.engine,
            &self.target,
            self.timeout,
            &self.options,
            key,
            delay,
            input_val,
        )
        .await?;
        Ok(WorkflowHandle::polling(
            target_id,
            self.engine.provider().clone(),
        ))
    }
}

/// Debounces a target workflow from an out-of-process [`Client`]. Create one with
/// [`Client::debouncer`]. Identical to [`Debouncer`] except the producer runs
/// outside any engine, so the target is not checked against a registry: a
/// launched engine with the target registered executes the coalesced run.
pub struct DebouncerClient<'c> {
    client: &'c Client,
    target: String,
    timeout: Duration,
    options: WorkflowOptions,
}

impl<'c> DebouncerClient<'c> {
    pub(crate) fn new(client: &'c Client, target: impl Into<String>) -> Self {
        DebouncerClient {
            client,
            target: target.into(),
            timeout: Duration::ZERO,
            options: WorkflowOptions::default(),
        }
    }

    /// Cap how far the start time can be pushed back from the first call.
    /// `Duration::ZERO` (the default) means no cap.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Run the debounced target with these [`WorkflowOptions`]; see
    /// [`Debouncer::options`].
    pub fn options(mut self, opts: WorkflowOptions) -> Self {
        self.options = opts;
        self
    }

    /// Schedule (or push back) the target's run for `key`, with the latest
    /// `input`. Returns a handle to the single eventual run; every call for the
    /// same active `key` returns a handle to the *same* run.
    ///
    /// # Errors
    ///
    /// [`Error::QueueDeduplicated`] when the debounce key is held by a run that
    /// is not a debounce of this workflow, and an application error for an
    /// option the debounce owns.
    pub async fn debounce<I, O>(
        &self,
        key: &str,
        delay: Duration,
        input: I,
    ) -> Result<WorkflowHandle<O>>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let input_val = serde_json::to_value(input)?;
        let target_id = run_debounce(
            self.client,
            &self.target,
            self.timeout,
            &self.options,
            key,
            delay,
            input_val,
        )
        .await?;
        Ok(WorkflowHandle::polling(
            target_id,
            self.client.provider().clone(),
        ))
    }
}

// ---------------------------------------------------------------------------
// The collector workflow debounces used to run through. Nothing creates one
// any more; the registration stays for one release so a collector a 0.5
// engine enqueued finishes its coalesced run after an upgrade instead of
// parking as an unknown workflow.
// ---------------------------------------------------------------------------

/// Registered name of the retired collector workflow.
pub(crate) const DEBOUNCER_WF: &str = "_dbos_debouncer";
/// Topic the collector receives pushed-back inputs on.
const DEBOUNCER_TOPIC: &str = "_dbos_debouncer_topic";

/// The options the collector carries to its target.
#[derive(Clone, Default, Deserialize)]
pub(crate) struct TargetOptions {
    queue: Option<String>,
    priority: i32,
    partition_key: Option<String>,
    app_version: Option<String>,
    timeout_ms: Option<i64>,
    authenticated_user: Option<String>,
    assumed_role: Option<String>,
    authenticated_roles: Vec<String>,
}

impl TargetOptions {
    /// Rebuild a [`WorkflowOptions`] for the target, pinned to `target_id`.
    fn into_options(self, target_id: &str) -> WorkflowOptions {
        let mut opts = WorkflowOptions::with_id(target_id);
        opts.queue = self.queue;
        opts.priority = self.priority;
        opts.partition_key = self.partition_key;
        opts.app_version = self.app_version;
        opts.timeout = self.timeout_ms.map(|ms| Duration::from_millis(ms as u64));
        opts.authenticated_user = self.authenticated_user;
        opts.assumed_role = self.assumed_role;
        opts.authenticated_roles = self.authenticated_roles;
        opts
    }
}

/// Input of the retired collector workflow.
#[derive(Clone, Deserialize)]
pub(crate) struct DebouncerInput {
    target_name: String,
    target_id: String,
    delay_ms: i64,
    timeout_ms: i64,
    initial_input: Value,
    #[serde(default)]
    target_opts: TargetOptions,
}

/// A pushed-back input sent to a running collector.
#[derive(Deserialize)]
struct DebounceMessage {
    input: Value,
    delay_ms: i64,
    /// ACK id: the collector sets an event under this key once it takes the
    /// message, so the producer knows it landed.
    id: String,
}

/// The retired collector: collect pushed-back inputs until the delay elapses
/// with no new input, then start the target once with the latest input.
pub(crate) async fn internal_debouncer(ctx: DurableContext, input: DebouncerInput) -> Result<()> {
    let start = ctx
        .step("DBOS.debounce.startTime", || async {
            Ok::<_, Error>(now_ms())
        })
        .await?;
    let mut current_input = input.initial_input.clone();
    let max_ms = start + input.timeout_ms;
    let cap = |t: i64| {
        if input.timeout_ms > 0 && t > max_ms {
            max_ms
        } else {
            t
        }
    };
    let mut target_ms = cap(start + input.delay_ms);

    loop {
        let now = ctx
            .step("DBOS.debounce.loopTime", || async {
                Ok::<_, Error>(now_ms())
            })
            .await?;
        let remaining = target_ms - now;
        if remaining <= 0 {
            break;
        }
        let Some(msg): Option<DebounceMessage> = ctx
            .recv(DEBOUNCER_TOPIC, Duration::from_millis(remaining as u64))
            .await?
        else {
            break;
        };
        current_input = msg.input;
        target_ms = cap(now + msg.delay_ms);
        if !msg.id.is_empty() {
            ctx.set_event(&msg.id, true).await?;
        }
    }

    let opts = input.target_opts.into_options(&input.target_id);
    ctx.start_workflow::<Value, Value>(&input.target_name, current_input, opts)
        .await?;
    Ok(())
}

#[cfg(test)]
mod retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A backend whose enqueue keeps losing the key to another producer whose
    /// run then vanishes before the bounce sees it.
    struct RacingBackend {
        provider: Arc<dyn StateProvider>,
        attempts: AtomicUsize,
    }
    impl DebounceBackend for RacingBackend {
        fn provider(&self) -> &Arc<dyn StateProvider> {
            &self.provider
        }
        async fn enqueue_debounced(
            &self,
            _: &str,
            _: Value,
            opts: WorkflowOptions,
            _: Option<i64>,
        ) -> Result<String> {
            if self.attempts.fetch_add(1, Ordering::SeqCst) < 12 {
                Err(Error::queue_deduplicated(
                    opts.queue.unwrap(),
                    opts.dedup_id.unwrap(),
                ))
            } else {
                Ok("fresh".into())
            }
        }
    }

    #[tokio::test]
    async fn a_lost_enqueue_race_retries_without_a_budget() -> Result<()> {
        let backend = RacingBackend {
            provider: Arc::new(crate::InMemoryProvider::new()),
            attempts: AtomicUsize::new(0),
        };
        let id = run_debounce(
            &backend,
            "target",
            Duration::ZERO,
            &WorkflowOptions::default(),
            "key",
            Duration::ZERO,
            Value::Null,
        )
        .await?;
        assert_eq!(id, "fresh");
        assert_eq!(backend.attempts.load(Ordering::SeqCst), 13);
        Ok(())
    }

    #[tokio::test]
    async fn owned_options_are_refused() {
        let base = WorkflowOptions::default();
        for opts in [
            WorkflowOptions {
                dedup_id: Some("k".into()),
                ..base.clone()
            },
            WorkflowOptions {
                delay: Some(Duration::from_secs(1)),
                ..base.clone()
            },
            WorkflowOptions {
                partition_key: Some("p".into()),
                ..base.clone()
            },
            WorkflowOptions {
                dedup_policy: DeduplicationPolicy::ReturnExisting,
                ..base.clone()
            },
        ] {
            assert!(reject_conflicting_options(&opts).is_err());
        }
        assert!(reject_conflicting_options(&base).is_ok());
    }
}
