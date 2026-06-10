use crate::error::{Error, Result};
use crate::provider::StateProvider;
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Handle passed into every workflow function. It carries the workflow id, the
/// state backend, and a deterministic per-execution step counter.
///
/// All durable operations a workflow performs go through this context:
/// [`DurableContext::step`] for checkpointed work and
/// [`DurableContext::sleep`] for durable timers.
#[derive(Clone)]
pub struct DurableContext {
    workflow_id: String,
    provider: Arc<dyn StateProvider>,
    // Monotonic step index. Because the workflow's control flow is
    // deterministic, the same code path yields the same seq on every replay,
    // which is how we match a step call to its stored checkpoint.
    seq: Arc<AtomicI32>,
}

impl DurableContext {
    pub(crate) fn new(workflow_id: String, provider: Arc<dyn StateProvider>) -> Self {
        Self {
            workflow_id,
            provider,
            seq: Arc::new(AtomicI32::new(0)),
        }
    }

    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    fn next_seq(&self) -> i32 {
        self.seq.fetch_add(1, Ordering::SeqCst)
    }

    /// Run a durable step.
    ///
    /// On the first execution, `f` runs and its result is checkpointed to the
    /// state backend. On any later replay (e.g. after a crash) the stored
    /// result is returned and `f` is **not** run again — so side effects inside
    /// `f` execute at most once per logical step under normal operation.
    ///
    /// `f` is `FnOnce`: it is invoked at most once per call to `step`.
    pub async fn step<T, F, Fut>(&self, name: &str, f: F) -> Result<T>
    where
        T: Serialize + DeserializeOwned,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let seq = self.next_seq();

        // Already done in a previous (possibly crashed) run? Return the checkpoint.
        if let Some(stored) = self.provider.get_step_result(&self.workflow_id, seq).await? {
            return Ok(serde_json::from_value(stored)?);
        }

        // First time: run the user's code, then durably record the result.
        let result = f().await?;
        let json = serde_json::to_value(&result)?;
        let canonical = self
            .provider
            .record_step_result(&self.workflow_id, seq, name, json)
            .await?;

        // `record_step_result` returns the canonical stored value, which may be
        // a value written by a racing execution. Deserialize that so every
        // caller agrees on the same result.
        Ok(serde_json::from_value(canonical)?)
    }

    /// Durably sleep for `dur`.
    ///
    /// The absolute wake time is fixed and persisted on the first call, so the
    /// timer does not drift if the workflow crashes and is replayed: a replay
    /// reads the same wake instant and only waits the *remaining* time.
    ///
    /// NOTE (v0.1): this holds an async task for the remaining duration rather
    /// than evicting the workflow from memory. True scale-to-zero (write the
    /// timer, drop the task, let an external poller re-invoke the workflow when
    /// due) is the next milestone — see README.
    pub async fn sleep(&self, dur: Duration) -> Result<()> {
        let seq = self.next_seq();
        let wake_at = self
            .provider
            .get_or_set_wakeup(&self.workflow_id, seq, dur)
            .await?;

        let now = chrono::Utc::now();
        if wake_at > now {
            let remaining = (wake_at - now).to_std().unwrap_or(Duration::ZERO);
            tokio::time::sleep(remaining).await;
        }
        Ok(())
    }

    /// Escape hatch for building application errors inside steps.
    pub fn err(&self, msg: impl Into<String>) -> Error {
        Error::app(msg)
    }
}
