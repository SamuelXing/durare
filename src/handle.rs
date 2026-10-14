use crate::error::{Error, Result};
use crate::provider::{
    is_terminal, StateProvider, WorkflowStatus, STATUS_CANCELLED, STATUS_ERROR,
    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED,
};
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Options for [`WorkflowHandle::result_with_options`].
///
/// Polling applies to persisted results; local handles await their task directly.
#[derive(Clone, Copy, Debug)]
pub struct WorkflowResultOptions {
    /// Time between database polls. Must be greater than zero.
    /// Defaults to 100 milliseconds.
    pub polling_interval: Duration,
    /// Maximum time to wait, including waiting for a sibling handle's local task.
    /// `None` waits indefinitely. Expiration returns [`Error::Timeout`] without
    /// cancelling the workflow or consuming its local task.
    pub timeout: Option<Duration>,
}

impl Default for WorkflowResultOptions {
    fn default() -> Self {
        Self {
            polling_interval: Duration::from_millis(100),
            timeout: None,
        }
    }
}

/// A reference to a workflow execution.
///
/// Returned by [`start`](crate::DurableEngine::start),
/// [`start_with`](crate::DurableEngine::start_with), and `retrieve_workflow`. It
/// lets the caller observe the workflow's result without blocking the call that
/// started it.
///
/// Await it directly for the typed output, or share it:
///
/// ```no_run
/// # use durare::{DurableEngine, WorkflowHandle, Result};
/// # async fn f(engine: &DurableEngine) -> Result<()> {
/// let handle: WorkflowHandle<i64> = engine.start("add", 1_i64, Default::default()).await?;
/// let observer = handle.clone();          // hand a copy to another task
/// let total: i64 = handle.await?;         // or `handle.result().await?`
/// # let _ = observer;
/// # Ok(())
/// # }
/// ```
///
/// Two flavors, transparent to the caller:
/// - **Local**: started on this process, so the handle holds the task's
///   [`JoinHandle`] and awaits it directly (lowest latency, and a panicking
///   task surfaces as an error rather than hanging).
/// - **Polling**: obtained for a workflow running elsewhere (or already
///   persisted); the result is observed by polling the status row until it
///   reaches a terminal state. This is how DBOS handles cross-process /
///   post-restart waits.
///
/// [`clone`](Clone::clone) is cheap and yields another handle to the *same*
/// workflow. The in-process task can be awaited only once, so whichever clone
/// resolves first consumes it and the rest fall back to polling the persisted
/// status. Every normal outcome — success, error, cancellation, timeout — is
/// written to that status before the task returns, so all clones observe the
/// same result. An interrupted execution (a workflow panic or infrastructure
/// failure) is different: the owning clone returns `RecoveryRequired`, while
/// polling clones wait for recovery to produce a terminal outcome. A polling
/// read or stored-error decoding fault returns `ObservationFailed`; none of
/// these observation failures can become another step's recorded business error.
pub struct WorkflowHandle<O> {
    id: String,
    provider: Arc<dyn StateProvider>,
    // Shared so a handle stays `Clone`. The local task can be awaited only once;
    // a resolver borrows it across the await and only removes it on completion.
    // Cancelling the wait releases the lock while leaving the task available.
    join: Arc<Mutex<Option<JoinHandle<Result<Value>>>>>,
    // `fn() -> O` keeps the handle `Send + Sync` regardless of `O` (it never
    // holds an `O` — the output is deserialized on demand).
    _marker: PhantomData<fn() -> O>,
}

impl<O> Clone for WorkflowHandle<O> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            provider: self.provider.clone(),
            join: self.join.clone(),
            _marker: PhantomData,
        }
    }
}

impl<O> WorkflowHandle<O> {
    /// Handle for a workflow whose task this process owns.
    pub(crate) fn local(
        id: String,
        provider: Arc<dyn StateProvider>,
        join: JoinHandle<Result<Value>>,
    ) -> Self {
        Self {
            id,
            provider,
            join: Arc::new(Mutex::new(Some(join))),
            _marker: PhantomData,
        }
    }

    /// Handle for a workflow this process does not own; results are observed by
    /// polling the state backend.
    pub(crate) fn polling(id: String, provider: Arc<dyn StateProvider>) -> Self {
        Self {
            id,
            provider,
            join: Arc::new(Mutex::new(None)),
            _marker: PhantomData,
        }
    }

    /// The workflow id (its idempotency key).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The current persisted status row.
    pub async fn get_status(&self) -> Result<WorkflowStatus> {
        self.provider
            .get_workflow_status(&self.id)
            .await
            .map_err(crate::execution::observation_error)?
            .ok_or_else(|| Error::UnknownWorkflow(self.id.clone()))
    }
}

impl<O: DeserializeOwned> WorkflowHandle<O> {
    /// Wait for the workflow to finish and return its typed output.
    ///
    /// For a local handle this awaits the in-process task the first time it is
    /// called; for a polling handle (or a clone whose sibling already consumed
    /// the task) it reads the status row every 100 milliseconds until the
    /// workflow reaches a terminal state, then deserializes the stored output.
    ///
    /// A handle can also be awaited directly (`handle.await?`), which consumes
    /// it; `result(&self)` borrows, so it works on a shared or cloned handle.
    ///
    /// # Errors
    ///
    /// The workflow's own error if it finished in `ERROR` (reconstructed from
    /// its checkpoint — a portable error keeps its structure);
    /// [`Error::Cancelled`] if it was cancelled; a decode error if the stored
    /// output does not deserialize as `O`. An interrupted task returns
    /// [`Error::RecoveryRequired`]. A storage read or unreadable error envelope
    /// returns [`Error::ObservationFailed`], which does not establish that the
    /// target stopped; retry the observation instead of recovering the target.
    pub async fn result(&self) -> Result<O> {
        self.result_with_options(WorkflowResultOptions::default())
            .await
    }

    /// Wait for the typed output with a polling interval and optional timeout.
    ///
    /// A timeout only ends this wait; the workflow continues running. This
    /// method and [`result`](Self::result) are cancellation-safe: dropping the
    /// future (including through `tokio::time::timeout`) leaves a pending local
    /// task available to this handle and its clones.
    ///
    /// ```no_run
    /// # use durare::{WorkflowHandle, WorkflowResultOptions, Result};
    /// # use std::time::Duration;
    /// # async fn example(handle: WorkflowHandle<i64>) -> Result<()> {
    /// let result = handle.result_with_options(WorkflowResultOptions {
    ///     polling_interval: Duration::from_millis(20),
    ///     timeout: Some(Duration::from_secs(5)),
    /// }).await?;
    /// # let _ = result;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`Error::Timeout`] when the wait expires, an application error
    /// for a zero polling interval, or the same errors as [`result`](Self::result).
    pub async fn result_with_options(&self, options: WorkflowResultOptions) -> Result<O> {
        if options.polling_interval.is_zero() {
            return Err(Error::app("polling interval must be greater than zero"));
        }
        let wait = self.wait_for_result(options.polling_interval);
        match options.timeout {
            Some(timeout) => tokio::time::timeout(timeout, wait)
                .await
                .map_err(|_| Error::Timeout)?,
            None => wait.await,
        }
    }

    async fn wait_for_result(&self, polling_interval: Duration) -> Result<O> {
        // Await by mutable reference: cancellation must not detach the task.
        // Tokio's mutex permits holding this guard across the await, and a
        // cancelled waiter releases it so another clone can finish the join.
        let mut owned = self.join.lock().await;
        if let Some(join) = owned.as_mut() {
            let result = join.await;
            owned.take();
            drop(owned);
            return match result {
                Ok(res) => {
                    let value = res?;
                    Ok(serde_json::from_value(value)?)
                }
                Err(e) => Err(crate::execution::recovery_error(Error::app(format!(
                    "workflow task failed: {e}"
                )))),
            };
        }
        drop(owned);

        // No task to await: poll the status row until terminal. A polling handle
        // may name a workflow that does not exist yet (e.g. a debounced run the
        // collector has not started); treat that as "not ready" and keep waiting.
        loop {
            match self.get_status().await {
                Ok(status) if is_terminal(&status.status) => {
                    return self.terminal_to_result(status);
                }
                Ok(_) | Err(Error::UnknownWorkflow(_)) => {}
                Err(e) => return Err(e),
            }
            tokio::time::sleep(polling_interval).await;
        }
    }

    /// Convert a terminal status row into a typed `Result`.
    fn terminal_to_result(&self, status: WorkflowStatus) -> Result<O> {
        match status.status.as_str() {
            STATUS_CANCELLED => Err(Error::Cancelled(self.id.clone())),
            // A workflow parked past its recovery-attempt cap has no result;
            // surface the typed error instead of trying to decode a null output
            // (which for a unit-typed workflow would spuriously succeed).
            STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED => {
                Err(Error::MaxRecoveryAttemptsExceeded(self.id.clone()))
            }
            // Use the same decoder as initial completion and step replay.
            STATUS_ERROR => Err(crate::recorded_error::try_from_parts(
                status
                    .error
                    .unwrap_or_else(|| "workflow failed".to_string()),
                status.error_info,
            )
            .map_err(crate::execution::observation_error)?),
            _ => {
                let output = status.output.unwrap_or(Value::Null);
                Ok(serde_json::from_value(output)?)
            }
        }
    }
}

/// `handle.await` resolves to the workflow's typed output — sugar for
/// [`result`](WorkflowHandle::result) that consumes the handle.
impl<O: DeserializeOwned + 'static> IntoFuture for WorkflowHandle<O> {
    type Output = Result<O>;
    type IntoFuture = Pin<Box<dyn Future<Output = Result<O>> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.result().await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryProvider;

    // A panic leaves no persisted terminal row, so resolving it after an
    // interrupted wait proves that the local join was retained rather than
    // silently switching to database polling.
    #[tokio::test]
    async fn interrupted_wait_preserves_local_join() {
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let handle = WorkflowHandle::<i64>::local(
            "panic".into(),
            Arc::new(InMemoryProvider::new()),
            tokio::spawn(async move {
                gate.await.unwrap();
                panic!("test workflow panic");
            }),
        );
        let observer = handle.clone();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), handle.result())
                .await
                .is_err()
        );
        release.send(()).unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), observer.result())
            .await
            .expect("local join was lost")
            .unwrap_err();
        assert!(matches!(error, Error::RecoveryRequired(_)));
    }

    #[tokio::test]
    async fn configured_timeout_preserves_local_join() {
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let handle = WorkflowHandle::<i64>::local(
            "local".into(),
            Arc::new(InMemoryProvider::new()),
            tokio::spawn(async move {
                gate.await.unwrap();
                Ok(Value::from(42))
            }),
        );
        assert!(matches!(
            handle
                .result_with_options(WorkflowResultOptions {
                    timeout: Some(Duration::from_millis(10)),
                    ..Default::default()
                })
                .await,
            Err(Error::Timeout)
        ));
        release.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), handle.clone().result())
                .await
                .expect("local join was lost")
                .unwrap(),
            42
        );
    }

    #[tokio::test]
    async fn timeout_covers_waiting_for_another_clone() {
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let handle = WorkflowHandle::<i64>::local(
            "local".into(),
            Arc::new(InMemoryProvider::new()),
            tokio::spawn(async move {
                gate.await.unwrap();
                Ok(Value::from(42))
            }),
        );
        let owner = handle.result();
        tokio::pin!(owner);
        assert!(futures_util::poll!(&mut owner).is_pending());
        assert!(matches!(
            handle
                .clone()
                .result_with_options(WorkflowResultOptions {
                    timeout: Some(Duration::from_millis(10)),
                    ..Default::default()
                })
                .await,
            Err(Error::Timeout)
        ));
        release.send(()).unwrap();
        assert_eq!(owner.await.unwrap(), 42);
    }
}
