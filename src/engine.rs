use crate::context::DurableContext;
use crate::error::{Error, Result};
use crate::provider::{StateProvider, WorkflowRecord};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A type-erased workflow handler: takes a context + JSON input, returns JSON output.
pub type WorkflowFn = Arc<
    dyn Fn(DurableContext, Value) -> Pin<Box<dyn Future<Output = Result<Value>> + Send>>
        + Send
        + Sync,
>;

/// Erase a typed `async fn(DurableContext, Input) -> Result<Output>` into the
/// JSON-in / JSON-out [`WorkflowFn`] the engine stores.
///
/// This is the single place input/output (de)serialization happens. Both
/// [`DurableEngine::register`] and the `#[durust::workflow]` macro funnel
/// through it, so the manual and auto-registered paths behave identically.
pub fn erase<I, O, F, Fut>(f: F) -> WorkflowFn
where
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(DurableContext, I) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    let f = Arc::new(f);
    Arc::new(move |ctx, input_json| {
        let f = f.clone();
        Box::pin(async move {
            let input: I = serde_json::from_value(input_json)?;
            let output: O = f(ctx, input).await?;
            Ok(serde_json::to_value(output)?)
        })
    })
}

/// A compile-time workflow registration emitted by `#[durust::workflow]`.
///
/// Collected via the `inventory` crate: every annotated workflow in the binary
/// submits one of these, and [`DurableEngine::new`] iterates them so no manual
/// `register` call is needed.
pub struct WorkflowRegistration {
    /// The name the workflow is registered (and persisted) under.
    pub name: &'static str,
    /// Builds the type-erased handler. Typically `|| durust::erase(my_fn)`.
    pub builder: fn() -> WorkflowFn,
}

inventory::collect!(WorkflowRegistration);

/// The durable execution engine.
///
/// Holds the state backend and a registry of workflow functions by name. There
/// is no separate server process: the engine is a library that lives in your
/// worker and talks directly to the [`StateProvider`].
pub struct DurableEngine {
    provider: Arc<dyn StateProvider>,
    workflows: HashMap<String, WorkflowFn>,
}

impl DurableEngine {
    /// Create an engine and initialize the backend schema.
    ///
    /// Every workflow annotated with `#[durust::workflow]` anywhere in the
    /// binary is auto-registered here (via `inventory`), so the common case
    /// needs no `register` call. You can still [`register`](Self::register)
    /// extra workflows or override an auto-registered name afterwards.
    pub async fn new(provider: Arc<dyn StateProvider>) -> Result<Self> {
        provider.init().await?;
        let mut workflows = HashMap::new();
        for reg in inventory::iter::<WorkflowRegistration> {
            workflows.insert(reg.name.to_string(), (reg.builder)());
        }
        Ok(Self {
            provider,
            workflows,
        })
    }

    /// Register a workflow under `name`.
    ///
    /// The handler is a plain async function `(DurableContext, Input) -> Result<Output>`.
    /// `Input` and `Output` only need to be serde-serializable.
    pub fn register<I, O, F, Fut>(&mut self, name: &str, f: F)
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(DurableContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.workflows.insert(name.to_string(), erase(f));
    }

    /// Start (or resume) a workflow instance under a caller-chosen `id`.
    ///
    /// `id` is the idempotency key: starting the same `id` twice runs the
    /// workflow once. The call returns the workflow's JSON output; use
    /// [`DurableEngine::start_typed`] to get a deserialized value.
    pub async fn start<I>(&self, name: &str, id: &str, input: I) -> Result<Value>
    where
        I: Serialize,
    {
        let input_json = serde_json::to_value(input)?;
        let record = self
            .provider
            .start_workflow(id, name, &input_json)
            .await?;
        self.execute(&record).await
    }

    /// Like [`start`](Self::start) but deserializes the output into `O`.
    pub async fn start_typed<I, O>(&self, name: &str, id: &str, input: I) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let out = self.start(name, id, input).await?;
        Ok(serde_json::from_value(out)?)
    }

    /// Re-run every workflow that is not in a terminal state. Completed steps
    /// are served from their checkpoints, so recovery resumes exactly where the
    /// previous run left off. Call this once on worker startup.
    ///
    /// Returns the number of workflows that were resumed.
    pub async fn recover(&self) -> Result<usize> {
        let pending = self.provider.list_incomplete_workflows().await?;
        let mut resumed = 0;
        for record in pending {
            if self.workflows.contains_key(&record.name) {
                // Best-effort: a workflow that fails again is marked FAILED by
                // `execute`; we keep going with the rest.
                let _ = self.execute(&record).await;
                resumed += 1;
            } else {
                tracing::warn!(
                    workflow = %record.name,
                    id = %record.id,
                    "skipping recovery: no handler registered for this workflow name"
                );
            }
        }
        Ok(resumed)
    }

    /// Run a workflow instance to completion, recording the terminal state.
    async fn execute(&self, record: &WorkflowRecord) -> Result<Value> {
        let handler = self
            .workflows
            .get(&record.name)
            .cloned()
            .ok_or_else(|| Error::UnknownWorkflow(record.name.clone()))?;

        let ctx = DurableContext::new(record.id.clone(), self.provider.clone());

        match handler(ctx, record.input.clone()).await {
            Ok(output) => {
                self.provider.complete_workflow(&record.id, &output).await?;
                Ok(output)
            }
            Err(e) => {
                self.provider
                    .fail_workflow(&record.id, &e.to_string())
                    .await?;
                Err(e)
            }
        }
    }
}
