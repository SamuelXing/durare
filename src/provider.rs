use crate::error::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::time::Duration;

/// Terminal and non-terminal states a workflow instance can be in.
pub const STATUS_PENDING: &str = "PENDING";
pub const STATUS_COMPLETED: &str = "COMPLETED";
pub const STATUS_FAILED: &str = "FAILED";

/// A persisted workflow instance row.
#[derive(Clone, Debug)]
pub struct WorkflowRecord {
    pub id: String,
    pub name: String,
    pub input: Value,
    pub status: String,
}

/// The pluggable durable-state backend.
///
/// This is the single seam that decouples the runtime from storage. The v0.1
/// ships a Postgres implementation and an in-memory one; a DynamoDB / Aurora
/// DSQL implementation can be added later **without touching the engine** —
/// that is the whole point of this trait.
///
/// Every method must be **idempotent** with respect to its keys, because the
/// engine may re-run a workflow after a crash and replay completed steps.
#[async_trait]
pub trait StateProvider: Send + Sync {
    /// Create tables / indexes if they do not yet exist.
    async fn init(&self) -> Result<()>;

    /// Idempotently create a workflow instance. If `id` already exists, the
    /// existing row is returned unchanged (so a re-submitted workflow id is a
    /// no-op, not a duplicate).
    async fn start_workflow(&self, id: &str, name: &str, input: &Value) -> Result<WorkflowRecord>;

    /// Return a previously checkpointed step result, if any.
    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>>;

    /// Idempotently record a step result keyed by `(workflow_id, seq)`.
    ///
    /// Returns the **canonical** stored value: if a concurrent/duplicate
    /// execution already wrote this step, the previously-stored value wins and
    /// is returned, guaranteeing every caller observes the same result.
    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
    ) -> Result<Value>;

    /// Idempotently resolve the wake time for a durable sleep keyed by
    /// `(workflow_id, seq)`. The first call fixes `now + dur`; later calls
    /// (e.g. after a crash) return the *same* absolute instant so timers do
    /// not drift across replays.
    async fn get_or_set_wakeup(
        &self,
        workflow_id: &str,
        seq: i32,
        dur: Duration,
    ) -> Result<DateTime<Utc>>;

    /// Mark a workflow COMPLETED with its output.
    async fn complete_workflow(&self, id: &str, output: &Value) -> Result<()>;

    /// Mark a workflow FAILED with an error message.
    async fn fail_workflow(&self, id: &str, error: &str) -> Result<()>;

    /// All workflows that are not in a terminal state — the recovery set.
    async fn list_incomplete_workflows(&self) -> Result<Vec<WorkflowRecord>>;
}
