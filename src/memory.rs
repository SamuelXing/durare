use crate::error::Result;
use crate::provider::{StateProvider, WorkflowRecord, STATUS_COMPLETED, STATUS_FAILED, STATUS_PENDING};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Clone)]
struct WfRow {
    name: String,
    input: Value,
    status: String,
}

#[derive(Default)]
struct Inner {
    workflows: HashMap<String, WfRow>,
    steps: HashMap<(String, i32), Value>,
    timers: HashMap<(String, i32), DateTime<Utc>>,
}

/// In-memory [`StateProvider`] for tests and quick starts (no database needed).
///
/// State lives only in this process, so it demonstrates step idempotency and
/// in-process recovery, but NOT crash-recovery across process restarts — for
/// that, use [`crate::PostgresProvider`].
#[derive(Default)]
pub struct InMemoryProvider {
    inner: Mutex<Inner>,
}

impl InMemoryProvider {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateProvider for InMemoryProvider {
    async fn init(&self) -> Result<()> {
        Ok(())
    }

    async fn start_workflow(&self, id: &str, name: &str, input: &Value) -> Result<WorkflowRecord> {
        let mut g = self.inner.lock().await;
        let row = g
            .workflows
            .entry(id.to_string())
            .or_insert_with(|| WfRow {
                name: name.to_string(),
                input: input.clone(),
                status: STATUS_PENDING.to_string(),
            })
            .clone();
        Ok(WorkflowRecord {
            id: id.to_string(),
            name: row.name,
            input: row.input,
            status: row.status,
        })
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let g = self.inner.lock().await;
        Ok(g.steps.get(&(workflow_id.to_string(), seq)).cloned())
    }

    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        _name: &str,
        value: Value,
    ) -> Result<Value> {
        let mut g = self.inner.lock().await;
        let canonical = g
            .steps
            .entry((workflow_id.to_string(), seq))
            .or_insert(value)
            .clone();
        Ok(canonical)
    }

    async fn get_or_set_wakeup(
        &self,
        workflow_id: &str,
        seq: i32,
        dur: Duration,
    ) -> Result<DateTime<Utc>> {
        let proposed = Utc::now()
            + chrono::Duration::from_std(dur).unwrap_or_else(|_| chrono::Duration::zero());
        let mut g = self.inner.lock().await;
        let wake = *g
            .timers
            .entry((workflow_id.to_string(), seq))
            .or_insert(proposed);
        Ok(wake)
    }

    async fn complete_workflow(&self, id: &str, _output: &Value) -> Result<()> {
        let mut g = self.inner.lock().await;
        if let Some(row) = g.workflows.get_mut(id) {
            row.status = STATUS_COMPLETED.to_string();
        }
        Ok(())
    }

    async fn fail_workflow(&self, id: &str, _error: &str) -> Result<()> {
        let mut g = self.inner.lock().await;
        if let Some(row) = g.workflows.get_mut(id) {
            row.status = STATUS_FAILED.to_string();
        }
        Ok(())
    }

    async fn list_incomplete_workflows(&self) -> Result<Vec<WorkflowRecord>> {
        let g = self.inner.lock().await;
        Ok(g.workflows
            .iter()
            .filter(|(_, r)| r.status == STATUS_PENDING)
            .map(|(id, r)| WorkflowRecord {
                id: id.clone(),
                name: r.name.clone(),
                input: r.input.clone(),
                status: r.status.clone(),
            })
            .collect())
    }
}
