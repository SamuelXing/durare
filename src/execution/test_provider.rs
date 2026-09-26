//! Provider fault injection at the actual read/write boundary.
use crate::provider::{NotificationInfo, RecordedStep, StepOutcome};
use crate::*;
use async_trait::async_trait;
use serde_json::{Map, Value};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Fault {
    Read,
    WriteBefore,
    WriteAfter,
    TerminalBefore,
    TerminalAfter,
    TransactionBefore,
    TransactionAfter,
    Corrupt,
    ChildBefore,
    ChildAfter,
}

pub struct FaultProvider {
    pub inner: Arc<dyn StateProvider>,
    pub fault: Mutex<Option<Fault>>,
}
impl FaultProvider {
    pub fn new(inner: Arc<dyn StateProvider>, fault: Fault) -> Self {
        Self {
            inner,
            fault: Mutex::new(Some(fault)),
        }
    }
    fn take(&self, fault: Fault) -> bool {
        let mut armed = self.fault.lock().unwrap();
        if *armed == Some(fault) {
            *armed = None;
            true
        } else {
            false
        }
    }
    fn failure<T>() -> Result<T> {
        Err(Error::Db(sqlx::Error::PoolTimedOut))
    }
}

#[async_trait]
impl StateProvider for FaultProvider {
    fn serializer(&self) -> Serializer {
        self.inner.serializer()
    }
    async fn init(&self) -> Result<()> {
        self.inner.init().await
    }
    async fn insert_workflow_status(
        &self,
        status: WorkflowStatus,
    ) -> Result<(WorkflowStatus, bool)> {
        let child = status.parent_workflow_id.is_some();
        if child && self.take(Fault::ChildBefore) {
            return Self::failure();
        }
        let result = self.inner.insert_workflow_status(status).await?;
        if child && self.take(Fault::ChildAfter) {
            return Self::failure();
        }
        Ok(result)
    }
    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        dedup_id: &str,
    ) -> Result<Option<String>> {
        self.inner
            .get_deduplicated_workflow(queue_name, dedup_id)
            .await
    }
    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>> {
        self.inner.get_workflow_status(id).await
    }
    async fn set_workflow_status(
        &self,
        id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<bool> {
        if self.take(Fault::TerminalBefore) {
            return Self::failure();
        }
        let result = self
            .inner
            .set_workflow_status(id, status, output, error)
            .await?;
        if self.take(Fault::TerminalAfter) {
            return Self::failure();
        }
        Ok(result)
    }
    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<RecordedStep>> {
        if self.take(Fault::Corrupt) {
            return Ok(Some(RecordedStep {
                name: "effect".into(),
                outcome: StepOutcome::Failure {
                    message: "unreadable record".into(),
                    info: Some(PortableWorkflowError {
                        name: "durare.RecordedError".into(),
                        message: "unreadable record".into(),
                        code: None,
                        data: Some(serde_json::json!({"version": 999})),
                    }),
                },
            }));
        }
        if self.take(Fault::Read) {
            return Self::failure();
        }
        self.inner.get_step_result(workflow_id, seq).await
    }
    #[allow(clippy::too_many_arguments)]
    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
        error: Option<&str>,
        started_at_ms: Option<i64>,
        executor_id: Option<&str>,
    ) -> Result<StepOutcome> {
        if self.take(Fault::WriteBefore) {
            return Self::failure();
        }
        let result = self
            .inner
            .record_step_result(
                workflow_id,
                seq,
                name,
                value,
                error,
                started_at_ms,
                executor_id,
            )
            .await?;
        if self.take(Fault::WriteAfter) {
            return Self::failure();
        }
        Ok(result)
    }
    async fn run_transaction_step(
        &self,
        workflow_id: &str,
        seq: i32,
        started_at_ms: i64,
        opts: &TransactionOptions,
        body: TxBody<'_>,
    ) -> Result<Value> {
        if self.take(Fault::TransactionBefore) {
            return Self::failure();
        }
        let result = self
            .inner
            .run_transaction_step(workflow_id, seq, started_at_ms, opts, body)
            .await;
        if self.take(Fault::TransactionAfter) {
            return Self::failure();
        }
        result
    }
    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        self.inner.dequeue_workflows(req).await
    }
    async fn transition_delayed_workflows(&self, now_ms: i64) -> Result<u64> {
        self.inner.transition_delayed_workflows(now_ms).await
    }
    async fn queue_partitions(&self, queue_name: &str) -> Result<Vec<String>> {
        self.inner.queue_partitions(queue_name).await
    }
    async fn insert_notification(
        &self,
        destination_id: &str,
        topic: &str,
        message: Value,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        self.inner
            .insert_notification(destination_id, topic, message, idempotency_key)
            .await
    }
    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
        seq: i32,
        step_name: &str,
    ) -> Result<Option<Value>> {
        self.inner
            .consume_notification(workflow_id, topic, seq, step_name)
            .await
    }
    async fn upsert_event(&self, workflow_id: &str, key: &str, value: Value) -> Result<()> {
        self.inner.upsert_event(workflow_id, key, value).await
    }
    async fn get_event_value(&self, workflow_id: &str, key: &str) -> Result<Option<Value>> {
        self.inner.get_event_value(workflow_id, key).await
    }
    async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>> {
        self.inner.list_workflows(filter).await
    }
    async fn get_workflow_aggregates(
        &self,
        query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>> {
        self.inner.get_workflow_aggregates(query).await
    }
    async fn get_step_aggregates(&self, query: &StepAggregateQuery) -> Result<Vec<StepAggregate>> {
        self.inner.get_step_aggregates(query).await
    }
    async fn cancel_workflow(&self, id: &str) -> Result<()> {
        self.inner.cancel_workflow(id).await
    }
    async fn resume_workflow(&self, id: &str) -> Result<bool> {
        self.inner.resume_workflow(id).await
    }
    async fn enqueue_existing(&self, id: &str, queue: &str) -> Result<()> {
        self.inner.enqueue_existing(id, queue).await
    }
    async fn cancel_workflows(&self, ids: &[String]) -> Result<()> {
        self.inner.cancel_workflows(ids).await
    }
    async fn resume_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        self.inner.resume_workflows(ids).await
    }
    async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()> {
        self.inner.delete_workflows(ids, delete_children).await
    }
    async fn set_workflow_delay(&self, id: &str, delay_until_ms: i64) -> Result<bool> {
        self.inner.set_workflow_delay(id, delay_until_ms).await
    }
    async fn set_workflow_attributes(
        &self,
        id: &str,
        attributes: Option<&Map<String, Value>>,
    ) -> Result<()> {
        self.inner.set_workflow_attributes(id, attributes).await
    }
    async fn fork_workflow(&self, params: &ForkParams) -> Result<()> {
        self.inner.fork_workflow(params).await
    }
    async fn claim_for_recovery(&self, req: &RecoveryClaimRequest<'_>) -> Result<RecoveryClaim> {
        self.inner.claim_for_recovery(req).await
    }
    async fn record_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
        name: &str,
        child_id: &str,
    ) -> Result<()> {
        self.inner
            .record_child_workflow(parent_id, seq, name, child_id)
            .await
    }
    async fn check_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
    ) -> Result<Option<(String, String)>> {
        self.inner.check_child_workflow(parent_id, seq).await
    }
    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        self.inner.get_workflow_steps(workflow_id).await
    }
    async fn get_step_name(&self, workflow_id: &str, seq: i32) -> Result<Option<String>> {
        self.inner.get_step_name(workflow_id, seq).await
    }
    async fn record_patch(&self, workflow_id: &str, seq: i32, name: &str) -> Result<()> {
        self.inner.record_patch(workflow_id, seq, name).await
    }
    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: Option<Value>,
        function_id: i32,
    ) -> Result<()> {
        self.inner
            .write_stream(workflow_id, key, value, function_id)
            .await
    }
    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)> {
        self.inner.read_stream(workflow_id, key, from_offset).await
    }
    async fn list_workflow_events(&self, workflow_id: &str) -> Result<Vec<(String, Value)>> {
        self.inner.list_workflow_events(workflow_id).await
    }
    async fn list_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<NotificationInfo>> {
        self.inner.list_workflow_notifications(workflow_id).await
    }
    async fn list_workflow_streams(&self, workflow_id: &str) -> Result<Vec<(String, Vec<Value>)>> {
        self.inner.list_workflow_streams(workflow_id).await
    }
    async fn create_schedule(&self, schedule: &WorkflowSchedule) -> Result<()> {
        self.inner.create_schedule(schedule).await
    }
    async fn apply_schedules(&self, schedules: &[WorkflowSchedule]) -> Result<()> {
        self.inner.apply_schedules(schedules).await
    }
    async fn list_schedules(&self, filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>> {
        self.inner.list_schedules(filter).await
    }
    async fn set_schedule_status(&self, name: &str, status: ScheduleStatus) -> Result<bool> {
        self.inner.set_schedule_status(name, status).await
    }
    async fn set_schedule_last_fired(&self, name: &str, at_ms: i64) -> Result<()> {
        self.inner.set_schedule_last_fired(name, at_ms).await
    }
    async fn delete_schedule(&self, name: &str) -> Result<bool> {
        self.inner.delete_schedule(name).await
    }
    async fn create_application_version(&self, version_name: &str) -> Result<()> {
        self.inner.create_application_version(version_name).await
    }
    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        self.inner.list_application_versions().await
    }
    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        self.inner.get_latest_application_version().await
    }
    async fn set_latest_application_version(&self, version_name: &str) -> Result<bool> {
        self.inner
            .set_latest_application_version(version_name)
            .await
    }
    async fn upsert_queue(
        &self,
        queue: &crate::WorkflowQueue,
        update_existing: bool,
    ) -> Result<()> {
        self.inner.upsert_queue(queue, update_existing).await
    }
    async fn list_queues(&self) -> Result<Vec<crate::WorkflowQueue>> {
        self.inner.list_queues().await
    }
    async fn export_workflow(
        &self,
        workflow_id: &str,
        export_children: bool,
    ) -> Result<Vec<ExportedWorkflow>> {
        self.inner
            .export_workflow(workflow_id, export_children)
            .await
    }
    async fn import_workflow(&self, workflows: &[ExportedWorkflow]) -> Result<()> {
        self.inner.import_workflow(workflows).await
    }
}
