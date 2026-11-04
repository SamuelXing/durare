//! A `StateProvider` implemented outside the crate. Every type named by a
//! required method is spelled through a `durare::` path, so a type that is
//! public in `provider.rs` but missing from the crate-root re-exports fails to
//! compile here instead of in a downstream provider crate. The method bodies are
//! never called.
use async_trait::async_trait;
use durare::{
    DequeueRequest, ExportedWorkflow, ForkParams, ListFilter, NotificationInfo, RecordedStep,
    RecoveryClaim, RecoveryClaimRequest, Result, ScheduleFilter, ScheduleStatus, StateProvider,
    StepAggregate, StepAggregateQuery, StepInfo, StepOutcome, TransactionOptions, TxBody,
    VersionInfo, WorkflowAggregate, WorkflowAggregateQuery, WorkflowQueue, WorkflowSchedule,
    WorkflowStatus,
};
use serde_json::{json, Map, Value};
use std::sync::Arc;

struct ExternalProvider;

#[async_trait]
impl StateProvider for ExternalProvider {
    async fn init(&self) -> Result<()> {
        unimplemented!()
    }

    async fn insert_workflow_status(
        &self,
        _status: WorkflowStatus,
    ) -> Result<(WorkflowStatus, bool)> {
        unimplemented!()
    }

    async fn get_deduplicated_workflow(
        &self,
        _queue_name: &str,
        _dedup_id: &str,
    ) -> Result<Option<String>> {
        unimplemented!()
    }

    async fn get_workflow_status(&self, _id: &str) -> Result<Option<WorkflowStatus>> {
        unimplemented!()
    }

    async fn set_workflow_status(
        &self,
        _id: &str,
        _status: &str,
        _output: Option<&Value>,
        _error: Option<&str>,
    ) -> Result<bool> {
        unimplemented!()
    }

    async fn get_step_result(&self, _workflow_id: &str, _seq: i32) -> Result<Option<RecordedStep>> {
        unimplemented!()
    }

    #[allow(clippy::too_many_arguments)] // one argument per checkpoint column
    async fn record_step_result(
        &self,
        _workflow_id: &str,
        _seq: i32,
        _name: &str,
        _value: Value,
        _error: Option<&str>,
        _started_at_ms: Option<i64>,
        _executor_id: Option<&str>,
    ) -> Result<StepOutcome> {
        unimplemented!()
    }

    async fn run_transaction_step(
        &self,
        _workflow_id: &str,
        _seq: i32,
        _started_at_ms: i64,
        _opts: &TransactionOptions,
        _body: TxBody<'_>,
    ) -> Result<Value> {
        unimplemented!()
    }

    async fn dequeue_workflows(&self, _req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        unimplemented!()
    }

    async fn transition_delayed_workflows(&self, _now_ms: i64) -> Result<u64> {
        unimplemented!()
    }

    async fn queue_partitions(&self, _queue_name: &str) -> Result<Vec<String>> {
        unimplemented!()
    }

    async fn insert_notification(
        &self,
        _destination_id: &str,
        _topic: &str,
        _message: Value,
        _idempotency_key: Option<&str>,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn consume_notification(
        &self,
        _workflow_id: &str,
        _topic: &str,
        _seq: i32,
        _step_name: &str,
    ) -> Result<Option<Value>> {
        unimplemented!()
    }

    async fn upsert_event(&self, _workflow_id: &str, _key: &str, _value: Value) -> Result<()> {
        unimplemented!()
    }

    async fn get_event_value(&self, _workflow_id: &str, _key: &str) -> Result<Option<Value>> {
        unimplemented!()
    }

    async fn list_workflows(&self, _filter: &ListFilter) -> Result<Vec<WorkflowStatus>> {
        unimplemented!()
    }

    async fn get_workflow_aggregates(
        &self,
        _query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>> {
        unimplemented!()
    }

    async fn get_step_aggregates(&self, _query: &StepAggregateQuery) -> Result<Vec<StepAggregate>> {
        unimplemented!()
    }

    async fn cancel_workflow(&self, _id: &str) -> Result<()> {
        unimplemented!()
    }

    async fn resume_workflow(&self, _id: &str) -> Result<bool> {
        unimplemented!()
    }

    async fn enqueue_existing(&self, _id: &str, _queue: &str) -> Result<()> {
        unimplemented!()
    }

    async fn cancel_workflows(&self, _ids: &[String]) -> Result<()> {
        unimplemented!()
    }

    async fn resume_workflows(&self, _ids: &[String]) -> Result<Vec<String>> {
        unimplemented!()
    }

    async fn delete_workflows(&self, _ids: &[String], _delete_children: bool) -> Result<()> {
        unimplemented!()
    }

    async fn set_workflow_delay(&self, _id: &str, _delay_until_ms: i64) -> Result<bool> {
        unimplemented!()
    }

    async fn set_workflow_attributes(
        &self,
        _id: &str,
        _attributes: Option<&Map<String, Value>>,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn fork_workflow(&self, _params: &ForkParams) -> Result<()> {
        unimplemented!()
    }

    async fn claim_for_recovery(&self, _req: &RecoveryClaimRequest<'_>) -> Result<RecoveryClaim> {
        unimplemented!()
    }

    async fn record_child_workflow(
        &self,
        _parent_id: &str,
        _seq: i32,
        _name: &str,
        _child_id: &str,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn check_child_workflow(
        &self,
        _parent_id: &str,
        _seq: i32,
    ) -> Result<Option<(String, String)>> {
        unimplemented!()
    }

    async fn get_workflow_steps(&self, _workflow_id: &str) -> Result<Vec<StepInfo>> {
        unimplemented!()
    }

    async fn get_step_name(&self, _workflow_id: &str, _seq: i32) -> Result<Option<String>> {
        unimplemented!()
    }

    async fn record_patch(&self, _workflow_id: &str, _seq: i32, _name: &str) -> Result<()> {
        unimplemented!()
    }

    async fn write_stream(
        &self,
        _workflow_id: &str,
        _key: &str,
        _value: Option<Value>,
        _function_id: i32,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn read_stream(
        &self,
        _workflow_id: &str,
        _key: &str,
        _from_offset: i32,
    ) -> Result<(Vec<Value>, bool)> {
        unimplemented!()
    }

    async fn list_workflow_events(&self, _workflow_id: &str) -> Result<Vec<(String, Value)>> {
        unimplemented!()
    }

    async fn list_workflow_notifications(
        &self,
        _workflow_id: &str,
    ) -> Result<Vec<NotificationInfo>> {
        unimplemented!()
    }

    async fn list_workflow_streams(&self, _workflow_id: &str) -> Result<Vec<(String, Vec<Value>)>> {
        unimplemented!()
    }

    async fn create_schedule(&self, _schedule: &WorkflowSchedule) -> Result<()> {
        unimplemented!()
    }

    async fn apply_schedules(&self, _schedules: &[WorkflowSchedule]) -> Result<()> {
        unimplemented!()
    }

    async fn list_schedules(&self, _filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>> {
        unimplemented!()
    }

    async fn set_schedule_status(&self, _name: &str, _status: ScheduleStatus) -> Result<bool> {
        unimplemented!()
    }

    async fn set_schedule_last_fired(&self, _name: &str, _at_ms: i64) -> Result<()> {
        unimplemented!()
    }

    async fn delete_schedule(&self, _name: &str) -> Result<bool> {
        unimplemented!()
    }

    async fn create_application_version(&self, _version_name: &str) -> Result<()> {
        unimplemented!()
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        unimplemented!()
    }

    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        unimplemented!()
    }

    async fn set_latest_application_version(&self, _version_name: &str) -> Result<bool> {
        unimplemented!()
    }

    async fn upsert_queue(&self, _queue: &WorkflowQueue, _update_existing: bool) -> Result<()> {
        unimplemented!()
    }

    async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        unimplemented!()
    }

    async fn export_workflow(
        &self,
        _workflow_id: &str,
        _export_children: bool,
    ) -> Result<Vec<ExportedWorkflow>> {
        unimplemented!()
    }

    async fn import_workflow(&self, _workflows: &[ExportedWorkflow]) -> Result<()> {
        unimplemented!()
    }
}

#[test]
fn external_provider_is_a_state_provider() {
    let _provider: Arc<dyn StateProvider> = Arc::new(ExternalProvider);
}

#[test]
fn external_provider_can_build_step_and_notification_values() {
    let step = RecordedStep {
        name: "charge".to_string(),
        outcome: StepOutcome::Failure {
            message: "declined".to_string(),
            info: None,
        },
    };
    assert!(matches!(step.outcome, StepOutcome::Failure { .. }));

    let note = NotificationInfo {
        topic: None,
        message: json!("hello"),
        created_at_ms: 0,
        consumed: false,
    };
    assert!(!note.consumed);
}
