//! Forks have independent timeouts, armed at dequeue rather than inherited deadlines.
use durare::{
    Client, DequeueRequest, DurableEngine, ForkParams, InMemoryProvider, Result, StateProvider,
    WorkflowOptions, WorkflowStatus, STATUS_ENQUEUED, STATUS_PENDING, STATUS_SUCCESS,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
mod common;

async fn contract(provider: Arc<dyn StateProvider>) -> Result<()> {
    let engine = DurableEngine::new(provider.clone()).await?;
    let client = Client::new(provider.clone());
    let mut original =
        WorkflowStatus::new("source", "work", json!(7), STATUS_PENDING, "owner", "v");
    original.timeout_ms = Some(25);
    original.deadline_ms = Some(1); // A long-expired source must not expire its fork.
    provider.insert_workflow_status(original).await?;
    assert!(
        provider
            .set_workflow_status("source", STATUS_SUCCESS, Some(&json!(42)), None)
            .await?
    );
    provider
        .record_step_result("source", 0, "saved", json!(9), None, None, None)
        .await?;
    for via_client in [false, true] {
        for timeout in [None, Some(Duration::from_millis(50)), Some(Duration::ZERO)] {
            let id = format!("fork-{via_client}-{timeout:?}");
            let opts = WorkflowOptions {
                workflow_id: Some(id.clone()),
                queue: Some(id.clone()),
                timeout,
                ..Default::default()
            };
            if via_client {
                client.fork_workflow::<i64>("source", 1, opts).await?;
            } else {
                engine.fork_workflow::<i64>("source", 1, opts).await?;
            }
            let fork = provider.get_workflow_status(&id).await?.unwrap();
            assert_eq!(fork.status, STATUS_ENQUEUED);
            assert_eq!(fork.timeout_ms, timeout.map(|d| d.as_millis() as i64));
            assert_eq!(fork.deadline_ms, None);
            assert_eq!(fork.started_at_ms, None);
            assert_eq!(fork.forked_from.as_deref(), Some("source"));
            assert_eq!(
                provider.get_workflow_steps(&id).await?[0].output,
                Some(json!(9))
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
            let claimed = provider
                .dequeue_workflows(&DequeueRequest {
                    queue_name: id.clone(),
                    executor_id: "exec".into(),
                    app_version: "v".into(),
                    partition_key: None,
                    max_tasks: 1,
                    global_concurrency: None,
                    rate_limit_max: None,
                    rate_limit_period_ms: None,
                })
                .await?;
            assert_eq!(claimed.len(), 1);
            let row = &claimed[0];
            let started = row.started_at_ms.unwrap();
            assert!(started >= fork.created_at.timestamp_millis() + 10);
            assert_eq!(row.deadline_ms, row.timeout_ms.map(|t| started + t));
        }
    }
    // Oversized Duration values must never wrap to a negative SQL integer.
    for via_client in [false, true] {
        let id = format!("overflow-{via_client}");
        let opts = WorkflowOptions {
            workflow_id: Some(id.clone()),
            timeout: Some(Duration::MAX),
            ..Default::default()
        };
        let result = if via_client {
            client.fork_workflow::<i64>("source", 0, opts).await
        } else {
            engine.fork_workflow::<i64>("source", 0, opts).await
        };
        assert!(result.is_err());
        assert!(provider.get_workflow_status(&id).await?.is_none());
    }
    assert!(provider
        .fork_workflow_with_timeout(
            &ForkParams {
                original_id: "source".into(),
                new_id: "negative".into(),
                start_step: 0,
                app_version: None,
                queue_name: "q".into(),
                partition_key: None,
            },
            Some(-1)
        )
        .await
        .is_err());
    assert!(provider.get_workflow_status("negative").await?.is_none());
    let source = provider.get_workflow_status("source").await?.unwrap();
    assert_eq!(source.timeout_ms, Some(25));
    assert_eq!(source.deadline_ms, Some(1));
    assert_eq!(source.output, Some(json!(42)));
    Ok(())
}

#[tokio::test]
async fn memory_fork_timeout() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_fork_timeout() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_fork_timeout() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "fork_timeout").await;
    let result =
        async { contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await }.await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[tokio::test]
async fn fork_timeout_cancels_a_running_workflow() -> Result<()> {
    use durare::{DurableContext, STATUS_CANCELLED};
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("slow", |_ctx: DurableContext, _: ()| async {
        std::future::pending::<Result<()>>().await
    });
    let mut source = WorkflowStatus::new("done", "slow", json!(null), STATUS_SUCCESS, "owner", "");
    source.deadline_ms = Some(1);
    provider.insert_workflow_status(source).await?;
    let fork = engine
        .fork_workflow::<()>(
            "done",
            0,
            WorkflowOptions {
                workflow_id: Some("live-fork".into()),
                timeout: Some(Duration::from_millis(30)),
                ..Default::default()
            },
        )
        .await?;
    engine.launch().await?;
    let result = tokio::time::timeout(Duration::from_secs(3), fork.result()).await;
    engine.shutdown(Duration::from_secs(1)).await?;
    assert!(result.expect("fork did not time out").is_err());
    let row = provider.get_workflow_status("live-fork").await?.unwrap();
    assert_eq!(row.status, STATUS_CANCELLED);
    assert_eq!(row.error.as_deref(), Some("deadline exceeded"));
    assert_eq!(row.deadline_ms, row.started_at_ms.map(|s| s + 30));
    Ok(())
}
