//! Out-of-process `Client`: enqueue work and observe it without a local
//! registry. A `Client` and a `DurableEngine` share one provider — the client
//! produces, the engine consumes.

use durust::{
    Client, DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result,
    WorkflowOptions, WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

/// A client enqueues a workflow it does not register; a separate engine claims
/// it, runs it, and the client observes the result, the row, and its steps.
#[tokio::test]
async fn client_enqueues_work_an_engine_runs() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("double", |ctx: DurableContext, n: i64| async move {
        ctx.step("mul", || async { Ok::<_, Error>(n * 2) }).await
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    // The client has no registry — it only enqueues and observes.
    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some("job-1".to_string()),
        ..Default::default()
    };
    let mut handle = client.enqueue::<_, i64>("q", "double", 21i64, opts).await?;
    assert_eq!(handle.id(), "job-1");
    assert_eq!(
        handle.get_result().await?,
        42,
        "engine ran the enqueued work"
    );

    // The client observes the persisted row and its step.
    let rows = client
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some("job-".to_string()),
            ..Default::default()
        })
        .await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].name, "double");
    let steps = client.get_workflow_steps("job-1").await?;
    assert!(steps.iter().any(|s| s.name == "mul"));

    // retrieve_workflow returns a handle; an unknown id errors.
    let mut again: durust::WorkflowHandle<i64> = client.retrieve_workflow("job-1").await?;
    assert_eq!(again.get_result().await?, 42);
    assert!(client.retrieve_workflow::<i64>("nope").await.is_err());

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A client sends a message to a workflow waiting in `recv`, then reads the
/// event the workflow sets — the cross-process messaging path.
#[tokio::test]
async fn client_sends_messages_and_reads_events() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());

    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("waiter", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("topic", Duration::from_secs(5)).await?;
        let msg = msg.unwrap_or_default();
        ctx.set_event("echo", &msg).await?;
        Ok::<_, Error>(msg)
    });
    engine.register_queue(WorkflowQueue::new("q"));
    engine.launch().await?;

    let client = Client::new(provider.clone());
    let opts = WorkflowOptions {
        workflow_id: Some("waiter-1".to_string()),
        ..Default::default()
    };
    let mut handle = client.enqueue::<_, String>("q", "waiter", (), opts).await?;

    // Deliver the message the workflow is waiting for.
    client
        .send("waiter-1", "hello".to_string(), "topic")
        .await?;
    assert_eq!(handle.get_result().await?, "hello");

    // The event the workflow set is now readable.
    let event: Option<String> = client
        .get_event("waiter-1", "echo", Duration::from_secs(2))
        .await?;
    assert_eq!(event.as_deref(), Some("hello"));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// Enqueue rejects an empty queue or workflow name, and an incompatible
/// partition-key + deduplication-id pair.
#[tokio::test]
async fn client_enqueue_validates() -> Result<()> {
    let client = Client::new(Arc::new(InMemoryProvider::new()));
    assert!(client
        .enqueue::<_, ()>("", "w", 1i64, WorkflowOptions::default())
        .await
        .is_err());
    assert!(client
        .enqueue::<_, ()>("q", "", 1i64, WorkflowOptions::default())
        .await
        .is_err());
    let opts = WorkflowOptions {
        partition_key: Some("p".to_string()),
        dedup_id: Some("d".to_string()),
        ..Default::default()
    };
    assert!(client.enqueue::<_, ()>("q", "w", 1i64, opts).await.is_err());
    Ok(())
}
