//! Regression coverage for the October 2026 SDK sync.
use durare::{
    Client, DeduplicationPolicy, DurableContext, DurableEngine, Error, InMemoryProvider,
    ListFilter, Result, StateProvider, WorkflowOptions,
};
use serde_json::json;
use std::sync::Arc;

mod common;

async fn fork_retains_attributes(provider: Arc<dyn StateProvider>, can_filter: bool) -> Result<()> {
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("source", |_ctx: DurableContext, _: ()| async {
        Ok::<_, Error>(())
    });
    let expected = json!({"customer": "acme", "region": {"zone": "eu"}});
    engine
        .start::<_, ()>(
            "source",
            (),
            WorkflowOptions::with_id("source").attributes(expected.as_object().unwrap().clone()),
        )
        .await?
        .result()
        .await?;
    // Do not launch a dispatcher: inspect the enqueued fork before it can run.
    engine
        .fork_workflow::<()>("source", 0, WorkflowOptions::with_id("engine-fork"))
        .await?;
    Client::new(provider.clone())
        .fork_workflow::<()>("source", 0, WorkflowOptions::with_id("client-fork"))
        .await?;
    for id in ["engine-fork", "client-fork"] {
        assert_eq!(
            provider.get_workflow_status(id).await?.unwrap().attributes,
            Some(expected.clone())
        );
    }
    if can_filter {
        let rows = engine
            .list_workflows(&ListFilter {
                attributes: Some(expected.as_object().unwrap().clone()),
                ..Default::default()
            })
            .await?;
        for id in ["engine-fork", "client-fork"] {
            assert!(rows.iter().any(|row| row.id == id));
        }
    }
    Ok(())
}

#[tokio::test]
async fn memory_fork_retains_attributes() -> Result<()> {
    fork_retains_attributes(Arc::new(InMemoryProvider::new()), true).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_fork_retains_attributes() -> Result<()> {
    fork_retains_attributes(
        Arc::new(durare::SqliteProvider::connect("sqlite::memory:").await?),
        false,
    )
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_fork_retains_attributes() -> Result<()> {
    let Ok(base_url) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base_url, "sdk_sync").await;
    let result = fork_retains_attributes(
        Arc::new(durare::PostgresProvider::connect(&url).await?),
        true,
    )
    .await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[tokio::test]
async fn engine_and_client_reject_invalid_deduplication_before_inserting() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("source", |_ctx: DurableContext, _: ()| async {
        Ok::<_, Error>(())
    });
    let client = Client::new(provider.clone());
    let invalid = [
        WorkflowOptions {
            partition_key: Some("partition".into()),
            dedup_id: Some("key".into()),
            ..Default::default()
        },
        WorkflowOptions {
            dedup_policy: DeduplicationPolicy::ReturnExisting,
            ..Default::default()
        },
    ];
    for opts in invalid {
        let engine_error = engine
            .start::<_, ()>("source", (), opts.clone())
            .await
            .err()
            .unwrap();
        let client_error = client
            .enqueue::<_, ()>("q", "source", (), opts)
            .await
            .err()
            .unwrap();
        assert_eq!(engine_error.to_string(), client_error.to_string());
    }
    let missing_queue = WorkflowOptions {
        dedup_id: Some("key".into()),
        dedup_policy: DeduplicationPolicy::ReturnExisting,
        ..Default::default()
    };
    assert!(engine
        .start::<_, ()>("source", (), missing_queue.clone())
        .await
        .err()
        .unwrap()
        .to_string()
        .contains("requires a queue name"));
    // Client::enqueue supplies the queue separately, so opts.queue is optional.
    client
        .enqueue::<_, ()>("q", "source", (), missing_queue)
        .await?;
    assert_eq!(
        engine.list_workflows(&ListFilter::default()).await?.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn nested_transaction_and_stream_calls_claim_no_positions() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("nested", |ctx: DurableContext, _: ()| async move {
        let inner = ctx.clone();
        ctx.step("outer", move || {
            let inner = inner.clone();
            async move {
                let tx =
                    inner.transaction("nested-tx", |_tx| Box::pin(async { Ok::<_, Error>(()) }));
                assert!(matches!(tx.await, Err(Error::NestedDurableCall { .. })));
                assert!(matches!(
                    inner.close_stream("progress").await,
                    Err(Error::NestedDurableCall { .. })
                ));
                assert!(matches!(
                    inner.write_stream("progress", 1).await,
                    Err(Error::NestedDurableCall { .. })
                ));
                Ok::<_, Error>(())
            }
        })
        .await?;
        ctx.step("after", || async { Ok::<_, Error>(()) }).await
    });
    engine
        .start::<_, ()>("nested", (), WorkflowOptions::with_id("nested"))
        .await?
        .result()
        .await?;
    assert_eq!(
        common::recorded(&engine, "nested").await?,
        vec![(0, "outer".into()), (1, "after".into())]
    );
    assert_eq!(engine.verify_replay("nested").await?.divergence, None);
    Ok(())
}
