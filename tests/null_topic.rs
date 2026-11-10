//! An empty topic is the topic-less mailbox the other DBOS SDKs store as
//! `__null__topic__`, so the two directions interoperate on every provider.
use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
mod common;

const SENTINEL: &str = "__null__topic__";

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    let mut engine = DurableEngine::new(db.clone()).await?;
    engine.register("receiver", |ctx: DurableContext, _: ()| async move {
        ctx.recv::<String>("", Duration::from_secs(5)).await
    });
    engine.register("sender", |ctx: DurableContext, to: String| async move {
        ctx.send(&to, "from-durare".to_string(), "").await?;
        Ok::<_, Error>(())
    });

    // A peer's topic-less send is a row carrying the sentinel; a topic-less
    // durare recv takes it.
    let handle = engine
        .start::<_, Option<String>>("receiver", (), WorkflowOptions::with_id("rx"))
        .await?;
    db.insert_notification("rx", SENTINEL, json!("from-peer"), None)
        .await?;
    assert_eq!(handle.result().await?, Some("from-peer".to_string()));

    // A topic-less durare send, from a workflow and from outside one, is
    // stored under the sentinel, where a peer's topic-less recv looks; the
    // engine's listing surfaces it as no topic.
    common::seed_pending(db.as_ref(), &["peer"]).await?;
    engine
        .start::<_, ()>("sender", "peer".to_string(), WorkflowOptions::with_id("tx"))
        .await?
        .result()
        .await?;
    engine.send("peer", "to-peer".to_string(), "").await?;
    for seq in 0..2 {
        let taken = db
            .consume_notification("peer", SENTINEL, seq, "DBOS.recv")
            .await?;
        assert!(taken.is_some(), "message {seq} stored under the sentinel");
    }
    let listed = engine.list_workflow_notifications("peer").await?;
    assert_eq!(listed.len(), 2);
    assert!(listed.iter().all(|n| n.topic.is_none()));
    Ok(())
}

#[tokio::test]
async fn null_topic_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn null_topic_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn null_topic_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "null_topic").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
