//! Listing a partitioned queue's active partitions: the distinct keys with
//! `ENQUEUED` work, each listed once, read by walking the partition dequeue
//! index instead of scanning the backlog. Provider-level, on every backend.
use durare::{
    InMemoryProvider, Result, StateProvider, WorkflowStatus, STATUS_ENQUEUED, STATUS_PENDING,
};
use serde_json::json;
use std::sync::Arc;
mod common;

fn row(id: &str, queue: &str, key: Option<&str>, status: &str) -> WorkflowStatus {
    let mut row = WorkflowStatus::new(id, "wf", json!(null), status, "", "");
    row.queue_name = Some(queue.into());
    row.queue_partition_key = key.map(Into::into);
    row
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    db.init().await?;
    assert!(db.queue_partitions("q").await?.is_empty(), "no work yet");

    // Several rows per key, inserted out of key order: each key is listed once.
    for (i, key) in ["m", "a", "z", "a", "m", "a"].into_iter().enumerate() {
        db.insert_workflow_status(row(&format!("wf-{i}"), "q", Some(key), STATUS_ENQUEUED))
            .await?;
    }
    // None of these is an active partition of `q`: a keyless row, a partition
    // whose only run is already claimed, and another queue's partition.
    db.insert_workflow_status(row("keyless", "q", None, STATUS_ENQUEUED))
        .await?;
    db.insert_workflow_status(row("claimed", "q", Some("b"), STATUS_PENDING))
        .await?;
    db.insert_workflow_status(row("elsewhere", "other", Some("c"), STATUS_ENQUEUED))
        .await?;

    // The order is the backend's own (a collation's, on Postgres).
    let mut keys = db.queue_partitions("q").await?;
    keys.sort();
    assert_eq!(keys, ["a", "m", "z"]);
    assert_eq!(db.queue_partitions("other").await?, ["c"]);
    assert!(db.queue_partitions("unknown").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn queue_partitions_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn queue_partitions_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn queue_partitions_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "queue_partitions").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
