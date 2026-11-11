//! The debounce columns of `workflow_status`: a debounced run holds its
//! debounce key as its deduplication id only while `DELAYED`, and the list
//! filter tells debounced runs apart. Provider-level, on every backend.
use durare::{InMemoryProvider, ListFilter, Result, StateProvider, WorkflowStatus, STATUS_DELAYED};
use serde_json::json;
use std::sync::Arc;
mod common;

fn delayed(id: &str, key: &str, delay_until_ms: i64, debounced: bool) -> WorkflowStatus {
    let mut row = WorkflowStatus::new(id, "wf", json!(null), STATUS_DELAYED, "", "");
    row.queue_name = Some("q".into());
    row.dedup_id = Some(key.into());
    row.delay_until_ms = Some(delay_until_ms);
    row.is_debounced = debounced;
    row.debounce_deadline_ms = debounced.then_some(delay_until_ms + 60_000);
    row
}

async fn ids(db: &dyn StateProvider, debounced: bool) -> Result<Vec<String>> {
    let mut rows: Vec<String> = db
        .list_workflows(&ListFilter {
            is_debounced: Some(debounced),
            ..Default::default()
        })
        .await?
        .into_iter()
        .map(|r| r.id)
        .collect();
    rows.sort();
    Ok(rows)
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    db.init().await?;
    let now = chrono::Utc::now().timestamp_millis();
    // Due: one debounced, one plainly deduplicated. Not due: debounced.
    db.insert_workflow_status(delayed("due-debounced", "wf-a", now - 1, true))
        .await?;
    db.insert_workflow_status(delayed("due-plain", "b", now - 1, false))
        .await?;
    db.insert_workflow_status(delayed("waiting", "wf-c", now + 60_000, true))
        .await?;
    let stored = db.get_workflow_status("due-debounced").await?.unwrap();
    assert!(stored.is_debounced);
    assert_eq!(stored.debounce_deadline_ms, Some(now - 1 + 60_000));

    assert_eq!(db.transition_delayed_workflows(now).await?, 2);
    let promoted = db.get_workflow_status("due-debounced").await?.unwrap();
    assert_eq!(promoted.status, "ENQUEUED");
    assert_eq!(
        promoted.dedup_id, None,
        "a debounce key is released on promotion"
    );
    assert!(promoted.is_debounced, "the marker stays for the record");
    let plain = db.get_workflow_status("due-plain").await?.unwrap();
    assert_eq!(plain.status, "ENQUEUED");
    assert_eq!(
        plain.dedup_id.as_deref(),
        Some("b"),
        "an ordinary key is kept"
    );
    let waiting = db.get_workflow_status("waiting").await?.unwrap();
    assert_eq!(
        waiting.dedup_id.as_deref(),
        Some("wf-c"),
        "held while DELAYED"
    );

    // What the release is for: the same debounce key is free again, while the
    // ordinary key is still taken.
    db.insert_workflow_status(delayed("next", "wf-a", now + 1, true))
        .await?;
    let taken = db
        .insert_workflow_status(delayed("again", "b", now + 1, false))
        .await;
    assert!(
        taken.is_err_and(|e| e.code() == durare::ErrorCode::QueueDeduplicated),
        "an ordinary key stays held after promotion"
    );

    assert_eq!(
        ids(db.as_ref(), true).await?,
        ["due-debounced", "next", "waiting"]
    );
    assert_eq!(ids(db.as_ref(), false).await?, ["due-plain"]);

    // Both columns travel with the portable export.
    let exported = db.export_workflow("waiting", false).await?;
    let status = &exported[0].workflow_status;
    assert_eq!(status.get("is_debounced"), Some(&json!(true)));
    assert_eq!(
        status.get("debounce_deadline_epoch_ms"),
        Some(&json!(now + 120_000))
    );
    Ok(())
}

#[tokio::test]
async fn debounce_columns_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn debounce_columns_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn debounce_columns_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "debounce_columns").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
