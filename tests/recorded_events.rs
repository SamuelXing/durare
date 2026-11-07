//! A workflow's event set or attribute replacement commits with its
//! checkpoint, so a position that already holds a recorded outcome writes
//! nothing more, on every provider. In particular an execution lagging behind
//! another cannot put back a value a later position has already replaced.
use durare::{ErrorCode, InMemoryProvider, Result, StateProvider};
use serde_json::{json, Map, Value};
use std::sync::Arc;
mod common;

const WF: &str = "wf";
const TARGET: &str = "target";
const KEY: &str = "status";
const SET_EVENT: &str = "DBOS.setEvent";
const UPDATE_ATTRIBUTES: &str = "DBOS.updateWorkflowAttributes";

async fn set(db: &dyn StateProvider, seq: i32, name: &str, value: &str) -> Result<()> {
    db.set_event_from_workflow(WF, seq, name, KEY, json!(value))
        .await
}

async fn event(db: &dyn StateProvider) -> Result<Option<Value>> {
    db.get_event_value(WF, KEY).await
}

fn stage(value: &str) -> Map<String, Value> {
    let mut attributes = Map::new();
    attributes.insert("stage".into(), json!(value));
    attributes
}

async fn replace(
    db: &dyn StateProvider,
    seq: i32,
    name: &str,
    id: &str,
    value: Option<&str>,
) -> Result<()> {
    let attributes = value.map(stage);
    db.set_workflow_attributes_from_workflow(WF, seq, name, id, attributes.as_ref())
        .await
}

async fn attributes(db: &dyn StateProvider) -> Result<Option<Value>> {
    Ok(db
        .get_workflow_status(TARGET)
        .await?
        .expect("target exists")
        .attributes)
}

async fn record(db: &dyn StateProvider, seq: i32, name: &str) -> Result<()> {
    common::record_null(db, WF, seq, name).await
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    let db = db.as_ref();
    common::seed_pending(db, &[WF, TARGET]).await?;

    // --- events ---
    // A position another execution already recorded sets nothing.
    record(db, 0, SET_EVENT).await?;
    set(db, 0, SET_EVENT, "dup").await?;
    assert_eq!(event(db).await?, None);
    // A fresh position sets and checkpoints.
    set(db, 1, SET_EVENT, "v1").await?;
    assert_eq!(event(db).await?, Some(json!("v1")));
    assert_eq!(db.get_step_name(WF, 1).await?.as_deref(), Some(SET_EVENT));
    // A different step at the position is a divergent replay; nothing is set.
    record(db, 2, "DBOS.sleep").await?;
    let err = set(db, 2, SET_EVENT, "never")
        .await
        .expect_err("sleep at 2");
    assert_eq!(err.code(), ErrorCode::UnexpectedStep);
    assert_eq!(event(db).await?, Some(json!("v1")));
    // Two executions racing one position both succeed.
    let (a, b) = tokio::join!(set(db, 3, SET_EVENT, "v3"), set(db, 3, SET_EVENT, "v3"));
    a?;
    b?;
    assert_eq!(event(db).await?, Some(json!("v3")));
    // One execution set v4 at 4 and v5 at 5. Another, which passed the replay
    // check at 4 before that checkpoint landed, now writes v4 there: it
    // succeeds and changes nothing.
    set(db, 4, SET_EVENT, "v4").await?;
    set(db, 5, SET_EVENT, "v5").await?;
    set(db, 4, SET_EVENT, "v4").await?;
    assert_eq!(event(db).await?, Some(json!("v5")));

    // --- attributes ---
    record(db, 10, UPDATE_ATTRIBUTES).await?;
    replace(db, 10, UPDATE_ATTRIBUTES, TARGET, Some("dup")).await?;
    assert_eq!(attributes(db).await?, None);
    replace(db, 11, UPDATE_ATTRIBUTES, TARGET, Some("a")).await?;
    assert_eq!(attributes(db).await?, Some(json!({ "stage": "a" })));
    assert_eq!(
        db.get_step_name(WF, 11).await?.as_deref(),
        Some(UPDATE_ATTRIBUTES)
    );
    record(db, 12, "DBOS.sleep").await?;
    let err = replace(db, 12, UPDATE_ATTRIBUTES, TARGET, Some("never"))
        .await
        .expect_err("sleep at 12");
    assert_eq!(err.code(), ErrorCode::UnexpectedStep);
    assert_eq!(attributes(db).await?, Some(json!({ "stage": "a" })));
    // A missing target fails without a checkpoint.
    let err = replace(db, 13, UPDATE_ATTRIBUTES, "nobody", Some("x"))
        .await
        .expect_err("target does not exist");
    assert_eq!(err.code(), ErrorCode::NonExistentWorkflow);
    assert_eq!(db.get_step_name(WF, 13).await?, None);
    let (a, b) = tokio::join!(
        replace(db, 14, UPDATE_ATTRIBUTES, TARGET, Some("b")),
        replace(db, 14, UPDATE_ATTRIBUTES, TARGET, Some("b"))
    );
    a?;
    b?;
    assert_eq!(attributes(db).await?, Some(json!({ "stage": "b" })));
    // The lagging write again: a clear at 16 is not undone by a late 15.
    replace(db, 15, UPDATE_ATTRIBUTES, TARGET, Some("c")).await?;
    replace(db, 16, UPDATE_ATTRIBUTES, TARGET, None).await?;
    replace(db, 15, UPDATE_ATTRIBUTES, TARGET, Some("c")).await?;
    assert_eq!(attributes(db).await?, None);
    Ok(())
}

#[tokio::test]
async fn recorded_events_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn recorded_events_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn recorded_events_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "recorded_events").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
