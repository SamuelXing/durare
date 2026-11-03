//! A `recv` position that already holds a recorded outcome serves it and
//! consumes nothing more, on every provider.
use durare::{ErrorCode, InMemoryProvider, Result, StateProvider, WorkflowStatus, STATUS_PENDING};
use serde_json::{json, Value};
use std::sync::Arc;
mod common;

const WF: &str = "receiver";
const TOPIC: &str = "t";
const RECV: &str = "DBOS.recv";

async fn send(db: &dyn StateProvider, message: &str) -> Result<()> {
    db.insert_notification(WF, TOPIC, json!(message), None)
        .await
}

async fn consume(db: &dyn StateProvider, seq: i32) -> Result<Option<Value>> {
    db.consume_notification(WF, TOPIC, seq, RECV).await
}

async fn record(db: &dyn StateProvider, seq: i32, name: &str, output: Value) -> Result<()> {
    db.record_step_result(WF, seq, name, output, None, None, None)
        .await
        .map(|_| ())
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    let db = db.as_ref();
    db.init().await?;
    db.insert_workflow_status(WorkflowStatus::new(
        WF,
        "receiver",
        json!(null),
        STATUS_PENDING,
        "executor",
        "version",
    ))
    .await?;

    // A message another execution recorded at the position is served, and the
    // message waiting in the mailbox is left for the next position.
    record(db, 0, RECV, json!("taken")).await?;
    send(db, "later").await?;
    assert_eq!(consume(db, 0).await?, Some(json!("taken")));
    assert_eq!(consume(db, 1).await?, Some(json!("later")));

    // A recorded timeout is served as Null and consumes nothing either.
    record(db, 2, RECV, Value::Null).await?;
    send(db, "after").await?;
    assert_eq!(consume(db, 2).await?, Some(Value::Null));
    assert_eq!(consume(db, 3).await?, Some(json!("after")));

    // A different step at the position is a divergent replay.
    record(db, 4, "DBOS.sleep", json!("2026-01-01T00:00:00Z")).await?;
    send(db, "untouched").await?;
    let err = consume(db, 4)
        .await
        .expect_err("a sleep is recorded at position 4");
    assert_eq!(err.code(), ErrorCode::UnexpectedStep);
    assert_eq!(consume(db, 5).await?, Some(json!("untouched")));

    // Two executions racing one position take delivery of one message, once.
    // Each sees either the message or an empty mailbox to re-check; the
    // position then settles on that message and the next one is untouched.
    send(db, "only").await?;
    send(db, "spare").await?;
    let (first, second) = tokio::join!(consume(db, 6), consume(db, 6));
    for outcome in [first?, second?] {
        assert!(
            outcome.is_none() || outcome == Some(json!("only")),
            "{outcome:?}"
        );
    }
    assert_eq!(consume(db, 6).await?, Some(json!("only")));
    assert_eq!(consume(db, 7).await?, Some(json!("spare")));
    assert_eq!(consume(db, 8).await?, None);
    Ok(())
}

#[tokio::test]
async fn recv_consume_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn recv_consume_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn recv_consume_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "recv_consume").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
