//! A workflow's send or stream write commits with its checkpoint, so a
//! position that already holds a recorded outcome delivers or appends
//! nothing more, on every provider.
use durare::{
    ErrorCode, InMemoryProvider, NotificationInsert, Result, StateProvider, WorkflowStatus,
    STATUS_PENDING,
};
use serde_json::{json, Value};
use std::sync::Arc;
mod common;

const SENDER: &str = "sender";
const DEST: &str = "dest";
const TOPIC: &str = "t";
const KEY: &str = "progress";

fn row(message: &str) -> NotificationInsert {
    NotificationInsert {
        destination_id: DEST.into(),
        topic: TOPIC.into(),
        message: json!(message),
        idempotency_key: None,
    }
}

async fn send(db: &dyn StateProvider, seq: i32, name: &str, message: &str) -> Result<()> {
    db.send_from_workflow(SENDER, seq, name, &[row(message)])
        .await
}

async fn write(db: &dyn StateProvider, seq: i32, name: &str, value: Option<&str>) -> Result<()> {
    db.write_stream_from_workflow(SENDER, seq, name, KEY, value.map(|v| json!(v)))
        .await
}

async fn record(db: &dyn StateProvider, seq: i32, name: &str) -> Result<()> {
    db.record_step_result(SENDER, seq, name, Value::Null, None, None, None)
        .await
        .map(|_| ())
}

/// Drain the destination mailbox, consuming at successive positions from
/// `cursor`, which advances past what was taken.
async fn delivered(db: &dyn StateProvider, cursor: &mut i32) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    while let Some(m) = db
        .consume_notification(DEST, TOPIC, *cursor, "DBOS.recv")
        .await?
    {
        out.push(m);
        *cursor += 1;
    }
    Ok(out)
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    let db = db.as_ref();
    db.init().await?;
    for id in [SENDER, DEST] {
        db.insert_workflow_status(WorkflowStatus::new(
            id,
            "wf",
            json!(null),
            STATUS_PENDING,
            "executor",
            "version",
        ))
        .await?;
    }

    // --- send ---
    let mut inbox = 0;
    // A position another execution already recorded delivers nothing.
    record(db, 0, "DBOS.send").await?;
    send(db, 0, "DBOS.send", "dup").await?;
    assert!(delivered(db, &mut inbox).await?.is_empty());
    // A fresh position delivers and checkpoints.
    send(db, 1, "DBOS.send", "hello").await?;
    assert_eq!(delivered(db, &mut inbox).await?, vec![json!("hello")]);
    assert_eq!(
        db.get_step_name(SENDER, 1).await?.as_deref(),
        Some("DBOS.send")
    );
    // A different step at the position is a divergent replay; nothing is sent.
    record(db, 2, "DBOS.sleep").await?;
    let err = send(db, 2, "DBOS.send", "never")
        .await
        .expect_err("sleep at 2");
    assert_eq!(err.code(), ErrorCode::UnexpectedStep);
    assert!(delivered(db, &mut inbox).await?.is_empty());
    // A missing destination fails without a checkpoint.
    let missing = NotificationInsert {
        destination_id: "nobody".into(),
        ..row("x")
    };
    let err = db
        .send_from_workflow(SENDER, 3, "DBOS.send", &[missing])
        .await
        .expect_err("destination does not exist");
    assert_eq!(err.code(), ErrorCode::NonExistentWorkflow);
    assert_eq!(db.get_step_name(SENDER, 3).await?, None);
    // Two executions racing one position deliver once.
    let (a, b) = tokio::join!(
        send(db, 4, "DBOS.send", "once"),
        send(db, 4, "DBOS.send", "once")
    );
    a?;
    b?;
    assert_eq!(delivered(db, &mut inbox).await?, vec![json!("once")]);

    // --- streams ---
    record(db, 10, "DBOS.writeStream").await?;
    write(db, 10, "DBOS.writeStream", Some("dup")).await?;
    assert_eq!(db.read_stream(SENDER, KEY, 0).await?, (vec![], false));
    write(db, 11, "DBOS.writeStream", Some("a")).await?;
    assert_eq!(
        db.read_stream(SENDER, KEY, 0).await?,
        (vec![json!("a")], false)
    );
    let (a, b) = tokio::join!(
        write(db, 12, "DBOS.writeStream", Some("b")),
        write(db, 12, "DBOS.writeStream", Some("b"))
    );
    a?;
    b?;
    assert_eq!(
        db.read_stream(SENDER, KEY, 0).await?,
        (vec![json!("a"), json!("b")], false)
    );
    // A recorded close closes nothing. Two executions closing at one position,
    // concurrently and then again afterwards, converge instead of failing.
    record(db, 13, "DBOS.closeStream").await?;
    write(db, 13, "DBOS.closeStream", None).await?;
    assert_eq!(db.read_stream(SENDER, KEY, 2).await?, (vec![], false));
    let (a, b) = tokio::join!(
        write(db, 14, "DBOS.closeStream", None),
        write(db, 14, "DBOS.closeStream", None)
    );
    a?;
    b?;
    write(db, 14, "DBOS.closeStream", None).await?;
    assert_eq!(db.read_stream(SENDER, KEY, 2).await?, (vec![], true));
    record(db, 15, "DBOS.sleep").await?;
    let err = write(db, 15, "DBOS.writeStream", Some("never"))
        .await
        .expect_err("sleep at 15");
    assert_eq!(err.code(), ErrorCode::UnexpectedStep);
    Ok(())
}

#[tokio::test]
async fn recorded_sends_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn recorded_sends_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn recorded_sends_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "recorded_sends").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
