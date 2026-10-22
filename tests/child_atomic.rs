//! The provider commits child selection and its parent checkpoint together.
use durare::{
    Error, InMemoryProvider, Result, StateProvider, WorkflowStatus, STATUS_CANCELLED,
    STATUS_ENQUEUED, STATUS_PENDING, STATUS_SUCCESS,
};
use serde_json::json;
use std::sync::Arc;
mod common;

fn parent(id: &str) -> WorkflowStatus {
    WorkflowStatus::new(
        id,
        "parent",
        json!(null),
        STATUS_PENDING,
        "executor",
        "version",
    )
}
fn child(id: &str, parent: &str) -> WorkflowStatus {
    let mut row = WorkflowStatus::new(id, "child", json!(42), STATUS_ENQUEUED, "", "worker");
    row.parent_workflow_id = Some(parent.into());
    row.queue_name = Some("queue".into());
    row.dedup_id = Some("key".into());
    row
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    db.init().await?;
    db.insert_workflow_status(parent("parent")).await?;
    db.insert_workflow_status(child("existing", "parent"))
        .await?;
    let request = child("parent-0", "parent");
    let (adopted, created) = db
        .insert_child_workflow(request.clone(), "parent", 0, true)
        .await?
        .expect("atomic provider");
    assert_eq!(adopted.id, "existing");
    assert!(!created);
    assert_eq!(
        db.check_child_workflow("parent", 0).await?,
        Some(("existing".into(), "child".into()))
    );
    // Simulate a process dying immediately after the provider call. The child
    // completes and another producer takes its freed slot before recovery.
    db.set_workflow_status("existing", STATUS_PENDING, None, None)
        .await?;
    db.set_workflow_status("existing", STATUS_SUCCESS, Some(&json!(42)), None)
        .await?;
    db.insert_workflow_status(child("replacement", "parent"))
        .await?;
    let (replayed, created) = db
        .insert_child_workflow(request, "parent", 0, true)
        .await?
        .expect("atomic provider");
    assert_eq!(replayed.id, "existing");
    assert!(!created);
    assert!(db.get_workflow_status("parent-0").await?.is_none());

    db.insert_workflow_status(parent("race")).await?;
    let mut a = child("race-a", "race");
    a.dedup_id = None;
    let mut b = child("race-b", "race");
    b.dedup_id = None;
    let (a, b) = tokio::join!(
        db.insert_child_workflow(a, "race", 0, false),
        db.insert_child_workflow(b, "race", 0, false)
    );
    let (a, ac) = a?.expect("atomic provider");
    let (b, bc) = b?.expect("atomic provider");
    assert_eq!(a.id, b.id);
    assert_ne!(ac, bc);
    let loser = if a.id == "race-a" { "race-b" } else { "race-a" };
    assert!(db.get_workflow_status(loser).await?.is_none());

    db.insert_workflow_status(parent("conflict")).await?;
    db.record_step_result("conflict", 0, "ordinary", json!(1), None, None, None)
        .await?;
    assert!(matches!(
        db.insert_child_workflow(child("conflict-0", "conflict"), "conflict", 0, true)
            .await,
        Err(Error::WorkflowConflict(_))
    ));
    assert!(db.get_workflow_status("conflict-0").await?.is_none());

    db.insert_workflow_status(parent("mismatch")).await?;
    let mut wrong = child("mismatch-0", "mismatch");
    wrong.name = "different".into();
    assert!(matches!(
        db.insert_child_workflow(wrong, "mismatch", 0, true).await,
        Err(Error::App { .. })
    ));
    assert!(db.check_child_workflow("mismatch", 0).await?.is_none());
    assert!(db.get_workflow_status("mismatch-0").await?.is_none());

    db.insert_workflow_status(parent("cancelled")).await?;
    db.set_workflow_status("cancelled", STATUS_CANCELLED, None, None)
        .await?;
    assert!(matches!(
        db.insert_child_workflow(child("cancelled-0", "cancelled"), "cancelled", 0, true)
            .await,
        Err(Error::Cancelled(_))
    ));
    assert!(db.check_child_workflow("cancelled", 0).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn atomic_child_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn atomic_child_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn atomic_child_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "atomic_child").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

async fn assert_rollback(db: &dyn StateProvider) -> Result<()> {
    db.insert_workflow_status(parent("rollback")).await?;
    assert!(db
        .insert_child_workflow(child("rollback-0", "rollback"), "rollback", 0, false)
        .await
        .is_err());
    assert!(db.get_workflow_status("rollback-0").await?.is_none());
    assert!(db.check_child_workflow("rollback", 0).await?.is_none());
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn failed_checkpoint_rolls_back_sqlite_child() -> Result<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let db = durare::SqliteProvider::from_pool(pool.clone());
    db.init().await?;
    sqlx::query("CREATE TRIGGER refuse_child_checkpoint BEFORE INSERT ON operation_outputs BEGIN SELECT RAISE(ABORT, 'checkpoint unavailable'); END")
        .execute(&pool).await?;
    assert_rollback(&db).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn failed_checkpoint_rolls_back_postgres_child() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "child_rollback").await;
    let db = durare::PostgresProvider::connect(&url).await?;
    db.init().await?;
    sqlx::raw_sql("CREATE FUNCTION dbos.refuse_child_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'checkpoint unavailable'; END $$; CREATE TRIGGER refuse_child_checkpoint BEFORE INSERT ON dbos.operation_outputs FOR EACH ROW EXECUTE FUNCTION dbos.refuse_child_checkpoint();")
        .execute(db.pool()).await?;
    let result = assert_rollback(&db).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
