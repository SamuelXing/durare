//! A run's steps live and die with it without the database's help. Shared
//! migration 112 drops the foreign key from `operation_outputs` to
//! `workflow_status`, so durare must neither leave a deleted run's checkpoints
//! behind nor record new ones for it: a later run of the same id would replay
//! them.
use durare::{DurableContext, DurableEngine, Error, Result, StateProvider, WorkflowOptions};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
mod common;

/// Three finished runs, each with one step: `old` and `parent` -> `child`.
const SEED: &str = "
    INSERT INTO workflow_status (workflow_uuid, name, status, created_at, updated_at, completed_at, parent_workflow_id)
    VALUES ('old', 'wf', 'SUCCESS', 1, 1, 1, NULL),
           ('parent', 'wf', 'SUCCESS', 1, 1, 1, NULL),
           ('child', 'wf', 'SUCCESS', 1, 1, 1, 'parent');
    INSERT INTO operation_outputs (workflow_uuid, function_id, function_name)
    VALUES ('old', 0, 'step'), ('parent', 0, 'step'), ('child', 0, 'step');";

/// Every delete path leaves no steps behind for the runs it deleted, and no
/// write adds one afterwards. `run` executes a script such as [`SEED`];
/// `steps` lists the ids that still have a step.
async fn contract<R, RF, L, LF>(db: &dyn StateProvider, run: R, steps: L) -> Result<()>
where
    R: Fn(&'static str) -> RF,
    RF: std::future::Future<Output = ()>,
    L: Fn() -> LF,
    LF: std::future::Future<Output = Vec<String>>,
{
    run(SEED).await;
    db.delete_workflows(&["old".to_owned()], false).await?;
    assert_eq!(steps().await, ["child", "parent"]);
    db.delete_workflows(&["parent".to_owned()], true).await?;
    assert!(steps().await.is_empty(), "a deleted tree keeps no steps");

    run(SEED).await;
    assert_eq!(db.garbage_collect(Some(2), None).await?, 3);
    assert!(steps().await.is_empty(), "collected runs keep no steps");

    for written in [
        common::record_null(db, "old", 1, "step").await,
        db.record_child_workflow("old", 2, "wf", "child").await,
        db.record_patch("old", 3, "DBOS.patch-p").await,
    ] {
        assert!(
            matches!(written, Err(Error::NonExistentWorkflow(_))),
            "a deleted run records nothing: {written:?}"
        );
    }
    assert!(steps().await.is_empty(), "a deleted run gains no steps");

    // Steps a writer without this contract left behind go with the id.
    run(SEED).await;
    run("DELETE FROM workflow_status").await;
    db.delete_workflows(&["old".to_owned()], false).await?;
    assert_eq!(steps().await, ["child", "parent"], "a stranded step goes");
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_deletes_steps_without_the_foreign_key() -> Result<()> {
    use durare::PostgresProvider;
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "step_delete").await;
    let result = async {
        let db = PostgresProvider::connect(&url).await?;
        db.init().await?;
        let pool = &common::pg_raw_pool(&url).await;
        let run = |script| async move {
            sqlx::raw_sql(script).execute(pool).await.unwrap();
        };
        let steps = || async {
            sqlx::query_scalar("SELECT workflow_uuid FROM operation_outputs ORDER BY 1")
                .fetch_all(pool)
                .await
                .unwrap()
        };
        contract(&db, run, steps).await?;

        // The two writers wait for each other on the run's row: one holds its
        // transaction open until the other is seen waiting, or for long enough
        // that a writer which does not wait has finished.
        let release = |holder: sqlx::Transaction<'static, sqlx::Postgres>| async {
            for _ in 0..200 {
                let waiting: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database() AND wait_event_type = 'Lock')",
                )
                .fetch_one(pool)
                .await
                .unwrap();
                if waiting {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            holder.commit().await.unwrap();
        };
        run("DELETE FROM operation_outputs; DELETE FROM workflow_status").await;

        // A step written while its run is being deleted is refused.
        run(SEED).await;
        let mut deleting = pool.begin().await?;
        sqlx::raw_sql("DELETE FROM workflow_status WHERE workflow_uuid = 'old'")
            .execute(&mut *deleting)
            .await?;
        let step = common::record_null(&db, "old", 1, "step");
        let (step, ()) = tokio::join!(step, release(deleting));
        assert!(
            matches!(step, Err(Error::NonExistentWorkflow(_))),
            "{step:?}"
        );

        // A run deleted while a step is being written takes that step along.
        run("DELETE FROM operation_outputs; DELETE FROM workflow_status").await;
        run(SEED).await;
        let mut writing = pool.begin().await?;
        sqlx::raw_sql(
            "SELECT 1 FROM workflow_status WHERE workflow_uuid = 'parent' FOR KEY SHARE;
             INSERT INTO operation_outputs (workflow_uuid, function_id, function_name)
             VALUES ('parent', 1, 'step')",
        )
        .execute(&mut *writing)
        .await?;
        let tree = ["parent".to_owned()];
        let (delete, ()) = tokio::join!(db.delete_workflows(&tree, true), release(writing));
        delete?;
        assert_eq!(steps().await, ["old"], "the tree keeps no steps");

        pool.close().await;
        Ok(())
    }
    .await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_deletes_steps_without_the_foreign_key() -> Result<()> {
    use durare::SqliteProvider;
    let (url, path) = common::temp_db_url("step_delete");
    let db = SqliteProvider::connect(&url).await?;
    db.init().await?;
    let pool = &sqlx::SqlitePool::connect(&url).await?;
    let run = |script| async move {
        sqlx::raw_sql(script).execute(pool).await.unwrap();
    };
    let steps = || async {
        sqlx::query_scalar("SELECT workflow_uuid FROM operation_outputs ORDER BY 1")
            .fetch_all(pool)
            .await
            .unwrap()
    };
    contract(&db, run, steps).await?;
    pool.close().await;
    common::remove_sqlite_files(&path);
    Ok(())
}

/// A run deleted while it executes stops at its next durable call: the call
/// is refused, a transaction's writes roll back with it, and the body does
/// not run on past an error it chose to ignore.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn a_deleted_run_stops_executing() -> Result<()> {
    use durare::SqliteProvider;
    let (url, path) = common::temp_db_url("step_delete_engine");
    let provider = SqliteProvider::connect(&url).await?;
    let ds = provider.system_datasource();
    let pool = ds.pool().clone();
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    sqlx::raw_sql("CREATE TABLE effects (item TEXT NOT NULL)")
        .execute(&pool)
        .await?;

    let (started, deleted) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let after = Arc::new(AtomicU32::new(0));
    let (wf_started, wf_deleted, wf_after) = (started.clone(), deleted.clone(), after.clone());
    engine.register("wf", move |ctx: DurableContext, (): ()| {
        let (ds, started, deleted, after) = (
            ds.clone(),
            wf_started.clone(),
            wf_deleted.clone(),
            wf_after.clone(),
        );
        async move {
            ctx.step("first", || async { Ok::<_, Error>(1_i64) })
                .await?;
            started.notify_one();
            deleted.notified().await;
            let refused = ctx
                .transaction_on(&ds, "tx", async move |conn| {
                    sqlx::query("INSERT INTO effects (item) VALUES ('late')")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
                .await;
            assert!(refused.is_err(), "{refused:?}");
            ctx.step("after", || async {
                after.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(())
            })
            .await
        }
    });
    engine.launch().await?;

    let handle = engine
        .start::<(), ()>("wf", (), WorkflowOptions::with_id("run"))
        .await?;
    started.notified().await;
    engine.delete_workflows(&["run".to_owned()], false).await?;
    deleted.notify_one();
    let outcome = handle.await;
    assert!(
        matches!(&outcome, Err(Error::RecoveryRequired(cause))
            if matches!(**cause, Error::NonExistentWorkflow(_))),
        "{outcome:?}"
    );

    assert_eq!(
        after.load(Ordering::SeqCst),
        0,
        "no step ran past the refusal"
    );
    for table in ["operation_outputs", "effects"] {
        let rows: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&pool)
            .await?;
        assert_eq!(rows, 0, "{table} keeps nothing of the deleted run");
    }
    pool.close().await;
    common::remove_sqlite_files(&path);
    Ok(())
}
