//! Deleting a run deletes its steps without the database's help. Peers at
//! shared migration 112 or later drop the foreign key from `operation_outputs`
//! to `workflow_status`, and a durare process sharing that database must not
//! leave a deleted run's checkpoints behind for a later run of the same id.
//! The in-memory backend has no foreign key to lose.
use durare::{Result, StateProvider};
mod common;

/// Three finished runs, each with one step: `old` and `parent` -> `child`.
const SEED: &str = "
    INSERT INTO workflow_status (workflow_uuid, name, status, created_at, updated_at, completed_at, parent_workflow_id)
    VALUES ('old', 'wf', 'SUCCESS', 1, 1, 1, NULL),
           ('parent', 'wf', 'SUCCESS', 1, 1, 1, NULL),
           ('child', 'wf', 'SUCCESS', 1, 1, 1, 'parent');
    INSERT INTO operation_outputs (workflow_uuid, function_id, function_name)
    VALUES ('old', 0, 'step'), ('parent', 0, 'step'), ('child', 0, 'step');";

/// Every delete path leaves no steps behind for the runs it deleted. `seed`
/// writes [`SEED`]; `steps` lists the ids that still have a step.
async fn contract<S, SF, L, LF>(db: &dyn StateProvider, seed: S, steps: L) -> Result<()>
where
    S: Fn() -> SF,
    SF: std::future::Future<Output = ()>,
    L: Fn() -> LF,
    LF: std::future::Future<Output = Vec<String>>,
{
    seed().await;
    db.delete_workflows(&["old".to_owned()], false).await?;
    assert_eq!(steps().await, ["child", "parent"]);
    db.delete_workflows(&["parent".to_owned()], true).await?;
    assert!(steps().await.is_empty(), "a deleted tree keeps no steps");

    seed().await;
    assert_eq!(db.garbage_collect(Some(2), None).await?, 3);
    assert!(steps().await.is_empty(), "collected runs keep no steps");
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
        let pool = common::pg_raw_pool(&url).await;
        sqlx::raw_sql(
            "ALTER TABLE operation_outputs DROP CONSTRAINT operation_outputs_workflow_uuid_fkey",
        )
        .execute(&pool)
        .await?;
        let seed = || async {
            sqlx::raw_sql(SEED).execute(&pool).await.unwrap();
        };
        let steps = || async {
            sqlx::query_scalar("SELECT workflow_uuid FROM operation_outputs ORDER BY 1")
                .fetch_all(&pool)
                .await
                .unwrap()
        };
        contract(&db, seed, steps).await?;
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
    let pool = sqlx::SqlitePool::connect(&url).await?;
    // SQLite cannot drop a constraint in place; rebuild the table without it.
    sqlx::raw_sql(
        "CREATE TABLE operation_outputs_new AS SELECT * FROM operation_outputs;
         DROP TABLE operation_outputs;
         ALTER TABLE operation_outputs_new RENAME TO operation_outputs;
         CREATE UNIQUE INDEX operation_outputs_key ON operation_outputs (workflow_uuid, function_id);",
    )
    .execute(&pool)
    .await?;
    let seed = || async {
        sqlx::raw_sql(SEED).execute(&pool).await.unwrap();
    };
    let steps = || async {
        sqlx::query_scalar("SELECT workflow_uuid FROM operation_outputs ORDER BY 1")
            .fetch_all(&pool)
            .await
            .unwrap()
    };
    contract(&db, seed, steps).await?;
    pool.close().await;
    common::remove_sqlite_files(&path);
    Ok(())
}
