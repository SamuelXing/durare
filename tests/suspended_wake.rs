//! A send wakes a receiver another SDK parked. With suspend mode on, the
//! reference SDKs park a waiting `recv` in the database: status `SUSPENDED`,
//! `suspend_key` its topic, `delay_until_epoch_ms` its timeout. A pass of
//! theirs re-enqueues the suspended runs that are due, and a send makes its
//! receiver due at once. durare never parks, but its sends reach such
//! receivers too. The in-memory backend is never shared.
use durare::{NotificationInsert, Result, StateProvider};
use serde_json::json;
mod common;

/// `parked` waits on `orders` and `idle` on the topic-less mailbox, both until
/// 9000. `running` is not suspended and only carries a stale key. `sender` is
/// the run that sends from a workflow.
const SEED: &str = "
    DELETE FROM notifications;
    DELETE FROM operation_outputs;
    DELETE FROM workflow_status;
    INSERT INTO workflow_status
        (workflow_uuid, name, status, created_at, updated_at, suspend_key, delay_until_epoch_ms)
    VALUES ('parked', 'wf', 'SUSPENDED', 1, 1, 'orders', 9000),
           ('idle', 'wf', 'SUSPENDED', 1, 1, '__null__topic__', 9000),
           ('running', 'wf', 'PENDING', 1, 1, 'orders', 9000),
           ('sender', 'wf', 'PENDING', 1, 1, NULL, NULL);";

fn row(destination: &str, topic: &str) -> NotificationInsert {
    NotificationInsert {
        destination_id: destination.to_owned(),
        topic: topic.to_owned(),
        message: json!("m"),
        idempotency_key: None,
    }
}

/// Every send path makes exactly the receivers parked on its topic due.
/// `run` executes a script such as [`SEED`]; `due` lists the runs whose wake
/// time is now zero.
async fn contract<R, RF, D, DF>(db: &dyn StateProvider, run: R, due: D) -> Result<()>
where
    R: Fn(&'static str) -> RF,
    RF: std::future::Future<Output = ()>,
    D: Fn() -> DF,
    DF: std::future::Future<Output = Vec<String>>,
{
    let seed = || run(SEED);
    seed().await;
    db.insert_notification("parked", "orders", json!("m"), None)
        .await?;
    assert_eq!(due().await, ["parked"], "a single send wakes its receiver");

    seed().await;
    db.insert_notification("parked", "invoices", json!("m"), None)
        .await?;
    db.insert_notification("running", "orders", json!("m"), None)
        .await?;
    assert!(
        due().await.is_empty(),
        "another topic or status wakes no one"
    );

    seed().await;
    db.insert_notifications(&[row("parked", "orders"), row("idle", "__null__topic__")])
        .await?;
    assert_eq!(
        due().await,
        ["idle", "parked"],
        "a batch wakes each receiver"
    );

    seed().await;
    db.send_from_workflow("sender", 0, "DBOS.send", &[row("parked", "orders")])
        .await?;
    assert_eq!(
        due().await,
        ["parked"],
        "a workflow's send wakes its receiver"
    );

    seed().await;
    let refused = db
        .insert_notifications(&[row("parked", "orders"), row("missing", "orders")])
        .await;
    assert!(refused.is_err(), "{refused:?}");
    assert!(due().await.is_empty(), "a refused batch wakes no one");

    // A parked run is live work whatever else its row says.
    seed().await;
    run("UPDATE workflow_status SET completed_at = 1").await;
    assert_eq!(db.garbage_collect(Some(2), None).await?, 0);
    Ok(())
}

const DUE: &str =
    "SELECT workflow_uuid FROM workflow_status WHERE delay_until_epoch_ms = 0 ORDER BY 1";

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_send_wakes_a_suspended_receiver() -> Result<()> {
    use durare::PostgresProvider;
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "suspended_wake").await;
    let result = async {
        let db = PostgresProvider::connect(&url).await?;
        db.init().await?;
        let pool = &common::pg_raw_pool(&url).await;
        let run = |script| async move {
            sqlx::raw_sql(script).execute(pool).await.unwrap();
        };
        let due = || async move { sqlx::query_scalar(DUE).fetch_all(pool).await.unwrap() };
        contract(&db, run, due).await?;

        // A receiver parking while the message is sent is woken too. It holds
        // its row as it parks, the insert waits on that row, and the wake,
        // a statement of its own, then sees the park.
        run(SEED).await;
        let mut parking = pool.begin().await?;
        sqlx::raw_sql(
            "SELECT 1 FROM workflow_status WHERE workflow_uuid = 'running' FOR UPDATE;
             UPDATE workflow_status SET status = 'SUSPENDED' WHERE workflow_uuid = 'running'",
        )
        .execute(&mut *parking)
        .await?;
        let send = db.insert_notification("running", "orders", json!("m"), None);
        let (sent, ()) = tokio::join!(send, common::commit_when_awaited(pool, parking));
        sent?;
        assert_eq!(due().await, ["running"], "a park during the send is woken");

        pool.close().await;
        Ok(())
    }
    .await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_send_wakes_a_suspended_receiver() -> Result<()> {
    use durare::SqliteProvider;
    let (url, path) = common::temp_db_url("suspended_wake");
    let db = SqliteProvider::connect(&url).await?;
    db.init().await?;
    let pool = &sqlx::SqlitePool::connect(&url).await?;
    let run = |script| async move {
        sqlx::raw_sql(script).execute(pool).await.unwrap();
    };
    let due = || async move { sqlx::query_scalar(DUE).fetch_all(pool).await.unwrap() };
    contract(&db, run, due).await?;
    pool.close().await;
    common::remove_sqlite_files(&path);
    Ok(())
}
