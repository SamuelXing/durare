//! Durable transactions on a separate application database
//! (`ctx.transaction_on`): the body's writes and a `transaction_completion`
//! witness row commit atomically on the app database, the checkpoint goes to
//! the system database, and recovery replays in layers — checkpoint first,
//! then the completion row — so the body runs exactly once even across the
//! crash window between the two commits.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, Serializer, SqliteDataSource,
    TransactionOptions, WorkflowOptions,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

mod common;

/// A fresh SQLite application database (a real file — the pool may open more
/// than one connection) with an `orders` table, plus its `SqliteDataSource`.
async fn sqlite_app_db() -> (SqliteDataSource, sqlx::SqlitePool) {
    let mut path = std::env::temp_dir();
    path.push(format!("durare-ds-{}.db", uuid::Uuid::new_v4()));
    let pool = sqlx::sqlite::SqlitePool::connect(&format!("sqlite://{}?mode=rwc", path.display()))
        .await
        .expect("open sqlite app db");
    sqlx::query("CREATE TABLE orders (item TEXT NOT NULL)")
        .execute(&pool)
        .await
        .expect("create app table");
    let ds = SqliteDataSource::new(pool.clone())
        .await
        .expect("create datasource");
    (ds, pool)
}

async fn order_count(pool: &sqlx::SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM orders")
        .fetch_one(pool)
        .await
        .unwrap()
}

/// Happy path: the body's insert and the completion row commit together; the
/// output round-trips; starting the same workflow id again replays without
/// running the body.
#[tokio::test]
async fn commits_writes_with_witness_row_exactly_once() -> Result<()> {
    let (ds, pool) = sqlite_app_db().await;
    let runs = Arc::new(AtomicU32::new(0));

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let (wf_ds, wf_runs) = (ds.clone(), runs.clone());
    engine.register("order", move |ctx: DurableContext, item: String| {
        let (ds, runs) = (wf_ds.clone(), wf_runs.clone());
        async move {
            ctx.transaction_on(&ds, "record-order", move |conn| {
                let (item, runs) = (item.clone(), runs.clone());
                Box::pin(async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    sqlx::query("INSERT INTO orders(item) VALUES (?)")
                        .bind(&item)
                        .execute(&mut *conn)
                        .await?;
                    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
                        .fetch_one(&mut *conn)
                        .await?;
                    Ok(n)
                })
            })
            .await
        }
    });
    engine.launch().await?;

    let n: i64 = engine
        .start::<String, i64>(
            "order",
            "widget".into(),
            WorkflowOptions::with_id("ds-once"),
        )
        .await?
        .await?;
    assert_eq!(n, 1);
    assert_eq!(order_count(&pool).await, 1);

    // The witness row is in the application database, keyed by workflow+step.
    let (output, error): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT output, error FROM transaction_completion WHERE workflow_id = 'ds-once' AND step_id = 0",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(output.is_some(), "success row stores the output");
    assert!(error.is_none());

    // Starting the same workflow id again returns the recorded result without
    // re-running anything.
    let again: i64 = engine
        .start::<String, i64>(
            "order",
            "widget".into(),
            WorkflowOptions::with_id("ds-once"),
        )
        .await?
        .await?;
    assert_eq!(again, 1);
    assert_eq!(runs.load(Ordering::SeqCst), 1, "body ran exactly once");
    assert_eq!(order_count(&pool).await, 1);
    Ok(())
}

/// The crash window: the application transaction committed (completion row
/// present) but the system checkpoint was never written. The next execution
/// replays the stored output without running the body — layer-2 recovery.
#[tokio::test]
async fn completion_row_replays_without_rerunning_the_body() -> Result<()> {
    let (ds, pool) = sqlite_app_db().await;
    let runs = Arc::new(AtomicU32::new(0));

    // A prior incarnation committed the app transaction for step 0 of
    // "ds-window" and crashed before the checkpoint: plant its witness row,
    // encoded the way the engine's serializer would have written it.
    let ser = Serializer::Json;
    sqlx::query(
        "INSERT INTO transaction_completion (workflow_id, step_id, output, error, serialization, created_at)
         VALUES ('ds-window', 0, ?, NULL, ?, 0)",
    )
    .bind(ser.encode(&serde_json::json!(41))?)
    .bind(ser.name())
    .execute(&pool)
    .await
    .unwrap();

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let (wf_ds, wf_runs) = (ds.clone(), runs.clone());
    engine.register("window", move |ctx: DurableContext, (): ()| {
        let (ds, runs) = (wf_ds.clone(), wf_runs.clone());
        async move {
            let n: i64 = ctx
                .transaction_on(&ds, "record-order", move |conn| {
                    let runs = runs.clone();
                    Box::pin(async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        sqlx::query("INSERT INTO orders(item) VALUES ('should-not-run')")
                            .execute(&mut *conn)
                            .await?;
                        Ok(0)
                    })
                })
                .await?;
            Ok::<_, Error>(n)
        }
    });
    engine.launch().await?;

    let n: i64 = engine
        .start::<(), i64>("window", (), WorkflowOptions::with_id("ds-window"))
        .await?
        .await?;
    assert_eq!(n, 41, "the stored output replayed");
    assert_eq!(runs.load(Ordering::SeqCst), 0, "the body never ran");
    assert_eq!(order_count(&pool).await, 0, "no new writes");
    Ok(())
}

/// A permanent failure rolls back the body's writes, mirrors the error into
/// the completion table, and replays as the same error: a second workflow
/// given the first one's mirrored row surfaces the error without running.
#[tokio::test]
async fn failure_rolls_back_mirrors_and_replays() -> Result<()> {
    let (ds, pool) = sqlite_app_db().await;
    let runs = Arc::new(AtomicU32::new(0));

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let (wf_ds, wf_runs) = (ds.clone(), runs.clone());
    engine.register("doomed", move |ctx: DurableContext, (): ()| {
        let (ds, runs) = (wf_ds.clone(), wf_runs.clone());
        async move {
            ctx.transaction_on(&ds, "doomed-tx", move |conn| {
                let runs = runs.clone();
                Box::pin(async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    sqlx::query("INSERT INTO orders(item) VALUES ('rolled-back')")
                        .execute(&mut *conn)
                        .await?;
                    Err::<i64, _>(Error::app("boom"))
                })
            })
            .await
        }
    });
    engine.launch().await?;

    let err = engine
        .start::<(), i64>("doomed", (), WorkflowOptions::with_id("ds-fail"))
        .await?
        .await
        .expect_err("body error propagates");
    assert!(err.to_string().contains("boom"), "{err}");
    assert_eq!(order_count(&pool).await, 0, "the insert rolled back");

    // The failure was mirrored into the application database.
    let (output, error): (Option<String>, Option<String>) = sqlx::query_as(
        "SELECT output, error FROM transaction_completion WHERE workflow_id = 'ds-fail' AND step_id = 0",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(output.is_none());
    let error = error.expect("failure row stores the error");

    // Layer-2 error replay: plant that row under a fresh workflow id whose
    // body would succeed — the stored failure wins and the body never runs.
    sqlx::query(
        "INSERT INTO transaction_completion (workflow_id, step_id, output, error, serialization, created_at)
         VALUES ('ds-fail-replay', 0, NULL, ?, ?, 0)",
    )
    .bind(&error)
    .bind(Serializer::Json.name())
    .execute(&pool)
    .await
    .unwrap();
    let before = runs.load(Ordering::SeqCst);
    let err = engine
        .start::<(), i64>("doomed", (), WorkflowOptions::with_id("ds-fail-replay"))
        .await?
        .await
        .expect_err("stored failure replays");
    assert!(err.to_string().contains("boom"), "{err}");
    assert_eq!(runs.load(Ordering::SeqCst), before, "the body never re-ran");
    Ok(())
}

/// The application-error retry policy: the body re-runs on a fresh
/// transaction per attempt, failed attempts leave no writes behind, and only
/// the final success commits.
#[tokio::test]
async fn retry_policy_reruns_on_fresh_transactions() -> Result<()> {
    let (ds, pool) = sqlite_app_db().await;
    let attempts = Arc::new(AtomicU32::new(0));

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let (wf_ds, wf_attempts) = (ds.clone(), attempts.clone());
    engine.register("flaky", move |ctx: DurableContext, (): ()| {
        let (ds, attempts) = (wf_ds.clone(), wf_attempts.clone());
        async move {
            let opts = TransactionOptions::new("flaky-tx")
                .max_retries(3)
                .base_interval(std::time::Duration::from_millis(1));
            ctx.transaction_on_with(&ds, opts, move |conn| {
                let attempts = attempts.clone();
                Box::pin(async move {
                    let n = attempts.fetch_add(1, Ordering::SeqCst);
                    sqlx::query("INSERT INTO orders(item) VALUES ('attempt')")
                        .execute(&mut *conn)
                        .await?;
                    if n < 2 {
                        return Err(Error::app("transient"));
                    }
                    Ok(())
                })
            })
            .await
        }
    });
    engine.launch().await?;

    engine
        .start::<(), ()>("flaky", (), WorkflowOptions::with_id("ds-flaky"))
        .await?
        .await?;
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        3,
        "two failures, then success"
    );
    assert_eq!(
        order_count(&pool).await,
        1,
        "rolled-back attempts left nothing"
    );
    Ok(())
}

/// Nesting is refused up front, like the single-database transactional step.
#[tokio::test]
async fn nested_transaction_is_rejected() -> Result<()> {
    let (ds, _pool) = sqlite_app_db().await;

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let wf_ds = ds.clone();
    engine.register("nested", move |ctx: DurableContext, (): ()| {
        let ds = wf_ds.clone();
        async move {
            let inner_ctx = ctx.clone();
            let inner_ds = ds.clone();
            ctx.transaction_on(&ds, "outer", move |_conn| {
                let (ctx, ds) = (inner_ctx.clone(), inner_ds.clone());
                Box::pin(async move {
                    ctx.transaction_on(&ds, "inner", |_conn| Box::pin(async move { Ok(()) }))
                        .await
                })
            })
            .await
        }
    });
    engine.launch().await?;

    let err = engine
        .start::<(), ()>("nested", (), WorkflowOptions::with_id("ds-nested"))
        .await?
        .await
        .expect_err("nested transaction");
    assert!(
        err.to_string()
            .contains("cannot start a transaction inside another transaction"),
        "{err}"
    );
    Ok(())
}

/// Postgres end to end in a hermetic database: schema-qualified witness table,
/// custom schema support, hostile schema names rejected, and the raw-COMMIT
/// guard.
#[tokio::test]
async fn pg_datasource_end_to_end() -> Result<()> {
    use durare::PgDataSource;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_datasource_end_to_end: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_ds").await;
    let pool = sqlx::postgres::PgPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE orders (item TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    // A hostile schema name is rejected before any SQL runs.
    let hostile = PgDataSource::with_schema(pool.clone(), "bad\"; DROP TABLE orders; --").await;
    assert!(hostile.is_err(), "hostile schema name must be rejected");

    // A custom (plain-identifier) schema works and holds the witness table.
    let ds = PgDataSource::with_schema(pool.clone(), "app_durable").await?;

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let wf_ds = ds.clone();
    engine.register("order", move |ctx: DurableContext, (): ()| {
        let ds = wf_ds.clone();
        async move {
            ctx.transaction_on(&ds, "record-order", |conn| {
                Box::pin(async move {
                    sqlx::query("INSERT INTO orders(item) VALUES ($1)")
                        .bind("widget")
                        .execute(&mut *conn)
                        .await?;
                    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM orders")
                        .fetch_one(&mut *conn)
                        .await?;
                    Ok(n)
                })
            })
            .await
        }
    });
    let guard_ds = ds.clone();
    engine.register("escape", move |ctx: DurableContext, (): ()| {
        let ds = guard_ds.clone();
        async move {
            ctx.transaction_on(&ds, "escape-tx", |conn| {
                Box::pin(async move {
                    // A body must not end durare's transaction; this one does.
                    sqlx::query("COMMIT").execute(&mut *conn).await?;
                    Ok(0i64)
                })
            })
            .await
        }
    });
    engine.launch().await?;

    let n: i64 = engine
        .start::<(), i64>("order", (), WorkflowOptions::with_id("pg-ds-1"))
        .await?
        .await?;
    assert_eq!(n, 1);
    let witnesses: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM \"app_durable\".transaction_completion WHERE workflow_id = 'pg-ds-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(witnesses, 1, "witness row lives in the custom schema");

    // The raw-COMMIT guard: detected, refused, surfaced as the step's error.
    let err = engine
        .start::<(), i64>("escape", (), WorkflowOptions::with_id("pg-ds-escape"))
        .await?
        .await
        .expect_err("raw COMMIT inside the body");
    assert!(
        err.to_string()
            .contains("terminated the surrounding database transaction"),
        "{err}"
    );

    engine.shutdown(std::time::Duration::from_secs(5)).await?;
    pool.close().await;
    common::drop_hermetic_pg_db(&admin, &dbname).await;
    Ok(())
}

/// The system-datasource fast path: writes and checkpoint in one commit on the
/// system database's own pool, no witness table, native connection, replay
/// from the ordinary checkpoint.
#[tokio::test]
async fn system_datasource_single_commit_without_witness_table() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-sysds-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());
    let provider = SqliteProvider::connect(&url).await?;
    let ds = provider.system_datasource();
    let pool = ds.pool().clone();
    let runs = Arc::new(AtomicU32::new(0));

    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    sqlx::query("CREATE TABLE sys_orders (item TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let (wf_ds, wf_runs) = (ds.clone(), runs.clone());
    engine.register("sys-order", move |ctx: DurableContext, (): ()| {
        let (ds, runs) = (wf_ds.clone(), wf_runs.clone());
        async move {
            ctx.transaction_on(&ds, "sys-tx", move |conn| {
                let runs = runs.clone();
                Box::pin(async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    sqlx::query("INSERT INTO sys_orders(item) VALUES ('gear')")
                        .execute(&mut *conn)
                        .await?;
                    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sys_orders")
                        .fetch_one(&mut *conn)
                        .await?;
                    Ok(n)
                })
            })
            .await
        }
    });
    engine.launch().await?;

    let n: i64 = engine
        .start::<(), i64>("sys-order", (), WorkflowOptions::with_id("sysds-1"))
        .await?
        .await?;
    assert_eq!(n, 1);

    // No witness table was ever created — the fast path doesn't need one.
    let witness_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'transaction_completion'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(witness_tables, 0, "fast path creates no witness table");

    // The checkpoint committed with the writes, as an ordinary step row.
    let checkpoints: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM operation_outputs
         WHERE workflow_uuid = 'sysds-1' AND function_name = 'sys-tx' AND output IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(checkpoints, 1);

    // Replay: same workflow id returns the recorded result, body untouched.
    let again: i64 = engine
        .start::<(), i64>("sys-order", (), WorkflowOptions::with_id("sysds-1"))
        .await?
        .await?;
    assert_eq!(again, 1);
    assert_eq!(runs.load(Ordering::SeqCst), 1, "body ran exactly once");
    Ok(())
}

/// Fast-path failure: the body's writes roll back with nothing half-committed,
/// the error is recorded as an ordinary step failure, and a replay returns the
/// same error without re-running the body.
#[tokio::test]
async fn system_datasource_failure_rolls_back_and_replays() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-sysds-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());
    let provider = SqliteProvider::connect(&url).await?;
    let ds = provider.system_datasource();
    let pool = ds.pool().clone();
    let runs = Arc::new(AtomicU32::new(0));

    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    sqlx::query("CREATE TABLE sys_orders (item TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let (wf_ds, wf_runs) = (ds.clone(), runs.clone());
    engine.register("sys-doomed", move |ctx: DurableContext, (): ()| {
        let (ds, runs) = (wf_ds.clone(), wf_runs.clone());
        async move {
            ctx.transaction_on(&ds, "sys-doomed-tx", move |conn| {
                let runs = runs.clone();
                Box::pin(async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    sqlx::query("INSERT INTO sys_orders(item) VALUES ('rolled-back')")
                        .execute(&mut *conn)
                        .await?;
                    Err::<i64, _>(Error::app("sys-boom"))
                })
            })
            .await
        }
    });
    engine.launch().await?;

    let err = engine
        .start::<(), i64>("sys-doomed", (), WorkflowOptions::with_id("sysds-fail"))
        .await?
        .await
        .expect_err("body error propagates");
    assert!(err.to_string().contains("sys-boom"), "{err}");
    let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sys_orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows, 0, "the insert rolled back");

    let before = runs.load(Ordering::SeqCst);
    let err = engine
        .start::<(), i64>("sys-doomed", (), WorkflowOptions::with_id("sysds-fail"))
        .await?
        .await
        .expect_err("recorded failure replays");
    assert!(err.to_string().contains("sys-boom"), "{err}");
    assert_eq!(runs.load(Ordering::SeqCst), before, "body never re-ran");
    Ok(())
}

/// Postgres fast path, end to end: the motivating case — rich Postgres types
/// (jsonb, bigint arrays) bound natively in a single-commit durable
/// transaction — plus no witness table anywhere in the database.
#[tokio::test]
async fn pg_system_datasource_rich_types_single_commit() -> Result<()> {
    use durare::PostgresProvider;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_system_datasource_rich_types_single_commit: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_sysds").await;
    let provider = PostgresProvider::connect(&url).await?;
    let ds = provider.system_datasource();
    let pool = ds.pool().clone();

    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    sqlx::query("CREATE TABLE sys_orders (meta JSONB NOT NULL, tags BIGINT[] NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();

    let wf_ds = ds.clone();
    engine.register("sys-rich", move |ctx: DurableContext, (): ()| {
        let ds = wf_ds.clone();
        async move {
            ctx.transaction_on(&ds, "sys-rich-tx", |conn| {
                Box::pin(async move {
                    // Types Param cannot express, bound natively.
                    sqlx::query("INSERT INTO sys_orders(meta, tags) VALUES ($1, $2)")
                        .bind(serde_json::json!({"reason": "fee", "amount": 42}))
                        .bind(vec![7i64, 11, 13])
                        .execute(&mut *conn)
                        .await?;
                    let amount: i64 =
                        sqlx::query_scalar("SELECT (meta->>'amount')::bigint FROM sys_orders")
                            .fetch_one(&mut *conn)
                            .await?;
                    Ok(amount)
                })
            })
            .await
        }
    });
    engine.launch().await?;

    let amount: i64 = engine
        .start::<(), i64>("sys-rich", (), WorkflowOptions::with_id("pg-sysds-1"))
        .await?
        .await?;
    assert_eq!(
        amount, 42,
        "jsonb round-tripped through the native connection"
    );

    let tags: Vec<i64> = sqlx::query_scalar("SELECT tags FROM sys_orders")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(tags, vec![7, 11, 13], "array round-tripped");

    // No witness table anywhere: the fast path never creates one.
    let witness_tables: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'transaction_completion'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(witness_tables, 0, "fast path creates no witness table");

    // The checkpoint is an ordinary step row, committed with the writes.
    let checkpoints: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM operation_outputs
         WHERE workflow_uuid = 'pg-sysds-1' AND function_name = 'sys-rich-tx' AND output IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(checkpoints, 1);

    engine.shutdown(std::time::Duration::from_secs(5)).await?;
    pool.close().await;
    common::drop_hermetic_pg_db(&admin, &dbname).await;
    Ok(())
}
