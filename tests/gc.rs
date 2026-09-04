//! Garbage collection: the retention delete. History that *finished* strictly
//! before the resolved cutoff goes — steps, events, and streams with it — while
//! in-flight and still-queued work survives regardless of age. The cutoff is the
//! newer of the absolute bound and the `rows_threshold`-th-newest workflow's
//! `completed_at`, matching the other DBOS SDKs.
//!
//! The bound is `completed_at`, not `created_at`: a long-running workflow
//! created before the cutoff but finished after it has barely any history to
//! retain and must survive. That distinction is what
//! `long_running_workflow_survives_a_cutoff_after_its_creation` pins down.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result, WorkflowOptions,
    WorkflowQueue,
};
use std::sync::Arc;
use std::time::Duration;

mod common;

/// A cutoff far in the future: everything already created is "older".
fn far_future_ms() -> i64 {
    chrono::Utc::now().timestamp_millis() + 3_600_000
}

async fn all_ids(engine: &DurableEngine) -> Result<Vec<String>> {
    Ok(engine
        .list_workflows(&ListFilter::default())
        .await?
        .into_iter()
        .map(|w| w.id)
        .collect())
}

/// Seed a `workflow_status` row with exact `created_at` / `completed_at`
/// instants, through `import_workflow` so the same seed works on every backend.
/// `completed_at: None` leaves the row in flight.
async fn seed(
    engine: &DurableEngine,
    id: &str,
    status: &str,
    created_ms: i64,
    completed_ms: Option<i64>,
) -> Result<()> {
    let mut row = serde_json::Map::new();
    row.insert("workflow_uuid".into(), serde_json::json!(id));
    row.insert("status".into(), serde_json::json!(status));
    row.insert("name".into(), serde_json::json!("seeded"));
    row.insert("created_at".into(), serde_json::json!(created_ms));
    row.insert("updated_at".into(), serde_json::json!(created_ms));
    row.insert("completed_at".into(), serde_json::json!(completed_ms));
    // Import binds every column explicitly, so an absent `priority` becomes an
    // explicit NULL rather than falling back to SQLite's `NOT NULL DEFAULT 0`.
    row.insert("priority".into(), serde_json::json!(0));
    engine
        .import_workflow(&[durare::ExportedWorkflow {
            workflow_status: row,
            ..Default::default()
        }])
        .await
}

/// The behaviour that separates a `completed_at` bound from a `created_at` one.
///
/// `long-runner` is created before the cutoff and finishes after it: it holds
/// almost no history and must survive. Under the old `created_at` rule it was
/// deleted — a workflow could be collected moments after finishing, purely
/// because it started long ago.
///
/// `short-old` (created and finished before the cutoff) and `still-running`
/// (created before it, never finished) are the controls: the first must go, the
/// second must stay under any bound.
async fn assert_completed_at_is_the_bound(engine: &DurableEngine) -> Result<()> {
    const T0: i64 = 1_000_000; // creation, well before the cutoff
    const CUTOFF: i64 = 2_000_000;
    const T2: i64 = 3_000_000; // completion, after the cutoff

    seed(engine, "long-runner", "SUCCESS", T0, Some(T2)).await?;
    seed(engine, "short-old", "SUCCESS", T0, Some(T0 + 1)).await?;
    seed(engine, "still-running", "PENDING", T0, None).await?;

    let deleted = engine.garbage_collect(Some(CUTOFF), None).await?;
    assert_eq!(deleted, 1, "only the run that finished before the cutoff");

    let mut survivors = all_ids(engine).await?;
    survivors.sort();
    assert_eq!(
        survivors,
        vec!["long-runner", "still-running"],
        "a workflow created before the cutoff but finished after it must survive"
    );
    Ok(())
}

/// `rows_threshold` ranks by `completed_at`, not `created_at`. The two orders
/// are deliberately inverted here: the workflow created *first* finishes
/// *last*, so "keep the newest 1" keeps a different row under each rule.
async fn assert_threshold_ranks_by_completion(engine: &DurableEngine) -> Result<()> {
    // created first, finished last
    seed(engine, "slow-first", "SUCCESS", 1_000_000, Some(9_000_000)).await?;
    // created last, finished first
    seed(engine, "fast-second", "SUCCESS", 2_000_000, Some(3_000_000)).await?;

    let deleted = engine.garbage_collect(None, Some(1)).await?;
    assert_eq!(deleted, 1);
    assert_eq!(
        all_ids(engine).await?,
        vec!["slow-first"],
        "the newest by completion is kept, even though it was created first"
    );
    Ok(())
}

/// The GC predicate on the in-memory backend (the trait's default
/// implementation): terminal history goes, in-flight and queued work survives
/// any cutoff.
#[tokio::test]
async fn gc_deletes_terminal_history_and_spares_in_flight_work() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("done", |ctx: DurableContext, (): ()| async move {
        ctx.set_event("k", "v").await?;
        Ok::<_, Error>(())
    });
    engine.register("waiter", |ctx: DurableContext, (): ()| async move {
        ctx.recv::<String>("go", Duration::from_secs(30)).await?;
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("gc-parked"));
    engine.launch().await?;

    for n in 0..2 {
        engine
            .start::<(), ()>(
                "done",
                (),
                WorkflowOptions {
                    workflow_id: Some(format!("gc-done-{n}")),
                    ..Default::default()
                },
            )
            .await?
            .await?;
    }
    // ENQUEUED forever: nothing listens to this queue.
    engine
        .start::<(), ()>(
            "done",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-queued".into()),
                queue: Some("gc-parked".into()),
                ..Default::default()
            },
        )
        .await?;
    // PENDING: parked on a recv.
    let waiting = engine
        .start::<(), ()>(
            "waiter",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-pending".into()),
                ..Default::default()
            },
        )
        .await?;

    let deleted = engine.garbage_collect(Some(far_future_ms()), None).await?;
    assert_eq!(deleted, 2, "exactly the two terminal runs");
    let mut survivors = all_ids(&engine).await?;
    survivors.sort();
    assert_eq!(survivors, vec!["gc-pending", "gc-queued"]);

    // The survivor is intact and completes normally after collection.
    engine.send("gc-pending", "on".to_string(), "go").await?;
    waiting.await?;

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// `rows_threshold` on SQLite (the single-statement override): keep the N
/// newest, delete the rest — and the children of the deleted rows cascade.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_gc_rows_threshold_keeps_newest_and_cascades() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-gc-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("step-wf", |ctx: DurableContext, (): ()| async move {
        ctx.step("record", || async { Ok::<_, Error>(1) }).await?;
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    for n in 0..5 {
        engine
            .start::<(), ()>(
                "step-wf",
                (),
                WorkflowOptions {
                    workflow_id: Some(format!("gc-th-{n}")),
                    ..Default::default()
                },
            )
            .await?
            .await?;
        // The threshold cutoff compares `completed_at` in milliseconds; keep the
        // five runs on distinct timestamps so "the 2 newest" is well-defined.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let deleted = engine.garbage_collect(None, Some(2)).await?;
    assert_eq!(deleted, 3, "all but the 2 newest");
    let mut survivors = all_ids(&engine).await?;
    survivors.sort();
    assert_eq!(survivors, vec!["gc-th-3", "gc-th-4"]);

    // The deleted workflows' step checkpoints cascaded away with them.
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    let orphans: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM operation_outputs WHERE workflow_uuid = 'gc-th-0'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(orphans, 0, "cascade removed the step rows");
    pool.close().await;

    engine.shutdown(Duration::from_secs(2)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// With both bounds given, the more restrictive (newer) cutoff wins: a
/// harmless absolute cutoff plus `rows_threshold = 1` still trims to the
/// single newest workflow.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_gc_more_restrictive_bound_wins() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-gc-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    for n in 0..3 {
        engine
            .start::<(), ()>(
                "noop",
                (),
                WorkflowOptions {
                    workflow_id: Some(format!("gc-mix-{n}")),
                    ..Default::default()
                },
            )
            .await?
            .await?;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Absolute cutoff 0 would delete nothing on its own; the rows bound is
    // newer and takes precedence.
    let deleted = engine.garbage_collect(Some(0), Some(1)).await?;
    assert_eq!(deleted, 2);
    assert_eq!(all_ids(&engine).await?, vec!["gc-mix-2"]);

    engine.shutdown(Duration::from_secs(2)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// The edges: no bounds is a no-op, a non-positive threshold is an error, and
/// a threshold larger than the table (with no absolute cutoff) collects
/// nothing.
#[tokio::test]
async fn gc_edge_cases() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    engine
        .start::<(), ()>("noop", (), WorkflowOptions::default())
        .await?
        .await?;

    assert_eq!(engine.garbage_collect(None, None).await?, 0, "no bounds");
    assert!(engine.garbage_collect(None, Some(0)).await.is_err());
    assert!(engine.garbage_collect(None, Some(-3)).await.is_err());
    assert_eq!(
        engine.garbage_collect(None, Some(100)).await?,
        0,
        "fewer rows than the threshold"
    );
    assert_eq!(all_ids(&engine).await?.len(), 1, "nothing was collected");

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// The Postgres override, end to end in a hermetic database: cutoff
/// semantics, survivors, and step-row cascade.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_gc_deletes_terminal_history_and_cascades() -> Result<()> {
    use durare::PostgresProvider;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_gc_deletes_terminal_history_and_cascades: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_gc").await;

    let mut engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
    engine.register("step-wf", |ctx: DurableContext, (): ()| async move {
        ctx.step("record", || async { Ok::<_, Error>(1) }).await?;
        Ok::<_, Error>(())
    });
    engine.register_queue(WorkflowQueue::new("gc-parked"));
    engine.launch().await?;

    engine
        .start::<(), ()>(
            "step-wf",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-pg-done".into()),
                ..Default::default()
            },
        )
        .await?
        .await?;
    engine
        .start::<(), ()>(
            "step-wf",
            (),
            WorkflowOptions {
                workflow_id: Some("gc-pg-queued".into()),
                queue: Some("gc-parked".into()),
                ..Default::default()
            },
        )
        .await?;

    let deleted = engine.garbage_collect(Some(far_future_ms()), None).await?;
    assert_eq!(deleted, 1, "the completed run only");
    assert_eq!(all_ids(&engine).await?, vec!["gc-pg-queued"]);

    let pool = sqlx::postgres::PgPool::connect(&url).await.unwrap();
    let orphans: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM dbos.operation_outputs WHERE workflow_uuid = 'gc-pg-done'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(orphans, 0, "cascade removed the step rows");
    pool.close().await;

    engine.shutdown(Duration::from_secs(2)).await?;
    drop(engine);
    common::drop_hermetic_pg_db(&admin, &dbname).await;
    Ok(())
}

#[tokio::test]
async fn long_running_workflow_survives_a_cutoff_after_its_creation() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    assert_completed_at_is_the_bound(&engine).await
}

#[tokio::test]
async fn rows_threshold_ranks_by_completion_not_creation() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    assert_threshold_ranks_by_completion(&engine).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_long_running_workflow_survives_a_cutoff_after_its_creation() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-gc-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    assert_completed_at_is_the_bound(&engine).await?;
    assert_threshold_ranks_by_completion_after_reset(&engine).await?;

    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// The threshold assertion, run after the cutoff assertion has already left two
/// rows behind: clear them first so "keep the newest 1" is unambiguous.
async fn assert_threshold_ranks_by_completion_after_reset(engine: &DurableEngine) -> Result<()> {
    let existing = all_ids(engine).await?;
    engine.delete_workflows(&existing, false).await?;
    assert_threshold_ranks_by_completion(engine).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_long_running_workflow_survives_a_cutoff_after_its_creation() -> Result<()> {
    use durare::PostgresProvider;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_long_running_workflow_survives_a_cutoff_after_its_creation: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_gc_completed").await;

    let engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
    assert_completed_at_is_the_bound(&engine).await?;
    assert_threshold_ranks_by_completion_after_reset(&engine).await?;

    drop(engine);
    common::drop_hermetic_pg_db(&admin, &dbname).await;
    Ok(())
}

/// The retention knob end to end: a configured policy sweeps automatically
/// after launch — terminal history goes, in-flight work survives, the
/// collected count shows up in the metrics — and shutdown stops the sweeper.
#[tokio::test]
async fn retention_policy_sweeps_automatically() -> Result<()> {
    use durare::{EngineConfig, RetentionPolicy};

    let config = EngineConfig::default().retention(
        RetentionPolicy::new()
            .period(Duration::ZERO) // everything already-created is collectable
            .sweep_interval(Duration::from_millis(200)),
    );
    let mut engine = DurableEngine::with_config(Arc::new(InMemoryProvider::new()), config).await?;
    engine.register("done", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.register("waiter", |ctx: DurableContext, (): ()| async move {
        ctx.recv::<String>("go", Duration::from_secs(30)).await?;
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    engine
        .start::<(), ()>(
            "done",
            (),
            WorkflowOptions {
                workflow_id: Some("swept".into()),
                ..Default::default()
            },
        )
        .await?
        .await?;
    let waiting = engine
        .start::<(), ()>(
            "waiter",
            (),
            WorkflowOptions {
                workflow_id: Some("survivor".into()),
                ..Default::default()
            },
        )
        .await?;

    // Give the sweeper a few intervals to fire.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if all_ids(&engine).await? == vec!["survivor"] {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "sweep did not collect within 5s: {:?}",
            all_ids(&engine).await?
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(engine.metrics().await?.workflows_collected_total >= 1);

    // The in-flight survivor still completes normally.
    engine.send("survivor", "on".to_string(), "go").await?;
    waiting.await?;

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// A policy with neither bound is a configuration mistake, rejected at launch.
#[tokio::test]
async fn boundless_retention_policy_is_rejected() -> Result<()> {
    use durare::{EngineConfig, RetentionPolicy};

    let config = EngineConfig::default().retention(RetentionPolicy::new());
    let engine = DurableEngine::with_config(Arc::new(InMemoryProvider::new()), config).await?;
    let err = engine.launch().await.expect_err("boundless policy");
    assert!(err.to_string().contains("retention policy"), "{err}");
    Ok(())
}

/// Collection over a backlog larger than one delete batch (10k rows) loops
/// until done: 12k seeded terminal rows go in two batches, with the full
/// count returned.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_gc_batches_large_backlogs() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-gc-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    // Engine construction runs the migrations; the backlog is seeded raw.
    // `completed_at` is seeded alongside `created_at` because collection is
    // keyed on it — a terminal row without one is not collectable, which would
    // make this a test of nothing. The point being pinned is unchanged: 12,000
    // rows exceed the 10,000-row batch, so the delete must loop and still
    // report the full count.
    let engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    let pool = sqlx::sqlite::SqlitePool::connect(&url).await.unwrap();
    sqlx::query(
        "WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 12000)
         INSERT INTO workflow_status
             (workflow_uuid, status, name, created_at, updated_at, completed_at)
         SELECT 'seed-' || n, 'SUCCESS', 'seeded', 1000 + n, 1000 + n, 1000 + n FROM seq",
    )
    .execute(&pool)
    .await
    .unwrap();

    let deleted = engine.garbage_collect(Some(far_future_ms()), None).await?;
    assert_eq!(deleted, 12_000, "both batches counted");
    let left: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM workflow_status")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(left, 0);
    pool.close().await;

    let _ = std::fs::remove_file(&path);
    Ok(())
}
