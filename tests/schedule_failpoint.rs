//! Crash-tolerance of the scheduler, exercised with a failpoint.
//!
//! This binary holds a single test because `fail`'s registry is process-global;
//! keeping it alone avoids cross-test interference. The `schedule_tick_after_persist`
//! failpoint lives in the schedule fire loop and is a no-op unless armed here.

use durust::{
    DurableContext, DurableEngine, Error, ListFilter, Result, ScheduleOptions, SqliteProvider,
    StateProvider, STATUS_PENDING, STATUS_SUCCESS,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

static WORK_RUNS: AtomicUsize = AtomicUsize::new(0);

fn temp_db_url(tag: &str) -> (String, std::path::PathBuf) {
    let mut p = std::env::temp_dir();
    p.push(format!("durust-{tag}-{}.db", uuid::Uuid::new_v4()));
    (format!("sqlite://{}", p.display()), p)
}

fn register_job(engine: &mut DurableEngine) {
    engine.register("sched_job", |ctx: DurableContext, _at: String| async move {
        ctx.step("do_work", || async {
            WORK_RUNS.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        })
        .await?;
        Ok::<_, Error>(())
    });
}

/// An abrupt failure of the scheduling process *after* a tick is persisted but
/// *before* it runs must not lose or duplicate the tick: recovery completes the
/// orphaned PENDING row exactly once.
#[tokio::test]
async fn scheduled_tick_survives_crash_before_run() -> Result<()> {
    let (url, path) = temp_db_url("sched-failpoint");

    // "Process 1": the fire loop persists one tick, then the armed failpoint
    // aborts the loop before the workflow runs — as if the executor died.
    {
        let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
        register_job(&mut engine);
        engine
            .create_schedule("tick", "sched_job", "* * * * * *", ScheduleOptions::new())
            .await?;

        fail::cfg("schedule_tick_after_persist", "return").expect("arm failpoint");
        engine.launch().await?;
        // Enough for the reconciler to install the loop and fire one tick.
        tokio::time::sleep(Duration::from_millis(2500)).await;
        engine.shutdown(Duration::from_secs(1)).await?;
        fail::remove("schedule_tick_after_persist");
    }

    // The tick was persisted but never executed: exactly one PENDING `sched-` row
    // and no work done.
    let provider = Arc::new(SqliteProvider::connect(&url).await?);
    let pending = provider
        .list_workflows(&ListFilter {
            workflow_id_prefix: Some("sched-".to_string()),
            ..Default::default()
        })
        .await?;
    assert_eq!(pending.len(), 1, "exactly one tick was persisted");
    assert_eq!(pending[0].status, STATUS_PENDING, "tick never ran");
    assert_eq!(
        WORK_RUNS.load(Ordering::SeqCst),
        0,
        "the workflow did not run before the crash"
    );
    let tick_id = pending[0].id.clone();

    // "Process 2": a fresh engine over the same database recovers the orphaned
    // tick and runs it to completion.
    {
        let mut engine = DurableEngine::new(provider.clone()).await?;
        register_job(&mut engine);
        let resumed = engine.recover().await?;
        assert!(resumed >= 1, "recovery picked up the orphaned tick");
    }

    assert_eq!(
        WORK_RUNS.load(Ordering::SeqCst),
        1,
        "the recovered tick ran exactly once"
    );
    let recovered = provider
        .get_workflow_status(&tick_id)
        .await?
        .expect("tick row");
    assert_eq!(
        recovered.status, STATUS_SUCCESS,
        "tick finished after recovery"
    );

    let _ = std::fs::remove_file(path);
    Ok(())
}
