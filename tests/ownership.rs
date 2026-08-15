//! Ownership: one workflow id, at most one live execution, one recorded
//! outcome — no matter how many processes race to start, recover, or
//! complete it.
//!
//! Regressions for two holes the DBOS SDKs close with `owner_xid` fencing:
//! recovery double-dispatch (two recoverers both re-running the same
//! pending workflow) and terminal-state overwrites (a completion landing on
//! a row that already reached a different terminal state).

use durare::{DurableContext, DurableEngine, SqliteProvider, StateProvider, WorkflowOptions};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

static SLOW_STEP_RUNS: AtomicU32 = AtomicU32::new(0);

fn unique_db(tag: &str) -> (std::path::PathBuf, String) {
    let db = std::env::temp_dir().join(format!(
        "durare-ownership-{tag}-{}.db",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after 1970")
            .as_nanos()
    ));
    let url = format!("sqlite://{}?mode=rwc", db.display());
    (db, url)
}

async fn engine_over(url: &str, executor: &str) -> DurableEngine {
    let provider = Arc::new(SqliteProvider::connect(url).await.expect("connect"));
    let mut b = DurableEngine::builder(provider);
    b.executor_id(executor);
    b.register("tracked", |ctx: DurableContext, _input: i32| async move {
        // Slow enough that two racing replays overlap inside the step —
        // neither has checkpointed it when the other starts.
        ctx.step("slow", || async {
            tokio::time::sleep(Duration::from_millis(400)).await;
            SLOW_STEP_RUNS.fetch_add(1, Ordering::SeqCst);
            Ok::<_, durare::Error>(1_i64)
        })
        .await?;
        Ok::<_, durare::Error>("done".to_string())
    });
    b.build().await.expect("engine builds")
}

/// Two processes recovering the same dead executor must re-dispatch each of
/// its pending workflows exactly once. Recovery has to *claim* a row, not
/// just observe it: a bump-and-spawn recovery hands the same workflow to
/// both callers, and the doubled run re-executes any step that had not yet
/// checkpointed.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_recovery_dispatches_a_run_once() {
    let (db, url) = unique_db("concurrent-recovery");

    // "Process 0" crashes mid-step: the row stays PENDING under executor
    // "crashed" with nothing checkpointed.
    let crashed = engine_over(&url, "crashed").await;
    let _h: durare::WorkflowHandle<String> = crashed
        .start("tracked", 7, WorkflowOptions::with_id("wf-once"))
        .await
        .expect("start");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let _ = crashed.shutdown(Duration::from_millis(100)).await;
    drop(crashed);
    let after_crash = SLOW_STEP_RUNS.load(Ordering::SeqCst);

    // Two live processes race to recover the dead one.
    let e1 = engine_over(&url, "rec-1").await;
    let e2 = engine_over(&url, "rec-2").await;
    let dead = vec!["crashed".to_string()];
    let (r1, r2) = tokio::join!(e1.recover_pending_for(&dead), e2.recover_pending_for(&dead));
    let (r1, r2) = (r1.expect("recover 1"), r2.expect("recover 2"));
    assert_eq!(
        r1.len() + r2.len(),
        1,
        "exactly one recoverer claims the run (got {} + {})",
        r1.len(),
        r2.len()
    );

    // Let the claimed run finish, then check the step ran exactly once more.
    let provider = Arc::new(SqliteProvider::connect(&url).await.expect("connect"));
    let mut settled = false;
    for _ in 0..250 {
        if let Some(w) = provider
            .get_workflow_status("wf-once")
            .await
            .expect("status")
        {
            if w.status == "SUCCESS" {
                settled = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(settled, "the recovered run completes");
    assert_eq!(
        SLOW_STEP_RUNS.load(Ordering::SeqCst) - after_crash,
        1,
        "the un-checkpointed step re-executes exactly once"
    );

    let _ = e1.shutdown(Duration::from_secs(2)).await;
    let _ = e2.shutdown(Duration::from_secs(2)).await;
    let _ = std::fs::remove_file(&db);
}

/// A SUCCESS/ERROR completion may only land on a PENDING row. A row that
/// already reached a terminal state — parked by the recovery-attempt cap,
/// cancelled, or completed by whoever won the race — keeps that state; the
/// late writer's update is a no-op, not an overwrite.
#[tokio::test(flavor = "multi_thread")]
async fn completion_only_lands_on_a_pending_row() {
    let (db, url) = unique_db("terminal-guard");
    let provider = Arc::new(SqliteProvider::connect(&url).await.expect("connect"));
    provider.init().await.expect("init");

    let row = durare::WorkflowStatus::new(
        "wf-parked",
        "tracked",
        serde_json::json!(1),
        "PENDING",
        "crashed",
        "v1",
    );
    provider.insert_workflow_status(row).await.expect("insert");
    // Park it: a recovery claim against an exhausted attempt cap.
    let claim = provider
        .claim_for_recovery(&durare::RecoveryClaimRequest {
            workflow_id: "wf-parked",
            expected_executor: "crashed",
            expected_attempts: 0,
            new_executor: "rec-1",
            max_attempts: 0,
            requeue: false,
        })
        .await
        .expect("claim");
    assert_eq!(claim, durare::RecoveryClaim::Parked { attempts: 1 });
    let parked = provider
        .get_workflow_status("wf-parked")
        .await
        .expect("status")
        .expect("row exists");
    assert_eq!(parked.status, "MAX_RECOVERY_ATTEMPTS_EXCEEDED");

    // A zombie execution finishing late must not resurrect the row.
    let _ = provider
        .set_workflow_status(
            "wf-parked",
            "SUCCESS",
            Some(&serde_json::json!("late")),
            None,
        )
        .await;
    let after = provider
        .get_workflow_status("wf-parked")
        .await
        .expect("status")
        .expect("row exists");
    assert_eq!(
        after.status, "MAX_RECOVERY_ATTEMPTS_EXCEEDED",
        "a completion cannot overwrite a parked row"
    );
    assert_eq!(after.output, None, "the late output is discarded");

    let _ = std::fs::remove_file(&db);
}

/// Two live executions checkpointing the same step position: the loser gets
/// [`durare::Error::WorkflowConflict`] and must stop, while a replay or retry
/// of the *same* write (identical content and start instant) converges on the
/// stored outcome without conflict.
#[tokio::test(flavor = "multi_thread")]
async fn losing_a_step_checkpoint_race_is_a_conflict() {
    let (db, url) = unique_db("step-conflict");
    let provider = Arc::new(SqliteProvider::connect(&url).await.expect("connect"));
    provider.init().await.expect("init");
    provider
        .insert_workflow_status(durare::WorkflowStatus::new(
            "wf-race",
            "tracked",
            serde_json::json!(1),
            "PENDING",
            "exec-a",
            "v1",
        ))
        .await
        .expect("insert");

    // First execution checkpoints step 0.
    provider
        .record_step_result(
            "wf-race",
            0,
            "charge",
            serde_json::json!({"receipt": "a"}),
            None,
            Some(1_000),
            Some("exec-a"),
        )
        .await
        .expect("first checkpoint");

    // The same logical write again (a replayed/retried checkpoint): adopted
    // without conflict.
    provider
        .record_step_result(
            "wf-race",
            0,
            "charge",
            serde_json::json!({"receipt": "a"}),
            None,
            Some(1_000),
            Some("exec-a"),
        )
        .await
        .expect("identical re-record is adopted");

    // A rival live execution produced a different result at the same position.
    let conflict = provider
        .record_step_result(
            "wf-race",
            0,
            "charge",
            serde_json::json!({"receipt": "b"}),
            None,
            Some(2_000),
            Some("exec-b"),
        )
        .await
        .expect_err("a divergent write at a recorded position is a conflict");
    assert!(
        matches!(conflict, durare::Error::WorkflowConflict(ref id) if id == "wf-race"),
        "expected WorkflowConflict, got {conflict:?}"
    );

    // A different *operation* at the same position is non-determinism, which
    // stays its own error.
    let renamed = provider
        .record_step_result(
            "wf-race",
            0,
            "refund",
            serde_json::json!({"receipt": "a"}),
            None,
            Some(1_000),
            Some("exec-b"),
        )
        .await
        .expect_err("a renamed step at a recorded position is non-determinism");
    assert!(matches!(renamed, durare::Error::UnexpectedStep { .. }));

    let _ = std::fs::remove_file(&db);
}

/// Recovery of a queued workflow releases it back to `ENQUEUED` — and only
/// one of two racing sweeps gets to do it; the loser's claim misses.
#[tokio::test(flavor = "multi_thread")]
async fn queued_row_recovery_requeues_exactly_once() {
    let (db, url) = unique_db("requeue-cas");
    let provider = Arc::new(SqliteProvider::connect(&url).await.expect("connect"));
    provider.init().await.expect("init");

    let mut row = durare::WorkflowStatus::new(
        "wf-queued",
        "tracked",
        serde_json::json!(1),
        "PENDING",
        "crashed",
        "v1",
    );
    row.queue_name = Some("q".to_string());
    provider.insert_workflow_status(row).await.expect("insert");

    let req = durare::RecoveryClaimRequest {
        workflow_id: "wf-queued",
        expected_executor: "crashed",
        expected_attempts: 0,
        new_executor: "rec-1",
        max_attempts: 10,
        requeue: true,
    };
    let first = provider.claim_for_recovery(&req).await.expect("claim");
    assert_eq!(first, durare::RecoveryClaim::Requeued);
    let second = provider.claim_for_recovery(&req).await.expect("claim");
    assert_eq!(
        second,
        durare::RecoveryClaim::Lost,
        "a rival sweep's identical claim must miss once the row left PENDING"
    );

    let row = provider
        .get_workflow_status("wf-queued")
        .await
        .expect("status")
        .expect("row exists");
    assert_eq!(row.status, "ENQUEUED");
    assert_eq!(row.recovery_attempts, 1, "exactly one claim counted");
    assert_eq!(
        row.queue_name.as_deref(),
        Some("q"),
        "the row keeps its queue"
    );

    let _ = std::fs::remove_file(&db);
}

/// A transactional step that loses its checkpoint race must roll back the
/// body's writes and classify the stored row: an identical write (same name,
/// content, start instant) converges, a divergent one is
/// [`durare::Error::WorkflowConflict`], and a renamed step stays
/// non-determinism. Before this classification existed the losing transaction
/// *committed* — applying the body's writes a second time.
#[tokio::test(flavor = "multi_thread")]
async fn losing_a_transaction_checkpoint_race_is_a_conflict() {
    use durare::TransactionOptions;
    use serde_json::json;

    let (db, url) = unique_db("txn-conflict");
    let provider = Arc::new(SqliteProvider::connect(&url).await.expect("connect"));
    provider.init().await.expect("init");
    // The rival executor: a separate pool over the same database file.
    let rival = sqlx::sqlite::SqlitePool::connect(&url)
        .await
        .expect("rival pool");
    sqlx::query("CREATE TABLE app_t (x TEXT NOT NULL)")
        .execute(&rival)
        .await
        .expect("create app table");
    provider
        .insert_workflow_status(durare::WorkflowStatus::new(
            "wf-txr",
            "t",
            json!(null),
            "PENDING",
            "e1",
            "v1",
        ))
        .await
        .expect("insert");
    let ser = provider.serializer();
    let app_rows = |pool: sqlx::SqlitePool| async move {
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM app_t")
            .fetch_one(&pool)
            .await
            .expect("count")
    };

    // Divergent rival at seq 0, planted from inside the body before this
    // transaction takes SQLite's write lock — indistinguishable from a rival
    // committing in the race window.
    let fmt = ser.name().to_string();
    let (rp, planted, fmtc) = (
        rival.clone(),
        ser.encode(&json!("rival")).expect("enc"),
        fmt.clone(),
    );
    let res = provider
        .run_transaction_step(
            "wf-txr",
            0,
            1_000,
            &TransactionOptions::new("racy"),
            Box::new(move |tx| {
                let (rp, planted, fmt) = (rp.clone(), planted.clone(), fmtc.clone());
                Box::pin(async move {
                    sqlx::query(
                        "INSERT INTO operation_outputs
                             (workflow_uuid, function_id, function_name, output, serialization,
                              started_at_epoch_ms, completed_at_epoch_ms)
                         VALUES ('wf-txr', 0, 'racy', ?, ?, 2000, 2000)",
                    )
                    .bind(&planted)
                    .bind(&fmt)
                    .execute(&rp)
                    .await
                    .map_err(durare::Error::from)?;
                    tx.execute("INSERT INTO app_t (x) VALUES ('mine')", &durare::params![])
                        .await?;
                    Ok(json!(42))
                })
            }),
        )
        .await;
    assert!(
        matches!(res, Err(durare::Error::WorkflowConflict(ref id)) if id == "wf-txr"),
        "a divergent rival write must be a conflict, got {res:?}"
    );
    assert_eq!(
        app_rows(rival.clone()).await,
        0,
        "the losing body's writes must roll back, not commit"
    );

    // Identical write at seq 1 (same name, content, and start instant): a
    // replay/retry of the same logical write — converges, no conflict. The
    // rolled-back attempt's own app write still must not commit.
    let (rp, planted, fmtc) = (
        rival.clone(),
        ser.encode(&json!(7)).expect("enc"),
        fmt.clone(),
    );
    let res = provider
        .run_transaction_step(
            "wf-txr",
            1,
            5_000,
            &TransactionOptions::new("same"),
            Box::new(move |tx| {
                let (rp, planted, fmt) = (rp.clone(), planted.clone(), fmtc.clone());
                Box::pin(async move {
                    sqlx::query(
                        "INSERT INTO operation_outputs
                             (workflow_uuid, function_id, function_name, output, serialization,
                              started_at_epoch_ms, completed_at_epoch_ms)
                         VALUES ('wf-txr', 1, 'same', ?, ?, 5000, 5000)",
                    )
                    .bind(&planted)
                    .bind(&fmt)
                    .execute(&rp)
                    .await
                    .map_err(durare::Error::from)?;
                    tx.execute("INSERT INTO app_t (x) VALUES ('mine2')", &durare::params![])
                        .await?;
                    Ok(json!(7))
                })
            }),
        )
        .await;
    assert!(
        matches!(res, Ok(ref v) if *v == json!(7)),
        "an identical write converges on the stored outcome, got {res:?}"
    );
    assert_eq!(
        app_rows(rival.clone()).await,
        0,
        "the converged attempt's writes still roll back"
    );

    // A different step name at the position stays non-determinism.
    let rp = rival.clone();
    let planted = ser.encode(&json!(1)).expect("enc");
    let fmtc = fmt.clone();
    let res = provider
        .run_transaction_step(
            "wf-txr",
            2,
            9_000,
            &TransactionOptions::new("renamed"),
            Box::new(move |_tx| {
                let (rp, planted, fmt) = (rp.clone(), planted.clone(), fmtc.clone());
                Box::pin(async move {
                    sqlx::query(
                        "INSERT INTO operation_outputs
                             (workflow_uuid, function_id, function_name, output, serialization,
                              started_at_epoch_ms, completed_at_epoch_ms)
                         VALUES ('wf-txr', 2, 'original', ?, ?, 9000, 9000)",
                    )
                    .bind(&planted)
                    .bind(&fmt)
                    .execute(&rp)
                    .await
                    .map_err(durare::Error::from)?;
                    Ok(json!(1))
                })
            }),
        )
        .await;
    assert!(
        matches!(res, Err(durare::Error::UnexpectedStep { .. })),
        "a renamed step at a recorded position is non-determinism, got {res:?}"
    );

    let _ = std::fs::remove_file(&db);
}
