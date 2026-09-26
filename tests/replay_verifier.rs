//! `verify_replay` re-runs a recorded workflow against the current code and
//! reports the first durable operation the two disagree about.
//!
//! Every test here changes the code the way a deploy would: the workflow runs
//! under one engine, and a **second** engine over the same provider registers a
//! different function under the same name. The report is what the first engine's
//! history says about the second engine's code.

use durare::{
    params, Divergence, DurableContext, DurableEngine, Error, InMemoryProvider, Result,
    SqliteProvider, StateProvider, WorkflowOptions, STATUS_SUCCESS,
};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod common;

/// The one workflow name these tests use, and the one id.
const NAME: &str = "wf";
const ID: &str = "wf-1";

/// An engine over `provider` with `body` registered as the workflow — a stand-in
/// for one deployed version of the code.
async fn engine<F, Fut>(provider: &Arc<dyn StateProvider>, body: F) -> Result<DurableEngine>
where
    F: Fn(DurableContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<i64>> + Send + 'static,
{
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(NAME, move |ctx: DurableContext, _: ()| body(ctx));
    Ok(engine)
}

fn provider() -> Arc<dyn StateProvider> {
    Arc::new(InMemoryProvider::new())
}

/// Runs `body` to completion as the recorded workflow.
async fn record<F, Fut>(provider: &Arc<dyn StateProvider>, body: F) -> Result<()>
where
    F: Fn(DurableContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<i64>> + Send + 'static,
{
    common::run_body(provider, NAME, ID, body).await?;
    Ok(())
}

/// A workflow body of plain steps, named in order.
fn steps(
    names: &'static [&'static str],
) -> impl Fn(DurableContext) -> futures_util::future::BoxFuture<'static, Result<i64>>
       + Send
       + Sync
       + 'static {
    move |ctx: DurableContext| {
        Box::pin(async move {
            for name in names {
                ctx.step(name, || async { Ok::<_, Error>(1_i64) }).await?;
            }
            Ok(0)
        })
    }
}

/// Code that has not changed replays its own history exactly.
#[tokio::test]
async fn unchanged_code_is_deterministic() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a", "b", "c"])).await?;

    let report = engine(&provider, steps(&["a", "b", "c"]))
        .await?
        .verify_replay(ID)
        .await?;

    assert!(report.passes(), "{report:?}");
    assert_eq!(report.recorded, 3);
    assert_eq!(report.matched, 3);
    assert!(report.complete);
    assert_eq!(report.divergence, None);
    report.into_result()
}

/// Two steps swapped: the first position whose names differ, with both names.
#[tokio::test]
async fn swapped_steps_are_a_mismatch() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a", "b", "c"])).await?;

    let report = engine(&provider, steps(&["a", "c", "b"]))
        .await?
        .verify_replay(ID)
        .await?;

    assert_eq!(
        report.divergence,
        Some(Divergence::Mismatch {
            position: 1,
            expected: "c".into(),
            recorded: "b".into(),
        })
    );
    assert!(!report.passes());
    // Position 0 matched and nothing past 1 was reached.
    assert_eq!(report.matched, 1);
    assert_eq!(report.recorded, 3);

    let message = report.into_result().unwrap_err().to_string();
    assert!(message.contains("step 1"), "{message}");
    assert!(message.contains("`c`"), "{message}");
    assert!(message.contains("`b`"), "{message}");
    Ok(())
}

/// A step appended past the end of a completed history is an operation the
/// history does not hold.
#[tokio::test]
async fn a_step_added_at_the_end_is_extra() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a", "b"])).await?;

    let report = engine(&provider, steps(&["a", "b", "c"]))
        .await?
        .verify_replay(ID)
        .await?;

    assert_eq!(
        report.divergence,
        Some(Divergence::Extra {
            position: 2,
            operation: "c".into(),
        })
    );
    assert_eq!(report.matched, 2);
    assert_eq!(report.recorded, 2);
    Ok(())
}

/// A step inserted in the middle shifts every later position, so the first
/// disagreement is a name mismatch at the insertion point — `Extra` is what an
/// insertion looks like only at the end.
#[tokio::test]
async fn a_step_added_in_the_middle_is_a_mismatch() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a", "c"])).await?;

    let report = engine(&provider, steps(&["a", "b", "c"]))
        .await?
        .verify_replay(ID)
        .await?;

    assert_eq!(
        report.divergence,
        Some(Divergence::Mismatch {
            position: 1,
            expected: "b".into(),
            recorded: "c".into(),
        })
    );
    Ok(())
}

/// A trailing step removed: the history holds an operation the code no longer
/// reaches.
#[tokio::test]
async fn a_removed_trailing_step_is_missing() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a", "b", "c"])).await?;

    let report = engine(&provider, steps(&["a", "b"]))
        .await?
        .verify_replay(ID)
        .await?;

    assert_eq!(
        report.divergence,
        Some(Divergence::Missing {
            position: 2,
            recorded: "c".into(),
        })
    );
    assert_eq!(report.matched, 2);
    assert_eq!(report.recorded, 3);
    Ok(())
}

/// The divergence cell, not the returned error, is what the report is built
/// from: a body that swallows the refusal and finishes `Ok` is still reported.
#[tokio::test]
async fn a_swallowed_error_is_still_reported() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a"])).await?;

    let report = engine(&provider, |ctx: DurableContext| async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        // The classic shape: the error is dropped and the workflow carries on.
        let _ = ctx.step("b", || async { Ok::<_, Error>(2_i64) }).await;
        let _ = ctx.step("c", || async { Ok::<_, Error>(3_i64) }).await.ok();
        Ok(0)
    })
    .await?
    .verify_replay(ID)
    .await?;

    assert_eq!(
        report.divergence,
        Some(Divergence::Extra {
            position: 1,
            operation: "b".into(),
        }),
        "the workflow returned Ok, so only the cell could have reported this"
    );
    assert!(report.into_result().is_err());
    Ok(())
}

/// Verification is read-only: no step body runs — not the ones served from a
/// record, and not the added one with nothing recorded at its position, which a
/// live run would execute and checkpoint — and neither the history nor the status
/// row moves.
#[tokio::test]
async fn verification_writes_nothing_and_runs_nothing() -> Result<()> {
    static RAN: AtomicUsize = AtomicUsize::new(0);
    let provider = provider();

    // The recorded run: two steps whose bodies count themselves.
    record(&provider, |ctx: DurableContext| async move {
        for name in ["a", "b"] {
            ctx.step(name, || async {
                RAN.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(1_i64)
            })
            .await?;
        }
        Ok(0)
    })
    .await?;
    assert_eq!(
        RAN.load(Ordering::SeqCst),
        2,
        "the recorded run ran its steps"
    );

    let verifier = engine(&provider, |ctx: DurableContext| async move {
        // The two recorded names, plus one the history does not have. Every body
        // counts itself and then fails the test loudly.
        for name in ["a", "b", "c"] {
            ctx.step::<i64, _, _>(name, || async {
                RAN.fetch_add(1, Ordering::SeqCst);
                panic!("a step body ran during verification");
            })
            .await?;
        }
        Ok(0)
    })
    .await?;

    let before = snapshot(&verifier).await?;
    let report = verifier.verify_replay(ID).await?;
    let after = snapshot(&verifier).await?;

    assert_eq!(
        report.divergence,
        Some(Divergence::Extra {
            position: 2,
            operation: "c".into(),
        })
    );
    assert_eq!(RAN.load(Ordering::SeqCst), 2, "a step body ran");
    assert_eq!(before, after, "verification changed durable state");
    Ok(())
}

/// The durable state a verification must leave untouched: the recorded
/// operations and the workflow row's own fields.
type Snapshot = (
    Vec<(i32, String, Option<serde_json::Value>)>,
    (String, Option<serde_json::Value>, String, i32),
);

async fn snapshot(engine: &DurableEngine) -> Result<Snapshot> {
    let steps = engine
        .get_workflow_steps(ID)
        .await?
        .into_iter()
        .map(|s| (s.step_id, s.name, s.output))
        .collect();
    let status = engine
        .list_workflows(&durare::ListFilter {
            workflow_ids: vec![ID.to_string()],
            ..Default::default()
        })
        .await?
        .pop()
        .expect("the recorded workflow exists");
    Ok((
        steps,
        (
            status.status,
            status.output,
            status.executor_id,
            status.recovery_attempts,
        ),
    ))
}

/// A workflow that is still running has a history that legitimately ends early,
/// so reaching the end of it is not a divergence — while a genuine mismatch
/// inside that history still is.
#[tokio::test]
async fn a_running_workflow_may_end_its_history_early() -> Result<()> {
    let provider = provider();
    let _running = park_on_timer(&provider).await?;

    // The same code: it walks the history and then asks for `b`, which the
    // history does not hold *yet*. Not a divergence.
    let report = engine(&provider, parked_body)
        .await?
        .verify_replay(ID)
        .await?;
    assert!(!report.complete);
    assert!(report.passes(), "{report:?}");
    assert_eq!(report.matched, 2);

    // Changed code over the same partial history still diverges.
    let report = engine(&provider, steps(&["z", "b"]))
        .await?
        .verify_replay(ID)
        .await?;
    assert_eq!(
        report.divergence,
        Some(Divergence::Mismatch {
            position: 0,
            expected: "z".into(),
            recorded: "a".into(),
        })
    );
    Ok(())
}

/// The workflow the running-history tests verify: a step, a long durable timer,
/// a step. Parked on the timer, its row stays `PENDING` with `a` and the timer
/// recorded and `b` not yet reached.
fn parked_body(ctx: DurableContext) -> futures_util::future::BoxFuture<'static, Result<i64>> {
    Box::pin(async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.sleep(Duration::from_secs(3_600)).await?;
        ctx.step("b", || async { Ok::<_, Error>(2_i64) }).await?;
        Ok(0)
    })
}

/// Starts [`parked_body`] under `ID` and returns once `a` and the timer are
/// recorded. The engine is handed back so the parked run outlives the caller's
/// verification.
async fn park_on_timer(provider: &Arc<dyn StateProvider>) -> Result<DurableEngine> {
    let running = engine(provider, parked_body).await?;
    let _handle = running
        .start::<_, i64>(NAME, (), WorkflowOptions::with_id(ID))
        .await?;
    for _ in 0..400 {
        if running.get_workflow_steps(ID).await?.len() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let recorded = running.get_workflow_steps(ID).await?;
    assert_eq!(recorded.len(), 2, "expected `a` and the timer recorded");
    Ok(running)
}

/// Reaching the end of a running workflow's history stops the re-run, and the
/// stop is not the body's failure. Unchanged code that `.expect()`s the call
/// past the frontier panics on the verifier's own stop, and must still pass:
/// nothing about the code diverged from the history it was checked against.
#[tokio::test]
async fn a_body_that_panics_on_the_frontier_of_a_running_history_passes() -> Result<()> {
    let provider = provider();
    let _running = park_on_timer(&provider).await?;

    let report = engine(&provider, |ctx: DurableContext| async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.sleep(Duration::from_secs(3_600)).await?;
        ctx.step("b", || async { Ok::<_, Error>(2_i64) })
            .await
            .expect("b");
        Ok(0)
    })
    .await?
    .verify_replay(ID)
    .await?;

    assert_eq!(report.divergence, None, "{report:?}");
    assert_eq!(report.matched, 2, "`a` and the timer were both served");
    Ok(())
}

/// A recorded value that no longer decodes is a failure against a running
/// history too. The history being a prefix excuses the operations it does not
/// hold yet, not a re-run that fails on one it does hold.
#[tokio::test]
async fn a_value_that_no_longer_decodes_fails_against_a_running_history() -> Result<()> {
    let provider = provider();
    let _running = park_on_timer(&provider).await?;

    let report = engine(&provider, |ctx: DurableContext| async move {
        // Same name, now a String: the recorded `1` does not deserialize as one.
        let s: String = ctx
            .step("a", || async { Ok::<_, Error>("x".to_string()) })
            .await?;
        ctx.sleep(Duration::from_secs(3_600)).await?;
        Ok(s.len() as i64)
    })
    .await?
    .verify_replay(ID)
    .await?;

    assert_eq!(
        report.matched, 1,
        "the name matched before the value was decoded"
    );
    assert!(
        matches!(report.divergence, Some(Divergence::Failed { .. })),
        "a re-run that fails on a recorded value must not pass: {report:?}"
    );
    Ok(())
}

/// A body that panics on the operation it was refused is reported as the
/// divergence that caused it, not as the panic.
#[tokio::test]
async fn a_panicking_body_is_reported_as_its_divergence() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a"])).await?;

    let report = engine(&provider, |ctx: DurableContext| async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.step("b", || async { Ok::<_, Error>(2_i64) })
            .await
            .expect("b");
        Ok(0)
    })
    .await?
    .verify_replay(ID)
    .await?;

    assert_eq!(
        report.divergence,
        Some(Divergence::Extra {
            position: 1,
            operation: "b".into(),
        })
    );
    Ok(())
}

/// The two ways a caller can be wrong about what to verify.
#[tokio::test]
async fn an_unknown_workflow_or_name_is_an_error() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a"])).await?;

    let unknown_id = engine(&provider, steps(&["a"]))
        .await?
        .verify_replay("nope")
        .await;
    assert!(matches!(unknown_id, Err(Error::UnknownWorkflow(id)) if id == "nope"));

    // An engine that does not register the recorded workflow's name.
    let mut bare = DurableEngine::new(provider.clone()).await?;
    bare.register("other", |_: DurableContext, _: ()| async { Ok(0_i64) });
    let unknown_name = bare.verify_replay(ID).await;
    assert!(matches!(unknown_name, Err(Error::UnknownWorkflow(name)) if name == NAME));
    Ok(())
}

/// A run that ended on its own is reported as a complete history.
#[tokio::test]
async fn a_completed_history_is_complete() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a"])).await?;
    let report = engine(&provider, steps(&["a"]))
        .await?
        .verify_replay(ID)
        .await?;
    assert!(report.complete);
    assert_eq!(report.workflow_id, ID);
    assert_eq!(report.workflow_name, NAME);

    let status = engine(&provider, steps(&["a"]))
        .await?
        .list_workflows(&durare::ListFilter {
            workflow_ids: vec![ID.to_string()],
            ..Default::default()
        })
        .await?
        .pop()
        .expect("the recorded workflow exists");
    assert_eq!(status.status, STATUS_SUCCESS);
    Ok(())
}

/// A transaction the history does not hold must be refused like any other
/// operation, not executed. A transaction's own replay check lives inside the
/// provider, in the database transaction it opens, which is too late for a run
/// that must not open one; the verifier consults the record first. On SQLite,
/// since transactions need a SQL backend.
#[tokio::test]
async fn an_added_transaction_is_refused_not_run() -> Result<()> {
    static RAN: AtomicUsize = AtomicUsize::new(0);
    let (url, path) = common::temp_db_url("verify");
    let provider: Arc<dyn StateProvider> = Arc::new(SqliteProvider::connect(&url).await?);

    record(&provider, steps(&["a"])).await?;

    let verifier = engine(&provider, |ctx: DurableContext| async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.transaction("tx", |tx| {
            Box::pin(async move {
                RAN.fetch_add(1, Ordering::SeqCst);
                tx.execute("SELECT 1", &params![]).await?;
                Ok(2_i64)
            })
        })
        .await?;
        Ok(0)
    })
    .await?;

    let before = verifier.get_workflow_steps(ID).await?.len();
    let report = verifier.verify_replay(ID).await?;
    let after = verifier.get_workflow_steps(ID).await?.len();

    assert_eq!(
        report.divergence,
        Some(Divergence::Extra {
            position: 1,
            operation: "tx".into(),
        }),
        "{report:?}"
    );
    assert_eq!(RAN.load(Ordering::SeqCst), 0, "the transaction body ran");
    assert_eq!((before, after), (1, 1), "verification wrote a checkpoint");
    drop(verifier);
    common::remove_sqlite_files(&path);
    Ok(())
}

/// Every kind of durable call reaches the same gate, so an unrecorded one of any
/// kind is refused rather than run. Each row records `["a"]` to completion, then
/// verifies a body that replays `a` and issues the operation; the report must
/// name that operation at position 1, the history must be untouched, and the
/// side effects the in-memory provider can show must be absent. `transaction`
/// and `transaction_on` need a SQL backend and are covered on SQLite above.
#[tokio::test]
async fn every_kind_of_unrecorded_call_is_refused() -> Result<()> {
    type Body = Box<
        dyn Fn(DurableContext) -> futures_util::future::BoxFuture<'static, Result<i64>>
            + Send
            + Sync,
    >;
    // The operation name as `refuse_live_work` receives it, and a body that
    // replays `a` and then issues the operation.
    let rows: Vec<(&str, Body)> = vec![
        (
            "DBOS.sleep",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.sleep(Duration::from_millis(1)).await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.now",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.now().await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.uuid",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.uuid().await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.random",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.random().await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.send",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.send(ID, 1_i64, "topic").await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.updateWorkflowAttributes",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.set_workflow_attributes(ID, None).await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.recv",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.recv::<i64>("topic", Duration::from_millis(1)).await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.setEvent",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.set_event("key", 1_i64).await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.getEvent",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.get_event::<i64>(ID, "key", Duration::from_millis(1))
                        .await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.writeStream",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.write_stream("stream", 1_i64).await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.closeStream",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.close_stream("stream").await?;
                    Ok(0)
                })
            }),
        ),
        (
            "child",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.start_workflow::<_, i64>("child", (), WorkflowOptions::default())
                        .await?;
                    Ok(0)
                })
            }),
        ),
        (
            "DBOS.select",
            Box::new(|ctx| {
                Box::pin(async move {
                    ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
                    ctx.select(vec![Box::pin(async { 1_i64 })]).await?;
                    Ok(0)
                })
            }),
        ),
    ];

    for (operation, body) in rows {
        let provider = provider();
        record(&provider, steps(&["a"])).await?;
        let verifier = engine(&provider, body).await?;

        let report = verifier.verify_replay(ID).await?;

        assert_eq!(
            report.divergence,
            Some(Divergence::Extra {
                position: 1,
                operation: operation.into(),
            }),
            "{operation}: {report:?}"
        );
        assert_eq!(
            verifier.get_workflow_steps(ID).await?.len(),
            1,
            "{operation}: verification wrote a checkpoint"
        );
        assert!(
            verifier.list_workflow_notifications(ID).await?.is_empty(),
            "{operation}: a message was sent"
        );
        assert!(
            verifier.list_workflow_events(ID).await?.is_empty(),
            "{operation}: an event was set"
        );
        assert!(
            provider
                .get_workflow_status(&format!("{ID}-1"))
                .await?
                .is_none(),
            "{operation}: a child workflow was started"
        );
    }
    Ok(())
}

/// A call that is built and dropped claims a position without ever asking for
/// the record at it. That must not count as having verified the history there:
/// the recorded operation was never reached, whatever the position counter says.
#[tokio::test]
async fn a_built_and_dropped_call_does_not_verify_the_history_under_it() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a"])).await?;

    let report = engine(&provider, |ctx: DurableContext| async move {
        // Claims position 0 and never polls it. Under a counter-based check the
        // counter would read 1 and the history would look fully covered.
        drop(ctx.step("renamed", || async { Ok::<_, Error>(1_i64) }));
        Ok(0)
    })
    .await?
    .verify_replay(ID)
    .await?;

    assert_eq!(report.matched, 0);
    assert_eq!(
        report.divergence,
        Some(Divergence::Missing {
            position: 0,
            recorded: "a".into(),
        }),
        "{report:?}"
    );
    Ok(())
}

/// The same operation name with a changed return type: the name check passes,
/// the recorded value no longer decodes, and the body returns the decode error.
/// The recorded run succeeded, so a re-run that fails is not a clean pass even
/// though no operation disagreed.
#[tokio::test]
async fn a_recorded_value_that_no_longer_decodes_is_a_failure() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a"])).await?; // records the integer 1 at `a`

    let report = engine(&provider, |ctx: DurableContext| async move {
        // Same name, now a String: `1` does not deserialize as one.
        let s: String = ctx
            .step("a", || async { Ok::<_, Error>("x".to_string()) })
            .await?;
        Ok(s.len() as i64)
    })
    .await?
    .verify_replay(ID)
    .await?;

    assert_eq!(
        report.matched, 1,
        "the name matched before the value was decoded"
    );
    assert!(
        matches!(report.divergence, Some(Divergence::Failed { .. })),
        "a failed re-run of a successful history must not pass: {report:?}"
    );
    assert!(report.into_result().is_err());
    Ok(())
}
