//! `verify_replay` re-runs a recorded workflow against the current code and
//! reports the first durable operation the two disagree about.
//!
//! Every test here changes the code the way a deploy would: the workflow runs
//! under one engine, and a **second** engine over the same provider registers a
//! different function under the same name. The report is what the first engine's
//! history says about the second engine's code.

use durare::{
    Divergence, DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider,
    WorkflowOptions, STATUS_SUCCESS,
};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

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
    engine(provider, body)
        .await?
        .start::<_, i64>(NAME, (), WorkflowOptions::with_id(ID))
        .await?
        .result()
        .await?;
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

    assert!(report.is_deterministic(), "{report:?}");
    assert_eq!(report.recorded, 3);
    assert_eq!(report.matched, 3);
    assert!(report.terminal);
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
    assert!(!report.is_deterministic());
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

    // Parked on a long durable timer: the row stays PENDING with `a` and the
    // timer recorded.
    let running = engine(&provider, |ctx: DurableContext| async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.sleep(Duration::from_secs(3_600)).await?;
        ctx.step("b", || async { Ok::<_, Error>(2_i64) }).await?;
        Ok(0)
    })
    .await?;
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

    // The same code: it walks the history and then asks for `b`, which the
    // history does not hold *yet*. Not a divergence.
    let report = engine(&provider, |ctx: DurableContext| async move {
        ctx.step("a", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.sleep(Duration::from_secs(3_600)).await?;
        ctx.step("b", || async { Ok::<_, Error>(2_i64) }).await?;
        Ok(0)
    })
    .await?
    .verify_replay(ID)
    .await?;
    assert!(!report.terminal);
    assert!(report.is_deterministic(), "{report:?}");
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

/// The recorded run's terminal status is reported as-is for a completed run.
#[tokio::test]
async fn a_completed_history_is_terminal() -> Result<()> {
    let provider = provider();
    record(&provider, steps(&["a"])).await?;
    let report = engine(&provider, steps(&["a"]))
        .await?
        .verify_replay(ID)
        .await?;
    assert!(report.terminal);
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
