//! A durable call created inside a `select` branch desynchronises the replay.
//!
//! `select` records the winning `(index, value)` as one checkpoint and, on
//! replay, returns that record without polling its branches. Its docs require
//! the branches to be plain async work for exactly this reason, but nothing
//! enforces it. A durable call built inside a branch claims its position while
//! the branch is polled — after `select`'s own — and writes a checkpoint there
//! on the first run. On replay that branch never runs, so the position is never
//! claimed, and the next durable call after the `select` lands on the position
//! the inner call used. Whether that is noticed depends on the name: a
//! different name fails the replay with [`Error::UnexpectedStep`]; the same
//! name replays the inner call's value as the outer call's, silently.
//!
//! These tests pin the behaviour as it stands so that the nesting guard has a
//! failing test to make pass. Assertions marked **defect** describe the wrong
//! behaviour; the doc comment on each says what the guard replaces it with.
//!
//! # Forcing a replay
//!
//! The first run has to record the `select` and then stop before the workflow
//! completes, and the second run has to be a replay of the same id. The body
//! panics on its first attempt, right after the `select`: the engine treats a
//! panic in a workflow body as a crash rather than a returned error, so the
//! row stays `PENDING` and `recover()` re-dispatches it from its checkpoints.
//! This is the mechanism `tests/durability.rs` uses for the same purpose. It
//! needs no second engine, no timing on the first run, and no parked task —
//! `start(..).result()` returns as soon as the panic is caught, and the only
//! wait is polling the row for the recovered run's terminal status.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
    WorkflowStatus, STATUS_ERROR, STATUS_PENDING, STATUS_SUCCESS,
};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

const WORKFLOW: &str = "probe";
const ID: &str = "wf";

/// The `(position, operation)` pairs a workflow recorded, in position order.
async fn recorded(engine: &DurableEngine) -> Result<Vec<(i32, String)>> {
    Ok(engine
        .get_workflow_steps(ID)
        .await?
        .into_iter()
        .map(|step| (step.step_id, step.name))
        .collect())
}

/// Registers `body` and runs it twice under one id: the first attempt panics
/// right after `body` returns, once the `select` it holds has been recorded;
/// the second is the replay `recover()` dispatches. Returns the checkpoints the
/// first run left behind and the recovered run's terminal status row.
///
/// The `after` step is written here, outside `body`, so every test shares the
/// same call after the `select`: it claims the position immediately after the
/// select's, and what it finds there is what each test asserts on.
async fn replay<F, Fut>(body: F) -> Result<(Vec<(i32, String)>, WorkflowStatus)>
where
    F: Fn(DurableContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    engine.register(WORKFLOW, move |ctx: DurableContext, _: ()| {
        let attempts = attempts.clone();
        let select = body(ctx.clone());
        async move {
            select.await?;
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("crash after the select, before the workflow completes");
            }
            ctx.step("after", || async { Ok::<_, Error>(2_i64) }).await
        }
    });

    // First run: records the select, then crashes.
    engine
        .start::<_, i64>(WORKFLOW, (), WorkflowOptions::with_id(ID))
        .await?
        .result()
        .await
        .expect_err("the first attempt panics");
    let before = recorded(&engine).await?;
    assert_eq!(
        provider.get_workflow_status(ID).await?.unwrap().status,
        STATUS_PENDING,
        "a panicked run is left recoverable"
    );

    // Second run: recover() replays it from its checkpoints on a background
    // task; wait for the row to turn terminal.
    assert_eq!(engine.recover().await?, 1, "recovery picks up the run");
    let mut status = provider.get_workflow_status(ID).await?.unwrap();
    for _ in 0..200 {
        if status.status != STATUS_PENDING {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
        status = provider.get_workflow_status(ID).await?.unwrap();
    }
    assert_ne!(status.status, STATUS_PENDING, "the recovered run settles");
    Ok((before, status))
}

/// The healthy shape: a plain future in the branch. The `select` is the only
/// checkpoint the first run leaves, the replay serves it, and `after` takes the
/// position right behind it exactly as it did on the first run.
#[tokio::test]
async fn a_plain_branch_replays_cleanly() -> Result<()> {
    let (before, status) = replay(|ctx| async move {
        let (index, value) = ctx.select(vec![Box::pin(async { Some(1_i64) })]).await?;
        assert_eq!((index, value), (0, Some(1)));
        Ok(())
    })
    .await?;

    assert_eq!(before, [(0, "DBOS.select".to_string())]);
    assert_eq!(status.status, STATUS_SUCCESS);
    assert_eq!(status.output, Some(serde_json::json!(2)));
    Ok(())
}

/// A step created inside the branch under a name that differs from the call
/// after the `select`.
///
/// First run: `select` claims 0, polls the branch, and the branch's `step`
/// claims 1 and checkpoints there; `select` then checkpoints at 0. Replay:
/// `select` is served from 0 without polling the branch, so nothing claims 1,
/// and `after` takes 1 — where `inner` is recorded. The replay fails with
/// [`Error::UnexpectedStep`] and the workflow ends in `ERROR`, even though the
/// code has not changed between the two runs.
///
/// **Defect.** The assertions after `before` pin this. Once the nesting guard
/// lands, the inner `step` is refused at the call — before it claims a position
/// — so the first run records only `(0, DBOS.select)`, the branch yields
/// `None`, and the replay completes with `after` at 1 and output `2`, the same
/// as the plain-branch case above.
#[tokio::test]
async fn a_step_inside_a_branch_makes_the_next_call_unexpected() -> Result<()> {
    let (before, status) = replay(|ctx| async move {
        let inner = ctx.clone();
        let (index, value) = ctx
            .select(vec![Box::pin(async move {
                inner
                    .step("inner", || async { Ok::<_, Error>(1_i64) })
                    .await
                    .ok()
            })])
            .await?;
        assert_eq!((index, value), (0, Some(1)));
        Ok(())
    })
    .await?;

    assert_eq!(
        before,
        [(0, "DBOS.select".to_string()), (1, "inner".to_string())],
        "the inner step claimed the position after the select's and wrote there"
    );
    assert_eq!(
        status.status, STATUS_ERROR,
        "the replay of unchanged code fails as non-deterministic"
    );
    let error = status.error.expect("an ERROR row carries its error");
    assert!(
        error.contains("step 1 is `after` but `inner` is recorded there"),
        "`after` landed on the inner step's position: {error}"
    );
    Ok(())
}

/// The same shape with the inner step named like the call after the `select`.
///
/// Nothing notices: on replay `after` finds a record of its own name at 1 and
/// is served the inner step's value. The workflow completes, with the wrong
/// output and without ever running `after`.
///
/// **Defect.** The output assertion pins this. Once the nesting guard lands,
/// the inner call is refused before it claims a position, the branch yields
/// `None`, and `after` runs on the replay and records `2` at 1.
#[tokio::test]
async fn a_same_named_step_inside_a_branch_replays_the_wrong_value() -> Result<()> {
    let (before, status) = replay(|ctx| async move {
        let inner = ctx.clone();
        let (index, value) = ctx
            .select(vec![Box::pin(async move {
                inner
                    .step("after", || async { Ok::<_, Error>(1_i64) })
                    .await
                    .ok()
            })])
            .await?;
        assert_eq!((index, value), (0, Some(1)));
        Ok(())
    })
    .await?;

    assert_eq!(
        before,
        [(0, "DBOS.select".to_string()), (1, "after".to_string())]
    );
    assert_eq!(status.status, STATUS_SUCCESS);
    assert_eq!(
        status.output,
        Some(serde_json::json!(1)),
        "the outer `after` replayed the inner step's value in place of its own 2"
    );
    Ok(())
}
