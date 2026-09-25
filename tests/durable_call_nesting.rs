//! A durable call belongs to the workflow body, and is awaited where it is built.
//!
//! A position is claimed where a durable call is written, and a replay finds the
//! recorded outcome by asking for the position the first run asked for. Both
//! halves break when a call is made inside another durable operation's body — a
//! step inside a step, or a durable call inside a [`select`] branch. The inner
//! call claims a position and writes there on the first run; on a replay the
//! body never runs, because the outer operation is served from its record, so
//! nothing claims that position and every later call shifts onto it. Whether
//! anything notices depends only on the name: a different name fails somewhere
//! unrelated with `UnexpectedStep`, on code that did not change between the two
//! runs, and the same name silently serves the inner call's value as the outer
//! call's.
//!
//! So the engine refuses both, at the call, before the counter moves:
//!
//! - **Creating** a durable call while a body is being polled is
//!   `Error::NestedDurableCall`. It claims no position, so the calls around it
//!   keep the positions they would have had.
//! - **Polling** a durable call in a body other than the one it was built in is
//!   `Error::DurableCallCrossedBody`. Its position was claimed outside, where
//!   every replay claims it again, but the body it is awaited in does not run on
//!   a replay.
//!
//! The check is a task-local scope around the body's poll rather than a flag
//! held for the body's lifetime, which is what `a_sibling_built_while_a_body_is
//! _in_flight_is_allowed` is here to prove: between polls the body is in flight
//! but not running, and a call the workflow body makes then is ordinary code.
//!
//! # Forcing a replay
//!
//! Two of these tests need a first run that records the `select` and then stops
//! before the workflow completes, and a second run under the same id. The body
//! panics on its first attempt: the engine treats a panic in a workflow body as
//! a crash rather than a returned error, so the row stays `PENDING` and
//! `recover()` re-dispatches it from its checkpoints. This is the mechanism
//! `tests/durability.rs` uses for the same purpose. It needs no second engine,
//! no timing on the first run, and no parked task.
//!
//! [`select`]: durare::DurableContext::select

use durare::{
    DurableContext, DurableEngine, Error, ErrorCode, InMemoryProvider, Result, StateProvider,
    WorkflowOptions, WorkflowStatus, STATUS_PENDING, STATUS_SUCCESS,
};
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

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

/// Runs `body` once and reports what it recorded, in position order.
async fn positions<F, Fut>(body: F) -> Result<Vec<(i32, String)>>
where
    F: Fn(DurableContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<i64>> + Send + 'static,
{
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(WORKFLOW, move |ctx: DurableContext, _: ()| body(ctx));
    engine
        .start::<_, i64>(WORKFLOW, (), WorkflowOptions::with_id(ID))
        .await?
        .result()
        .await?;
    recorded(&engine).await
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

/// The shape every other `select` test is measured against: a plain future in
/// the branch. The `select` is the only checkpoint the first run leaves, the
/// replay serves it, and `after` takes the position right behind it exactly as
/// it did on the first run.
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

/// A step created inside a `select` branch is refused, and the replay is then
/// indistinguishable from the plain-branch case above.
///
/// Without the refusal this is the silent-corruption shape: the inner step would
/// claim the position after the select's and write there, the replay would serve
/// the select without polling the branch, and `after` would land on the inner
/// step's row — reported as `UnexpectedStep` under a different name, and not
/// reported at all under the same one.
#[tokio::test]
async fn a_step_inside_a_select_branch_is_refused() -> Result<()> {
    let (before, status) = replay(|ctx| async move {
        let inner = ctx.clone();
        let (index, value) = ctx
            .select(vec![Box::pin(async move {
                let refused = inner
                    .step("inner", || async { Ok::<_, Error>(1_i64) })
                    .await
                    .expect_err("a durable call in a branch is refused");
                assert_eq!(refused.code(), ErrorCode::NestedDurableCall);
                Some(refused.to_string())
            })])
            .await?;
        assert_eq!(index, 0);
        assert!(
            value.unwrap().contains("`step` was created inside another"),
            "the branch sees the refusal, not a checkpoint"
        );
        Ok(())
    })
    .await?;

    assert_eq!(
        before,
        [(0, "DBOS.select".to_string())],
        "the refused call claimed no position, so the select is still the only row"
    );
    assert_eq!(status.status, STATUS_SUCCESS);
    assert_eq!(
        status.output,
        Some(serde_json::json!(2)),
        "`after` ran on the replay and returned its own value"
    );
    Ok(())
}

/// The same shape with the inner step named like the call after the `select` —
/// the case nothing could detect after the fact, because the name check passes.
#[tokio::test]
async fn a_same_named_step_inside_a_select_branch_is_refused() -> Result<()> {
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
        assert_eq!((index, value), (0, None), "the inner call was refused");
        Ok(())
    })
    .await?;

    assert_eq!(before, [(0, "DBOS.select".to_string())]);
    assert_eq!(status.status, STATUS_SUCCESS);
    assert_eq!(
        status.output,
        Some(serde_json::json!(2)),
        "`after` ran and recorded its own 2, not the inner call's 1"
    );
    Ok(())
}

/// A step created inside a step body is refused, and the refusal costs nothing:
/// the call written after the outer step takes the position directly behind it.
#[tokio::test]
async fn a_step_inside_a_step_body_is_refused_and_claims_no_position() -> Result<()> {
    let recorded = positions(|ctx| async move {
        let inner = ctx.clone();
        let refused = ctx
            .step("outer", || async move {
                let e = inner
                    .step("inner", || async { Ok::<_, Error>(1_i64) })
                    .await
                    .expect_err("a step inside a step body is refused");
                assert_eq!(e.code(), ErrorCode::NestedDurableCall);
                Ok::<_, Error>(e.to_string())
            })
            .await?;
        assert!(refused.contains("`step` was created inside another"));
        ctx.step("after", || async { Ok::<_, Error>(2_i64) }).await
    })
    .await?;

    assert_eq!(
        recorded,
        [(0, "outer".to_string()), (1, "after".to_string())],
        "the refused inner call left the counter alone"
    );
    Ok(())
}

/// A durable call built in the workflow body and carried into a step body is
/// refused when it is polled there.
///
/// Its position was claimed outside, so every replay claims it again; but the
/// body it would be awaited in does not run on a replay, and the outcome it
/// records would be served to an execution that never asked for it. Retry,
/// cancellation and timeout have no owner across that boundary either.
#[tokio::test]
async fn a_call_built_outside_and_polled_inside_a_body_is_refused() -> Result<()> {
    let recorded = positions(|ctx| async move {
        let carried = ctx.step("carried", || async { Ok::<_, Error>(1_i64) });
        let message = ctx
            .step("outer", move || async move {
                let e = carried
                    .await
                    .expect_err("a call built outside this body is refused in it");
                assert_eq!(e.code(), ErrorCode::NestedDurableCall);
                Ok::<_, Error>(e.to_string())
            })
            .await?;
        assert!(
            message.contains("was built outside the durable body"),
            "got: {message}"
        );
        ctx.step("after", || async { Ok::<_, Error>(3_i64) }).await
    })
    .await?;

    assert_eq!(
        recorded,
        [(1, "outer".to_string()), (2, "after".to_string())],
        "position 0 is a gap: the carried call spent it where it was built, as \
         any built-and-dropped call does, and the refusal stopped it before it \
         wrote anything there. The positions after it are unmoved, and a replay \
         builds the same call in the same place and spends 0 again."
    );
    Ok(())
}

/// A call the workflow body makes while another step's body is parked mid-await
/// is ordinary code, and must not be mistaken for a nested one.
///
/// This is what the task-local scope buys over a flag held for the body's
/// lifetime. `held` is inside its body and waiting; with a lifetime flag the
/// answer would be on shared state, `beside` would read *itself* as nested, and
/// it would be refused although it is written in the workflow body. The scope is
/// set only while a body is actually being polled, and `held` parked, so nothing
/// is set when `beside` is built.
///
/// The two notifies are the whole of the interleaving and there is no sleep in
/// it: `beside` is built only once `held` says it is inside its body, and `held`
/// returns only once `beside` has been awaited.
#[tokio::test]
async fn a_sibling_built_while_a_body_is_in_flight_is_allowed() -> Result<()> {
    let recorded = positions(|ctx| async move {
        let inside = Arc::new(Notify::new());
        let go = Arc::new(Notify::new());
        let (held, beside) = tokio::join!(
            ctx.step("held", {
                let (inside, go) = (inside.clone(), go.clone());
                move || async move {
                    inside.notify_one();
                    go.notified().await;
                    Ok::<_, Error>(1_i64)
                }
            }),
            async {
                inside.notified().await;
                let beside = ctx.step("beside", || async { Ok::<_, Error>(2_i64) }).await;
                go.notify_one();
                beside
            }
        );
        Ok(held? + beside?)
    })
    .await?;

    assert_eq!(
        recorded,
        [(0, "held".to_string()), (1, "beside".to_string())],
        "`beside` is a step of the workflow, written in the workflow body, and \
         records under the position it claimed there"
    );
    Ok(())
}

/// The scope is restored however a body's poll ends, so a body that fails or
/// panics does not leave the workflow unable to make durable calls afterwards.
#[tokio::test]
async fn a_body_that_errors_or_panics_leaves_the_scope_clean() -> Result<()> {
    let recorded = positions(|ctx| async move {
        ctx.step("errored", || async {
            Err::<i64, _>(Error::app("business failure"))
        })
        .await
        .expect_err("the body returned an error");

        ctx.step("panicked", || async {
            panic!("the body panicked");
            #[allow(unreachable_code)]
            Ok::<i64, Error>(0)
        })
        .await
        .expect_err("the panic is caught and recorded as a failure");

        // Both bodies unwound out of their scope. If either had left it set,
        // this call would be refused as nested.
        ctx.step("after", || async { Ok::<_, Error>(7_i64) }).await
    })
    .await?;

    assert_eq!(
        recorded,
        [
            (0, "errored".to_string()),
            (1, "panicked".to_string()),
            (2, "after".to_string())
        ],
        "a failed body records its failure and the next call proceeds normally"
    );
    Ok(())
}
