//! A durable call claims its position where it is **written**, not where it is
//! first polled.
//!
//! The position is the `(workflow_id, seq)` key a checkpoint is written under,
//! and a replay finds the recorded result only by asking for the position the
//! first run asked for. Taking it inside an `async fn` body would tie it to poll
//! order — which `tokio::select!` randomises and an out-of-order await reverses —
//! so these pin it to the source instead.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
};
use std::sync::Arc;

mod common;

const WORKFLOW: &str = "probe";
const ID: &str = "wf";

/// The `(position, operation)` pairs the workflow recorded, in position order.
/// A position nothing was written at is simply absent, which is how a call that
/// claimed one without running shows up.
async fn recorded(engine: &DurableEngine) -> Result<Vec<(i32, String)>> {
    common::recorded(engine, ID).await
}

/// Runs `body` as a workflow and reports which operation landed at each position.
async fn positions<F, Fut>(body: F) -> Result<Vec<(i32, String)>>
where
    F: Fn(DurableContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<i64>> + Send + 'static,
{
    let provider: Arc<dyn StateProvider> = Arc::new(InMemoryProvider::new());
    let engine = common::run_body(&provider, WORKFLOW, ID, body).await?;
    recorded(&engine).await
}

/// Awaiting two steps in the opposite order to the one they were written in must
/// not swap the positions they occupy.
#[tokio::test]
async fn awaiting_out_of_order_does_not_move_a_position() -> Result<()> {
    let in_order = positions(|ctx| async move {
        let first = ctx.step("first", || async { Ok::<_, Error>(1_i64) });
        let second = ctx.step("second", || async { Ok::<_, Error>(2_i64) });
        first.await?;
        second.await?;
        Ok(0)
    })
    .await?;

    let reversed = positions(|ctx| async move {
        let first = ctx.step("first", || async { Ok::<_, Error>(1_i64) });
        let second = ctx.step("second", || async { Ok::<_, Error>(2_i64) });
        second.await?;
        first.await?;
        Ok(0)
    })
    .await?;

    assert_eq!(
        in_order,
        [(0, "first".to_string()), (1, "second".to_string())]
    );
    assert_eq!(
        reversed, in_order,
        "positions must follow the order the calls are written, not the order they are awaited"
    );
    Ok(())
}

/// `tokio::select!` polls its branches in a randomised order. The branch that
/// wins may differ from run to run — the positions the branches hold may not.
#[tokio::test(flavor = "multi_thread")]
async fn a_randomised_poll_order_does_not_move_a_position() -> Result<()> {
    for _ in 0..24 {
        let seen = positions(|ctx| async move {
            let mut a = ctx.step("a", || async { Ok::<_, Error>(1_i64) });
            let mut b = ctx.step("b", || async { Ok::<_, Error>(2_i64) });
            tokio::select! {
                _ = &mut a => { let _ = b.await; }
                _ = &mut b => { let _ = a.await; }
            }
            Ok(0)
        })
        .await?;
        assert_eq!(
            seen,
            [(0, "a".to_string()), (1, "b".to_string())],
            "a randomised poll order reached the counter in a different order"
        );
    }
    Ok(())
}

/// The whole library's calls share one counter, so the rule has to hold across
/// the kinds of call, not only between two steps.
#[tokio::test]
async fn the_rule_holds_across_the_kinds_of_call() -> Result<()> {
    let seen = positions(|ctx| async move {
        let step = ctx.step("a_step", || async { Ok::<_, Error>(1_i64) });
        let event = ctx.set_event("a_key", 7_i64);
        let value = ctx.uuid();
        // Awaited back to front; the positions must still read front to back.
        value.await?;
        event.await?;
        step.await?;
        Ok(0)
    })
    .await?;
    assert_eq!(
        seen,
        [
            (0, "a_step".to_string()),
            (1, "DBOS.setEvent".to_string()),
            (2, "DBOS.uuid".to_string())
        ]
    );
    Ok(())
}

/// A call that is built and dropped has still spent its position — deterministic,
/// because a replay runs the same code and skips the same position, but no longer
/// the no-op it would be for a plain future.
#[tokio::test]
async fn a_built_call_that_is_dropped_has_spent_its_position() -> Result<()> {
    let seen = positions(|ctx| async move {
        let abandoned = ctx.step("abandoned", || async { Ok::<_, Error>(1_i64) });
        drop(abandoned);
        ctx.step("recorded", || async { Ok::<_, Error>(2_i64) })
            .await?;
        Ok(0)
    })
    .await?;
    assert_eq!(
        seen,
        [(1, "recorded".to_string())],
        "the dropped call spent position 0 and wrote nothing there, so the next \
         call took 1 rather than 0"
    );
    Ok(())
}

#[durare::step]
async fn macro_first(ctx: &DurableContext, n: i64) -> Result<i64> {
    Ok(n + 1)
}

#[durare::step]
async fn macro_second(ctx: &DurableContext, n: i64) -> Result<i64> {
    Ok(n + 2)
}

/// `#[durare::step]` emits a `PendingStep`-returning `fn` rather than an
/// `async fn`, so a macro-written call obeys the same rule as one written by
/// hand — the macros are the preferred way to write a step, and an exception
/// there would be an exception for most code.
#[tokio::test]
async fn a_macro_written_call_claims_its_position_where_it_is_written() -> Result<()> {
    let seen = positions(|ctx| async move {
        let first = macro_first(&ctx, 1);
        let second = macro_second(&ctx, 2);
        // Awaited back to front, as in the hand-written case above.
        second.await?;
        first.await?;
        Ok(0)
    })
    .await?;
    assert_eq!(
        seen,
        [
            (0, "macro_first".to_string()),
            (1, "macro_second".to_string())
        ]
    );
    Ok(())
}

/// A transaction that is refused for nesting never runs, so it must leave the
/// counter where it found it: the call that follows takes the next position,
/// not the one after a hole.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn a_refused_nested_transaction_leaves_the_counter_alone() -> Result<()> {
    use durare::SqliteProvider;
    use std::time::Duration;

    let (url, path) = common::temp_db_url("nested-seq");
    let provider = SqliteProvider::connect(&url).await?;
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register(WORKFLOW, |ctx: DurableContext, _: ()| async move {
        let nested_ctx = ctx.clone();
        ctx.transaction::<(), _>("outer", move |_tx| {
            let nested_ctx = nested_ctx.clone();
            Box::pin(async move {
                nested_ctx
                    .transaction::<(), _>("inner", |_tx| Box::pin(async { Ok(()) }))
                    .await
                    .expect_err("a transaction inside a transaction is refused");
                Ok(())
            })
        })
        .await?;
        ctx.step("after", || async { Ok::<_, Error>(1_i64) })
            .await?;
        Ok::<_, Error>(0_i64)
    });
    engine.launch().await?;
    engine
        .start::<_, i64>(WORKFLOW, (), WorkflowOptions::with_id(ID))
        .await?
        .result()
        .await?;

    assert_eq!(
        recorded(&engine).await?,
        [(0, "outer".to_string()), (1, "after".to_string())],
        "the refused nested transaction must not have spent position 1"
    );

    engine.shutdown(Duration::from_secs(1)).await?;
    common::remove_sqlite_files(&path);
    Ok(())
}
