//! A durable call claims its position where it is **written**, not where it is
//! first polled.
//!
//! The position is the `(workflow_id, seq)` key a checkpoint is written under,
//! and a replay finds the recorded result only by asking for the position the
//! first run asked for. Taking it inside an `async fn` body would tie it to poll
//! order — which `tokio::select!` randomises and an out-of-order await reverses —
//! so these pin it to the source instead.

use durare::{
    workflow_fn, BoxFuture, DurableContext, DurableEngine, Error, InMemoryProvider, Result,
    WorkflowOptions,
};
use std::sync::Arc;

mod common;

const ID: &str = "wf";

/// The `(position, operation)` pairs the workflow recorded, in position order.
/// A position nothing was written at is simply absent, which is how a call that
/// claimed one without running shows up.
async fn recorded(engine: &DurableEngine) -> Result<Vec<(i32, String)>> {
    common::recorded(engine, ID).await
}

/// Runs `body` as a workflow and reports which operation landed at each position.
async fn positions<F>(body: F) -> Result<Vec<(i32, String)>>
where
    F: for<'a> Fn(&'a DurableContext) -> BoxFuture<'a, Result<i64>> + Send + Sync + 'static,
{
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("probe", workflow_fn(move |ctx, _: ()| body(ctx)));
    engine
        .start::<_, i64>("probe", (), WorkflowOptions::with_id("wf"))
        .await?
        .result()
        .await?;
    recorded(&engine).await
}

/// Awaiting two steps in the opposite order to the one they were written in must
/// not swap the positions they occupy.
#[tokio::test]
async fn awaiting_out_of_order_does_not_move_a_position() -> Result<()> {
    let in_order = positions(|ctx| {
        Box::pin(async move {
            let first = ctx.step("first", |_| async { Ok::<_, Error>(1_i64) });
            let second = ctx.step("second", |_| async { Ok::<_, Error>(2_i64) });
            first.await?;
            second.await?;
            Ok(0)
        })
    })
    .await?;

    let reversed = positions(|ctx| {
        Box::pin(async move {
            let first = ctx.step("first", |_| async { Ok::<_, Error>(1_i64) });
            let second = ctx.step("second", |_| async { Ok::<_, Error>(2_i64) });
            second.await?;
            first.await?;
            Ok(0)
        })
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
        let seen = positions(|ctx| {
            Box::pin(async move {
                let mut a = ctx.step("a", |_| async { Ok::<_, Error>(1_i64) });
                let mut b = ctx.step("b", |_| async { Ok::<_, Error>(2_i64) });
                tokio::select! {
                    _ = &mut a => { let _ = b.await; }
                    _ = &mut b => { let _ = a.await; }
                }
                Ok(0)
            })
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
    let seen = positions(|ctx| {
        Box::pin(async move {
            let step = ctx.step("a_step", |_| async { Ok::<_, Error>(1_i64) });
            let event = ctx.set_event("a_key", 7_i64);
            let value = ctx.uuid();
            // Awaited back to front; the positions must still read front to back.
            value.await?;
            event.await?;
            step.await?;
            Ok(0)
        })
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
    let seen = positions(|ctx| {
        Box::pin(async move {
            let abandoned = ctx.step("abandoned", |_| async { Ok::<_, Error>(1_i64) });
            drop(abandoned);
            ctx.step("recorded", |_| async { Ok::<_, Error>(2_i64) })
                .await?;
            Ok(0)
        })
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
    let seen = positions(|ctx| {
        Box::pin(async move {
            let first = macro_first(ctx, 1);
            let second = macro_second(ctx, 2);
            // Awaited back to front, as in the hand-written case above.
            second.await?;
            first.await?;
            Ok(0)
        })
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

// A transaction nested inside another transaction's body through a captured
// context is no longer a runtime refusal: `transaction`'s body must be `'static`
// and the context is only ever lent to a workflow, so the capture does not
// compile (see tests/compile_fail/nested_transaction.rs). The runtime guard
// remains for `transaction_on`, whose body may borrow the context; see
// tests/datasource.rs.
