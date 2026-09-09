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

/// Runs `body` as a workflow and reports which operation landed at each position.
async fn positions<F, Fut>(body: F) -> Result<Vec<String>>
where
    F: Fn(DurableContext) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<i64>> + Send + 'static,
{
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("probe", move |ctx: DurableContext, _: ()| body(ctx));
    engine
        .start::<_, i64>("probe", (), WorkflowOptions::with_id("wf"))
        .await?
        .result()
        .await?;

    let mut seen = Vec::new();
    for seq in 0.. {
        match provider.get_step_result("wf", seq).await? {
            Some(record) => seen.push(record.name),
            None => break,
        }
    }
    Ok(seen)
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

    assert_eq!(in_order, ["first", "second"]);
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
            let a = ctx.step("a", || async { Ok::<_, Error>(1_i64) });
            let b = ctx.step("b", || async { Ok::<_, Error>(2_i64) });
            let mut a = Box::pin(a);
            let mut b = Box::pin(b);
            tokio::select! {
                _ = &mut a => { let _ = b.await; }
                _ = &mut b => { let _ = a.await; }
            }
            Ok(0)
        })
        .await?;
        assert_eq!(
            seen,
            ["a", "b"],
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
    assert_eq!(seen, ["a_step", "DBOS.setEvent", "DBOS.uuid"]);
    Ok(())
}

/// A call that is built and dropped has still spent its position — deterministic,
/// because a replay runs the same code and skips the same position, but no longer
/// the no-op it would be for a plain future.
#[tokio::test]
async fn a_built_call_that_is_dropped_has_spent_its_position() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("probe", |ctx: DurableContext, _: ()| async move {
        let abandoned = ctx.step("abandoned", || async { Ok::<_, Error>(1_i64) });
        drop(abandoned);
        ctx.step("recorded", || async { Ok::<_, Error>(2_i64) })
            .await?;
        Ok::<_, Error>(0_i64)
    });
    engine
        .start::<_, i64>("probe", (), WorkflowOptions::with_id("wf"))
        .await?
        .result()
        .await?;

    let name_at = |seq: i32| {
        let provider = provider.clone();
        async move {
            provider
                .get_step_result("wf", seq)
                .await
                .map(|r| r.map(|r| r.name))
        }
    };
    assert_eq!(
        name_at(0).await?,
        None,
        "the dropped call claimed position 0 and wrote nothing there"
    );
    assert_eq!(
        name_at(1).await?,
        Some("recorded".to_string()),
        "the next call took the position after the abandoned one, not position 0"
    );
    Ok(())
}
