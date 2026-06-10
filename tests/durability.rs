//! Backend-free tests using the in-memory provider.

use durust::{DurableContext, DurableEngine, Error, InMemoryProvider, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A step's side effect must run exactly once even if the workflow is executed
/// again under the same id (the core durable-execution guarantee).
#[tokio::test]
async fn step_runs_once_across_replays() -> Result<()> {
    static CHARGES: AtomicUsize = AtomicUsize::new(0);

    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;

    engine.register("charge", |ctx: DurableContext, _: ()| async move {
        let amount = ctx
            .step("charge_card", || async {
                CHARGES.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(4999_i64)
            })
            .await?;
        Ok::<_, Error>(amount)
    });

    // First execution runs the step.
    let a: i64 = engine.start_typed("charge", "wf-1", ()).await?;
    // Re-executing the same workflow id replays from checkpoints.
    let b: i64 = engine.start_typed("charge", "wf-1", ()).await?;

    assert_eq!(a, 4999);
    assert_eq!(b, 4999);
    assert_eq!(
        CHARGES.load(Ordering::SeqCst),
        1,
        "the charge side effect must execute exactly once across replays"
    );
    Ok(())
}

/// Multiple steps keep their order and individual results across a replay.
#[tokio::test]
async fn multi_step_results_are_stable() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;

    engine.register("pipeline", |ctx: DurableContext, start: i64| async move {
        let a = ctx
            .step("double", || async { Ok::<_, Error>(start * 2) })
            .await?;
        let b = ctx
            .step("plus_one", || async { Ok::<_, Error>(a + 1) })
            .await?;
        Ok::<_, Error>(b)
    });

    let out: i64 = engine.start_typed("pipeline", "wf-2", 10_i64).await?;
    assert_eq!(out, 21);

    // Replay yields the identical answer.
    let out2: i64 = engine.start_typed("pipeline", "wf-2", 10_i64).await?;
    assert_eq!(out2, 21);
    Ok(())
}
