use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StepCtx, StepOptions,
    WorkflowOptions,
};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn step_body_sees_its_claimed_position_even_when_polled_out_of_order() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("positions", |ctx: DurableContext, _: ()| async move {
        let workflow_id = ctx.workflow_id().to_owned();
        let a = ctx.step("a", |step: StepCtx| async move {
            assert_eq!(step.workflow_id(), workflow_id);
            assert_eq!(step.attempt(), 0);
            assert_eq!(step.max_attempts(), 1);
            Ok::<_, Error>(step.step_id())
        });
        let b = ctx.step("b", |step: StepCtx| async move {
            assert_eq!(step.attempt(), 0);
            Ok::<_, Error>(step.step_id())
        });
        assert_eq!(ctx.current_step_id(), 2, "this is the next position");
        let (b, a) = tokio::join!(b, a);
        Ok::<_, Error>((a?, b?))
    });
    engine.launch().await?;
    let result = engine
        .start::<_, (i32, i32)>("positions", (), WorkflowOptions::with_id("position-id"))
        .await?
        .result()
        .await?;
    assert_eq!(result, (0, 1));
    assert_eq!(engine.verify_replay("position-id").await?.divergence, None);
    Ok(())
}

#[tokio::test]
async fn step_with_body_sees_each_attempt_without_changing_identity() -> Result<()> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let seen_by_body = seen.clone();
    engine.register("retry", move |ctx: DurableContext, _: ()| {
        let seen = seen_by_body.clone();
        async move {
            ctx.step_with(
                StepOptions::new("retry")
                    .max_retries(2)
                    .base_interval(std::time::Duration::ZERO),
                move |step: StepCtx| {
                    let seen = seen.clone();
                    async move {
                        seen.lock().unwrap().push((
                            step.step_id(),
                            step.attempt(),
                            step.max_attempts(),
                        ));
                        if step.attempt() < 2 {
                            Err(Error::app("try again"))
                        } else {
                            Ok(42_i32)
                        }
                    }
                },
            )
            .await
        }
    });
    engine.launch().await?;
    let value = engine
        .start::<_, i32>("retry", (), WorkflowOptions::with_id("retry-id"))
        .await?
        .result()
        .await?;
    assert_eq!(value, 42);
    assert_eq!(*seen.lock().unwrap(), vec![(0, 0, 3), (0, 1, 3), (0, 2, 3)]);
    assert_eq!(engine.verify_replay("retry-id").await?.divergence, None);
    assert_eq!(
        seen.lock().unwrap().len(),
        3,
        "replay must not call the body"
    );
    Ok(())
}

#[tokio::test]
async fn maximum_retry_budget_does_not_overflow_total_attempts() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("large-budget", |ctx: DurableContext, _: ()| async move {
        ctx.step_with(
            StepOptions::new("large-budget").max_retries(u32::MAX),
            |step: StepCtx| async move { Ok::<_, Error>(step.max_attempts()) },
        )
        .await
    });
    engine.launch().await?;
    let value = engine
        .start::<_, u64>(
            "large-budget",
            (),
            WorkflowOptions::with_id("large-budget-id"),
        )
        .await?
        .result()
        .await?;
    assert_eq!(value, u64::from(u32::MAX) + 1);
    Ok(())
}
