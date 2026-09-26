//! Executable examples of the prototype's useful paths, beyond compile rejection.
use durare::{
    workflow_fn, DurableContext, DurableEngine, Error, InMemoryProvider, Result, StepOptions,
    WorkflowOptions,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

async fn helper(ctx: &DurableContext, n: i64) -> Result<i64> {
    ctx.step("helper", |_| async move { Ok(n + 1) }).await
}

async fn child(ctx: &DurableContext, n: i64) -> Result<i64> {
    helper(ctx, n).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn borrowed_handlers_keep_helpers_children_and_plain_tasks_usable() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("child", child);
    let dependency = Arc::new(10_i64);
    engine.register(
        "parent",
        workflow_fn(move |ctx, n: i64| {
            let dependency = dependency.clone();
            Box::pin(async move {
                let (a, b) = tokio::join!(
                    helper(ctx, n),
                    ctx.start_workflow::<_, i64>(
                        "child",
                        n,
                        WorkflowOptions::with_id("borrowed-child")
                    )
                );
                // Ordinary task work remains valid inside a checkpointed body.
                let c = ctx
                    .step("plain-task", |_| async move {
                        Ok(tokio::spawn(async move { *dependency }).await.unwrap())
                    })
                    .await?;
                Ok(a? + b?.result().await? + c)
            })
        }),
    );
    assert_eq!(
        engine
            .start::<_, i64>("parent", 2_i64, WorkflowOptions::with_id("borrowed-parent"))
            .await?
            .result()
            .await?,
        16
    );
    Ok(())
}

#[tokio::test]
async fn step_metadata_tracks_attempts_without_changing_the_key() -> Result<()> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capture = seen.clone();
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register(
        "retry",
        workflow_fn(move |ctx, (): ()| {
            let seen = capture.clone();
            Box::pin(async move {
                ctx.step_with(
                    StepOptions::new("effect")
                        .max_retries(2)
                        .base_interval(Duration::ZERO),
                    |step| {
                        let seen = seen.clone();
                        async move {
                            seen.lock().unwrap().push((
                                step.step_id,
                                step.attempt,
                                step.max_attempts,
                                step.is_last_attempt(),
                                step.idempotency_key(),
                            ));
                            if step.attempt < 2 {
                                Err(Error::app("retry"))
                            } else {
                                Ok(())
                            }
                        }
                    },
                )
                .await
            })
        }),
    );
    engine
        .start::<_, ()>(
            "retry",
            (),
            WorkflowOptions::with_id("key:with/\"separators"),
        )
        .await?
        .result()
        .await?;
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    for (i, (position, attempt, max, last, key)) in seen.iter().enumerate() {
        assert_eq!((*position, *attempt, *max, *last), (0, i as u32, 3, i == 2));
        assert_eq!(key, &seen[0].4);
    }
    Ok(())
}
