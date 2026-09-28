use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StepCtx, StepOptions,
    WorkflowOptions,
};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn key_is_stable_across_step_retries_and_distinct_for_effects() -> Result<()> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let seen_by_body = seen.clone();
    engine.register("keys", move |ctx: DurableContext, _: ()| {
        let seen = seen_by_body.clone();
        async move {
            ctx.step_with(
                StepOptions::new("call").max_retries(1),
                move |step: StepCtx| {
                    let seen = seen.clone();
                    async move {
                        let charge = step.idempotency_key_for("charge");
                        let email = step.idempotency_key_for("email");
                        assert_ne!(charge, email);
                        seen.lock().unwrap().push(charge.clone());
                        if step.attempt() == 0 {
                            Err(Error::app("retry"))
                        } else {
                            Ok(charge)
                        }
                    }
                },
            )
            .await
        }
    });
    engine.launch().await?;
    let key: String = engine
        .start("keys", (), WorkflowOptions::with_id("retry-key"))
        .await?
        .result()
        .await?;
    assert_eq!(
        key, "durare-step-v1-O5EBVPryP9FC1ddtGYWibrzJHLX6hZS8NxNrAxd492w",
        "the public key encoding must remain stable"
    );
    assert_eq!(*seen.lock().unwrap(), vec![key.clone(), key]);
    assert_eq!(engine.verify_replay("retry-key").await?.divergence, None);
    assert_eq!(seen.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn patch_marker_is_not_mistaken_for_the_step_identity() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("patched", |ctx: DurableContext, _: ()| async move {
        assert!(ctx.patch("v2").await?);
        ctx.step("effect", |step: StepCtx| async move {
            assert_eq!(step.step_id(), 1, "the marker owns position zero");
            Ok::<_, Error>(step.idempotency_key_for("effect"))
        })
        .await
    });
    engine.launch().await?;
    let key: String = engine
        .start("patched", (), WorkflowOptions::with_id("patched-key"))
        .await?
        .result()
        .await?;
    assert!(key.starts_with("durare-step-v1-"));
    assert_eq!(engine.verify_replay("patched-key").await?.divergence, None);
    Ok(())
}

#[tokio::test]
async fn repeated_step_name_does_not_reuse_the_first_effect_key() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("repeated", |ctx: DurableContext, _: ()| async move {
        let first = ctx
            .step("effect", |step: StepCtx| async move {
                Ok::<_, Error>(step.idempotency_key_for("call"))
            })
            .await?;
        let second = ctx
            .step("effect", |step: StepCtx| async move {
                Ok::<_, Error>(step.idempotency_key_for("call"))
            })
            .await?;
        Ok::<_, Error>((first, second))
    });
    engine.launch().await?;
    let (first, second): (String, String) = engine
        .start("repeated", (), WorkflowOptions::with_id("repeated-id"))
        .await?
        .result()
        .await?;
    assert_ne!(first, second);
    Ok(())
}

#[tokio::test]
async fn fork_gets_new_key_for_rerun_step() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("fork-keys", |ctx: DurableContext, _: ()| async move {
        ctx.step("first", |step: StepCtx| async move {
            Ok::<_, Error>(step.idempotency_key_for("effect"))
        })
        .await?;
        ctx.step("second", |step: StepCtx| async move {
            Ok::<_, Error>(step.idempotency_key_for("effect"))
        })
        .await
    });
    engine.launch().await?;
    let original: String = engine
        .start("fork-keys", (), WorkflowOptions::with_id("original"))
        .await?
        .result()
        .await?;
    let forked: String = engine
        .fork_workflow("original", 1, WorkflowOptions::with_id("fork"))
        .await?
        .result()
        .await?;
    assert_ne!(original, forked);
    Ok(())
}
