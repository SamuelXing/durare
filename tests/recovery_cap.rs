//! A workflow's own recovery cap, set at the definition or on the engine,
//! parks it after that many recovery claims while its neighbours keep the
//! engine-wide cap.
use durare::{
    DurableContext, DurableEngine, Error, ErrorCode, InMemoryProvider, Result, WorkflowOptions,
    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED,
};
use std::sync::Arc;
use std::time::Duration;

#[durare::workflow(max_recovery_attempts = 1)]
async fn fragile(_ctx: DurableContext, (): ()) -> Result<()> {
    panic!("fragile body");
    #[allow(unreachable_code)]
    Ok(())
}

/// Capped at 1 by its attribute, raised to the engine's cap below.
#[durare::workflow(max_recovery_attempts = 1)]
async fn overridden(_ctx: DurableContext, (): ()) -> Result<()> {
    panic!("overridden body");
    #[allow(unreachable_code)]
    Ok(())
}

#[tokio::test]
async fn a_workflow_parks_at_its_own_cap() -> Result<()> {
    let mut b = DurableEngine::builder(Arc::new(InMemoryProvider::new()));
    b.register("sturdy", |_ctx: DurableContext, (): ()| async move {
        panic!("sturdy body");
        #[allow(unreachable_code)]
        Ok::<_, Error>(())
    });
    b.register("tuned", |_ctx: DurableContext, (): ()| async move {
        panic!("tuned body");
        #[allow(unreachable_code)]
        Ok::<_, Error>(())
    });
    b.max_recovery_attempts(3);
    b.max_recovery_attempts_for("tuned", 1);
    b.max_recovery_attempts_for("overridden", 3);
    let engine = b.build().await?;
    engine.launch().await?;

    for name in ["fragile", "overridden", "sturdy", "tuned"] {
        let run = engine
            .start::<(), ()>(name, (), WorkflowOptions::with_id(name))
            .await?
            .await;
        assert!(run.is_err(), "{name}: the panic surfaces to the caller");
    }

    let me = [engine.executor_id().to_string()];
    let mut rounds = Vec::new();
    for _ in 0..4 {
        let mut recovered = engine.recover_pending_for(&me).await?;
        recovered.sort();
        rounds.push(recovered);
    }
    // Claim 1 is within every cap. Claims 2 and 3 are within a cap of 3 only:
    // the two capped at 1 are parked on claim 2, the others on claim 4.
    assert_eq!(
        rounds,
        vec![
            vec![
                "fragile".to_string(),
                "overridden".into(),
                "sturdy".into(),
                "tuned".into()
            ],
            vec!["overridden".to_string(), "sturdy".into()],
            vec!["overridden".to_string(), "sturdy".into()],
            vec![],
        ]
    );
    for name in ["fragile", "overridden", "sturdy", "tuned"] {
        let status = engine
            .retrieve_workflow::<()>(name)
            .await?
            .get_status()
            .await?;
        assert_eq!(
            status.status, STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED,
            "{name}"
        );
    }
    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

#[tokio::test]
async fn a_cap_must_name_a_registered_workflow() -> Result<()> {
    let mut b = DurableEngine::builder(Arc::new(InMemoryProvider::new()));
    b.max_recovery_attempts_for("missing", 1);
    let Err(err) = b.build().await else {
        panic!("a cap for an unregistered workflow must be refused");
    };
    assert_eq!(err.code(), ErrorCode::WorkflowNotRegistered);
    Ok(())
}

#[tokio::test]
async fn cap_set_after_first_start_before_launch_is_used() -> Result<()> {
    use durare::{StateProvider, WorkflowStatus};
    let db = Arc::new(InMemoryProvider::new());
    let mut builder = DurableEngine::builder(db.clone());
    builder.register("done", |_ctx: DurableContext, (): ()| async { Ok(()) });
    let mut engine = builder.build().await?;
    engine
        .start::<_, ()>("done", (), WorkflowOptions::with_id("warmup"))
        .await?
        .result()
        .await?;
    engine.max_recovery_attempts_for("done", 0);
    engine.launch().await?;
    db.insert_workflow_status(WorkflowStatus::new(
        "pending",
        "done",
        serde_json::Value::Null,
        "PENDING",
        "dead",
        engine.app_version(),
    ))
    .await?;
    assert!(engine
        .recover_pending_for(&["dead".into()])
        .await?
        .is_empty());
    assert_eq!(
        engine
            .retrieve_workflow::<()>("pending")
            .await?
            .get_status()
            .await?
            .status,
        STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
    );
    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}
