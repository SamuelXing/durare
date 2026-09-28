use durare::{
    Client, DurableContext, DurableEngine, Error, InMemoryProvider, Result, StepCtx, StepOptions,
    WorkflowOptions,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

fn register_waiter(
    engine: &mut DurableEngine,
    attempts: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    observed: Arc<Notify>,
) {
    engine.register("waiter", move |ctx: DurableContext, _: ()| {
        let attempts = attempts.clone();
        let entered = entered.clone();
        let observed = observed.clone();
        async move {
            ctx.step_with(
                StepOptions::new("long-call").max_retries(3),
                move |step: StepCtx| {
                    let attempts = attempts.clone();
                    let entered = entered.clone();
                    let observed = observed.clone();
                    async move {
                        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                            entered.notify_one();
                            step.cancelled().await?;
                            observed.notify_one();
                            Err(Error::Cancelled(step.workflow_id().to_owned()))
                        } else {
                            Ok(42_i32)
                        }
                    }
                },
            )
            .await
        }
    });
}

#[tokio::test]
async fn external_cancel_reaches_running_step_without_retry_or_checkpoint() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let observed = Arc::new(Notify::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    register_waiter(
        &mut engine,
        attempts.clone(),
        entered.clone(),
        observed.clone(),
    );
    engine.launch().await?;

    let handle = engine
        .start::<_, i32>("waiter", (), WorkflowOptions::with_id("cancel-step"))
        .await?;
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("step did not start");
    // The client has no access to the engine's tasks or local tokens.
    Client::new(provider.clone())
        .cancel_workflow("cancel-step")
        .await?;
    tokio::time::timeout(Duration::from_secs(3), observed.notified())
        .await
        .expect("running step did not observe persisted cancellation");
    assert!(matches!(handle.result().await, Err(Error::Cancelled(_))));
    engine.shutdown(Duration::from_secs(2)).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        engine.get_workflow_steps("cancel-step").await?.is_empty(),
        "cancellation must not checkpoint a failed step"
    );

    let mut resumed_engine = DurableEngine::new(provider.clone()).await?;
    register_waiter(&mut resumed_engine, attempts.clone(), entered, observed);
    resumed_engine.launch().await?;
    let resumed = resumed_engine.resume_workflow::<i32>("cancel-step").await?;
    assert_eq!(resumed.result().await?, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    resumed_engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

#[tokio::test]
async fn plain_step_cancellation_is_not_saved_as_a_business_failure() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let runs = Arc::new(AtomicUsize::new(0));
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let runs_for_body = runs.clone();
    engine.register("plain", move |ctx: DurableContext, _: ()| {
        let runs = runs_for_body.clone();
        async move {
            ctx.step("work", move || async move {
                if runs.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(Error::Cancelled("plain-id".to_owned()))
                } else {
                    Ok(7_i32)
                }
            })
            .await
        }
    });
    engine.launch().await?;
    let first = engine
        .start::<_, i32>("plain", (), WorkflowOptions::with_id("plain-id"))
        .await?;
    assert!(matches!(first.result().await, Err(Error::Cancelled(_))));
    engine.shutdown(Duration::from_secs(2)).await?;
    assert!(engine.get_workflow_steps("plain-id").await?.is_empty());

    let mut resumed_engine = DurableEngine::new(provider).await?;
    let runs_for_body = runs.clone();
    resumed_engine.register("plain", move |ctx: DurableContext, _: ()| {
        let runs = runs_for_body.clone();
        async move {
            ctx.step("work", move || async move {
                runs.fetch_add(1, Ordering::SeqCst);
                Ok::<_, Error>(7_i32)
            })
            .await
        }
    });
    resumed_engine.launch().await?;
    let resumed = resumed_engine.resume_workflow::<i32>("plain-id").await?;
    assert_eq!(resumed.result().await?, 7);
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    resumed_engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

#[tokio::test]
async fn cancelled_child_result_is_a_recorded_parent_step_failure() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider).await?;
    let child_slot = Arc::new(std::sync::Mutex::new(None::<durare::WorkflowHandle<i32>>));
    let parent_attempts = Arc::new(AtomicUsize::new(0));
    let retry_attempts = Arc::new(AtomicUsize::new(0));
    engine.register("child", |_: DurableContext, _: ()| async {
        Err::<i32, _>(Error::Cancelled("child-id".to_owned()))
    });
    let parent_child_slot = child_slot.clone();
    let parent_attempts_for_body = parent_attempts.clone();
    engine.register("parent", move |ctx: DurableContext, _: ()| {
        let child = parent_child_slot
            .lock()
            .expect("child handle mutex poisoned")
            .as_ref()
            .expect("child handle installed")
            .clone();
        let attempts = parent_attempts_for_body.clone();
        async move {
            ctx.step("wait-child", move || async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                child.result().await
            })
            .await
        }
    });
    let retry_child_slot = child_slot.clone();
    let retry_attempts_for_body = retry_attempts.clone();
    engine.register("parent-retry", move |ctx: DurableContext, _: ()| {
        let child = retry_child_slot
            .lock()
            .expect("child handle mutex poisoned")
            .as_ref()
            .expect("child handle installed")
            .clone();
        let attempts = retry_attempts_for_body.clone();
        async move {
            ctx.step_with(
                StepOptions::new("wait-child")
                    .max_retries(1)
                    .base_interval(Duration::from_millis(1)),
                move || {
                    let child = child.clone();
                    let attempts = attempts.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        child.result().await
                    }
                },
            )
            .await
        }
    });
    engine.launch().await?;
    let child = engine
        .start::<_, i32>("child", (), WorkflowOptions::with_id("child-id"))
        .await?;
    assert!(matches!(child.result().await, Err(Error::Cancelled(id)) if id == "child-id"));

    *child_slot.lock().expect("child handle mutex poisoned") =
        Some(engine.retrieve_workflow::<i32>("child-id").await?);
    let parent = engine
        .start::<_, i32>("parent", (), WorkflowOptions::with_id("parent-id"))
        .await?;
    let _ = parent.result().await;
    let status = parent.get_status().await?;
    assert_eq!(status.status, "ERROR", "the parent was never cancelled");
    assert_eq!(
        engine.get_workflow_steps("parent-id").await?.len(),
        1,
        "the child's cancellation must be checkpointed as the parent's step failure"
    );
    let retry_parent = engine
        .start::<_, i32>(
            "parent-retry",
            (),
            WorkflowOptions::with_id("parent-retry-id"),
        )
        .await?;
    let _ = retry_parent.result().await;
    assert_eq!(retry_parent.get_status().await?.status, "ERROR");
    assert_eq!(retry_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(engine.get_workflow_steps("parent-retry-id").await?.len(), 1);
    let replay = engine.verify_replay("parent-id").await?;
    assert!(replay.passes(), "{replay:#?}");
    assert_eq!(parent_attempts.load(Ordering::SeqCst), 1);
    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn separate_sqlite_client_cancels_a_running_step() -> Result<()> {
    use durare::SqliteProvider;

    struct TempDb(std::path::PathBuf);
    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let path = format!("{}{suffix}", self.0.display());
                let _ = std::fs::remove_file(path);
            }
        }
    }
    let path = std::env::temp_dir().join(format!("durare-step-cancel-{}.db", uuid::Uuid::new_v4()));
    let db = TempDb(path);
    let url = format!("sqlite://{}", db.0.display());
    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let observed = Arc::new(Notify::new());
    register_waiter(
        &mut engine,
        attempts.clone(),
        entered.clone(),
        observed.clone(),
    );
    engine.launch().await?;
    let handle = engine
        .start::<_, i32>("waiter", (), WorkflowOptions::with_id("sqlite-cancel-step"))
        .await?;
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .expect("SQLite step did not start");
    let client = Client::new(Arc::new(SqliteProvider::connect(&url).await?));
    client.cancel_workflow("sqlite-cancel-step").await?;
    tokio::time::timeout(Duration::from_secs(3), observed.notified())
        .await
        .expect("step did not see cancellation from a second SQLite connection");
    assert!(matches!(handle.result().await, Err(Error::Cancelled(_))));
    engine.shutdown(Duration::from_secs(2)).await?;
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}
