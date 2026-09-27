use super::test_provider::{Fault, FaultProvider};
use crate::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

#[tokio::test]
async fn parent_recovery_observes_an_existing_running_child() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::ChildRecordBefore));
    let mut engine = DurableEngine::new(provider).await?;
    let effects = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (counted, gate) = (effects.clone(), release.clone());
    engine.register("child", move |ctx: DurableContext, _: ()| {
        let (counted, gate) = (counted.clone(), gate.clone());
        async move {
            ctx.step("effect", || async {
                counted.fetch_add(1, Ordering::SeqCst);
                gate.acquire().await.unwrap().forget();
                Ok(42)
            })
            .await
        }
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        ctx.start_workflow::<_, i32>("child", (), WorkflowOptions::default())
            .await?
            .result()
            .await
    });
    let failed = engine
        .start::<_, i32>("parent", (), WorkflowOptions::with_id("parent"))
        .await?
        .result()
        .await;
    assert!(matches!(failed, Err(Error::RecoveryRequired(_))));
    tokio::time::timeout(Duration::from_secs(4), async {
        while inner.check_child_workflow("parent", 0).await?.is_none() {
            tokio::task::yield_now().await;
        }
        Ok::<_, Error>(())
    })
    .await
    .expect("parent recovery must record the existing child")?;
    let running = effects.load(Ordering::SeqCst);
    release.add_permits(4);
    let result = tokio::time::timeout(
        Duration::from_secs(4),
        engine.retrieve_workflow::<i32>("parent").await?.result(),
    )
    .await;
    engine.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(
        running, 1,
        "recovery must not dispatch the live child twice"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(result.unwrap()?, 42);
    Ok(())
}

#[tokio::test]
async fn live_observation_faults_cannot_become_workflow_business_failures() -> Result<()> {
    for mode in ["status", "drain", "snapshot", "values"] {
        let inner = Arc::new(InMemoryProvider::new());
        let provider = Arc::new(FaultProvider::new(
            inner.clone(),
            if mode == "status" {
                Fault::StatusRead
            } else {
                Fault::StreamRead
            },
        ));
        let mut engine = DurableEngine::new(provider.clone()).await?;
        inner
            .insert_workflow_status(WorkflowStatus::new(
                "producer",
                "unused",
                serde_json::Value::Null,
                STATUS_SUCCESS,
                "other",
                engine.app_version(),
            ))
            .await?;
        let handle = WorkflowHandle::<()>::polling("producer".into(), provider);
        engine.register("observe", move |ctx: DurableContext, _: ()| {
            let handle = handle.clone();
            async move {
                match mode {
                    "status" => {
                        handle.get_status().await?;
                    }
                    "drain" => {
                        ctx.read_stream::<i32>("producer", "events").await?;
                    }
                    "snapshot" => {
                        ctx.read_stream_snapshot::<i32>("producer", "events", 0)
                            .await?;
                    }
                    _ => {
                        use futures_util::{FutureExt, StreamExt};
                        let values = ctx.read_stream_values::<i32>("producer", "events");
                        futures_util::pin_mut!(values);
                        if let Some(value) = values
                            .next()
                            .now_or_never()
                            .expect("in-memory read completes immediately")
                        {
                            value?;
                        }
                    }
                }
                Ok(())
            }
        });
        let result = engine
            .start::<_, ()>("observe", (), WorkflowOptions::with_id("observe"))
            .await?
            .result()
            .await;
        engine.shutdown(Duration::from_secs(1)).await?;
        assert!(
            matches!(result, Err(Error::RecoveryRequired(_))),
            "{mode}: {result:?}"
        );
        let row = inner.get_workflow_status("observe").await?.unwrap();
        assert_eq!(row.status, STATUS_PENDING, "{mode}");
        assert!(row.error.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn replay_verification_returns_observation_faults_without_divergence() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(inner.clone()).await?;
    engine.register("observe", |_: DurableContext, _: ()| async {
        Err::<(), _>(Error::ObservationFailed(Arc::new(Error::Db(
            sqlx::Error::PoolTimedOut,
        ))))
    });
    inner
        .insert_workflow_status(WorkflowStatus::new(
            "observe",
            "observe",
            serde_json::Value::Null,
            STATUS_PENDING,
            "stopped",
            engine.app_version(),
        ))
        .await?;
    let result = engine.verify_replay("observe").await;
    assert!(
        matches!(result, Err(Error::ObservationFailed(_))),
        "{result:?}"
    );
    Ok(())
}

#[tokio::test]
async fn panic_parks_once_and_releases_queue_capacity() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(inner.clone()).await?;
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    engine.register("panic", move |_: DurableContext, _: ()| {
        let counted = counted.clone();
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            panic!("deterministic panic after an external effect");
            #[allow(unreachable_code)]
            Ok(())
        }
    });
    engine.register("healthy", |_: DurableContext, _: ()| async { Ok(42) });
    engine.register_queue(
        WorkflowQueue::new("one")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;
    let panicked = engine
        .start::<_, ()>("panic", (), WorkflowOptions::with_id("panic").queue("one"))
        .await?;
    let parked = tokio::time::timeout(Duration::from_secs(2), panicked.result()).await;
    let healthy = engine
        .start::<_, i32>(
            "healthy",
            (),
            WorkflowOptions::with_id("healthy").queue("one"),
        )
        .await?;
    let next = tokio::time::timeout(Duration::from_secs(2), healthy.result()).await;
    engine.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        parked
            .expect("panic must park immediately")
            .unwrap_err()
            .code(),
        ErrorCode::MaxRecoveryAttemptsExceeded
    );
    assert_eq!(next.expect("parking must release capacity")?, 42);
    Ok(())
}

#[tokio::test]
async fn deactivate_stops_an_in_flight_automatic_recovery_claim() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteBefore));
    provider.block_claim.store(true, Ordering::SeqCst);
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    engine.register("effect", move |ctx: DurableContext, _: ()| {
        let counted = counted.clone();
        async move {
            ctx.step("effect", || async {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        }
    });
    let result = engine
        .start::<_, ()>("effect", (), WorkflowOptions::with_id("effect"))
        .await?
        .result()
        .await;
    assert!(matches!(result, Err(Error::RecoveryRequired(_))));
    tokio::time::timeout(Duration::from_secs(2), provider.claim_started.notified())
        .await
        .unwrap();
    engine.deactivate();
    provider.claim_permit.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while engine.metrics().await.unwrap().workflows_in_flight != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the stopped recovery task must drain without shutdown");
    engine.shutdown(Duration::from_secs(2)).await?;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        inner.get_workflow_status("effect").await?.unwrap().status,
        STATUS_PENDING
    );
    Ok(())
}

#[tokio::test]
async fn ambiguous_direct_creation_retry_observes_without_dispatching() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::InsertAfter));
    let mut engine = DurableEngine::new(provider).await?;
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    engine.register("create", move |_: DurableContext, _: ()| {
        counted.fetch_add(1, Ordering::SeqCst);
        async { Ok(42) }
    });
    assert!(matches!(
        engine
            .start::<_, i32>("create", (), WorkflowOptions::with_id("create"))
            .await,
        Err(Error::ObservationFailed(_))
    ));
    let observer = engine
        .start::<_, i32>("create", (), WorkflowOptions::with_id("create"))
        .await?;
    assert_eq!(observer.get_status().await?.status, STATUS_PENDING);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(engine.metrics().await?.workflows_in_flight, 0);
    // Here the test knows the creator finished and never dispatched. A retry
    // caller cannot infer this fact from PENDING alone.
    assert_eq!(engine.recover().await?, 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observer.result())
            .await
            .unwrap()?,
        42
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn stream_type_mismatches_remain_catchable_and_observation_causes_stay_flat() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "producer",
            "unused",
            serde_json::Value::Null,
            STATUS_SUCCESS,
            "other",
            "version",
        ))
        .await?;
    provider
        .write_stream("producer", "events", Some(serde_json::json!("string")), 0)
        .await?;
    provider.write_stream("producer", "events", None, 1).await?;
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("types", |ctx: DurableContext, _: ()| async move {
        assert!(matches!(
            ctx.read_stream::<i32>("producer", "events").await,
            Err(Error::Serde(_))
        ));
        assert!(matches!(
            ctx.read_stream_snapshot::<i32>("producer", "events", 0)
                .await,
            Err(Error::Serde(_))
        ));
        Ok(42)
    });
    assert_eq!(
        engine
            .start::<_, i32>("types", (), WorkflowOptions::default())
            .await?
            .result()
            .await?,
        42
    );
    let execution = super::Execution::default();
    let cause = Arc::new(Error::Db(sqlx::Error::PoolTimedOut));
    let error = execution.body_error(Error::ObservationFailed(cause.clone()));
    let Error::RecoveryRequired(actual) = error else {
        panic!("expected interruption")
    };
    assert!(Arc::ptr_eq(&actual, &cause));
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn bulk_resume_unparks_without_reopening_completed_workflows() -> Result<()> {
    let provider = InMemoryProvider::new();
    for (id, status) in [
        ("parked", STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED),
        ("success", STATUS_SUCCESS),
        ("error", STATUS_ERROR),
    ] {
        let mut row = WorkflowStatus::new(
            id,
            "unused",
            serde_json::Value::Null,
            status,
            "other",
            "version",
        );
        row.recovery_attempts = 9;
        provider.insert_workflow_status(row).await?;
    }
    assert_eq!(
        provider
            .resume_workflows(&["parked".into(), "success".into(), "error".into()])
            .await?,
        vec!["parked"]
    );
    assert_eq!(
        provider
            .get_workflow_status("parked")
            .await?
            .unwrap()
            .recovery_attempts,
        0
    );
    assert_eq!(
        provider
            .get_workflow_status("success")
            .await?
            .unwrap()
            .status,
        STATUS_SUCCESS
    );
    assert_eq!(
        provider.get_workflow_status("error").await?.unwrap().status,
        STATUS_ERROR
    );
    Ok(())
}

#[tokio::test]
async fn a_panicked_direct_child_does_not_hold_its_parents_queue_slot() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(inner.clone()).await?;
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    engine.register("child", move |_: DurableContext, _: ()| {
        let counted = counted.clone();
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            panic!("child cannot complete until repaired");
            #[allow(unreachable_code)]
            Ok(42)
        }
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        ctx.start_workflow::<_, i32>("child", (), WorkflowOptions::default())
            .await?
            .result()
            .await
    });
    engine.register("healthy", |_: DurableContext, _: ()| async { Ok(42) });
    engine.register_queue(
        WorkflowQueue::new("one")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;
    let parent = engine
        .start::<_, i32>(
            "parent",
            (),
            WorkflowOptions::with_id("parent").queue("one"),
        )
        .await?;
    let outcome = tokio::time::timeout(Duration::from_secs(3), parent.result()).await;
    let healthy = engine
        .start::<_, i32>(
            "healthy",
            (),
            WorkflowOptions::with_id("healthy").queue("one"),
        )
        .await?;
    let next = tokio::time::timeout(Duration::from_secs(3), healthy.result()).await;
    engine.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(
        outcome.unwrap().unwrap_err().code(),
        ErrorCode::MaxRecoveryAttemptsExceeded
    );
    assert_eq!(
        inner.get_workflow_status("parent-0").await?.unwrap().status,
        STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
    );
    assert_eq!(next.unwrap()?, 42);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    Ok(())
}
