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
    let row = inner.get_workflow_status("effect").await?.unwrap();
    assert_eq!(row.status, STATUS_PENDING);
    assert_eq!(
        row.recovery_attempts, 0,
        "deactivation must stop a direct restart claim"
    );
    assert!(provider.claim_outcomes.lock().unwrap().is_empty());
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
            assert_ne!(
                counted.fetch_add(1, Ordering::SeqCst),
                0,
                "child cannot complete until repaired"
            );
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
    let parent_before = inner.get_workflow_status("parent").await?.unwrap();
    assert_eq!(parent_before.status, STATUS_ERROR);

    // Resuming the child repairs only that workflow, not its parent's recorded
    // error. Explicitly resuming an ERROR parent must also be a no-op.
    let child = engine.resume_workflow::<i32>("parent-0").await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), child.result())
            .await
            .unwrap()?,
        42
    );
    let parent = engine.resume_workflow::<i32>("parent").await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), parent.result())
            .await
            .unwrap()
            .unwrap_err()
            .code(),
        ErrorCode::MaxRecoveryAttemptsExceeded
    );
    let parent_after = inner.get_workflow_status("parent").await?.unwrap();
    assert_eq!(parent_after.status, STATUS_ERROR);
    assert_eq!(parent_after.error, parent_before.error);
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

async fn wait_for_attempts_to_stop(engine: &DurableEngine) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while engine.metrics().await?.workflows_in_flight != 0 {
            tokio::task::yield_now().await;
        }
        Ok(())
    })
    .await
    .expect("workflow and recovery tasks must finish")
}

#[tokio::test]
async fn workflow_error_encoding_failure_uses_recovery_settlement() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let mut builder = DurableEngine::builder(inner.clone());
    builder.max_recovery_attempts(0);
    let mut engine = builder.build().await?;
    engine.register("unrecordable", |_: DurableContext, _: ()| async {
        let signal = Error::RecoveryRequired(Arc::new(Error::app("interrupted")));
        Err::<(), _>(Error::Recorded(Box::new(RecordedError::capture(&signal))))
    });
    let error = engine
        .start::<_, ()>("unrecordable", (), WorkflowOptions::with_id("unrecordable"))
        .await?
        .result()
        .await
        .unwrap_err();
    wait_for_attempts_to_stop(&engine).await?;
    engine.shutdown(Duration::from_secs(1)).await?;
    assert!(
        matches!(error, Error::RecoveryRequired(ref cause) if matches!(**cause, Error::Serialization(_))),
        "an error that cannot be recorded must enter settlement: {error:?}"
    );
    let row = inner.get_workflow_status("unrecordable").await?.unwrap();
    assert_eq!(row.status, STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED);
    assert_eq!(row.recovery_attempts, 1);
    assert!(
        row.error.is_none(),
        "an interruption is not a business failure"
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum DeactivateAt {
    Body,
    Claim,
}

async fn settle_while_deactivated(at: DeactivateAt, stop: Option<&str>) -> Result<()> {
    // Requeue, queued panic, direct panic, and a direct run at its retry cap.
    for (queued, panics, cap) in [
        (true, false, 100),
        (true, true, 100),
        (false, true, 100),
        (false, false, 0),
    ] {
        let inner = Arc::new(InMemoryProvider::new());
        let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteBefore));
        provider
            .block_claim
            .store(matches!(at, DeactivateAt::Claim), Ordering::SeqCst);
        let mut builder = DurableEngine::builder(provider.clone());
        builder.max_recovery_attempts(cap);
        let mut engine = builder.build().await?;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let runs = Arc::new(AtomicUsize::new(0));
        let (started, gate, counted) = (entered.clone(), release.clone(), runs.clone());
        engine.register("fault", move |ctx: DurableContext, _: ()| {
            let (started, gate, counted) = (started.clone(), gate.clone(), counted.clone());
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                gate.acquire().await.unwrap().forget();
                assert!(!panics, "workflow panic after an external effect");
                ctx.step("effect", || async { Ok(()) }).await
            }
        });
        engine.register("healthy", |_: DurableContext, _: ()| async { Ok(42) });
        engine.register_queue(
            WorkflowQueue::new("one")
                .global_concurrency(1)
                .base_polling_interval(Duration::from_millis(10)),
        );
        engine.launch().await?;
        let options = WorkflowOptions::with_id("fault");
        let options = if queued {
            options.queue("one")
        } else {
            options
        };
        engine.start::<_, ()>("fault", (), options).await?;
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        if queued {
            engine
                .start::<_, i32>("healthy", (), WorkflowOptions::with_id("next").queue("one"))
                .await?;
        }
        if matches!(at, DeactivateAt::Body) {
            engine.deactivate();
            // Deactivation must not claim or park an execution still in its body.
            assert_eq!(provider.claim_calls.load(Ordering::SeqCst), 0);
            assert_eq!(
                inner.get_workflow_status("fault").await?.unwrap().status,
                STATUS_PENDING
            );
        }
        release.add_permits(1);
        if matches!(at, DeactivateAt::Claim) {
            tokio::time::timeout(Duration::from_secs(3), provider.claim_started.notified())
                .await
                .unwrap();
            engine.deactivate();
        }
        match stop {
            Some("shutdown") => {
                engine.shutdown(Duration::ZERO).await?;
            }
            Some("cancel") => {
                engine.cancel_workflow("fault").await?;
            }
            _ => {}
        }
        // On shutdown, keep the provider gate held: the recovery task must stop
        // without waiting for a reply. Otherwise let the CAS finish or lose.
        if stop != Some("shutdown") {
            provider.claim_permit.add_permits(1);
        }
        wait_for_attempts_to_stop(&engine).await?;
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "no new body on the draining executor"
        );
        let row = inner.get_workflow_status("fault").await?.unwrap();
        let expected = match stop {
            Some("shutdown") => STATUS_PENDING,
            Some("cancel") => STATUS_CANCELLED,
            _ if queued && !panics => STATUS_ENQUEUED,
            _ => STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED,
        };
        assert_eq!(
            row.status, expected,
            "queued={queued}, panics={panics}, cap={cap}, stop={stop:?}"
        );
        assert_eq!(row.recovery_attempts, if stop.is_some() { 0 } else { 1 });
        assert!(row.error.is_none());
        if queued && stop.is_none() {
            let rows = inner
                .dequeue_workflows(&DequeueRequest {
                    queue_name: "one".into(),
                    executor_id: "other".into(),
                    app_version: engine.app_version().into(),
                    partition_key: None,
                    max_tasks: 1,
                    global_concurrency: Some(1),
                    rate_limit_max: None,
                    rate_limit_period_ms: None,
                })
                .await?;
            assert_eq!(
                rows.len(),
                1,
                "the stopped run must release global capacity"
            );
        }
        engine.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn deactivation_allows_stopped_bodies_to_requeue_or_park() -> Result<()> {
    settle_while_deactivated(DeactivateAt::Body, None).await
}

#[tokio::test]
async fn deactivation_allows_in_flight_requeue_and_parking_claims() -> Result<()> {
    settle_while_deactivated(DeactivateAt::Claim, None).await
}

#[tokio::test]
async fn deactivated_settlement_still_respects_shutdown_and_cancellation() -> Result<()> {
    for action in ["shutdown", "cancel"] {
        settle_while_deactivated(DeactivateAt::Claim, Some(action)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn shutdown_timeout_does_not_stop_a_running_body() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(inner.clone()).await?;
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (started, gate) = (entered.clone(), release.clone());
    engine.register("held", move |ctx: DurableContext, _: ()| {
        let (started, gate) = (started.clone(), gate.clone());
        async move {
            ctx.step("held", || async {
                started.notify_one();
                gate.acquire().await.unwrap().forget();
                Ok(42)
            })
            .await
        }
    });
    let handle = engine
        .start::<_, i32>("held", (), WorkflowOptions::with_id("held"))
        .await?;
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    engine.shutdown(Duration::ZERO).await?;
    assert_eq!(engine.metrics().await?.workflows_in_flight, 1);
    assert_eq!(
        inner.get_workflow_status("held").await?.unwrap().status,
        STATUS_PENDING
    );
    release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), handle.result())
            .await
            .unwrap()?,
        42
    );
    wait_for_attempts_to_stop(&engine).await?;
    assert_eq!(
        inner.get_workflow_status("held").await?.unwrap().status,
        STATUS_SUCCESS
    );
    Ok(())
}

#[tokio::test]
async fn lost_recovery_claim_reply_never_authorizes_direct_dispatch() -> Result<()> {
    for queued in [false, true] {
        let inner = Arc::new(InMemoryProvider::new());
        let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteBefore));
        provider.arm([Fault::WriteBefore, Fault::ClaimAfter]);
        let mut engine = DurableEngine::new(provider.clone()).await?;
        let effects = Arc::new(AtomicUsize::new(0));
        let counted = effects.clone();
        engine.register("effect", move |ctx: DurableContext, _: ()| {
            let counted = counted.clone();
            async move {
                ctx.step("effect", || async {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(42)
                })
                .await
            }
        });
        let mut options = WorkflowOptions::with_id("effect");
        if queued {
            engine.register_queue(
                WorkflowQueue::new("one")
                    .global_concurrency(1)
                    .base_polling_interval(Duration::from_millis(10)),
            );
            engine.launch().await?;
            options = options.queue("one");
        }
        let handle = engine.start::<_, i32>("effect", (), options).await?;
        let outcome = tokio::time::timeout(Duration::from_secs(5), handle.result())
            .await
            .unwrap();
        if queued {
            assert_eq!(outcome?, 42);
        } else {
            assert!(matches!(outcome, Err(Error::RecoveryRequired(_))));
        }
        wait_for_attempts_to_stop(&engine).await?;
        let claims = provider.claim_outcomes.lock().unwrap().clone();
        assert_eq!(provider.claim_calls.load(Ordering::SeqCst), 2);
        assert_eq!(claims.len(), 2);
        assert_eq!(claims[1], RecoveryClaim::Lost);
        let row = inner.get_workflow_status("effect").await?.unwrap();
        if queued {
            assert_eq!(claims[0], RecoveryClaim::Requeued);
            assert_eq!(row.status, STATUS_SUCCESS);
            assert_eq!(effects.load(Ordering::SeqCst), 2);
        } else {
            assert_eq!(claims[0], RecoveryClaim::Claimed { attempts: 1 });
            assert_eq!(row.status, STATUS_PENDING);
            assert_eq!(row.recovery_attempts, 1);
            assert!(row.error.is_none());
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            // This test has drained the stopped attempt and its supervisor.
            // Lost/PENDING alone would not authorize this explicit takeover.
            assert_eq!(
                engine.recover_pending_for(&[row.executor_id]).await?,
                vec!["effect"]
            );
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(3),
                    engine.retrieve_workflow::<i32>("effect").await?.result()
                )
                .await
                .unwrap()?,
                42
            );
            assert_eq!(effects.load(Ordering::SeqCst), 2);
        }
        engine.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn stop_or_cancel_after_a_committed_claim_never_dispatches_its_body() -> Result<()> {
    for action in ["deactivate", "shutdown", "cancel"] {
        let inner = Arc::new(InMemoryProvider::new());
        let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteBefore));
        provider.arm([Fault::WriteBefore, Fault::ClaimAfter]);
        provider.block_claim_reply.store(true, Ordering::SeqCst);
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
        assert!(matches!(
            engine
                .start::<_, ()>("effect", (), WorkflowOptions::with_id("effect"))
                .await?
                .result()
                .await,
            Err(Error::RecoveryRequired(_))
        ));
        tokio::time::timeout(Duration::from_secs(3), provider.claim_committed.notified())
            .await
            .expect("claim must commit before the stop/cancel action");
        let row = inner.get_workflow_status("effect").await?.unwrap();
        assert_eq!(row.status, STATUS_PENDING);
        assert_eq!(row.recovery_attempts, 1);
        match action {
            "deactivate" => engine.deactivate(),
            "shutdown" => engine.shutdown(Duration::ZERO).await?,
            _ => engine.cancel_workflow("effect").await?,
        }
        provider.claim_reply_permit.add_permits(1);
        wait_for_attempts_to_stop(&engine).await?;
        assert_eq!(effects.load(Ordering::SeqCst), 1, "{action}");
        let row = inner.get_workflow_status("effect").await?.unwrap();
        assert_eq!(row.recovery_attempts, 1);
        assert_eq!(
            row.status,
            if action == "cancel" {
                STATUS_CANCELLED
            } else {
                STATUS_PENDING
            }
        );
        assert!(row.error.is_none());
        let claims = provider.claim_outcomes.lock().unwrap().clone();
        assert_eq!(claims[0], RecoveryClaim::Claimed { attempts: 1 });
        if action == "cancel" {
            assert_eq!(
                claims,
                vec![RecoveryClaim::Claimed { attempts: 1 }, RecoveryClaim::Lost]
            );
        } else {
            assert_eq!(claims.len(), 1);
        }
        engine.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}
