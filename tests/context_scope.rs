use durare::{
    DurableContext, DurableEngine, ErrorCode, InMemoryProvider, Result, StateProvider, StepOptions,
    TransactionOptions, WorkflowOptions,
};
use std::time::Duration;

/// A retry predicate cannot authorize repeating a durable-scope violation.
#[tokio::test]
async fn a_spawned_call_does_not_retry_its_enclosing_step() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let predicates = Arc::new(AtomicUsize::new(0));
    let calls = attempts.clone();
    let checks = predicates.clone();
    engine.register("retry", move |ctx: DurableContext, _: ()| {
        let calls = calls.clone();
        let checks = checks.clone();
        async move {
            ctx.step_with(
                StepOptions::new("outer")
                    .max_retries(3)
                    .base_interval(Duration::ZERO)
                    .retry_if(move |_| {
                        checks.fetch_add(1, Ordering::SeqCst);
                        true
                    }),
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let escaped = ctx.clone();
                    async move {
                        tokio::spawn(
                            async move { escaped.step("escaped", || async { Ok(()) }).await },
                        )
                        .await
                        .unwrap()
                    }
                },
            )
            .await
        }
    });
    refused(
        engine
            .start::<_, ()>("retry", (), WorkflowOptions::with_id("retry"))
            .await?
            .result()
            .await,
    );
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(predicates.load(Ordering::SeqCst), 0);
    assert_eq!(engine.get_workflow_steps("retry").await?.len(), 1);
    let replay = engine.verify_replay("retry").await?;
    assert_eq!((replay.recorded, replay.matched), (1, 1));
    assert_eq!(replay.divergence, None);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn a_spawned_call_does_not_retry_its_enclosing_transactions() -> Result<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let provider = Arc::new(durare::SqliteProvider::from_pool(pool.clone()));
    let ds = provider.system_datasource();
    let mut engine = DurableEngine::new(provider).await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let predicates = Arc::new(AtomicUsize::new(0));
    let calls = attempts.clone();
    let checks = predicates.clone();
    engine.register("retry", move |ctx: DurableContext, native: bool| {
        let calls = calls.clone();
        let checks = checks.clone();
        let ds = ds.clone();
        async move {
            let opts = TransactionOptions::new("outer")
                .max_retries(3)
                .base_interval(Duration::ZERO)
                .retry_if(move |_| {
                    checks.fetch_add(1, Ordering::SeqCst);
                    true
                });
            let body_ctx = ctx.clone();
            let escape = move || {
                calls.fetch_add(1, Ordering::SeqCst);
                let escaped = body_ctx.clone();
                async move {
                    tokio::spawn(async move { escaped.step("escaped", || async { Ok(()) }).await })
                        .await
                        .unwrap()
                }
            };
            if native {
                ctx.transaction_on_with(&ds, opts, async move |_conn| escape().await)
                    .await
            } else {
                ctx.transaction_with(opts, move |_tx| Box::pin(escape()))
                    .await
            }
        }
    });
    for (i, native) in [false, true].into_iter().enumerate() {
        let id = format!("retry-{i}");
        refused(
            engine
                .start::<_, ()>("retry", native, WorkflowOptions::with_id(&id))
                .await?
                .result()
                .await,
        );
        assert_eq!(attempts.load(Ordering::SeqCst), i + 1);
        assert_eq!(predicates.load(Ordering::SeqCst), 0);
        assert_eq!(engine.get_workflow_steps(&id).await?.len(), 1);
        let replay = engine.verify_replay(&id).await?;
        assert_eq!((replay.recorded, replay.matched), (1, 1));
        assert_eq!(replay.divergence, None);
        assert_eq!(attempts.load(Ordering::SeqCst), i + 1);
    }
    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn body_boundary_errors_do_not_retry_the_enclosing_step() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let calls = attempts.clone();
    engine.register("boundary", move |ctx: DurableContext, crossed: bool| {
        let calls = calls.clone();
        async move {
            let mut pending = crossed.then(|| ctx.step("inner", || async { Ok(()) }));
            ctx.step_with(
                StepOptions::new("outer")
                    .max_retries(3)
                    .base_interval(Duration::ZERO),
                || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    pending
                        .take()
                        .unwrap_or_else(|| ctx.step("inner", || async { Ok(()) }))
                },
            )
            .await
        }
    });
    for (i, crossed) in [false, true].into_iter().enumerate() {
        let error = engine
            .start::<_, ()>("boundary", crossed, WorkflowOptions::default())
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::NestedDurableCall);
        assert_eq!(attempts.load(Ordering::SeqCst), i + 1);
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn native_transactions_refuse_spawned_calls_before_effect_or_position() -> Result<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let provider = Arc::new(durare::SqliteProvider::from_pool(pool.clone()));
    let ds = provider.system_datasource();
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("native", move |ctx: DurableContext, _: ()| {
        let escaped = ctx.clone();
        let ds = ds.clone();
        async move {
            tokio::spawn(async move {
                let escaped_call = escaped.transaction_on(&ds, "escaped", async |_conn| {
                    panic!("refused transaction body ran");
                    #[allow(unreachable_code)]
                    Ok(())
                });
                assert_eq!(escaped.current_step_id(), 0);
                refused(escaped_call.await);
                assert_eq!(escaped.current_step_id(), 0);
                let escaped_with = escaped.transaction_on_with(
                    &ds,
                    TransactionOptions::new("escaped-with"),
                    async |_conn| {
                        panic!("refused transaction body ran");
                        #[allow(unreachable_code)]
                        Ok(())
                    },
                );
                assert_eq!(escaped.current_step_id(), 0);
                refused(escaped_with.await);
                assert_eq!(escaped.current_step_id(), 0);
            })
            .await
            .unwrap();
            assert_eq!(ctx.current_step_id(), 0);
            ctx.step("valid", || async { Ok(()) }).await
        }
    });
    engine
        .start::<_, ()>("native", (), WorkflowOptions::with_id("native"))
        .await?
        .result()
        .await?;
    let steps = engine.get_workflow_steps("native").await?;
    assert_eq!(steps.len(), 1);
    assert_eq!((steps[0].step_id, steps[0].name.as_str()), (0, "valid"));
    pool.close().await;
    Ok(())
}
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

fn refused<T>(r: Result<T>) {
    let error = r.err().expect("escaped durable work must be refused");
    assert_eq!(error.code(), durare::ErrorCode::DurableCallOutsideExecution);
    assert!(
        matches!(error, durare::Error::DurableCallOutsideExecution { workflow_id, operation }
        if !workflow_id.is_empty() && !operation.is_empty())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawned_owned_context_is_refused_before_effect_or_position() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    engine.register("parent", move |ctx: DurableContext, _: ()| {
        let count = count.clone();
        async move {
            let escaped = ctx.clone();
            refused(
                tokio::spawn(async move {
                    escaped
                        .step("escaped", || async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        })
                        .await
                })
                .await
                .unwrap(),
            );
            assert_eq!(ctx.current_step_id(), 0);
            ctx.step("valid", || async { Ok(7) }).await
        }
    });
    assert_eq!(
        engine
            .start::<_, i32>("parent", (), WorkflowOptions::with_id("spawn"))
            .await?
            .result()
            .await?,
        7
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(provider.get_step_result("spawn", 0).await?.is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scoped_thread_rejects_borrow_and_prebuilt_pending() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("scoped", |ctx: DurableContext, _: ()| async move {
        let runtime = tokio::runtime::Handle::current();
        std::thread::scope(|s| {
            refused(
                s.spawn(|| runtime.block_on(ctx.step("outside", || async { Ok(()) })))
                    .join()
                    .unwrap(),
            )
        });
        assert_eq!(ctx.current_step_id(), 0);
        let pending = ctx.step("built", || async {
            panic!("moved pending body ran");
            #[allow(unreachable_code)]
            Ok(())
        });
        std::thread::scope(|s| refused(s.spawn(|| runtime.block_on(pending)).join().unwrap()));
        assert_eq!(ctx.current_step_id(), 1);
        Ok(())
    });
    engine
        .start::<_, ()>("scoped", (), WorkflowOptions::default())
        .await?
        .result()
        .await
}

#[tokio::test]
async fn a_context_from_another_execution_is_refused() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let saved = Arc::new(Mutex::new(None));
    let save = saved.clone();
    engine.register("save", move |ctx: DurableContext, _: ()| {
        *save.lock().unwrap() = Some(ctx);
        async { Ok(()) }
    });
    engine.register("use", move |_ctx: DurableContext, _: ()| {
        let old = saved.lock().unwrap().take().unwrap();
        async move {
            refused(old.step("stale", || async { Ok(()) }).await);
            Ok(())
        }
    });
    engine
        .start::<_, ()>("save", (), WorkflowOptions::default())
        .await?
        .result()
        .await?;
    engine
        .start::<_, ()>("use", (), WorkflowOptions::default())
        .await?
        .result()
        .await
}

async fn helper(ctx: &DurableContext) -> Result<i32> {
    ctx.step("helper", || async { Ok(2) }).await
}
#[durare::workflow]
async fn macro_example(ctx: DurableContext, x: i32) -> Result<i32> {
    ctx.step("macro", || async move { Ok(x) }).await
}

#[tokio::test]
async fn ordinary_composition_macros_children_handles_and_replay_work() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("child", |ctx: DurableContext, _: ()| async move {
        ctx.step("child-value", || async { Ok(3) }).await
    });
    let dependency = Arc::new(1);
    engine.register("parent", move |ctx: DurableContext, _: ()| {
        let dependency = dependency.clone();
        async move {
            let (a, b, c) = tokio::join!(
                ctx.step("first", || async move { Ok(*dependency) }),
                helper(&ctx),
                ctx.start_workflow::<_, i32>("child", (), WorkflowOptions::with_id("child"))
            );
            let plain = ctx
                .step("plain-spawn", || async {
                    Ok(tokio::spawn(async { 4 }).await.unwrap())
                })
                .await?;
            Ok(a? + b? + c?.result().await? + plain)
        }
    });
    assert_eq!(
        engine
            .start::<_, i32>("parent", (), WorkflowOptions::with_id("parent"))
            .await?
            .result()
            .await?,
        10
    );
    engine.verify_replay("parent").await?;
    assert_eq!(
        engine
            .start_with(MacroExample, 8, WorkflowOptions::default())
            .await?
            .result()
            .await?,
        8
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_is_checked_again_after_a_pending_poll() -> Result<()> {
    use std::{
        future::{poll_fn, Future},
        pin::Pin,
        task::Poll,
    };
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let finished = Arc::new(AtomicUsize::new(0));
    let count = finished.clone();
    engine.register("move-after-poll", move |ctx: DurableContext, _: ()| {
        let count = count.clone();
        async move {
            let mut pending = ctx.step("moving", || async move {
                tokio::task::yield_now().await;
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            poll_fn(|cx| {
                assert!(Pin::new(&mut pending).poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let runtime = tokio::runtime::Handle::current();
            std::thread::scope(|scope| {
                refused(scope.spawn(|| runtime.block_on(pending)).join().unwrap())
            });
            Ok(())
        }
    });
    engine
        .start::<_, ()>("move-after-poll", (), WorkflowOptions::default())
        .await?
        .result()
        .await?;
    assert_eq!(finished.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn every_durable_constructor_refuses_a_spawned_context_without_spending_positions(
) -> Result<()> {
    use std::time::Duration;
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("all", |ctx: DurableContext, _: ()| async move {
        let escaped = ctx.clone();
        tokio::spawn(async move {
            // Metadata access is harmless and remains available outside scope.
            assert_eq!(escaped.workflow_id(), "all");
            refused(escaped.step("step", || async { Ok(()) }).await);
            refused(
                escaped
                    .step_with(durare::StepOptions::new("retry"), || async { Ok(()) })
                    .await,
            );
            refused(
                escaped
                    .transaction("tx", |_tx| Box::pin(async { Ok(()) }))
                    .await,
            );
            refused(
                escaped
                    .start_workflow::<_, ()>("child", (), WorkflowOptions::default())
                    .await,
            );
            refused(escaped.select(vec![Box::pin(async { 1 })]).await);
            refused(escaped.sleep(Duration::ZERO).await);
            refused(escaped.now().await);
            refused(escaped.uuid().await);
            refused(escaped.random().await);
            refused(escaped.send("all", (), "topic").await);
            refused(escaped.send_bulk::<()>(&[]).await);
            refused(escaped.set_workflow_attributes("all", None).await);
            refused(escaped.recv::<()>("topic", Duration::ZERO).await);
            refused(escaped.set_event("key", ()).await);
            refused(escaped.get_event::<()>("all", "key", Duration::ZERO).await);
            refused(escaped.write_stream("stream", ()).await);
            refused(escaped.close_stream("stream").await);
            refused(escaped.patch("version").await);
            refused(escaped.deprecate_patch("version").await);
            assert_eq!(escaped.current_step_id(), 0);
        })
        .await
        .unwrap();
        assert_eq!(ctx.current_step_id(), 0);
        Ok(())
    });
    engine
        .start::<_, ()>("all", (), WorkflowOptions::with_id("all"))
        .await?
        .result()
        .await?;
    assert!(engine.get_workflow_steps("all").await?.is_empty());
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn a_placement_error_is_a_recordable_programming_error_not_recovery() -> Result<()> {
    for serializer in [durare::Serializer::Json, durare::Serializer::Portable] {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let provider =
            Arc::new(durare::SqliteProvider::from_pool(pool.clone()).with_serializer(serializer));
        let mut engine = DurableEngine::new(provider.clone()).await?;
        engine.register("escape", |ctx: DurableContext, _: ()| async move {
            tokio::spawn(async move { ctx.step("escaped", || async { Ok(()) }).await })
                .await
                .unwrap()
        });
        let first = engine
            .start::<_, ()>("escape", (), WorkflowOptions::with_id("escape"))
            .await?
            .result()
            .await;
        refused(first);
        let restored = engine
            .retrieve_workflow::<()>("escape")
            .await?
            .result()
            .await;
        refused(restored);
        let row = provider.get_workflow_status("escape").await?.unwrap();
        assert_eq!(row.status, durare::STATUS_ERROR);
        assert_eq!(row.recovery_attempts, 0);
        assert!(engine.get_workflow_steps("escape").await?.is_empty());
        pool.close().await;
    }
    Ok(())
}
