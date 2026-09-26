//! Storage failures must not become terminal business outcomes.
#[path = "../tests/common/mod.rs"]
mod common;
#[path = "execution/test_provider.rs"]
mod fault_provider;

use crate::*;
use fault_provider::{Fault, FaultProvider};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

async fn sweep(inner: Arc<dyn StateProvider>) -> Result<()> {
    for fault in [
        Fault::Read,
        Fault::WriteBefore,
        Fault::WriteAfter,
        Fault::TerminalBefore,
        Fault::TerminalAfter,
    ] {
        for swallow in [false, true] {
            let id = uuid::Uuid::new_v4().to_string();
            let effects = Arc::new(AtomicUsize::new(0));
            let wrapped = Arc::new(FaultProvider::new(inner.clone(), fault));
            let mut engine = DurableEngine::new(wrapped).await?;
            let register = |engine: &mut DurableEngine| {
                let effects = effects.clone();
                engine.register("checkpoint", move |ctx: DurableContext, _: ()| {
                    let effects = effects.clone();
                    async move {
                        let result = ctx
                            .step("effect", || async {
                                effects.fetch_add(1, Ordering::SeqCst);
                                Ok(42)
                            })
                            .await;
                        if swallow {
                            return Ok(42); // catching the fault cannot authorize SUCCESS
                        }
                        result?;
                        ctx.step("after", || async { Ok(()) }).await?;
                        Ok(42)
                    }
                });
            };
            register(&mut engine);
            let first = engine
                .start::<_, i32>("checkpoint", (), WorkflowOptions::with_id(&id))
                .await?
                .result()
                .await;
            let status = inner.get_workflow_status(&id).await?.unwrap();
            if fault == Fault::TerminalAfter {
                assert_eq!(
                    first?, 42,
                    "reconcile a terminal commit whose response was lost"
                );
                assert_eq!(status.status, STATUS_SUCCESS);
            } else {
                assert!(first.is_err(), "{fault:?}, swallow={swallow}");
                assert_eq!(
                    status.status, STATUS_PENDING,
                    "{fault:?}, swallow={swallow}"
                );
                assert!(
                    status.error.is_none(),
                    "storage failures are not business errors"
                );
                if matches!(fault, Fault::Read | Fault::WriteBefore | Fault::WriteAfter) {
                    assert!(
                        inner.get_step_result(&id, 1).await?.is_none(),
                        "no later checkpoint after a fault"
                    );
                }
                let mut recovery = DurableEngine::new(inner.clone()).await?;
                register(&mut recovery);
                let recovered = recovery.recover_pending_for(&[status.executor_id]).await?;
                assert!(recovered.contains(&id));
                let value = tokio::time::timeout(
                    Duration::from_secs(5),
                    recovery.retrieve_workflow::<i32>(&id).await?.result(),
                )
                .await
                .expect("recovery must complete")?;
                assert_eq!(value, 42);
            }
            assert_eq!(
                effects.load(Ordering::SeqCst),
                if fault == Fault::WriteBefore { 2 } else { 1 }
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn memory_checkpoint_faults_remain_recoverable() -> Result<()> {
    sweep(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_checkpoint_faults_remain_recoverable() -> Result<()> {
    sweep(Arc::new(SqliteProvider::connect("sqlite::memory:").await?)).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_checkpoint_faults_remain_recoverable() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, db) = common::hermetic_pg_db(&base, "checkpoint_fault").await;
    let result = sweep(Arc::new(PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &db).await;
    result
}

#[tokio::test]
async fn unreadable_records_interrupt_even_when_the_workflow_catches_the_error() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::Corrupt));
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("corrupt", |ctx: DurableContext, _: ()| async move {
        let _ = ctx
            .step("effect", || async {
                panic!("must not rerun unreadable history");
                #[allow(unreachable_code)]
                Ok(())
            })
            .await;
        Ok(())
    });
    let error = engine
        .start::<_, ()>("corrupt", (), WorkflowOptions::with_id("corrupt"))
        .await?
        .result()
        .await
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::RecoveryRequired);
    assert_eq!(
        inner.get_workflow_status("corrupt").await?.unwrap().status,
        STATUS_PENDING
    );
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn transaction_storage_failures_are_distinct_from_recorded_body_failures() -> Result<()> {
    for fault in [Fault::TransactionBefore, Fault::TransactionAfter] {
        let inner = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
        let provider = Arc::new(FaultProvider::new(inner.clone(), fault));
        let effects = Arc::new(AtomicUsize::new(0));
        let mut engine = DurableEngine::new(provider).await?;
        let register = |engine: &mut DurableEngine| {
            let effects = effects.clone();
            engine.register("transaction", move |ctx: DurableContext, _: ()| {
                let effects = effects.clone();
                async move {
                    ctx.transaction("tx", move |_| {
                        effects.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async { Err::<(), _>(Error::Timeout) })
                    })
                    .await
                }
            });
        };
        register(&mut engine);
        let result = engine
            .start::<_, ()>("transaction", (), WorkflowOptions::with_id("transaction"))
            .await?
            .result()
            .await;
        if fault == Fault::TransactionBefore {
            assert_eq!(
                inner
                    .get_workflow_status("transaction")
                    .await?
                    .unwrap()
                    .status,
                STATUS_PENDING
            );
            let mut recovery = DurableEngine::new(inner.clone()).await?;
            register(&mut recovery);
            let owner = inner
                .get_workflow_status("transaction")
                .await?
                .unwrap()
                .executor_id;
            recovery.recover_pending_for(&[owner]).await?;
            let error = recovery
                .retrieve_workflow::<()>("transaction")
                .await?
                .result()
                .await
                .unwrap_err();
            assert_eq!(error.code(), ErrorCode::Timeout);
        } else {
            assert_eq!(result.unwrap_err().code(), ErrorCode::Timeout);
        }
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert_eq!(
            inner
                .get_workflow_status("transaction")
                .await?
                .unwrap()
                .status,
            STATUS_ERROR
        );
    }
    Ok(())
}

#[tokio::test]
async fn a_caught_fault_interrupts_a_workflow_waiting_forever() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::Read));
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("waiting", |ctx: DurableContext, _: ()| async move {
        let _ = ctx.step("read", || async { Ok(()) }).await;
        std::future::pending::<Result<()>>().await
    });
    let handle = engine
        .start::<_, ()>("waiting", (), WorkflowOptions::with_id("waiting"))
        .await?;
    let error = tokio::time::timeout(Duration::from_secs(2), handle.result())
        .await
        .expect("fault must wake the engine")
        .unwrap_err();
    assert_eq!(error.code(), ErrorCode::RecoveryRequired);
    assert_eq!(
        inner.get_workflow_status("waiting").await?.unwrap().status,
        STATUS_PENDING
    );
    Ok(())
}

#[tokio::test]
async fn a_fault_blocks_prebuilt_calls_and_context_clones() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::Read));
    let mut engine = DurableEngine::new(provider).await?;
    let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let ran = Arc::new(AtomicUsize::new(0));
    let (wf_observed, wf_ran) = (observed.clone(), ran.clone());
    engine.register("siblings", move |ctx: DurableContext, _: ()| {
        let (observed, ran) = (wf_observed.clone(), wf_ran.clone());
        async move {
            let clone = ctx.clone();
            let first = ctx.step("first", || async { Ok(()) });
            let later = clone.step("later", || async {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            });
            let first = first.await.map_err(|error| error.code());
            let later = later.await.map_err(|error| error.code());
            let new = clone
                .step("new", || async {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
                .map_err(|error| error.code());
            observed.lock().unwrap().extend([first, later, new]);
            Ok(())
        }
    });
    assert_eq!(
        engine
            .start::<_, ()>("siblings", (), WorkflowOptions::with_id("siblings"))
            .await?
            .result()
            .await
            .unwrap_err()
            .code(),
        ErrorCode::RecoveryRequired
    );
    assert_eq!(
        *observed.lock().unwrap(),
        vec![Err(ErrorCode::RecoveryRequired); 3]
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert!(inner.get_step_result("siblings", 1).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn database_errors_from_user_code_remain_business_failures() -> Result<()> {
    for inside_step in [false, true] {
        let inner = Arc::new(InMemoryProvider::new());
        let mut engine = DurableEngine::new(inner.clone()).await?;
        engine.register("business", move |ctx: DurableContext, _: ()| async move {
            if inside_step {
                ctx.step("business-db", || async {
                    Err::<(), _>(Error::Db(sqlx::Error::PoolTimedOut))
                })
                .await
            } else {
                Err::<(), _>(Error::Db(sqlx::Error::PoolTimedOut))
            }
        });
        let error = engine
            .start::<_, ()>("business", (), WorkflowOptions::with_id("business"))
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Database);
        assert_eq!(
            inner.get_workflow_status("business").await?.unwrap().status,
            STATUS_ERROR
        );
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn committed_application_transaction_survives_checkpoint_failure() -> Result<()> {
    for fault in [Fault::WriteBefore, Fault::WriteAfter] {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        sqlx::query("CREATE TABLE effects (id INTEGER)")
            .execute(&pool)
            .await?;
        let ds = SqliteDataSource::new(pool.clone()).await?;
        let inner = Arc::new(InMemoryProvider::new());
        let wrapped = Arc::new(FaultProvider::new(inner.clone(), fault));
        let register = |engine: &mut DurableEngine| {
            let ds = ds.clone();
            engine.register("witness", move |ctx: DurableContext, _: ()| {
                let ds = ds.clone();
                async move {
                    ctx.transaction_on(&ds, "insert", async |conn| {
                        sqlx::query("INSERT INTO effects VALUES (1)")
                            .execute(conn)
                            .await?;
                        Ok(42)
                    })
                    .await
                }
            });
        };
        let mut engine = DurableEngine::new(wrapped).await?;
        register(&mut engine);
        assert_eq!(
            engine
                .start::<_, i32>("witness", (), WorkflowOptions::with_id("witness"))
                .await?
                .result()
                .await
                .unwrap_err()
                .code(),
            ErrorCode::RecoveryRequired
        );
        let status = inner.get_workflow_status("witness").await?.unwrap();
        assert_eq!(status.status, STATUS_PENDING);
        let mut recovery = DurableEngine::new(inner).await?;
        register(&mut recovery);
        recovery.recover_pending_for(&[status.executor_id]).await?;
        let value = tokio::time::timeout(
            Duration::from_secs(3),
            recovery.retrieve_workflow::<i32>("witness").await?.result(),
        )
        .await
        .expect("recover from witness")?;
        assert_eq!(value, 42);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effects")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            count, 1,
            "the committed witness prevents repeating the body"
        );
        pool.close().await;
    }
    Ok(())
}

#[derive(Debug, serde::Deserialize)]
struct Unserializable;
impl serde::Serialize for Unserializable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom(
            "application result cannot be serialized",
        ))
    }
}

#[tokio::test]
async fn encoding_a_step_result_must_not_repeat_its_effect_on_recovery() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let attempts = Arc::new(AtomicUsize::new(0));
    let register = |engine: &mut DurableEngine| {
        let (effects, attempts) = (effects.clone(), attempts.clone());
        engine.register("encoding", move |ctx: DurableContext, _: ()| {
            let (effects, attempts) = (effects.clone(), attempts.clone());
            async move {
                let result = ctx
                    .step("effect", || async {
                        effects.fetch_add(1, Ordering::SeqCst);
                        Ok(Unserializable)
                    })
                    .await;
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("simulate crash after observing the step result");
                }
                result.map(|_| ())
            }
        });
    };
    let mut engine = DurableEngine::new(inner.clone()).await?;
    register(&mut engine);
    let _ = engine
        .start::<_, ()>("encoding", (), WorkflowOptions::with_id("encoding"))
        .await?
        .result()
        .await;
    let owner = inner
        .get_workflow_status("encoding")
        .await?
        .unwrap()
        .executor_id;
    let mut recovery = DurableEngine::new(inner.clone()).await?;
    register(&mut recovery);
    recovery.recover_pending_for(&[owner]).await?;
    recovery.shutdown(Duration::from_secs(3)).await?;
    assert_eq!(
        effects.load(Ordering::SeqCst),
        1,
        "encoding failure must be recorded before recovery"
    );
    assert_eq!(
        inner.get_workflow_status("encoding").await?.unwrap().status,
        STATUS_ERROR
    );
    assert!(matches!(
        inner.get_step_result("encoding", 0).await?.unwrap().outcome,
        crate::provider::StepOutcome::Failure { .. }
    ));
    Ok(())
}

async fn business_errors_remain_catchable(inner: Arc<dyn StateProvider>) -> Result<()> {
    for kind in ["send", "bulk", "attributes", "stream"] {
        let id = format!("business-{kind}");
        let mut engine = DurableEngine::new(inner.clone()).await?;
        engine.register(
            "business-op",
            move |ctx: DurableContext, _: ()| async move {
                let result = match kind {
                    "send" => ctx.send("absent", (), "topic").await,
                    "bulk" => {
                        ctx.send_bulk(&[SendMessage::new("absent", (), "topic")])
                            .await
                    }
                    "attributes" => ctx.set_workflow_attributes("absent", None).await,
                    "stream" => {
                        ctx.close_stream("closed").await?;
                        ctx.write_stream("closed", ()).await
                    }
                    _ => unreachable!(),
                };
                let code = result.unwrap_err().code();
                ctx.step("handled", || async { Ok(code) }).await
            },
        );
        let code = engine
            .start::<_, ErrorCode>("business-op", (), WorkflowOptions::with_id(&id))
            .await?
            .result()
            .await?;
        assert_eq!(
            code,
            if kind == "stream" {
                ErrorCode::Application
            } else {
                ErrorCode::NonExistentWorkflow
            }
        );
        assert_eq!(
            inner.get_workflow_status(&id).await?.unwrap().status,
            STATUS_SUCCESS
        );
    }
    Ok(())
}

#[tokio::test]
async fn memory_provider_business_errors_remain_catchable() -> Result<()> {
    business_errors_remain_catchable(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_provider_business_errors_remain_catchable() -> Result<()> {
    business_errors_remain_catchable(Arc::new(SqliteProvider::connect("sqlite::memory:").await?))
        .await
}

#[tokio::test]
async fn unavailable_transaction_backend_is_a_catchable_error() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(inner.clone()).await?;
    engine.register("unsupported", |ctx: DurableContext, _: ()| async move {
        let code = ctx
            .transaction("tx", |_| Box::pin(async { Ok(()) }))
            .await
            .unwrap_err()
            .code();
        ctx.step("handled", || async { Ok(code) }).await
    });
    assert_eq!(
        engine
            .start::<_, ErrorCode>("unsupported", (), WorkflowOptions::with_id("unsupported"))
            .await?
            .result()
            .await?,
        ErrorCode::Application
    );
    Ok(())
}

#[tokio::test]
async fn child_insert_storage_failures_leave_the_parent_recoverable() -> Result<()> {
    for fault in [Fault::ChildBefore, Fault::ChildAfter] {
        let inner = Arc::new(InMemoryProvider::new());
        let wrapped = Arc::new(FaultProvider::new(inner.clone(), fault));
        let mut engine = DurableEngine::new(wrapped).await?;
        engine.register("child", |_: DurableContext, _: ()| async { Ok(()) });
        engine.register("parent", |ctx: DurableContext, _: ()| async move {
            ctx.start_workflow::<_, ()>("child", (), WorkflowOptions::default())
                .await?;
            Ok(())
        });
        let error = engine
            .start::<_, ()>("parent", (), WorkflowOptions::with_id("parent"))
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::RecoveryRequired);
        assert_eq!(
            inner.get_workflow_status("parent").await?.unwrap().status,
            STATUS_PENDING
        );
        assert_eq!(
            inner.get_workflow_status("parent-0").await?.is_some(),
            fault == Fault::ChildAfter
        );
    }
    Ok(())
}

#[derive(Clone, Default)]
struct LogBuffer(Arc<std::sync::Mutex<Vec<u8>>>);
impl std::io::Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl LogBuffer {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

#[tokio::test]
async fn recovery_required_is_reported_for_every_execution_entry() -> Result<()> {
    for mode in [
        "direct",
        "recovered",
        "queued",
        "child",
        "scheduled",
        "terminal",
        "panic",
    ] {
        let logs = LogBuffer::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let inner = Arc::new(InMemoryProvider::new());
        let fault = if mode == "terminal" {
            Fault::TerminalBefore
        } else {
            Fault::WriteBefore
        };
        let provider = Arc::new(FaultProvider::new(inner.clone(), fault));
        let mut engine = DurableEngine::new(provider).await?;
        engine.register(
            "logged",
            move |ctx: DurableContext, _: serde_json::Value| async move {
                let result = ctx.step("work", || async { Ok(()) }).await;
                if mode == "panic" {
                    panic!("panic after a storage failure");
                }
                result
            },
        );
        let expected_id = match mode {
            "direct" | "terminal" | "panic" => {
                let _ = engine
                    .start::<_, ()>(
                        "logged",
                        serde_json::Value::Null,
                        WorkflowOptions::with_id(mode),
                    )
                    .await?
                    .result()
                    .await;
                mode.to_string()
            }
            "recovered" => {
                inner
                    .insert_workflow_status(WorkflowStatus::new(
                        "recovered",
                        "logged",
                        serde_json::Value::Null,
                        STATUS_PENDING,
                        "old-owner",
                        engine.app_version(),
                    ))
                    .await?;
                assert_eq!(
                    engine.recover_pending_for(&["old-owner".into()]).await?,
                    vec!["recovered"]
                );
                "recovered".to_string()
            }
            "queued" => {
                engine.register_queue(
                    WorkflowQueue::new("jobs").base_polling_interval(Duration::from_millis(10)),
                );
                engine.launch().await?;
                engine
                    .start::<_, ()>(
                        "logged",
                        serde_json::Value::Null,
                        WorkflowOptions::with_id("queued").queue("jobs"),
                    )
                    .await?;
                "queued".to_string()
            }
            "child" => {
                engine.register("parent", |ctx: DurableContext, _: ()| async move {
                    ctx.start_workflow::<_, ()>(
                        "logged",
                        serde_json::Value::Null,
                        WorkflowOptions::default(),
                    )
                    .await?;
                    Ok(())
                });
                engine
                    .start::<_, ()>("parent", (), WorkflowOptions::with_id("parent"))
                    .await?
                    .result()
                    .await?;
                "parent-0".to_string()
            }
            "scheduled" => {
                engine
                    .create_schedule(
                        "fault-tick",
                        "logged",
                        "* * * * * *",
                        ScheduleOptions::new(),
                    )
                    .await?;
                engine.launch().await?;
                "sched-fault-tick-".to_string()
            }
            _ => unreachable!(),
        };
        let observed = tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                let text = logs.text();
                if text.contains("recovery_required=true")
                    && text.contains(&expected_id)
                    && text.contains("pool timed out")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        engine.shutdown(Duration::from_secs(2)).await?;
        if mode == "panic" {
            assert!(logs.text().contains("workflow panicked"), "{}", logs.text());
        }
        assert!(
            observed.is_ok(),
            "{mode} must report the workflow id and storage cause: {}",
            logs.text()
        );
    }
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_provider_business_errors_remain_catchable() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, db) = common::hermetic_pg_db(&base, "business_fault").await;
    let result =
        business_errors_remain_catchable(Arc::new(PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &db).await;
    result
}

#[tokio::test]
async fn child_validation_errors_remain_catchable() -> Result<()> {
    for kind in ["workflow", "queue", "delay"] {
        let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
        engine.register("child", |_: DurableContext, _: ()| async { Ok(()) });
        engine.register("parent", move |ctx: DurableContext, _: ()| async move {
            let (name, opts) = match kind {
                "workflow" => ("absent", WorkflowOptions::default()),
                "queue" => ("child", WorkflowOptions::default().queue("absent")),
                "delay" => (
                    "child",
                    WorkflowOptions {
                        delay: Some(Duration::from_secs(1)),
                        ..Default::default()
                    },
                ),
                _ => unreachable!(),
            };
            let error = match ctx.start_workflow::<_, ()>(name, (), opts).await {
                Err(error) => error,
                Ok(_) => panic!("invalid child start must fail"),
            };
            ctx.step("handled", || async { Ok(error.code()) }).await
        });
        let code = engine
            .start::<_, ErrorCode>("parent", (), WorkflowOptions::default())
            .await?
            .result()
            .await?;
        assert_eq!(
            code,
            match kind {
                "workflow" => ErrorCode::WorkflowNotRegistered,
                "queue" => ErrorCode::QueueNotRegistered,
                _ => ErrorCode::Application,
            }
        );
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn transaction_result_encoding_failures_are_recorded_as_business_failures() -> Result<()> {
    for system in [false, true] {
        let inner = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
        let app_pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        let ds = if system {
            inner.system_datasource()
        } else {
            SqliteDataSource::new(app_pool.clone()).await?
        };
        let effects = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let register = |engine: &mut DurableEngine| {
            let (ds, effects, attempts) = (ds.clone(), effects.clone(), attempts.clone());
            engine.register("encoding-tx", move |ctx: DurableContext, _: ()| {
                let (ds, effects, attempts) = (ds.clone(), effects.clone(), attempts.clone());
                async move {
                    let result = ctx
                        .transaction_on(
                            &ds,
                            "effect",
                            async move |_: &mut sqlx::SqliteConnection| {
                                effects.fetch_add(1, Ordering::SeqCst);
                                Ok(Unserializable)
                            },
                        )
                        .await;
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        panic!("crash after transaction result");
                    }
                    result.map(|_| ())
                }
            });
        };
        let mut engine = DurableEngine::new(inner.clone()).await?;
        register(&mut engine);
        let _ = engine
            .start::<_, ()>("encoding-tx", (), WorkflowOptions::with_id("encoding-tx"))
            .await?
            .result()
            .await;
        let owner = inner
            .get_workflow_status("encoding-tx")
            .await?
            .unwrap()
            .executor_id;
        let mut recovery = DurableEngine::new(inner.clone()).await?;
        register(&mut recovery);
        recovery.recover_pending_for(&[owner]).await?;
        recovery.shutdown(Duration::from_secs(3)).await?;
        assert_eq!(effects.load(Ordering::SeqCst), 1, "system={system}");
        assert_eq!(
            inner
                .get_workflow_status("encoding-tx")
                .await?
                .unwrap()
                .status,
            STATUS_ERROR
        );
        let error = recovery
            .retrieve_workflow::<()>("encoding-tx")
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Serialization);
        app_pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn message_and_event_type_mismatches_remain_catchable() -> Result<()> {
    for kind in ["recv", "event"] {
        let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
        engine.register("conversion", move |ctx: DurableContext, _: ()| async move {
            let result = if kind == "recv" {
                ctx.send(ctx.workflow_id(), 42, "topic").await?;
                ctx.recv::<String>("topic", Duration::from_secs(1)).await
            } else {
                ctx.set_event("key", 42).await?;
                ctx.get_event::<String>(ctx.workflow_id(), "key", Duration::from_secs(1))
                    .await
            };
            let code = result.unwrap_err().code();
            ctx.step("handled", || async { Ok(code) }).await
        });
        assert_eq!(
            engine
                .start::<_, ErrorCode>("conversion", (), WorkflowOptions::with_id(kind))
                .await?
                .result()
                .await?,
            ErrorCode::Serialization
        );
    }
    Ok(())
}
