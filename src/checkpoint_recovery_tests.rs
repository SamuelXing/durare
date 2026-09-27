//! Storage failures must not become terminal business outcomes.
#[path = "../tests/common/mod.rs"]
mod common;
use crate::execution::test_provider as fault_provider;

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
    use futures_util::FutureExt;
    let result = std::panic::AssertUnwindSafe(async {
        sweep(Arc::new(PostgresProvider::connect(&url).await?)).await
    })
    .catch_unwind()
    .await;
    common::drop_hermetic_pg_db(&admin, &db).await;
    result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
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
        const LIMIT: usize = 64 * 1024;
        let mut buffer = self.0.lock().unwrap();
        let incoming = &bytes[bytes.len().saturating_sub(LIMIT)..];
        let overflow = (buffer.len() + incoming.len()).saturating_sub(LIMIT);
        buffer.drain(..overflow);
        buffer.extend_from_slice(incoming);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl LogBuffer {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

#[tokio::test]
async fn recovery_required_is_reported_for_every_execution_entry() -> Result<()> {
    // Run only this test in a child process: the global subscriber cannot
    // capture unrelated tests or survive this test's lifetime. Spawned tasks
    // still inherit the process subscriber, and the buffer is bounded.
    if std::env::var_os("DURARE_TRACE_TEST_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "checkpoint_recovery_tests::recovery_required_is_reported_for_every_execution_entry", "--nocapture"])
            .env("DURARE_TRACE_TEST_CHILD", "1")
            .output().unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return Ok(());
    }
    let logs = LogBuffer::default();
    let writer = logs.clone();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    )
    .unwrap();
    for mode in [
        "direct",
        "recovered",
        "queued",
        "child",
        "scheduled",
        "terminal",
        "adopt",
        "panic",
    ] {
        logs.0.lock().unwrap().clear();
        let inner = Arc::new(InMemoryProvider::new());
        let fault = if mode == "terminal" {
            Fault::TerminalBefore
        } else if mode == "adopt" {
            Fault::TerminalAfterThenAdoptRead
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
            "direct" | "terminal" | "adopt" | "panic" => {
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
                if text.lines().any(|line| {
                    line.contains("recovery_required=true")
                        && line.contains(&format!("workflow_id={expected_id}"))
                        && line.contains("pool timed out")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if mode != "panic" {
            tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    let rows = inner.list_workflows(&ListFilter::default()).await?;
                    if rows
                        .iter()
                        .any(|row| row.id.starts_with(&expected_id) && row.status == STATUS_SUCCESS)
                    {
                        return Ok::<_, Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("every execution entry must recover automatically")?;
        }
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
    use futures_util::FutureExt;
    let result = std::panic::AssertUnwindSafe(async {
        business_errors_remain_catchable(Arc::new(PostgresProvider::connect(&url).await?)).await
    })
    .catch_unwind()
    .await;
    common::drop_hermetic_pg_db(&admin, &db).await;
    result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
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

/// Fail inside the SQL provider, after the user body returned successfully.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_transaction_machinery_faults_are_not_recorded() -> Result<()> {
    for at_commit in [false, true] {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;
        let provider = Arc::new(SqliteProvider::from_pool(pool.clone()));
        let mut engine = DurableEngine::new(provider.clone()).await?;
        sqlx::raw_sql("PRAGMA foreign_keys = ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); CREATE TABLE effects (id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);")
            .execute(&pool).await?;
        if !at_commit {
            sqlx::raw_sql("CREATE TRIGGER fail_checkpoint BEFORE INSERT ON operation_outputs WHEN NEW.output IS NOT NULL BEGIN SELECT RAISE(ABORT, 'checkpoint insert unavailable'); END;")
                .execute(&pool).await?;
        }
        let retries = Arc::new(AtomicUsize::new(0));
        let counted = retries.clone();
        engine.register("sql-fault", move |ctx: DurableContext, _: ()| {
            let counted = counted.clone();
            async move {
                ctx.transaction_with(
                    TransactionOptions::new("write")
                        .retry_if(move |_| {
                            counted.fetch_add(1, Ordering::SeqCst);
                            true
                        })
                        .max_retries(1),
                    move |tx| {
                        Box::pin(async move {
                            if at_commit {
                                tx.execute("INSERT INTO effects VALUES (1)", &params![])
                                    .await?;
                            }
                            Ok(42)
                        })
                    },
                )
                .await
            }
        });
        let result = engine
            .start::<_, i32>("sql-fault", (), WorkflowOptions::with_id("sql-fault"))
            .await?
            .result()
            .await;
        assert_eq!(result.unwrap_err().code(), ErrorCode::RecoveryRequired);
        assert!(provider.get_step_result("sql-fault", 0).await?.is_none());
        assert_eq!(
            provider
                .get_workflow_status("sql-fault")
                .await?
                .unwrap()
                .status,
            STATUS_PENDING
        );
        assert_eq!(
            retries.load(Ordering::SeqCst),
            0,
            "machinery faults do not enter the business retry predicate"
        );
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM effects")
            .fetch_one(&pool)
            .await?;
        assert_eq!(
            count, 0,
            "application writes roll back with the failed checkpoint"
        );
        if at_commit {
            sqlx::query("INSERT INTO parent VALUES (1)")
                .execute(&pool)
                .await?;
        } else {
            sqlx::query("DROP TRIGGER fail_checkpoint")
                .execute(&pool)
                .await?;
        }
        let owner = provider
            .get_workflow_status("sql-fault")
            .await?
            .unwrap()
            .executor_id;
        engine.recover_pending_for(&[owner]).await?;
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(5),
                engine.retrieve_workflow::<i32>("sql-fault").await?.result()
            )
            .await
            .unwrap()?,
            42
        );
        engine.shutdown(Duration::from_secs(5)).await?;
    }
    Ok(())
}

async fn queue_recovers_capacity(inner: Arc<dyn StateProvider>) -> Result<()> {
    // Both the checkpoint and the first recovery claim fail. The latter must
    // retry without running the workflow until ownership has been acquired.
    let provider = Arc::new(FaultProvider::new(
        inner.clone(),
        Fault::WriteThenClaimFailure,
    ));
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("queued-recovery", |ctx: DurableContext, _: ()| async move {
        ctx.step("work", || async { Ok(42) }).await
    });
    engine.register_queue(
        WorkflowQueue::new("limited")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;
    let first = engine
        .start::<_, i32>(
            "queued-recovery",
            (),
            WorkflowOptions::with_id("interrupted").queue("limited"),
        )
        .await?;
    let second = engine
        .start::<_, i32>(
            "queued-recovery",
            (),
            WorkflowOptions::with_id("following").queue("limited"),
        )
        .await?;
    let results = tokio::time::timeout(Duration::from_secs(5), async {
        (first.result().await, second.result().await)
    })
    .await;
    engine.shutdown(Duration::from_secs(2)).await?;
    let (first, second) =
        results.expect("a stopped run must not permanently consume the queue's global slot");
    assert_eq!(first?, 42);
    assert_eq!(second?, 42);
    assert_eq!(
        inner
            .get_workflow_status("interrupted")
            .await?
            .unwrap()
            .recovery_attempts
            + inner
                .get_workflow_status("following")
                .await?
                .unwrap()
                .recovery_attempts,
        1
    );
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_transaction_machinery_faults_are_not_recorded() -> Result<()> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let admin = sqlx::PgPool::connect(&url).await?;
    for at_commit in [false, true] {
        let schema = format!("checkpoint_fault_{}", uuid::Uuid::new_v4().simple());
        use futures_util::FutureExt;
        let result = std::panic::AssertUnwindSafe(async {
        let provider = Arc::new(PostgresProvider::connect_with_schema(&url, &schema).await?);
        let mut engine = DurableEngine::new(provider.clone()).await?;
        let timing = if at_commit {
            "CONSTRAINT TRIGGER fail_checkpoint AFTER INSERT"
        } else {
            "TRIGGER fail_checkpoint BEFORE INSERT"
        };
        let deferred = if at_commit {
            "DEFERRABLE INITIALLY DEFERRED"
        } else {
            ""
        };
        sqlx::raw_sql(&format!("CREATE TABLE {schema}.effects (id INTEGER); CREATE FUNCTION {schema}.reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'checkpoint unavailable' USING ERRCODE = '53100'; END $$; CREATE {timing} ON {schema}.operation_outputs {deferred} FOR EACH ROW WHEN (NEW.output IS NOT NULL) EXECUTE FUNCTION {schema}.reject_checkpoint();"))
            .execute(&admin).await?;
        let retries = Arc::new(AtomicUsize::new(0));
        let counted = retries.clone();
        engine.register("sql-fault", move |ctx: DurableContext, _: ()| {
            let counted = counted.clone();
            async move {
                ctx.transaction_with(
                    TransactionOptions::new("write")
                        .max_retries(1)
                        .retry_if(move |_| {
                            counted.fetch_add(1, Ordering::SeqCst);
                            true
                        }),
                    |tx| {
                        Box::pin(async move {
                            tx.execute("INSERT INTO effects VALUES (1)", &params![])
                                .await?;
                            Ok(42)
                        })
                    },
                )
                .await
            }
        });
        let result = engine
            .start::<_, i32>("sql-fault", (), WorkflowOptions::with_id("sql-fault"))
            .await?
            .result()
            .await;
        assert_eq!(result.unwrap_err().code(), ErrorCode::RecoveryRequired);
        assert!(provider.get_step_result("sql-fault", 0).await?.is_none());
        assert_eq!(
            provider
                .get_workflow_status("sql-fault")
                .await?
                .unwrap()
                .status,
            STATUS_PENDING
        );
        assert_eq!(retries.load(Ordering::SeqCst), 0);
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {schema}.effects"))
            .fetch_one(&admin)
            .await?;
        assert_eq!(
            count, 0,
            "the failed checkpoint rolls back application writes"
        );
        sqlx::raw_sql(&format!(
            "DROP TRIGGER fail_checkpoint ON {schema}.operation_outputs"
        ))
        .execute(&admin)
        .await?;
        let owner = provider
            .get_workflow_status("sql-fault")
            .await?
            .unwrap()
            .executor_id;
        engine.recover_pending_for(&[owner]).await?;
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(5),
                engine.retrieve_workflow::<i32>("sql-fault").await?.result()
            )
            .await
            .unwrap()?,
            42
        );
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {schema}.effects"))
            .fetch_one(&admin)
            .await?;
        assert_eq!(count, 1);
        engine.shutdown(Duration::from_secs(5)).await?;
        Ok::<(), Error>(())
        }).catch_unwind().await;
        let cleanup = sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&admin)
            .await;
        match result {
            Ok(outcome) => {
                cleanup?;
                outcome?;
            }
            Err(panic) => {
                let _ = cleanup;
                std::panic::resume_unwind(panic);
            }
        }
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn native_transaction_is_refused_after_an_execution_fault() -> Result<()> {
    let inner = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
    let ds = inner.system_datasource();
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::Read));
    let mut engine = DurableEngine::new(provider).await?;
    let observed = Arc::new(std::sync::Mutex::new(None));
    let ran = Arc::new(AtomicUsize::new(0));
    let (seen, body_runs) = (observed.clone(), ran.clone());
    engine.register("native-gate", move |ctx: DurableContext, _: ()| {
        let (ds, seen, body_runs) = (ds.clone(), seen.clone(), body_runs.clone());
        async move {
            let _ = ctx.step("fault", || async { Ok(()) }).await;
            let position = ctx.current_step_id();
            let result = ctx
                .transaction_on(
                    &ds,
                    "blocked",
                    async move |_: &mut sqlx::SqliteConnection| {
                        body_runs.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await;
            *seen.lock().unwrap() =
                Some((result.unwrap_err().code(), position, ctx.current_step_id()));
            Ok(())
        }
    });
    let result = engine
        .start::<_, ()>("native-gate", (), WorkflowOptions::with_id("native-gate"))
        .await?
        .result()
        .await;
    assert_eq!(result.unwrap_err().code(), ErrorCode::RecoveryRequired);
    assert_eq!(
        *observed.lock().unwrap(),
        Some((ErrorCode::RecoveryRequired, 1, 1))
    );
    assert_eq!(ran.load(Ordering::SeqCst), 0);
    assert!(inner.get_step_result("native-gate", 1).await?.is_none());
    Ok(())
}

#[tokio::test]
async fn interrupted_queue_runs_release_global_capacity_after_recovery() -> Result<()> {
    queue_recovers_capacity(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_interrupted_queue_recovers_capacity() -> Result<()> {
    queue_recovers_capacity(Arc::new(SqliteProvider::connect("sqlite::memory:").await?)).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_interrupted_queue_recovers_capacity() -> Result<()> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let schema = format!("queue_fault_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::PgPool::connect(&url).await?;
    use futures_util::FutureExt;
    let result = std::panic::AssertUnwindSafe(async {
        let provider = Arc::new(PostgresProvider::connect_with_schema(&url, &schema).await?);
        queue_recovers_capacity(provider).await
    })
    .catch_unwind()
    .await;
    let cleanup = sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&admin)
        .await;
    let result = result.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup?;
    result
}

#[tokio::test]
async fn persistent_queue_faults_park_at_the_recovery_cap() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteAlways));
    let mut builder = DurableEngine::builder(provider);
    builder.max_recovery_attempts(1);
    let mut engine = builder.build().await?;
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = runs.clone();
    engine.register("fault", move |ctx: DurableContext, _: ()| {
        let counted = counted.clone();
        async move {
            ctx.step("fault", || async {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        }
    });
    engine.register("healthy", |_: DurableContext, _: ()| async { Ok(42) });
    engine.register_queue(
        WorkflowQueue::new("limited")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;
    let bad = engine
        .start::<_, ()>(
            "fault",
            (),
            WorkflowOptions::with_id("fault").queue("limited"),
        )
        .await?;
    let stopped = tokio::time::timeout(Duration::from_secs(5), bad.result()).await;
    assert!(stopped
        .expect("persistent fault must reach the cap")
        .is_err());
    assert_eq!(
        inner.get_workflow_status("fault").await?.unwrap().status,
        STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "initial run plus one recovery"
    );
    let healthy = engine
        .start::<_, i32>(
            "healthy",
            (),
            WorkflowOptions::with_id("healthy").queue("limited"),
        )
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), healthy.result())
            .await
            .unwrap()?,
        42
    );
    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

#[tokio::test]
async fn queue_recovery_respects_cancellation_and_shutdown() -> Result<()> {
    for cancel_workflow in [false, true] {
        let inner = Arc::new(InMemoryProvider::new());
        let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteAlways));
        let mut engine = DurableEngine::new(provider.clone()).await?;
        provider.block_claim.store(true, Ordering::SeqCst);
        let runs = Arc::new(AtomicUsize::new(0));
        let counted = runs.clone();
        engine.register("fault", move |ctx: DurableContext, _: ()| {
            let counted = counted.clone();
            async move {
                ctx.step("fault", || async {
                    counted.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            }
        });
        engine.register_queue(
            WorkflowQueue::new("limited").base_polling_interval(Duration::from_millis(10)),
        );
        engine.launch().await?;
        engine
            .start::<_, ()>(
                "fault",
                (),
                WorkflowOptions::with_id("fault").queue("limited"),
            )
            .await?;
        tokio::time::timeout(Duration::from_secs(5), provider.claim_started.notified())
            .await
            .expect("recovery must reach the claim gate");
        if cancel_workflow {
            engine.cancel_workflow("fault").await?;
            provider.claim_permit.add_permits(1);
            tokio::time::timeout(Duration::from_secs(2), provider.claim_finished.notified())
                .await
                .expect("the claim must observe cancellation");
        }
        engine.shutdown(Duration::from_secs(2)).await?;
        let status = inner.get_workflow_status("fault").await?.unwrap();
        assert_eq!(
            status.status,
            if cancel_workflow {
                STATUS_CANCELLED
            } else {
                STATUS_PENDING
            }
        );
        assert_eq!(status.recovery_attempts, 0);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[derive(Clone)]
struct FaultDataSource {
    inner: SqliteDataSource,
    attempts: Arc<AtomicUsize>,
    fail_begin: bool,
    duplicate: bool,
}

#[cfg(feature = "sqlite")]
#[async_trait::async_trait]
impl crate::datasource::sealed::Backend for FaultDataSource {
    type Conn = sqlx::SqliteConnection;
    type NativeTx = sqlx::Transaction<'static, sqlx::Sqlite>;
    async fn begin(&self, isolation: IsolationLevel, read_only: bool) -> Result<Self::NativeTx> {
        if self.fail_begin {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            return Err(Error::Db(sqlx::Error::Protocol(
                "transaction setup failed".into(),
            )));
        }
        self.inner.begin(isolation, read_only).await
    }
    async fn commit(&self, tx: Self::NativeTx) -> Result<()> {
        self.inner.commit(tx).await
    }
    async fn rollback(&self, tx: Self::NativeTx) -> Result<()> {
        self.inner.rollback(tx).await?;
        if self.duplicate {
            // Simulate the winner becoming observable as our losing attempt
            // releases its connection. Commit a real authoritative row, then
            // lose this attempt's rollback response.
            let encoded = Serializer::Json.encode(&serde_json::json!(42))?;
            let mut winner = self
                .inner
                .begin(IsolationLevel::ReadCommitted, false)
                .await?;
            if matches!(
                self.inner.kind(),
                crate::datasource::DataSourceKind::System(_)
            ) {
                self.inner
                    .insert_checkpoint(
                        &mut winner,
                        "duplicate",
                        0,
                        "effect",
                        &encoded,
                        crate::serialize::DBOS_JSON,
                        1,
                    )
                    .await?;
            } else {
                self.inner
                    .insert_completion(
                        &mut winner,
                        "duplicate",
                        0,
                        Some(&encoded),
                        None,
                        crate::serialize::DBOS_JSON,
                    )
                    .await?;
            }
            self.inner.commit(winner).await?;
        }
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(Error::Db(sqlx::Error::Protocol(
            "rollback response lost".into(),
        )))
    }
    async fn tx_fingerprint(&self, conn: &mut Self::Conn) -> Result<Option<String>> {
        self.inner.tx_fingerprint(conn).await
    }
    async fn fetch_completion(
        &self,
        id: &str,
        seq: i32,
    ) -> Result<Option<crate::datasource::CompletionRow>> {
        self.inner.fetch_completion(id, seq).await
    }
    async fn insert_completion(
        &self,
        conn: &mut Self::Conn,
        id: &str,
        seq: i32,
        output: Option<&str>,
        error: Option<&str>,
        serialization: &str,
    ) -> Result<bool> {
        if self.duplicate {
            return Ok(false);
        }
        self.inner
            .insert_completion(conn, id, seq, output, error, serialization)
            .await
    }
    async fn insert_failure(
        &self,
        id: &str,
        seq: i32,
        error: &str,
        serialization: &str,
    ) -> Result<()> {
        self.inner
            .insert_failure(id, seq, error, serialization)
            .await
    }
    fn kind(&self) -> &crate::datasource::DataSourceKind {
        self.inner.kind()
    }
    async fn insert_checkpoint(
        &self,
        conn: &mut Self::Conn,
        id: &str,
        seq: i32,
        name: &str,
        output: &str,
        serialization: &str,
        started: i64,
    ) -> Result<bool> {
        if self.duplicate {
            return Ok(false);
        }
        self.inner
            .insert_checkpoint(conn, id, seq, name, output, serialization, started)
            .await
    }
}
#[cfg(feature = "sqlite")]
impl crate::datasource::DataSource for FaultDataSource {}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn datasource_setup_failures_bypass_the_business_retry_policy() -> Result<()> {
    for system in [false, true] {
        let inner = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
        let attempts = Arc::new(AtomicUsize::new(0));
        let retries = Arc::new(AtomicUsize::new(0));
        let body_runs = Arc::new(AtomicUsize::new(0));
        let ds = FaultDataSource {
            inner: if system {
                inner.system_datasource()
            } else {
                SqliteDataSource::new(sqlx::SqlitePool::connect("sqlite::memory:").await?).await?
            },
            attempts: attempts.clone(),
            fail_begin: true,
            duplicate: false,
        };
        // Forward provider identity so the system data-source path is exercised.
        let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::ChildBefore));
        let mut engine = DurableEngine::new(provider).await?;
        let (predicates, bodies) = (retries.clone(), body_runs.clone());
        engine.register("setup", move |ctx: DurableContext, _: ()| {
            let (ds, predicates, bodies) = (ds.clone(), predicates.clone(), bodies.clone());
            async move {
                ctx.transaction_on_with(
                    &ds,
                    TransactionOptions::new("setup")
                        .max_retries(3)
                        .retry_if(move |_| {
                            predicates.fetch_add(1, Ordering::SeqCst);
                            true
                        }),
                    async move |_: &mut sqlx::SqliteConnection| {
                        bodies.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                )
                .await
            }
        });
        let result = engine
            .start::<_, ()>("setup", (), WorkflowOptions::with_id("setup"))
            .await?
            .result()
            .await;
        assert_eq!(result.unwrap_err().code(), ErrorCode::RecoveryRequired);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(retries.load(Ordering::SeqCst), 0);
        assert_eq!(body_runs.load(Ordering::SeqCst), 0);
        assert!(inner.get_step_result("setup", 0).await?.is_none());
        assert_eq!(
            inner.get_workflow_status("setup").await?.unwrap().status,
            STATUS_PENDING
        );
    }
    Ok(())
}

#[tokio::test]
async fn queued_panics_recover_or_park_without_leaking_capacity() -> Result<()> {
    for permanent in [false, true] {
        let inner = Arc::new(InMemoryProvider::new());
        let mut builder = DurableEngine::builder(inner.clone());
        builder.max_recovery_attempts(1);
        let mut engine = builder.build().await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        let counted = attempts.clone();
        engine.register("panics", move |_: DurableContext, _: ()| {
            let counted = counted.clone();
            async move {
                if counted.fetch_add(1, Ordering::SeqCst) == 0 || permanent {
                    panic!("recoverable workflow panic");
                }
                Ok(42)
            }
        });
        engine.register("healthy", |_: DurableContext, _: ()| async { Ok(42) });
        engine.register_queue(
            WorkflowQueue::new("limited")
                .global_concurrency(1)
                .base_polling_interval(Duration::from_millis(10)),
        );
        engine.launch().await?;
        let first = engine
            .start::<_, i32>(
                "panics",
                (),
                WorkflowOptions::with_id("panics").queue("limited"),
            )
            .await?;
        let result = tokio::time::timeout(Duration::from_secs(3), first.result()).await;
        if result.is_err() {
            engine.shutdown(Duration::from_secs(1)).await?;
        }
        let result = result.expect("an interrupted queued run must recover or park");
        if permanent {
            assert_eq!(
                result.unwrap_err().code(),
                ErrorCode::MaxRecoveryAttemptsExceeded
            );
        } else {
            assert_eq!(result?, 42);
        }
        let next = engine
            .start::<_, i32>(
                "healthy",
                (),
                WorkflowOptions::with_id("healthy").queue("limited"),
            )
            .await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), next.result())
                .await
                .unwrap()?,
            42
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        engine.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn a_child_recovery_error_cannot_be_checkpointed_by_its_parent() -> Result<()> {
    for panics in [false, true] {
        let child_store = Arc::new(InMemoryProvider::new());
        let mut child = DurableEngine::new(Arc::new(FaultProvider::new(
            child_store.clone(),
            if panics {
                Fault::ChildBefore
            } else {
                Fault::WriteBefore
            },
        )))
        .await?;
        let attempts = Arc::new(AtomicUsize::new(0));
        child.register("child", move |ctx: DurableContext, _: ()| {
            let attempts = attempts.clone();
            async move {
                if panics && attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("child workflow interrupted");
                }
                ctx.step("work", || async { Ok(42) }).await
            }
        });
        let child = Arc::new(child);
        let parent_store = Arc::new(InMemoryProvider::new());
        let mut parent = DurableEngine::new(parent_store.clone()).await?;
        let child_engine = child.clone();
        parent.register("parent", move |ctx: DurableContext, _: ()| {
            let child = child_engine.clone();
            async move {
                let _ = ctx
                    .step("child-result", || async {
                        child
                            .start::<_, i32>("child", (), WorkflowOptions::with_id("child"))
                            .await?
                            .result()
                            .await
                    })
                    .await;
                Ok(42)
            }
        });
        let result = parent
            .start::<_, i32>("parent", (), WorkflowOptions::with_id("parent"))
            .await?
            .result()
            .await;
        assert!(
            matches!(result, Err(Error::RecoveryRequired(_))),
            "child storage failure must interrupt parent: {result:?}"
        );
        assert!(parent_store.get_step_result("parent", 0).await?.is_none());
        assert_eq!(
            parent_store
                .get_workflow_status("parent")
                .await?
                .unwrap()
                .status,
            STATUS_PENDING
        );
        let owner = child_store
            .get_workflow_status("child")
            .await?
            .unwrap()
            .executor_id;
        child.recover_pending_for(&[owner]).await?;
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                child.retrieve_workflow::<i32>("child").await?.result()
            )
            .await
            .unwrap()?,
            42
        );
        let owner = parent_store
            .get_workflow_status("parent")
            .await?
            .unwrap()
            .executor_id;
        parent.recover_pending_for(&[owner]).await?;
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                parent.retrieve_workflow::<i32>("parent").await?.result()
            )
            .await
            .unwrap()?,
            42
        );
        child.shutdown(Duration::from_secs(1)).await?;
        parent.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn terminal_reconciliation_preserves_the_recovery_channel() -> Result<()> {
    for fault in [Fault::TerminalWrapped, Fault::TerminalAfterThenAdoptRead] {
        let inner = Arc::new(InMemoryProvider::new());
        let mut engine =
            DurableEngine::new(Arc::new(FaultProvider::new(inner.clone(), fault))).await?;
        engine.register("terminal", |_: DurableContext, _: ()| async { Ok(42) });
        let error = engine
            .start::<_, i32>("terminal", (), WorkflowOptions::with_id("terminal"))
            .await?
            .result()
            .await
            .unwrap_err();
        let Error::RecoveryRequired(cause) = error else {
            panic!("expected recovery channel, got {error:?}");
        };
        assert!(
            matches!(&*cause, Error::Db(sqlx::Error::PoolTimedOut)),
            "cause must not be double-wrapped: {cause:?}"
        );
        let status = inner.get_workflow_status("terminal").await?.unwrap();
        assert_eq!(
            status.status,
            if fault == Fault::TerminalWrapped {
                STATUS_PENDING
            } else {
                STATUS_SUCCESS
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn verification_storage_fault_is_not_reported_as_divergence() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::ChildBefore));
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("verify", |ctx: DurableContext, _: ()| async move {
        let _ = ctx.step("work", || async { Ok(42) }).await;
        Ok(42)
    });
    engine
        .start::<_, i32>("verify", (), WorkflowOptions::with_id("verify"))
        .await?
        .result()
        .await?;
    provider.arm([Fault::Read]);
    let result = engine.verify_replay("verify").await;
    assert!(
        matches!(result, Err(Error::RecoveryRequired(_))),
        "storage failure is not a determinism finding: {result:?}"
    );
    assert_eq!(
        inner.get_workflow_status("verify").await?.unwrap().status,
        STATUS_SUCCESS
    );
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn native_rollback_failures_interrupt_instead_of_recording_body_errors() -> Result<()> {
    for system in [false, true] {
        let inner = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
        let attempts = Arc::new(AtomicUsize::new(0));
        let ds = FaultDataSource {
            inner: if system {
                inner.system_datasource()
            } else {
                SqliteDataSource::new(sqlx::SqlitePool::connect("sqlite::memory:").await?).await?
            },
            attempts: attempts.clone(),
            fail_begin: false,
            duplicate: false,
        };
        let mut engine = DurableEngine::new(inner.clone()).await?;
        engine.register("rollback", move |ctx: DurableContext, _: ()| {
            let ds = ds.clone();
            async move {
                let _ = ctx
                    .transaction_on(&ds, "rollback", async |_: &mut sqlx::SqliteConnection| {
                        Err::<(), _>(Error::app("business rejection"))
                    })
                    .await;
                Ok(())
            }
        });
        let result = engine
            .start::<_, ()>("rollback", (), WorkflowOptions::with_id("rollback"))
            .await?
            .result()
            .await;
        assert!(matches!(result, Err(Error::RecoveryRequired(_))));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(inner.get_step_result("rollback", 0).await?.is_none());
        assert_eq!(
            inner.get_workflow_status("rollback").await?.unwrap().status,
            STATUS_PENDING
        );
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn recovery_signal_crosses_every_error_recording_boundary_without_being_saved() -> Result<()>
{
    fn interruption(observation: bool) -> Error {
        let cause = Arc::new(Error::Db(sqlx::Error::PoolTimedOut));
        if observation {
            Error::ObservationFailed(cause)
        } else {
            Error::RecoveryRequired(cause)
        }
    }
    for observation in [false, true] {
        for mode in [
            "workflow",
            "step-retry",
            "transaction",
            "external",
            "system",
        ] {
            let inner = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
            let mut engine = DurableEngine::new(inner.clone()).await?;
            let predicates = Arc::new(AtomicUsize::new(0));
            let counted = predicates.clone();
            let ds = if mode == "external" {
                SqliteDataSource::new(sqlx::SqlitePool::connect("sqlite::memory:").await?).await?
            } else {
                inner.system_datasource()
            };
            let workflow_ds = ds.clone();
            engine.register("boundary", move |ctx: DurableContext, _: ()| {
                let (counted, ds) = (counted.clone(), workflow_ds.clone());
                async move {
                    if mode == "workflow" {
                        return Err::<(), _>(interruption(observation));
                    }
                    let retry = move |_: &Error| {
                        counted.fetch_add(1, Ordering::SeqCst);
                        true
                    };
                    let _ = match mode {
                        "step-retry" => {
                            ctx.step_with(
                                StepOptions::new("boundary").max_retries(2).retry_if(retry),
                                || async move { Err::<(), _>(interruption(observation)) },
                            )
                            .await
                        }
                        "transaction" => {
                            ctx.transaction_with(
                                TransactionOptions::new("boundary")
                                    .max_retries(2)
                                    .retry_if(retry),
                                move |_| {
                                    Box::pin(async move { Err::<(), _>(interruption(observation)) })
                                },
                            )
                            .await
                        }
                        _ => {
                            ctx.transaction_on_with(
                                &ds,
                                TransactionOptions::new("boundary")
                                    .max_retries(2)
                                    .retry_if(retry),
                                async move |_: &mut sqlx::SqliteConnection| {
                                    Err::<(), _>(interruption(observation))
                                },
                            )
                            .await
                        }
                    };
                    Ok(()) // catching a rejected record must not authorize SUCCESS
                }
            });
            let result = engine
                .start::<_, ()>("boundary", (), WorkflowOptions::with_id("boundary"))
                .await?
                .result()
                .await;
            assert!(
                matches!(result, Err(Error::RecoveryRequired(_))),
                "{mode}: {result:?}"
            );
            assert_eq!(
                predicates.load(Ordering::SeqCst),
                0,
                "{mode}: infrastructure signals skip business retry"
            );
            assert!(
                inner.get_step_result("boundary", 0).await?.is_none(),
                "{mode}"
            );
            assert_eq!(
                inner.get_workflow_status("boundary").await?.unwrap().status,
                STATUS_PENDING,
                "{mode}"
            );
            if mode == "external" {
                use crate::datasource::sealed::Backend;
                assert!(ds.fetch_completion("boundary", 0).await?.is_none());
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn queued_business_failures_do_not_spend_recovery_attempts() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(inner.clone()).await?;
    let attempts = Arc::new(AtomicUsize::new(0));
    let counted = attempts.clone();
    engine.register("business", move |_: DurableContext, _: ()| {
        let counted = counted.clone();
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            Err::<(), _>(Error::app("declined"))
        }
    });
    engine.register_queue(
        WorkflowQueue::new("limited")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;
    let result = engine
        .start::<_, ()>(
            "business",
            (),
            WorkflowOptions::with_id("business").queue("limited"),
        )
        .await?
        .result()
        .await;
    assert!(result.is_err());
    tokio::time::sleep(Duration::from_millis(250)).await;
    let status = inner.get_workflow_status("business").await?.unwrap();
    assert_eq!(status.status, STATUS_ERROR);
    assert_eq!(status.recovery_attempts, 0);
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn fault_wrapper_preserves_atomic_sql_notification_batches() -> Result<()> {
    let inner = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
    let engine = DurableEngine::new(inner.clone()).await?;
    inner
        .insert_workflow_status(WorkflowStatus::new(
            "target",
            "unused",
            serde_json::Value::Null,
            STATUS_PENDING,
            "owner",
            engine.app_version(),
        ))
        .await?;
    let wrapper = FaultProvider::new(inner.clone(), Fault::ChildBefore);
    let rows = ["target", "absent"].map(|id| NotificationInsert {
        destination_id: id.into(),
        topic: "topic".into(),
        message: serde_json::json!(42),
        idempotency_key: None,
    });
    assert!(wrapper.insert_notifications(&rows).await.is_err());
    assert!(
        inner
            .list_workflow_notifications("target")
            .await?
            .is_empty(),
        "the wrapper must call the provider's atomic batch, not the sequential default"
    );
    Ok(())
}

#[tokio::test]
async fn polling_handle_failures_preserve_infrastructure_origin() -> Result<()> {
    for mode in ["read", "envelope", "type"] {
        let inner = Arc::new(InMemoryProvider::new());
        let mut status = WorkflowStatus::new(
            "observed",
            "unused",
            serde_json::Value::Null,
            if mode == "envelope" {
                STATUS_ERROR
            } else {
                STATUS_SUCCESS
            },
            "owner",
            "version",
        );
        status.output = Some(serde_json::json!(42));
        if mode == "envelope" {
            status.error = Some("unreadable".into());
            status.error_info = Some(PortableWorkflowError {
                name: crate::recorded_error::NAME.into(),
                message: "unreadable".into(),
                code: None,
                data: Some(serde_json::json!({"version":999})),
            });
        }
        inner.insert_workflow_status(status).await?;
        let provider = Arc::new(FaultProvider::new(
            inner,
            if mode == "read" {
                Fault::StatusRead
            } else {
                Fault::ChildBefore
            },
        ));
        let handle = WorkflowHandle::<String>::polling("observed".into(), provider);
        let error = handle.result().await.unwrap_err();
        assert_eq!(
            error.code(),
            if mode == "read" {
                ErrorCode::Database
            } else {
                ErrorCode::Serialization
            },
            "{mode}: {error:?}"
        );
        assert_eq!(matches!(error, Error::ObservationFailed(_)), mode != "type");
    }
    Ok(())
}

#[tokio::test]
async fn a_cancelled_workflow_task_is_an_interruption_not_a_business_error() -> Result<()> {
    let join = tokio::spawn(std::future::pending::<Result<serde_json::Value>>());
    join.abort();
    let handle =
        WorkflowHandle::<()>::local("aborted".into(), Arc::new(InMemoryProvider::new()), join);
    assert!(matches!(
        handle.result().await,
        Err(Error::RecoveryRequired(_))
    ));
    Ok(())
}

#[tokio::test]
async fn creating_or_retrieving_a_workflow_preserves_storage_origin() -> Result<()> {
    for client_api in [false, true] {
        for fault in [Fault::InsertBefore, Fault::InsertAfter, Fault::StatusRead] {
            let inner = Arc::new(InMemoryProvider::new());
            let provider = Arc::new(FaultProvider::new(inner.clone(), fault));
            let mut engine = DurableEngine::new(provider.clone()).await?;
            engine.register("child", |_: DurableContext, _: ()| async { Ok(()) });
            let client = Client::new(provider);
            if fault == Fault::StatusRead {
                inner
                    .insert_workflow_status(WorkflowStatus::new(
                        "child",
                        "child",
                        serde_json::Value::Null,
                        STATUS_PENDING,
                        "owner",
                        engine.app_version(),
                    ))
                    .await?;
            }
            let result = if fault == Fault::StatusRead {
                if client_api {
                    client.retrieve_workflow::<()>("child").await
                } else {
                    engine.retrieve_workflow::<()>("child").await
                }
            } else if client_api {
                client
                    .enqueue::<_, ()>("jobs", "child", (), WorkflowOptions::with_id("child"))
                    .await
            } else {
                engine
                    .start::<_, ()>("child", (), WorkflowOptions::with_id("child"))
                    .await
            };
            let error = result.err().expect("injected failure must surface");
            assert!(
                matches!(error, Error::ObservationFailed(_)),
                "client={client_api}, {fault:?}: {error:?}"
            );
            assert_eq!(
                inner.get_workflow_status("child").await?.is_some(),
                fault != Fault::InsertBefore
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn queued_parent_does_not_strand_on_an_interrupted_unqueued_child() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteBefore));
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("child", |ctx: DurableContext, _: ()| async move {
        ctx.step("child-effect", || async { Ok(42) }).await
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        ctx.start_workflow::<_, i32>("child", (), WorkflowOptions::default())
            .await?
            .result()
            .await
    });
    engine.register_queue(
        WorkflowQueue::new("parents")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;
    let handle = engine
        .start::<_, i32>(
            "parent",
            (),
            WorkflowOptions::with_id("parent").queue("parents"),
        )
        .await?;
    let result = tokio::time::timeout(Duration::from_secs(3), handle.result()).await;
    engine.shutdown(Duration::from_secs(1)).await?;
    assert_eq!(
        result.expect("child must recover without an external sweep")?,
        42
    );
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn transaction_output_encoding_never_enters_the_body_retry_policy() -> Result<()> {
    for mode in ["legacy", "external", "system"] {
        let provider = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
        let ds = if mode == "system" {
            provider.system_datasource()
        } else {
            SqliteDataSource::new(sqlx::SqlitePool::connect("sqlite::memory:").await?).await?
        };
        let effects = Arc::new(AtomicUsize::new(0));
        let predicates = Arc::new(AtomicUsize::new(0));
        let mut engine = DurableEngine::new(provider.clone()).await?;
        let (count, decisions) = (effects.clone(), predicates.clone());
        engine.register("encoding", move |ctx: DurableContext, _: ()| {
            let (ds, count, decisions) = (ds.clone(), count.clone(), decisions.clone());
            async move {
                let opts = TransactionOptions::new("effect")
                    .max_retries(3)
                    .base_interval(Duration::ZERO)
                    .retry_if(move |_| {
                        decisions.fetch_add(1, Ordering::SeqCst);
                        true
                    });
                let result = if mode == "legacy" {
                    ctx.transaction_with(opts, move |_| {
                        let count = count.clone();
                        Box::pin(async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(Unserializable)
                        })
                    })
                    .await
                } else {
                    ctx.transaction_on_with(
                        &ds,
                        opts,
                        async move |_: &mut sqlx::SqliteConnection| {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(Unserializable)
                        },
                    )
                    .await
                };
                result.map(|_| ())
            }
        });
        let result = engine
            .start::<_, ()>("encoding", (), WorkflowOptions::with_id("encoding"))
            .await?
            .result()
            .await;
        assert_eq!(
            result.unwrap_err().code(),
            ErrorCode::Serialization,
            "{mode}"
        );
        assert_eq!(
            effects.load(Ordering::SeqCst),
            1,
            "{mode}: output encoding must not repeat the body"
        );
        assert_eq!(
            predicates.load(Ordering::SeqCst),
            0,
            "{mode}: not a body failure"
        );
        assert!(provider.get_step_result("encoding", 0).await?.is_some());
        engine.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn verification_interrupts_a_body_that_catches_a_fault_and_parks() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::Read));
    inner
        .insert_workflow_status(WorkflowStatus::new(
            "verify-park",
            "verify-park",
            serde_json::Value::Null,
            STATUS_SUCCESS,
            "old",
            "",
        ))
        .await?;
    inner
        .record_step_result(
            "verify-park",
            0,
            "effect",
            serde_json::json!(42),
            None,
            None,
            None,
        )
        .await?;
    let mut engine = DurableEngine::new(provider).await?;
    engine.register("verify-park", |ctx: DurableContext, _: ()| async move {
        let _ = ctx.step("effect", || async { Ok(42) }).await;
        std::future::pending::<Result<()>>().await
    });
    let result = tokio::time::timeout(
        Duration::from_millis(300),
        engine.verify_replay("verify-park"),
    )
    .await;
    assert!(
        matches!(result, Ok(Err(Error::RecoveryRequired(_)))),
        "{result:?}"
    );
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn duplicate_native_transaction_converges_despite_lost_rollback_response() -> Result<()> {
    for system in [false, true] {
        let provider = Arc::new(SqliteProvider::connect("sqlite::memory:").await?);
        let ds = FaultDataSource {
            inner: if system {
                provider.system_datasource()
            } else {
                SqliteDataSource::new(sqlx::SqlitePool::connect("sqlite::memory:").await?).await?
            },
            attempts: Arc::new(AtomicUsize::new(0)),
            fail_begin: false,
            duplicate: true,
        };
        let bodies = Arc::new(AtomicUsize::new(0));
        let counted = bodies.clone();
        let mut engine = DurableEngine::new(provider.clone()).await?;
        engine.register("duplicate", move |ctx: DurableContext, _: ()| {
            let (ds, counted) = (ds.clone(), counted.clone());
            async move {
                ctx.transaction_on(
                    &ds,
                    "effect",
                    async move |_: &mut sqlx::SqliteConnection| {
                        counted.fetch_add(1, Ordering::SeqCst);
                        Ok(42)
                    },
                )
                .await
            }
        });
        let result = engine
            .start::<_, i32>("duplicate", (), WorkflowOptions::with_id("duplicate"))
            .await?
            .result()
            .await;
        engine.shutdown(Duration::from_secs(1)).await?;
        assert_eq!(result?, 42, "system={system}");
        assert_eq!(bodies.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider
                .get_workflow_status("duplicate")
                .await?
                .unwrap()
                .status,
            STATUS_SUCCESS
        );
    }
    Ok(())
}

#[tokio::test]
async fn observation_failure_does_not_claim_the_target_execution_stopped() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    inner
        .insert_workflow_status(WorkflowStatus::new(
            "running",
            "other",
            serde_json::Value::Null,
            STATUS_PENDING,
            "other-owner",
            "version",
        ))
        .await?;
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::StatusRead));
    let handle = WorkflowHandle::<()>::polling("running".into(), provider);
    let error = handle.result().await.unwrap_err();
    assert_eq!(
        error.code(),
        ErrorCode::Database,
        "a read outage is not evidence that the target stopped"
    );
    assert!(
        crate::serialize::encode_error(&Serializer::Json, &error).is_err(),
        "observation failures still cannot become business outcomes"
    );
    let status = inner.get_workflow_status("running").await?.unwrap();
    assert_eq!(status.status, STATUS_PENDING);
    assert_eq!(status.recovery_attempts, 0);
    Ok(())
}

#[tokio::test]
async fn permanent_recovery_claim_failure_stops_retrying_without_running_the_body() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteBefore));
    provider.arm([Fault::WriteBefore, Fault::ClaimAlways]);
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let runs = Arc::new(AtomicUsize::new(0));
    let count = runs.clone();
    engine.register("claims", move |ctx: DurableContext, _: ()| {
        let count = count.clone();
        async move {
            ctx.step("effect", || async {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        }
    });
    let _ = engine
        .start::<_, ()>("claims", (), WorkflowOptions::with_id("claims"))
        .await?
        .result()
        .await;
    tokio::time::timeout(Duration::from_secs(25), async {
        while provider.claim_calls.load(Ordering::SeqCst) < 8 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("claim retry budget must be bounded");
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(provider.claim_calls.load(Ordering::SeqCst), 8);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let status = inner.get_workflow_status("claims").await?.unwrap();
    assert_eq!(
        status.status, STATUS_PENDING,
        "cannot manufacture a parked row while storage rejects writes"
    );
    assert_eq!(status.recovery_attempts, 0);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn an_unrecoverable_child_parks_and_releases_its_queued_parent() -> Result<()> {
    let inner = Arc::new(InMemoryProvider::new());
    let provider = Arc::new(FaultProvider::new(inner.clone(), Fault::WriteAlways));
    let mut builder = DurableEngine::builder(provider);
    builder.max_recovery_attempts(1);
    let mut engine = builder.build().await?;
    engine.register("child", |ctx: DurableContext, _: ()| async move {
        ctx.step("effect", || async { Ok(42) }).await
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        ctx.start_workflow::<_, i32>("child", (), WorkflowOptions::default())
            .await?
            .result()
            .await
    });
    engine.register("healthy", |_: DurableContext, _: ()| async { Ok(42) });
    engine.register_queue(
        WorkflowQueue::new("parents")
            .global_concurrency(1)
            .base_polling_interval(Duration::from_millis(10)),
    );
    engine.launch().await?;
    let bad = engine
        .start::<_, i32>(
            "parent",
            (),
            WorkflowOptions::with_id("parent").queue("parents"),
        )
        .await?;
    let failed = tokio::time::timeout(Duration::from_secs(4), bad.result())
        .await
        .expect("child recovery is bounded");
    assert!(
        matches!(failed, Err(Error::MaxRecoveryAttemptsExceeded(_))),
        "{failed:?}"
    );
    let good = engine
        .start::<_, i32>("healthy", (), WorkflowOptions::default().queue("parents"))
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), good.result())
            .await
            .unwrap()?,
        42
    );
    assert_eq!(
        inner.get_workflow_status("parent-0").await?.unwrap().status,
        STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
    );
    assert_eq!(
        inner.get_workflow_status("parent").await?.unwrap().status,
        STATUS_ERROR
    );
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn verifier_history_read_faults_preserve_observation_origin() -> Result<()> {
    for fault in [Fault::StatusRead, Fault::HistoryRead] {
        let inner = Arc::new(InMemoryProvider::new());
        inner
            .insert_workflow_status(WorkflowStatus::new(
                "history",
                "history",
                serde_json::Value::Null,
                STATUS_SUCCESS,
                "owner",
                "version",
            ))
            .await?;
        let mut engine =
            DurableEngine::new(Arc::new(FaultProvider::new(inner.clone(), fault))).await?;
        engine.register("history", |_: DurableContext, _: ()| async {
            panic!("unreadable history must not run the body");
            #[allow(unreachable_code)]
            Ok(())
        });
        assert!(matches!(
            engine.verify_replay("history").await,
            Err(Error::ObservationFailed(_))
        ));
        assert_eq!(
            inner.get_workflow_status("history").await?.unwrap().status,
            STATUS_SUCCESS
        );
    }
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_transaction_output_encoding_skips_body_retries() -> Result<()> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let schema = format!("encoding_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::PgPool::connect(&url).await?;
    use futures_util::FutureExt;
    let result = std::panic::AssertUnwindSafe(async {
        let provider = Arc::new(PostgresProvider::connect_with_schema(&url, &schema).await?);
        let mut engine = DurableEngine::new(provider.clone()).await?;
        let effects = Arc::new(AtomicUsize::new(0));
        let count = effects.clone();
        engine.register("encoding", move |ctx: DurableContext, _: ()| {
            let count = count.clone();
            async move {
                ctx.transaction_with(
                    TransactionOptions::new("effect")
                        .max_retries(3)
                        .base_interval(Duration::ZERO)
                        .retry_if(|_| panic!("output conversion is not a body failure")),
                    move |_| {
                        let count = count.clone();
                        Box::pin(async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            Ok(Unserializable)
                        })
                    },
                )
                .await
                .map(|_| ())
            }
        });
        let error = engine
            .start::<_, ()>("encoding", (), WorkflowOptions::with_id("encoding"))
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Serialization);
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        assert!(provider.get_step_result("encoding", 0).await?.is_some());
        engine.shutdown(Duration::from_secs(1)).await?;
        Ok::<_, Error>(())
    })
    .catch_unwind()
    .await;
    let cleanup = sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&admin)
        .await;
    let result = result.unwrap_or_else(|panic| std::panic::resume_unwind(panic));
    cleanup?;
    result
}
