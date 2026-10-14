//! `WorkflowHandle` ergonomics: await it directly (`IntoFuture`), resolve it
//! through a shared `&self` (`result`), and `clone` it to observe the same
//! workflow from another task.

use durare::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

/// A launched in-memory engine with a `quick` workflow that returns `n + 1`.
async fn engine_with_quick() -> Result<DurableEngine> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("quick", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n + 1)
    });
    engine.launch().await?;
    Ok(engine)
}

/// `handle.await` resolves to the typed output (`IntoFuture`).
#[tokio::test]
async fn handle_awaits_directly() -> Result<()> {
    let engine = engine_with_quick().await?;
    let handle = engine
        .start::<_, i64>("quick", 1_i64, WorkflowOptions::default())
        .await?;
    let out: i64 = handle.await?;
    assert_eq!(out, 2);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// `result` takes `&self`, so it needs no `mut` binding and can be called more
/// than once — the second call falls back to the persisted status after the
/// first consumes the in-process task.
#[tokio::test]
async fn result_takes_shared_ref_and_is_reusable() -> Result<()> {
    let engine = engine_with_quick().await?;
    let handle = engine
        .start::<_, i64>("quick", 41_i64, WorkflowOptions::default())
        .await?;
    let first: i64 = handle.result().await?;
    let second: i64 = handle.result().await?;
    assert_eq!(first, 42);
    assert_eq!(second, 42);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// A handle is `Clone` and `Send`: a clone can be awaited inside a spawned task
/// (which requires the future be `Send`) and observes the same result.
#[tokio::test]
async fn handle_clones_and_is_send_across_spawn() -> Result<()> {
    let engine = engine_with_quick().await?;
    let handle = engine
        .start::<_, i64>("quick", 10_i64, WorkflowOptions::default())
        .await?;
    let observer = handle.clone();
    let spawned = tokio::spawn(async move { observer.await });

    let here: i64 = handle.await?;
    let there: i64 = spawned.await.expect("spawned task panicked")?;
    assert_eq!(here, 11);
    assert_eq!(there, 11);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

mod common;
use durare::{Client, StateProvider, WorkflowResultOptions};

async fn result_options_contract(provider: Arc<dyn StateProvider>) -> Result<()> {
    let gate = Arc::new(tokio::sync::Notify::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let workflow_gate = gate.clone();
    engine.register("gated", move |_ctx: DurableContext, _: ()| {
        let gate = workflow_gate.clone();
        async move {
            gate.notified().await;
            Ok::<_, Error>(42_i64)
        }
    });
    let local = engine
        .start::<_, i64>("gated", (), WorkflowOptions::default())
        .await?;
    let observer: durare::WorkflowHandle<i64> =
        Client::new(provider).retrieve_workflow(local.id()).await?;
    assert_eq!(
        WorkflowResultOptions::new().polling_interval,
        Duration::from_millis(100)
    );
    assert!(WorkflowResultOptions::new().timeout.is_none());
    let options = WorkflowResultOptions::new()
        .polling_interval(Duration::from_millis(5))
        .timeout(Duration::from_millis(30));
    assert!(matches!(
        observer.result_with_options(options).await,
        Err(Error::ObservationFailed(cause)) if matches!(cause.as_ref(), Error::Timeout)
    ));
    assert!(matches!(
        local.result_with_options(options).await,
        Err(Error::ObservationFailed(cause)) if matches!(cause.as_ref(), Error::Timeout)
    ));
    let wait_error = observer.result_with_options(options).await.unwrap_err();
    assert_eq!(wait_error.code(), durare::ErrorCode::Timeout);
    assert!(matches!(wait_error, Error::ObservationFailed(_)));
    assert_eq!(observer.get_status().await?.status, "PENDING");
    assert!(matches!(
        observer
            .result_with_options(WorkflowResultOptions::new().polling_interval(Duration::ZERO))
            .await,
        Err(Error::App { .. })
    ));
    gate.notify_one();
    assert_eq!(local.result().await?, 42);
    assert_eq!(
        observer
            .result_with_options(options.timeout(Duration::from_secs(2)))
            .await?,
        42
    );
    assert_eq!(local.clone().await?, 42);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn result_options_memory() -> Result<()> {
    result_options_contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn result_options_sqlite() -> Result<()> {
    let provider = durare::SqliteProvider::connect("sqlite::memory:").await?;
    result_options_contract(Arc::new(provider)).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn result_options_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "result_options").await;
    let provider = durare::PostgresProvider::connect(&url).await?;
    let result = result_options_contract(Arc::new(provider)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

// Poll once while pending, complete the workflow, and verify the configured
// long interval delays the next read. A new wait still reads immediately.
#[tokio::test]
async fn observer_uses_configured_polling_interval() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let gate = Arc::new(tokio::sync::Notify::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let workflow_gate = gate.clone();
    engine.register("gated", move |_ctx: DurableContext, _: ()| {
        let gate = workflow_gate.clone();
        async move {
            gate.notified().await;
            Ok::<_, Error>(42_i64)
        }
    });
    let local = engine
        .start::<_, i64>("gated", (), WorkflowOptions::default())
        .await?;
    let observer: durare::WorkflowHandle<i64> =
        Client::new(provider).retrieve_workflow(local.id()).await?;
    let wait = observer.result_with_options(
        WorkflowResultOptions::new().polling_interval(Duration::from_secs(3600)),
    );
    tokio::pin!(wait);
    assert!(futures_util::poll!(&mut wait).is_pending());
    gate.notify_one();
    assert_eq!(local.result().await?, 42);
    assert!(tokio::time::timeout(Duration::from_millis(250), &mut wait)
        .await
        .is_err());
    assert_eq!(observer.result().await?, 42);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}
