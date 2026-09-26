//! A recorded failure must not change the workflow's next action on replay.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
};

mod common;
use std::sync::Arc;

async fn assert_error_replays(make_error: fn() -> Error) -> Result<()> {
    assert_error_replays_on(Arc::new(InMemoryProvider::new()), make_error).await
}

async fn assert_error_replays_on(
    provider: Arc<dyn StateProvider>,
    make_error: fn() -> Error,
) -> Result<()> {
    let mut engine = DurableEngine::new(provider).await?;
    engine.register(
        "error-replay",
        move |ctx: DurableContext, _: ()| async move {
            let error = ctx
                .step("fail", || async { Err::<(), _>(make_error()) })
                .await
                .unwrap_err();
            assert_eq!(
                error.code(),
                make_error().code(),
                "do not erase classification"
            );
            // All of these observations are available to application code. Naming
            // the next operation after them detects a changed recovery path.
            let branch = format!(
                "{:?}:{}:{}:{}:{}:{}:{}",
                error.code(),
                error,
                error.is_retryable(),
                error.is_tx_conflict(),
                error.is_unique_violation(),
                error.is_foreign_key_violation(),
                std::error::Error::source(&error).is_some(),
            );
            ctx.step(&branch, || async { Ok(()) }).await
        },
    );
    engine
        .start::<_, ()>("error-replay", (), WorkflowOptions::with_id("error-replay"))
        .await?
        .result()
        .await?;
    let report = engine.verify_replay("error-replay").await?;
    assert!(report.passes(), "{report:#?}");
    assert_eq!(report.matched, 2);
    Ok(())
}

#[tokio::test]
async fn timeout_does_not_change_the_recovery_path() -> Result<()> {
    assert_error_replays(|| Error::Timeout).await
}

#[tokio::test]
async fn database_error_does_not_change_the_recovery_path() -> Result<()> {
    assert_error_replays(|| Error::Db(sqlx::Error::PoolTimedOut)).await
}

#[tokio::test]
async fn application_source_does_not_change_the_recovery_path() -> Result<()> {
    assert_error_replays(|| Error::app_source("failed", std::io::Error::other("cause"))).await
}

#[tokio::test]
async fn terminal_error_classification_survives_handle_retrieval() -> Result<()> {
    let cases: [fn() -> Error; 3] = [
        || Error::Timeout,
        || Error::Db(sqlx::Error::PoolTimedOut),
        || Error::app_source("failed", std::io::Error::other("cause")),
    ];
    for make_error in cases {
        let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
        engine.register(
            "terminal-error",
            move |_: DurableContext, _: ()| async move { Err::<(), _>(make_error()) },
        );
        let initial = engine
            .start::<_, ()>(
                "terminal-error",
                (),
                WorkflowOptions::with_id("terminal-error"),
            )
            .await?
            .result()
            .await
            .unwrap_err();
        let retrieved = engine
            .retrieve_workflow::<()>("terminal-error")
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(initial.code(), make_error().code());
        assert_eq!(initial.code(), retrieved.code());
        assert_eq!(initial.to_string(), retrieved.to_string());
        assert_eq!(
            std::mem::discriminant(&initial),
            std::mem::discriminant(&retrieved)
        );
        assert!(std::error::Error::source(&initial).is_none());
        assert!(std::error::Error::source(&retrieved).is_none());
    }
    Ok(())
}

#[tokio::test]
async fn exported_terminal_error_keeps_its_classification() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("export-error", |_: DurableContext, _: ()| async {
        Err::<(), _>(Error::Timeout)
    });
    let initial = engine
        .start::<_, ()>("export-error", (), WorkflowOptions::with_id("export-error"))
        .await?
        .result()
        .await
        .unwrap_err();
    let exported = engine.export_workflow("export-error", false).await?;
    let imported = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    imported.import_workflow(&exported).await?;
    let error = imported
        .retrieve_workflow::<()>("export-error")
        .await?
        .result()
        .await
        .unwrap_err();
    assert_eq!(error.code(), initial.code());
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_errors_replay_under_both_formats() -> Result<()> {
    for serializer in [durare::Serializer::Json, durare::Serializer::Portable] {
        let (url, path) = common::temp_db_url("error-replay");
        let provider = durare::SqliteProvider::connect(&url)
            .await?
            .with_serializer(serializer);
        assert_error_replays_on(Arc::new(provider), || Error::Timeout).await?;
        common::remove_sqlite_files(&path);
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn transaction_failure_keeps_its_classification() -> Result<()> {
    for serializer in [durare::Serializer::Json, durare::Serializer::Portable] {
        let (url, path) = common::temp_db_url("transaction-error-replay");
        let provider = durare::SqliteProvider::connect(&url)
            .await?
            .with_serializer(serializer);
        let mut engine = DurableEngine::new(Arc::new(provider)).await?;
        engine.register(
            "transaction-error",
            |ctx: DurableContext, _: ()| async move {
                let error = ctx
                    .transaction("fail", |_| Box::pin(async { Err::<(), _>(Error::Timeout) }))
                    .await
                    .unwrap_err();
                let next = format!("{:?}", error.code());
                ctx.step(&next, || async { Ok(()) }).await
            },
        );
        engine
            .start::<_, ()>(
                "transaction-error",
                (),
                WorkflowOptions::with_id("transaction-error"),
            )
            .await?
            .result()
            .await?;
        let report = engine.verify_replay("transaction-error").await?;
        assert!(report.passes(), "{report:#?}");
        common::remove_sqlite_files(&path);
    }
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn native_witness_error_survives_a_serializer_change() -> Result<()> {
    use durare::{ErrorCode, Serializer, SqliteDataSource, SqliteProvider};
    use std::sync::atomic::{AtomicUsize, Ordering};

    for (writer, reader) in [
        (Serializer::Json, Serializer::Portable),
        (Serializer::Portable, Serializer::Json),
    ] {
        let (url, path) = common::temp_db_url("witness-error");
        let provider = SqliteProvider::connect(&url).await?.with_serializer(writer);
        let (app_url, app_path) = common::temp_db_url("witness-app");
        let pool = sqlx::SqlitePool::connect(&format!("{app_url}?mode=rwc")).await?;
        let ds = SqliteDataSource::new(pool.clone()).await?;
        let runs = Arc::new(AtomicUsize::new(0));
        let register = |engine: &mut DurableEngine| {
            let (ds, runs) = (ds.clone(), runs.clone());
            engine.register("native-error", move |ctx: DurableContext, _: ()| {
                let (ds, runs) = (ds.clone(), runs.clone());
                async move {
                    ctx.transaction_on(&ds, "fail", async move |_| {
                        runs.fetch_add(1, Ordering::SeqCst);
                        Err::<(), _>(Error::Timeout)
                    })
                    .await
                }
            });
        };
        let mut engine = DurableEngine::new(Arc::new(provider)).await?;
        register(&mut engine);
        let error = engine
            .start::<_, ()>("native-error", (), WorkflowOptions::with_id("source"))
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Timeout);
        sqlx::query("INSERT INTO transaction_completion (workflow_id, step_id, output, error, serialization, created_at) SELECT 'recovered', step_id, output, error, serialization, created_at FROM transaction_completion WHERE workflow_id = 'source'")
            .execute(&pool).await?;

        let provider = SqliteProvider::connect(&url).await?.with_serializer(reader);
        let mut recovered = DurableEngine::new(Arc::new(provider)).await?;
        register(&mut recovered);
        let error = recovered
            .start::<_, ()>("native-error", (), WorkflowOptions::with_id("recovered"))
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), ErrorCode::Timeout);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "the witness must skip the body"
        );
        assert_eq!(
            recovered
                .retrieve_workflow::<()>("recovered")
                .await?
                .result()
                .await
                .unwrap_err()
                .code(),
            ErrorCode::Timeout
        );
        drop((engine, recovered, ds));
        pool.close().await;
        common::remove_sqlite_files(&path);
        common::remove_sqlite_files(&app_path);
    }
    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_step_transaction_and_handle_errors_are_stable() -> Result<()> {
    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping postgres error replay: DATABASE_URL unset");
        return Ok(());
    };
    for serializer in [durare::Serializer::Json, durare::Serializer::Portable] {
        let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_errors").await;
        let provider = Arc::new(
            durare::PostgresProvider::connect(&url)
                .await?
                .with_serializer(serializer),
        );
        assert_error_replays_on(provider.clone(), || Error::Timeout).await?;
        let ds = provider.system_datasource();
        let mut engine = DurableEngine::new(provider.clone()).await?;
        engine.register("transactions", move |ctx: DurableContext, _: ()| {
            let ds = ds.clone();
            async move {
                let error = ctx
                    .transaction("portable-tx", |_| {
                        Box::pin(async { Err::<(), _>(Error::Timeout) })
                    })
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), durare::ErrorCode::Timeout);
                let error = ctx
                    .transaction_on(&ds, "native-tx", async |_| Err::<(), _>(Error::Timeout))
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), durare::ErrorCode::Timeout);
                ctx.step("after", || async { Ok(()) }).await?;
                Err::<(), _>(Error::Timeout)
            }
        });
        let error = engine
            .start::<_, ()>("transactions", (), WorkflowOptions::with_id("transactions"))
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), durare::ErrorCode::Timeout);
        let retrieved = engine
            .retrieve_workflow::<()>("transactions")
            .await?
            .result()
            .await
            .unwrap_err();
        assert_eq!(error.code(), retrieved.code());
        let report = engine.verify_replay("transactions").await?;
        assert!(report.passes(), "{report:#?}");
        assert_eq!(
            report.matched, 3,
            "both transactions and the following step must replay"
        );
        drop((engine, provider));
        common::drop_hermetic_pg_db(&admin, &dbname).await;
    }
    Ok(())
}
