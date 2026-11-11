//! A run a schedule created records that schedule's name, on every path the
//! scheduler creates runs by and on no other, and the list filter finds it.
use durare::{
    Client, DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result,
    ScheduleOptions, ScheduledInput, StateProvider, WorkflowHandle, WorkflowOptions,
};
use std::collections::BTreeSet;
use std::sync::Arc;
mod common;

/// The ids of the schedule's runs.
async fn by_schedule(engine: &DurableEngine, name: &str) -> Result<BTreeSet<String>> {
    let rows = engine
        .list_workflows(&ListFilter {
            schedule_name: vec![name.to_string()],
            ..Default::default()
        })
        .await?;
    Ok(rows.into_iter().map(|r| r.id).collect())
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    use chrono::{TimeZone, Utc};
    let mut engine = DurableEngine::new(db.clone()).await?;
    engine.register(
        "report",
        |_ctx: DurableContext, _: ScheduledInput| async move { Ok::<_, Error>(()) },
    );
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    // A cron far in the future: only triggers and backfills create runs.
    engine
        .create_schedule("yearly", "report", "0 0 0 1 1 *", ScheduleOptions::new())
        .await?;

    // Triggered through the engine and through a client.
    let triggered: WorkflowHandle<()> = engine.trigger_schedule("yearly").await?;
    triggered.result().await?;
    let client = Client::new(db.clone());
    let by_client: WorkflowHandle<()> = client.trigger_schedule("yearly").await?;
    by_client.result().await?;

    // Backfilled through the engine and through a client: one past New Year
    // each (both window bounds are exclusive).
    let day = |y, m, d| Utc.with_ymd_and_hms(y, m, d, 0, 0, 0).unwrap();
    let backfilled = engine
        .backfill_schedule("yearly", day(2023, 12, 31), day(2024, 1, 2))
        .await?;
    let by_client_backfill = client
        .backfill_schedule("yearly", day(2024, 12, 31), day(2025, 1, 2))
        .await?;
    assert_eq!((backfilled.len(), by_client_backfill.len()), (1, 1));

    // A plain run of the same workflow, and a fork of a scheduled run, are not
    // the schedule's: neither Go nor Python stamps those.
    let plain = engine
        .start::<_, ()>("noop", (), WorkflowOptions::default())
        .await?;
    plain.result().await?;
    let fork: WorkflowHandle<()> = engine
        .fork_workflow(triggered.id(), 0, WorkflowOptions::default())
        .await?;
    fork.result().await?;
    assert_eq!(
        triggered.get_status().await?.schedule_name.as_deref(),
        Some("yearly")
    );
    assert_eq!(
        plain.get_status().await?.schedule_name,
        None,
        "a plain start records no schedule"
    );
    assert_eq!(
        fork.get_status().await?.schedule_name,
        None,
        "a fork records no schedule"
    );

    let expected: BTreeSet<String> = [
        triggered.id(),
        by_client.id(),
        backfilled[0].as_str(),
        by_client_backfill[0].as_str(),
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    assert_eq!(
        by_schedule(&engine, "yearly").await?,
        expected,
        "every scheduler-created run, nothing else"
    );
    assert!(by_schedule(&engine, "other").await?.is_empty());

    // The column survives the portable export.
    let exported = engine.export_workflow(triggered.id(), false).await?;
    assert_eq!(
        exported[0].workflow_status.get("schedule_name"),
        Some(&serde_json::json!("yearly"))
    );
    engine.shutdown(std::time::Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn schedule_name_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn schedule_name_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn schedule_name_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "schedule_name").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
