//! Step rows carry the timing the reference SDKs record: a child's start and
//! a sleep's wake instant as its completion.
use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
};
use std::sync::Arc;
use std::time::Duration;
mod common;

const SLEEP: Duration = Duration::from_millis(300);

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    let mut engine = DurableEngine::new(db).await?;
    engine.register("child", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        ctx.sleep(SLEEP).await?;
        ctx.step("work", || async { Ok::<_, Error>(1_i64) }).await?;
        ctx.start_workflow::<_, ()>("child", (), WorkflowOptions::default())
            .await?
            .result()
            .await?;
        Ok::<_, Error>(())
    });
    engine
        .start::<_, ()>("parent", (), WorkflowOptions::with_id("p"))
        .await?
        .result()
        .await?;

    let steps = engine.get_workflow_steps("p").await?;
    let names: Vec<&str> = steps.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, vec!["DBOS.sleep", "work", "child"]);

    // The sleep's row spans the sleep, from its start to the wake instant.
    let sleep = &steps[0];
    let (started, completed) = (
        sleep.started_at.expect("sleep start"),
        sleep.completed_at.expect("sleep end"),
    );
    let slept = (completed - started).to_std().unwrap_or_default();
    assert!(
        slept >= SLEEP - Duration::from_millis(5) && slept < SLEEP + Duration::from_secs(1),
        "{slept:?}"
    );
    // A child's checkpoint records when it was started.
    assert!(steps[2].started_at.is_some(), "child start");
    Ok(())
}

#[tokio::test]
async fn step_timing_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn step_timing_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn step_timing_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "step_timing").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
