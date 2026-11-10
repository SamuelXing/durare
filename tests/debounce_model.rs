//! A debounce is a `DELAYED` run holding its key as its deduplication id, the
//! rows the Go and Python SDKs write, so the two sides coalesce and the
//! collisions they refuse are refused here.
use durare::{
    Client, DurableContext, DurableEngine, Error, ErrorCode, InMemoryProvider, Result,
    StateProvider, WorkflowHandle, WorkflowOptions, WorkflowStatus, STATUS_DELAYED,
};
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
mod common;

const INTERNAL_QUEUE: &str = "_dbos_internal_queue";

static RUNS: AtomicUsize = AtomicUsize::new(0);

async fn engine(db: Arc<dyn StateProvider>) -> Result<DurableEngine> {
    let mut engine = DurableEngine::new(db).await?;
    engine.register("notify", |_ctx: DurableContext, msg: String| async move {
        RUNS.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(msg)
    });
    engine.register("a", |_ctx: DurableContext, msg: String| async move {
        Ok::<_, Error>(msg)
    });
    engine.register("a-b", |_ctx: DurableContext, msg: String| async move {
        Ok::<_, Error>(msg)
    });
    engine.launch().await?;
    Ok(engine)
}

fn code(result: Result<WorkflowHandle<String>>) -> Option<ErrorCode> {
    result.err().map(|e| e.code())
}

async fn contract(db: Arc<dyn StateProvider>) -> Result<()> {
    let engine = engine(db.clone()).await?;
    let long = Duration::from_secs(600);

    // The row a debounce creates: DELAYED on the internal queue, holding
    // `{workflow}-{key}`, marked, uncapped without a timeout.
    let h: WorkflowHandle<String> = engine
        .debouncer("notify")
        .debounce("k", long, "a".to_string())
        .await?;
    let row = h.get_status().await?;
    assert_eq!(row.status, STATUS_DELAYED);
    assert_eq!(row.queue_name.as_deref(), Some(INTERNAL_QUEUE));
    assert_eq!(row.dedup_id.as_deref(), Some("notify-k"));
    assert!(row.is_debounced);
    assert_eq!(row.debounce_deadline_ms, None);
    let first_wake = row.delay_until_ms.unwrap();

    // A bounce pushes the wake out and replaces the input in place.
    tokio::time::sleep(Duration::from_millis(20)).await;
    let again: WorkflowHandle<String> = engine
        .debouncer("notify")
        .debounce("k", long, "b".to_string())
        .await?;
    assert_eq!(again.id(), h.id());
    let bounced = h.get_status().await?;
    assert!(bounced.delay_until_ms.unwrap() > first_wake);
    assert_eq!(bounced.input, json!("b"));

    // A timeout caps both the first wait and every push-back.
    let capped: WorkflowHandle<String> = engine
        .debouncer("notify")
        .timeout(Duration::from_millis(300))
        .debounce("capped", long, "x".to_string())
        .await?;
    let row = capped.get_status().await?;
    let deadline = row.debounce_deadline_ms.expect("capped");
    // The first wait is the cap, not `long`; its instant is read a few
    // milliseconds after the deadline's, as the reference SDKs read it.
    let first_wait = row.delay_until_ms.unwrap();
    assert!(
        (first_wait - deadline).abs() < 100,
        "first wait ends at the cap: {first_wait} vs {deadline}"
    );
    engine
        .debouncer("notify")
        .timeout(Duration::from_millis(300))
        .debounce::<_, String>("capped", long, "y".to_string())
        .await?;
    assert_eq!(capped.get_status().await?.delay_until_ms, Some(deadline));
    assert_eq!(
        capped.result().await?,
        "y",
        "runs at the cap with the latest input"
    );

    // Collisions. A plain enqueue holding the key is a deduplication
    // conflict, not something to bounce.
    engine
        .start::<_, String>(
            "notify",
            "plain".to_string(),
            WorkflowOptions {
                queue: Some(INTERNAL_QUEUE.into()),
                dedup_id: Some("notify-held".into()),
                delay: Some(long),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        code(
            engine
                .debouncer("notify")
                .debounce("held", long, "z".to_string())
                .await
        ),
        Some(ErrorCode::QueueDeduplicated)
    );
    // Debounces of different workflows whose keys collide never share a run.
    engine
        .debouncer("a")
        .debounce::<_, String>("b-c", long, "one".to_string())
        .await?;
    assert_eq!(
        code(
            engine
                .debouncer("a-b")
                .debounce("c", long, "two".to_string())
                .await
        ),
        Some(ErrorCode::QueueDeduplicated)
    );

    // An option the debounce owns is refused before anything is written (the
    // four refused options are enumerated in the module's unit test).
    assert!(engine
        .debouncer("notify")
        .options(WorkflowOptions {
            delay: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .debounce::<_, String>("opt", long, "x".to_string())
        .await
        .is_err());
    assert!(db
        .get_deduplicated_workflow(INTERNAL_QUEUE, "notify-opt")
        .await?
        .is_none());

    // An unregistered target is refused by the engine; a client cannot know.
    assert_eq!(
        code(
            engine
                .debouncer("nowhere")
                .debounce("k", long, "x".to_string())
                .await
        ),
        Some(ErrorCode::WorkflowNotRegistered)
    );

    // A peer's debounced row (as Go or Python write it) is what a durare
    // client bounces: same id back, input replaced.
    let mut peer = WorkflowStatus::new("peer-run", "notify", json!("peer"), STATUS_DELAYED, "", "");
    peer.queue_name = Some(INTERNAL_QUEUE.into());
    peer.dedup_id = Some("notify-shared".into());
    peer.delay_until_ms = Some(chrono::Utc::now().timestamp_millis() + 600_000);
    peer.is_debounced = true;
    db.insert_workflow_status(peer).await?;
    let client = Client::new(db.clone());
    let joined: WorkflowHandle<String> = client
        .debouncer("notify")
        .debounce("shared", long, "from-durare".to_string())
        .await?;
    assert_eq!(joined.id(), "peer-run");
    assert_eq!(joined.get_status().await?.input, json!("from-durare"));

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn debounce_model_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn debounce_model_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn debounce_model_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "debounce_model").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
