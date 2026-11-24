//! A run's input and outcome are read from the payload tables the reference
//! SDKs write since shared migration 109, falling back to the
//! `workflow_status` columns, and are deleted with the run. The in-memory
//! backend shares no database with a peer.
use durare::{
    DebounceOutcome, DebounceRequest, DequeueRequest, ForkParams, ListFilter, Result, StateProvider,
};
use serde_json::json;
mod common;

/// `queued`, `done` and `failed` as a current peer writes them, payloads in
/// the payload tables only; `legacy` with its payload in the status columns.
/// `bounced` is a peer's debounced run, and `resumed` a run a peer failed that
/// is running again.
const SEED: &str = r#"
    INSERT INTO workflow_status (workflow_uuid, name, status, queue_name, created_at, updated_at, completed_at, serialization)
    VALUES ('queued', 'wf', 'ENQUEUED', 'q', 1, 1, NULL, 'portable_json'),
           ('done', 'wf', 'SUCCESS', NULL, 1, 1, 1, 'portable_json'),
           ('failed', 'wf', 'ERROR', NULL, 1, 1, 1, 'portable_json'),
           ('legacy', 'wf', 'SUCCESS', NULL, 1, 1, 1, 'portable_json'),
           ('resumed', 'wf', 'PENDING', NULL, 1, 1, NULL, NULL);
    INSERT INTO workflow_status (workflow_uuid, name, status, queue_name, deduplication_id,
        is_debounced, delay_until_epoch_ms, created_at, updated_at, serialization)
    VALUES ('bounced', 'wf', 'DELAYED', 'debounce', 'key', TRUE, 5, 1, 1, 'portable_json');
    UPDATE workflow_status SET inputs = '{"positionalArgs":[7],"namedArgs":{}}', output = '"old"'
    WHERE workflow_uuid = 'legacy';
    INSERT INTO workflow_input (workflow_uuid, inputs)
    VALUES ('queued', '{"positionalArgs":[42],"namedArgs":{}}'),
           ('done', '{"positionalArgs":[42],"namedArgs":{}}'),
           ('failed', '{"positionalArgs":[42],"namedArgs":{}}'),
           ('bounced', '{"positionalArgs":[1],"namedArgs":{}}');
    INSERT INTO workflow_output (workflow_uuid, output, error)
    VALUES ('done', '"ok"', NULL), ('failed', NULL, 'boom'), ('resumed', NULL, 'stale');"#;

/// `payload_rows` lists the ids that have a row in either payload table.
async fn contract<L, LF>(db: &dyn StateProvider, payload_rows: L) -> Result<()>
where
    L: Fn() -> LF,
    LF: std::future::Future<Output = Vec<String>>,
{
    let status = |id: &'static str| async move { db.get_workflow_status(id).await };

    let done = status("done").await?.expect("done");
    assert_eq!(done.input, json!(42));
    assert_eq!(done.output, Some(json!("ok")));
    let failed = status("failed").await?.expect("failed");
    assert!(failed.error.is_some_and(|e| e.contains("boom")));
    let legacy = status("legacy").await?.expect("legacy");
    assert_eq!(
        (legacy.input, legacy.output),
        (json!(7), Some(json!("old")))
    );

    let listed = db.list_workflows(&ListFilter::default()).await?;
    let listed_done = listed.iter().find(|w| w.id == "done").expect("listed");
    assert_eq!(listed_done.input, json!(42));
    assert_eq!(listed_done.output, Some(json!("ok")));

    let claimed = db
        .dequeue_workflows(&DequeueRequest {
            queue_name: "q".into(),
            executor_id: "e".into(),
            app_version: String::new(),
            partition_key: None,
            max_tasks: 10,
            global_concurrency: None,
            rate_limit_max: None,
            rate_limit_period_ms: None,
        })
        .await?;
    assert_eq!(claimed.len(), 1);
    assert_eq!(
        claimed[0].input,
        json!(42),
        "a claimed run carries its input"
    );

    let exported = db.export_workflow("done", false).await?;
    let row = &exported[0].workflow_status;
    assert_eq!(row["output"], json!("\"ok\""));
    assert!(row["inputs"].is_string());

    // A write durare makes is the value read back, payload row or not.
    let bounce = db
        .debounce_delayed_workflow(&DebounceRequest {
            workflow_name: "wf",
            class_name: None,
            config_name: None,
            queue_name: "debounce",
            dedup_id: "key",
            delay_until_ms: 9,
            input: &json!(2),
        })
        .await?;
    assert_eq!(bounce, DebounceOutcome::Bounced("bounced".into()));
    assert_eq!(status("bounced").await?.expect("bounced").input, json!(2));
    assert!(
        db.set_workflow_status("resumed", "SUCCESS", Some(&json!("new")), None)
            .await?
    );
    let resumed = status("resumed").await?.expect("resumed");
    assert_eq!((resumed.output, resumed.error), (Some(json!("new")), None));
    db.delete_workflows(&["bounced".to_owned(), "resumed".to_owned()], false)
        .await?;

    db.fork_workflow(&ForkParams {
        original_id: "done".into(),
        new_id: "fork".into(),
        start_step: 0,
        app_version: None,
        queue_name: "forks".into(),
        partition_key: None,
    })
    .await?;
    let fork = status("fork").await?.expect("fork");
    assert_eq!(fork.input, json!(42), "a fork carries the original's input");
    db.delete_workflows(&["fork".to_owned()], false).await?;

    db.delete_workflows(&["done".to_owned()], false).await?;
    assert_eq!(payload_rows().await, ["failed", "queued"]);
    assert_eq!(db.garbage_collect(Some(2), None).await?, 2);
    assert_eq!(
        payload_rows().await,
        ["queued"],
        "the live run keeps its input"
    );
    Ok(())
}

const PAYLOAD_ROWS: &str = "SELECT workflow_uuid FROM workflow_input
     UNION SELECT workflow_uuid FROM workflow_output ORDER BY 1";

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_reads_and_deletes_payload_rows() -> Result<()> {
    use durare::PostgresProvider;
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "payload_tables").await;
    let result = async {
        let db = PostgresProvider::connect(&url).await?;
        db.init().await?;
        let pool = common::pg_raw_pool(&url).await;
        sqlx::raw_sql(SEED).execute(&pool).await?;
        let payload_rows = || async {
            sqlx::query_scalar(PAYLOAD_ROWS)
                .fetch_all(&pool)
                .await
                .unwrap()
        };
        contract(&db, payload_rows).await?;
        pool.close().await;
        Ok(())
    }
    .await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_reads_and_deletes_payload_rows() -> Result<()> {
    use durare::SqliteProvider;
    let (url, path) = common::temp_db_url("payload_tables");
    let db = SqliteProvider::connect(&url).await?;
    db.init().await?;
    let pool = sqlx::SqlitePool::connect(&url).await?;
    sqlx::raw_sql(SEED).execute(&pool).await?;
    let payload_rows = || async {
        sqlx::query_scalar(PAYLOAD_ROWS)
            .fetch_all(&pool)
            .await
            .unwrap()
    };
    contract(&db, payload_rows).await?;
    pool.close().await;
    common::remove_sqlite_files(&path);
    Ok(())
}
