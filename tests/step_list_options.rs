//! Step listing windows count rows, not step ids, and metadata reads skip decoding.
use durare::{
    Client, DurableEngine, InMemoryProvider, Result, StateProvider, StepListOptions,
    WorkflowStatus, STATUS_SUCCESS,
};
use serde_json::json;
use std::sync::Arc;
mod common;

async fn contract(provider: Arc<dyn StateProvider>) -> Result<()> {
    let engine = DurableEngine::new(provider.clone()).await?;
    let client = Client::new(provider.clone());
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "steps",
            "work",
            json!(null),
            STATUS_SUCCESS,
            "owner",
            "v",
        ))
        .await?;
    // Insert out of order with gaps. Offset means rows, never function_id.
    for id in [20, 2, 7] {
        provider
            .record_step_result(
                "steps",
                id,
                &format!("s{id}"),
                json!(id),
                None,
                Some(1000 + i64::from(id)),
                None,
            )
            .await?;
    }
    provider
        .record_step_result(
            "steps",
            30,
            "failure",
            json!(null),
            Some("business failure"),
            Some(1030),
            None,
        )
        .await?;
    provider
        .record_child_workflow("steps", 40, "child", "child-id")
        .await?;
    let all = engine.get_workflow_steps("steps").await?;
    assert_eq!(
        all.iter().map(|s| s.step_id).collect::<Vec<_>>(),
        [2, 7, 20, 30, 40]
    );
    assert_eq!(all[0].output, Some(json!(2)));
    assert_eq!(client.get_workflow_steps("steps").await?.len(), 5);
    for (limit, offset, expected) in [
        (None, None, vec![2, 7, 20, 30, 40]),
        (Some(2), Some(1), vec![7, 20]),
        (None, Some(3), vec![30, 40]),
        (Some(0), None, vec![]),
        (Some(-1), Some(-10), vec![2, 7, 20, 30, 40]),
        (Some(100), Some(4), vec![40]),
        (None, Some(i64::MAX), vec![]),
        (Some(i64::MAX), None, vec![2, 7, 20, 30, 40]),
    ] {
        for load_output in [false, true] {
            let options = StepListOptions {
                limit,
                offset,
                load_output,
            };
            let steps = engine
                .get_workflow_steps_with_options("steps", options)
                .await?;
            let via_client = client
                .get_workflow_steps_with_options("steps", options)
                .await?;
            assert_eq!(
                steps.iter().map(|s| s.step_id).collect::<Vec<_>>(),
                expected
            );
            assert_eq!(
                via_client.iter().map(|s| s.step_id).collect::<Vec<_>>(),
                expected
            );
            for step in steps {
                let original = all.iter().find(|s| s.step_id == step.step_id).unwrap();
                assert_eq!(
                    step.output,
                    if load_output {
                        original.output.clone()
                    } else {
                        None
                    }
                );
                assert_eq!(step.error, original.error);
                assert_eq!(step.child_workflow_id, original.child_workflow_id);
                assert_eq!(step.started_at, original.started_at);
                assert_eq!(step.completed_at, original.completed_at);
                assert_eq!(step.name, original.name);
            }
        }
    }
    assert!(client
        .get_workflow_steps_with_options("missing", StepListOptions::default())
        .await?
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn memory_listing() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(any(feature = "sqlite", feature = "postgres"))]
async fn undecodable_output(provider: &dyn StateProvider) -> Result<()> {
    // The SQL tests replace the last output with an unsupported foreign format.
    assert!(provider.get_workflow_steps("steps").await.is_err());
    let page = provider
        .get_workflow_steps_with_options(
            "steps",
            StepListOptions {
                limit: Some(1),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(page[0].output, Some(json!(2)));
    let metadata = provider
        .get_workflow_steps_with_options(
            "steps",
            StepListOptions {
                load_output: false,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(metadata.len(), 5);
    assert!(metadata.iter().all(|s| s.output.is_none()));
    assert_eq!(metadata[3].error.as_deref(), Some("business failure"));
    assert_eq!(metadata[4].child_workflow_id.as_deref(), Some("child-id"));
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_listing() -> Result<()> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let provider = Arc::new(durare::SqliteProvider::from_pool(pool.clone()));
    contract(provider.clone()).await?;
    sqlx::query("UPDATE operation_outputs SET output = 'foreign-payload', serialization = 'unknown-codec' WHERE function_id = 40")
        .execute(&pool).await?;
    undecodable_output(provider.as_ref()).await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_listing() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "step_listing").await;
    let result = async {
        let provider = Arc::new(durare::PostgresProvider::connect(&url).await?);
        contract(provider.clone()).await?;
        sqlx::query("UPDATE dbos.operation_outputs SET output = 'foreign-payload', serialization = 'unknown-codec' WHERE function_id = 40")
            .execute(provider.pool()).await?;
        undecodable_output(provider.as_ref()).await
    }.await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}
