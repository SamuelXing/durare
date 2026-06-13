//! Postgres backend tests. Skipped unless `DATABASE_URL` points at a reachable
//! Postgres instance (ideally an empty database — `init` runs the migrations).
//!
//!   createdb durust_test && DATABASE_URL=postgres://localhost/durust_test cargo test --test postgres

use durust::{
    DurableContext, DurableEngine, Error, PostgresProvider, Result, Serializer, WorkflowOptions,
};
use std::sync::Arc;

fn database_url() -> Option<String> {
    std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty())
}

async fn engine_with(url: &str, fmt: Serializer) -> Result<DurableEngine> {
    let provider = PostgresProvider::connect(url).await?.with_serializer(fmt);
    let mut engine = DurableEngine::new(Arc::new(provider)).await?;
    engine.register("greet", |ctx: DurableContext, name: String| async move {
        let msg = ctx
            .step("build", || async { Ok::<_, Error>(format!("hi {name}")) })
            .await?;
        Ok::<_, Error>(msg)
    });
    Ok(engine)
}

/// Round-trip a workflow's input/step-output/result through Postgres, and prove
/// a provider in a different serialization format still decodes them.
#[tokio::test]
async fn pg_serialization_cross_format() -> Result<()> {
    let Some(url) = database_url() else {
        eprintln!("skipping pg_serialization_cross_format: DATABASE_URL unset");
        return Ok(());
    };
    let id = format!("wf-ser-{}", uuid::Uuid::new_v4());

    {
        let engine = engine_with(&url, Serializer::Portable).await?;
        let out: String = engine
            .run_workflow::<_, String>("greet", "ada".to_string(), WorkflowOptions::with_id(&id))
            .await?
            .get_result()
            .await?;
        assert_eq!(out, "hi ada");
    }
    {
        let engine = engine_with(&url, Serializer::Json).await?;
        let mut handle = engine.retrieve_workflow::<String>(&id).await?;
        let status = handle.get_status().await?;
        assert_eq!(status.input, serde_json::json!("ada"));
        assert_eq!(status.output, Some(serde_json::json!("hi ada")));
        assert_eq!(handle.get_result().await?, "hi ada");
    }
    Ok(())
}
