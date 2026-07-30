//! Searchable custom workflow attributes: set at creation, replaced later,
//! filtered by containment. The cross-SDK semantics — replace-not-merge,
//! no child inheritance, containment filtering on Postgres only — with the
//! in-memory backend emulating containment for tests.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, ListFilter, Result, WorkflowOptions,
};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Duration;

mod common;

fn attrs(v: Value) -> Map<String, Value> {
    v.as_object().expect("object literal").clone()
}

/// Attributes attached at start are stored, and containment filtering matches
/// exactly the workflows whose attributes contain all given pairs — including
/// nested values — while attribute-less workflows never match.
#[tokio::test]
async fn attributes_set_at_start_and_filtered_by_containment() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    for (id, a) in [
        (
            "acme-eu",
            json!({"customer": "acme", "region": {"zone": "eu"}}),
        ),
        (
            "acme-us",
            json!({"customer": "acme", "region": {"zone": "us"}}),
        ),
        ("globex", json!({"customer": "globex"})),
    ] {
        engine
            .start::<(), ()>(
                "noop",
                (),
                WorkflowOptions::with_id(id).attributes(attrs(a)),
            )
            .await?
            .await?;
    }
    // And one with no attributes at all.
    engine
        .start::<(), ()>("noop", (), WorkflowOptions::with_id("bare"))
        .await?
        .await?;

    let acme = engine
        .list_workflows(&ListFilter {
            attributes: Some(attrs(json!({"customer": "acme"}))),
            ..Default::default()
        })
        .await?;
    let mut ids: Vec<&str> = acme.iter().map(|w| w.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["acme-eu", "acme-us"]);

    // Nested containment: the filter value is an exact sub-object.
    let eu = engine
        .list_workflows(&ListFilter {
            attributes: Some(attrs(json!({"region": {"zone": "eu"}}))),
            ..Default::default()
        })
        .await?;
    assert_eq!(eu.len(), 1);
    assert_eq!(eu[0].id, "acme-eu");
    assert_eq!(
        eu[0].attributes.as_ref().unwrap()["customer"],
        json!("acme"),
        "attributes read back on the listed row"
    );

    // No workflow contains this pair.
    let none = engine
        .list_workflows(&ListFilter {
            attributes: Some(attrs(json!({"customer": "initech"}))),
            ..Default::default()
        })
        .await?;
    assert!(none.is_empty());

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// set_workflow_attributes replaces the whole set (never merges), clears on
/// `None`, and errors on a missing workflow.
#[tokio::test]
async fn attributes_replace_clear_and_missing_id() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    engine
        .start::<(), ()>(
            "noop",
            (),
            WorkflowOptions::with_id("subject").attributes(attrs(json!({"a": 1, "b": 2}))),
        )
        .await?
        .await?;

    // Replace: `b` is gone, not merged.
    engine
        .set_workflow_attributes("subject", Some(attrs(json!({"a": 9}))))
        .await?;
    let w = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec!["subject".into()],
            ..Default::default()
        })
        .await?;
    assert_eq!(w[0].attributes, Some(json!({"a": 9})));

    // Clear.
    engine.set_workflow_attributes("subject", None).await?;
    let w = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec!["subject".into()],
            ..Default::default()
        })
        .await?;
    assert_eq!(w[0].attributes, None);

    let err = engine
        .set_workflow_attributes("no-such", Some(attrs(json!({"x": 1}))))
        .await
        .expect_err("missing workflow");
    assert!(matches!(err, Error::NonExistentWorkflow(_)), "{err}");

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// A child workflow does not inherit its parent's attributes (the cross-SDK
/// rule) — only an explicit `attributes` on the child's options attaches any.
#[tokio::test]
async fn attributes_are_not_inherited_by_children() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("child", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.register("parent", |ctx: DurableContext, (): ()| async move {
        ctx.start_workflow::<(), ()>("child", (), WorkflowOptions::with_id("attr-child"))
            .await?
            .await?;
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    engine
        .start::<(), ()>(
            "parent",
            (),
            WorkflowOptions::with_id("attr-parent").attributes(attrs(json!({"team": "iris"}))),
        )
        .await?
        .await?;

    let rows = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec!["attr-parent".into(), "attr-child".into()],
            ..Default::default()
        })
        .await?;
    for w in rows {
        match w.id.as_str() {
            "attr-parent" => assert!(w.attributes.is_some()),
            "attr-child" => assert!(w.attributes.is_none(), "no inheritance"),
            other => panic!("unexpected row {other}"),
        }
    }

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// From workflow code the replacement is one durable step with the cross-SDK
/// name, so recovery replays it instead of re-running it.
#[tokio::test]
async fn ctx_set_attributes_records_one_step() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("tagger", |ctx: DurableContext, (): ()| async move {
        let id = ctx.workflow_id().to_string();
        ctx.set_workflow_attributes(
            &id,
            Some(
                serde_json::json!({"phase": "done"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await?;
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    engine
        .start::<(), ()>("tagger", (), WorkflowOptions::with_id("self-tag"))
        .await?
        .await?;

    let w = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec!["self-tag".into()],
            ..Default::default()
        })
        .await?;
    assert_eq!(w[0].attributes, Some(json!({"phase": "done"})));

    let steps = engine.get_workflow_steps("self-tag").await?;
    let recorded: Vec<_> = steps
        .iter()
        .filter(|s| s.name == "DBOS.updateWorkflowAttributes")
        .collect();
    assert_eq!(recorded.len(), 1, "one step for the replacement: {steps:?}");

    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}

/// SQLite stores and reads attributes but refuses containment filtering — the
/// reference behavior (filtering requires Postgres).
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_stores_attributes_but_rejects_the_filter() -> Result<()> {
    use durare::SqliteProvider;

    let mut path = std::env::temp_dir();
    path.push(format!("durare-attrs-{}.db", uuid::Uuid::new_v4()));
    let url = format!("sqlite://{}", path.display());

    let mut engine = DurableEngine::new(Arc::new(SqliteProvider::connect(&url).await?)).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    engine
        .start::<(), ()>(
            "noop",
            (),
            WorkflowOptions::with_id("lite").attributes(attrs(json!({"k": "v"}))),
        )
        .await?
        .await?;

    // Storage and read-back work.
    let w = engine
        .list_workflows(&ListFilter {
            workflow_ids: vec!["lite".into()],
            ..Default::default()
        })
        .await?;
    assert_eq!(w[0].attributes, Some(json!({"k": "v"})));
    engine
        .set_workflow_attributes("lite", Some(attrs(json!({"k": "w"}))))
        .await?;

    // Filtering is Postgres-only.
    let err = engine
        .list_workflows(&ListFilter {
            attributes: Some(attrs(json!({"k": "w"}))),
            ..Default::default()
        })
        .await
        .expect_err("attribute filter on sqlite");
    assert!(err.to_string().contains("not supported on SQLite"), "{err}");

    engine.shutdown(Duration::from_secs(2)).await?;
    let _ = std::fs::remove_file(&path);
    Ok(())
}

/// The real thing on Postgres: JSONB `@>` containment through the GIN index,
/// replace/clear, and the missing-id error.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_attributes_containment_end_to_end() -> Result<()> {
    use durare::PostgresProvider;

    let Some(base) = std::env::var("DATABASE_URL").ok().filter(|s| !s.is_empty()) else {
        eprintln!("skipping pg_attributes_containment_end_to_end: DATABASE_URL unset");
        return Ok(());
    };
    let (admin, url, dbname) = common::hermetic_pg_db(&base, "durare_attrs").await;

    let mut engine = DurableEngine::new(Arc::new(PostgresProvider::connect(&url).await?)).await?;
    engine.register("noop", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    for (id, a) in [
        ("pg-acme", json!({"customer": "acme", "tier": {"level": 3}})),
        ("pg-globex", json!({"customer": "globex"})),
    ] {
        engine
            .start::<(), ()>(
                "noop",
                (),
                WorkflowOptions::with_id(id).attributes(attrs(a)),
            )
            .await?
            .await?;
    }

    let hit = engine
        .list_workflows(&ListFilter {
            attributes: Some(attrs(json!({"tier": {"level": 3}}))),
            ..Default::default()
        })
        .await?;
    assert_eq!(hit.len(), 1);
    assert_eq!(hit[0].id, "pg-acme");
    assert_eq!(hit[0].attributes.as_ref().unwrap()["customer"], "acme");

    engine
        .set_workflow_attributes("pg-acme", Some(attrs(json!({"customer": "acme"}))))
        .await?;
    let miss = engine
        .list_workflows(&ListFilter {
            attributes: Some(attrs(json!({"tier": {"level": 3}}))),
            ..Default::default()
        })
        .await?;
    assert!(miss.is_empty(), "replaced attributes dropped the tier");

    engine.set_workflow_attributes("pg-globex", None).await?;
    let err = engine
        .set_workflow_attributes("pg-none", Some(attrs(json!({"x": 1}))))
        .await
        .expect_err("missing workflow");
    assert!(matches!(err, Error::NonExistentWorkflow(_)), "{err}");

    engine.shutdown(Duration::from_secs(2)).await?;
    drop(engine);
    common::drop_hermetic_pg_db(&admin, &dbname).await;
    Ok(())
}
