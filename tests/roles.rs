//! Declarative role-based authorization: `require_roles` declarations are
//! enforced before a workflow body runs, on every execution path. The first
//! required role the caller holds becomes the run's assumed role; a denial is
//! terminal — the row is finalized `ERROR`, never left to be redequeued.

use durare::{
    DurableContext, DurableEngine, Error, ErrorCode, InMemoryProvider, ListFilter, Result,
    WorkflowOptions, WorkflowQueue,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

async fn engine_with_admin_wf() -> Result<DurableEngine> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("delete-tenant", |ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(ctx.assumed_role().unwrap_or_default().to_string())
    });
    engine.require_roles("delete-tenant", ["admin", "operator"]);
    Ok(engine)
}

/// A caller holding one of the required roles runs, and the first matching
/// required role becomes the run's assumed role.
#[tokio::test]
async fn matching_role_runs_and_is_assumed() -> Result<()> {
    let engine = engine_with_admin_wf().await?;
    engine.launch().await?;

    let assumed: String = engine
        .start::<(), String>(
            "delete-tenant",
            (),
            WorkflowOptions::with_id("authz-ok")
                .authenticated_user("alice")
                .authenticated_roles(["viewer", "operator"]),
        )
        .await?
        .await?;
    // "admin" is required first but alice doesn't hold it; "operator" matches.
    assert_eq!(assumed, "operator");
    Ok(())
}

/// No authentication information at all: denied before the body, row ERROR.
#[tokio::test]
async fn missing_auth_is_denied_terminally() -> Result<()> {
    let runs = Arc::new(AtomicU32::new(0));
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let counter = runs.clone();
    engine.register("delete-tenant", move |_ctx: DurableContext, (): ()| {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(String::new())
        }
    });
    engine.require_roles("delete-tenant", ["admin", "operator"]);
    engine.launch().await?;

    let err = engine
        .start::<(), String>("delete-tenant", (), WorkflowOptions::with_id("authz-none"))
        .await?
        .await
        .expect_err("no auth info");
    assert_eq!(err.code(), ErrorCode::NotAuthorized, "{err}");
    assert_eq!(runs.load(Ordering::SeqCst), 0, "body never ran");

    let row = &engine
        .list_workflows(&ListFilter {
            workflow_ids: vec!["authz-none".into()],
            ..Default::default()
        })
        .await?[0];
    assert_eq!(row.status, "ERROR", "denial is finalized, not left pending");
    assert!(
        row.error
            .as_deref()
            .unwrap_or_default()
            .contains("requires a role"),
        "recorded: {:?}",
        row.error
    );
    Ok(())
}

/// Roles present but none match: denied with the has-roles message.
#[tokio::test]
async fn wrong_roles_are_denied() -> Result<()> {
    let engine = engine_with_admin_wf().await?;
    engine.launch().await?;

    let err = engine
        .start::<(), String>(
            "delete-tenant",
            (),
            WorkflowOptions::with_id("authz-wrong").authenticated_roles(["viewer"]),
        )
        .await?
        .await
        .expect_err("no matching role");
    assert_eq!(err.code(), ErrorCode::NotAuthorized);
    assert!(
        err.to_string().contains("not authenticated for any"),
        "{err}"
    );
    Ok(())
}

/// The queued path: an unauthorized enqueue is dequeued once, denied, and
/// finalized ERROR — it does not loop through the dispatcher forever.
#[tokio::test]
async fn queued_denial_finalizes_instead_of_looping() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("guarded", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.require_roles("guarded", ["admin"]);
    engine.register_queue(WorkflowQueue::new("authz-q"));
    engine.launch().await?;

    engine
        .start::<(), ()>(
            "guarded",
            (),
            WorkflowOptions {
                workflow_id: Some("authz-queued".into()),
                queue: Some("authz-q".into()),
                ..Default::default()
            },
        )
        .await?;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = engine
            .list_workflows(&ListFilter {
                workflow_ids: vec!["authz-queued".into()],
                ..Default::default()
            })
            .await?
            .remove(0);
        if row.status == "ERROR" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queued denial not finalized within 5s (status {})",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    Ok(())
}

/// Workflows without a declaration are unrestricted, authenticated or not.
#[tokio::test]
async fn undeclared_workflows_are_unrestricted() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("open", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.launch().await?;
    engine
        .start::<(), ()>("open", (), WorkflowOptions::with_id("authz-open"))
        .await?
        .await?;
    Ok(())
}

/// A declaration for an unregistered workflow is a configuration typo,
/// rejected at launch.
#[tokio::test]
async fn declaration_for_unknown_workflow_is_rejected_at_launch() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("real", |_ctx: DurableContext, (): ()| async move {
        Ok::<_, Error>(())
    });
    engine.require_roles("no-such-workflow", ["admin"]);
    let err = engine.launch().await.expect_err("typo declaration");
    assert!(err.to_string().contains("no-such-workflow"), "{err}");
    Ok(())
}
