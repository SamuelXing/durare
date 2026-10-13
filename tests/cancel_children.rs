//! Recursive cancellation must preserve completed results and unrelated trees.
use durare::{
    CancelOptions, Client, DurableEngine, InMemoryProvider, Result, StateProvider, WorkflowStatus,
    STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR,
    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED, STATUS_PENDING, STATUS_SUCCESS,
};
use serde_json::json;
use std::sync::Arc;

mod common;

async fn cancellation_contract(provider: Arc<dyn StateProvider>) -> Result<()> {
    let engine = DurableEngine::new(provider.clone()).await?;
    let client = Client::new(provider.clone());
    for mode in 0..4 {
        let id = |name: &str| format!("{mode}-{name}");
        let cases = [
            (
                "root",
                None,
                if mode == 0 || mode == 3 {
                    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
                } else {
                    STATUS_PENDING
                },
            ),
            ("child", Some("root"), STATUS_ENQUEUED),
            ("grandchild", Some("child"), STATUS_DELAYED),
            ("done", Some("root"), STATUS_SUCCESS),
            ("behind-done", Some("done"), STATUS_PENDING),
            ("error", Some("root"), STATUS_ERROR),
            ("behind-error", Some("error"), STATUS_PENDING),
            (
                "parked",
                Some("root"),
                STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED,
            ),
            ("unrelated", None, STATUS_PENDING),
            ("unrelated-child", Some("unrelated"), STATUS_PENDING),
            ("fork", None, STATUS_PENDING),
            ("orphan", Some("missing"), STATUS_PENDING),
            ("orphan-grandchild", Some("orphan"), STATUS_PENDING),
            ("cycle-a", Some("cycle-b"), STATUS_PENDING),
            ("cycle-b", Some("cycle-a"), STATUS_PENDING),
        ];
        for (name, parent, status) in cases {
            let mut row = WorkflowStatus::new(id(name), "noop", json!(null), status, "owner", "v");
            row.parent_workflow_id = parent.map(id);
            if name == "fork" {
                row.forked_from = Some(id("root"));
            }
            row.output = (status == STATUS_SUCCESS).then(|| json!(42));
            row.error = (status == STATUS_ERROR).then(|| "business failure".to_owned());
            row.completed_at_ms = matches!(status, STATUS_SUCCESS | STATUS_ERROR).then_some(1234);
            row.queue_name = Some("q".into());
            row.dedup_id = Some(id(name));
            row.started_at_ms = Some(1000);
            provider.insert_workflow_status(row).await?;
        }
        let completed_before = provider.get_workflow_status(&id("done")).await?.unwrap();
        let error_before = provider.get_workflow_status(&id("error")).await?.unwrap();
        let unrelated_before = provider
            .get_workflow_status(&id("unrelated"))
            .await?
            .unwrap();
        // Existing APIs and default options must leave descendants alone.
        match mode {
            0 => engine.cancel_workflow(&id("root")).await?,
            1 => engine.cancel_workflows(&[id("root")]).await?,
            2 => client.cancel_workflow(&id("root")).await?,
            _ => {
                client
                    .cancel_workflows_with_options(&[id("root")], CancelOptions::default())
                    .await?
            }
        }
        assert_eq!(
            provider
                .get_workflow_status(&id("root"))
                .await?
                .unwrap()
                .status,
            STATUS_CANCELLED
        );
        assert_eq!(
            provider
                .get_workflow_status(&id("child"))
                .await?
                .unwrap()
                .status,
            STATUS_ENQUEUED
        );
        let options = CancelOptions {
            cancel_children: true,
        };
        engine.cancel_workflows_with_options(&[], options).await?;
        client
            .cancel_workflows_with_options(&[id("missing")], options)
            .await?;
        for name in ["orphan", "orphan-grandchild"] {
            assert_eq!(
                provider
                    .get_workflow_status(&id(name))
                    .await?
                    .unwrap()
                    .status,
                STATUS_CANCELLED
            );
        }
        match mode {
            0 => {
                engine
                    .cancel_workflow_with_options(&id("root"), options)
                    .await?
            }
            1 => {
                engine
                    .cancel_workflows_with_options(
                        &[id("root"), id("child"), id("root"), id("missing")],
                        options,
                    )
                    .await?
            }
            2 => {
                client
                    .cancel_workflow_with_options(&id("root"), options)
                    .await?
            }
            _ => {
                client
                    .cancel_workflows_with_options(&[id("root"), id("child")], options)
                    .await?
            }
        }
        for name in [
            "root",
            "child",
            "grandchild",
            "behind-done",
            "behind-error",
            "parked",
        ] {
            let row = provider.get_workflow_status(&id(name)).await?.unwrap();
            assert_eq!(row.status, STATUS_CANCELLED, "mode {mode}: {name}");
            assert!(row.completed_at_ms.is_some());
            assert!(row.queue_name.is_none());
            assert!(row.dedup_id.is_none());
            assert!(row.started_at_ms.is_none());
        }
        for (name, before) in [
            ("done", completed_before),
            ("error", error_before),
            ("unrelated", unrelated_before),
        ] {
            let after = provider.get_workflow_status(&id(name)).await?.unwrap();
            assert_eq!(after.status, before.status);
            assert_eq!(after.output, before.output);
            assert_eq!(after.error, before.error);
            assert_eq!(after.completed_at_ms, before.completed_at_ms);
            assert_eq!(after.updated_at, before.updated_at);
        }
        for name in ["unrelated-child", "fork"] {
            assert_eq!(
                provider
                    .get_workflow_status(&id(name))
                    .await?
                    .unwrap()
                    .status,
                STATUS_PENDING
            );
        }
        // Recursive traversal must terminate even on imported cyclic lineage.
        engine
            .cancel_workflows_with_options(&[id("cycle-a"), id("cycle-b")], options)
            .await?;
        for name in ["cycle-a", "cycle-b"] {
            assert_eq!(
                provider
                    .get_workflow_status(&id(name))
                    .await?
                    .unwrap()
                    .status,
                STATUS_CANCELLED
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn memory_cancels_descendants() -> Result<()> {
    cancellation_contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_cancels_descendants() -> Result<()> {
    cancellation_contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_cancels_descendants() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "cancel_children").await;
    let result =
        cancellation_contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[tokio::test]
async fn client_recursive_cancel_reaches_a_running_child_step() -> Result<()> {
    use durare::{DurableContext, Error, StepCtx, WorkflowOptions};
    use std::time::Duration;
    use tokio::sync::Notify;
    let provider = Arc::new(InMemoryProvider::new());
    let entered = Arc::new(Notify::new());
    let observed = Arc::new(Notify::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let (body_entered, body_observed) = (entered.clone(), observed.clone());
    engine.register("child", move |ctx: DurableContext, _: ()| {
        let entered = body_entered.clone();
        let observed = body_observed.clone();
        async move {
            ctx.step("wait-for-cancel", move |step: StepCtx| {
                let entered = entered.clone();
                let observed = observed.clone();
                async move {
                    entered.notify_one();
                    step.cancelled().await?;
                    observed.notify_one();
                    Err::<(), _>(Error::Cancelled(step.workflow_id().to_owned()))
                }
            })
            .await
        }
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        ctx.start_workflow::<_, ()>("child", (), WorkflowOptions::with_id("live-child"))
            .await?
            .result()
            .await
    });
    let handle = engine
        .start::<_, ()>("parent", (), WorkflowOptions::with_id("live-parent"))
        .await?;
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("child did not start");
    Client::new(provider.clone())
        .cancel_workflow_with_options(
            "live-parent",
            CancelOptions {
                cancel_children: true,
            },
        )
        .await?;
    tokio::time::timeout(Duration::from_secs(3), observed.notified())
        .await
        .expect("child did not observe cancellation");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), handle.result())
            .await
            .unwrap(),
        Err(Error::Cancelled(_))
    ));
    for id in ["live-parent", "live-child"] {
        assert_eq!(
            provider.get_workflow_status(id).await?.unwrap().status,
            STATUS_CANCELLED
        );
    }
    assert!(
        engine.get_workflow_steps("live-child").await?.is_empty(),
        "cooperative cancellation must not checkpoint a step failure"
    );
    engine.shutdown(Duration::from_secs(2)).await?;
    Ok(())
}
