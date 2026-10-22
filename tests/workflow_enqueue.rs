//! Named queued children share start_workflow's durable relationship protocol.
use durare::{
    Client, DeduplicationPolicy, DurableContext, DurableEngine, Error, InMemoryProvider,
    ListFilter, Result, StateProvider, WorkflowOptions, WorkflowQueue, STATUS_PENDING,
};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
mod common;

struct Unserializable;
impl serde::Serialize for Unserializable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("old input serialization failure"))
    }
}

async fn contract(provider: Arc<dyn StateProvider>) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let effects = Arc::new(AtomicUsize::new(0));
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register_queue(WorkflowQueue::new("remote"));
    engine.listen_queues(Vec::<String>::new());
    let invoked = calls.clone();
    let ran = effects.clone();
    engine.register("parent", move |ctx: DurableContext, _: ()| {
        let attempt = invoked.fetch_add(1, Ordering::SeqCst);
        let ran = ran.clone();
        async move {
            let a = ctx
                .enqueue_workflow::<_, ()>(
                    "remote",
                    "external",
                    attempt,
                    WorkflowOptions {
                        // The argument overrides this queue; empty IDs are derived.
                        workflow_id: Some(String::new()),
                        app_version: Some("target-version".into()),
                        queue: Some("ignored".into()),
                        delay: Some(Duration::from_secs(3600)),
                        timeout: Some(Duration::from_secs(5)),
                        priority: 7,
                        config_name: Some("instance".into()),
                        class_name: Some("Orders".into()),
                        assumed_role: Some("worker".into()),
                        attributes: Some(serde_json::from_value(json!({"tag":"remote"}))?),
                        ..Default::default()
                    },
                )
                .await?;
            // Existing queued start_workflow also accepts a remote target and
            // retains its established parent-version default.
            let b = ctx
                .start_workflow::<_, ()>(
                    "external",
                    (),
                    WorkflowOptions::default()
                        .queue("remote")
                        .app_version("target-version"),
                )
                .await?;
            ctx.step("after", || async {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await?;
            Ok(vec![a.id().to_owned(), b.id().to_owned()])
        }
    });
    engine.register("dedup", |ctx: DurableContext, _: ()| async move {
        let child = ctx
            .enqueue_workflow::<_, ()>(
                "remote",
                "external",
                (),
                WorkflowOptions::default()
                    .dedup_id("key")
                    .dedup_policy(DeduplicationPolicy::ReturnExisting)
                    .app_version("target-version"),
            )
            .await?;
        Ok::<_, Error>(child.id().to_string())
    });
    engine.register("gap", |ctx: DurableContext, _: ()| async move {
        let child = ctx
            .enqueue_workflow::<_, ()>(
                "remote",
                "external",
                "retry-input",
                WorkflowOptions::default().app_version("target-version"),
            )
            .await?;
        Ok::<_, Error>(child.id().to_owned())
    });
    engine.register("mismatched", |ctx: DurableContext, _: ()| async move {
        for (name, class, config) in [
            ("different", None, None),
            ("external", Some("other"), None),
            ("external", None, Some("other")),
        ] {
            let error = ctx
                .enqueue_workflow::<_, ()>(
                    "remote",
                    name,
                    (),
                    WorkflowOptions {
                        app_version: Some("target-version".into()),
                        dedup_id: Some("key".into()),
                        dedup_policy: DeduplicationPolicy::ReturnExisting,
                        class_name: class.map(str::to_owned),
                        config_name: config.map(str::to_owned),
                        ..Default::default()
                    },
                )
                .await
                .err()
                .expect("mismatched target was adopted");
            assert!(matches!(error, Error::App { .. }));
        }
        ctx.step("after-mismatches", || async { Ok(()) }).await
    });
    let refusal_effects = Arc::new(AtomicUsize::new(0));
    let effect_counter = refusal_effects.clone();
    engine.register("refusals", move |ctx: DurableContext, _: ()| {
        let effect_counter = effect_counter.clone();
        async move {
            // The branch must observe the same typed error during execution,
            // ordinary recovery, and read-only verification.
            match ctx
                .enqueue_workflow::<_, ()>(
                    "missing",
                    "external",
                    (),
                    WorkflowOptions::default().app_version("target-version"),
                )
                .await
            {
                Err(Error::UnknownQueue(queue)) => assert_eq!(queue, "missing"),
                Err(error) => panic!("wrong UnknownQueue branch: {error:?}"),
                Ok(_) => panic!("UnknownQueue enqueue unexpectedly succeeded"),
            }
            assert!(matches!(
                ctx.start_workflow::<_, ()>(
                    "",
                    (),
                    WorkflowOptions::default()
                        .queue("remote")
                        .app_version("target-version")
                )
                .await,
                Err(Error::App { .. })
            ));
            assert!(matches!(ctx.start_workflow::<_, ()>("typo", (),
                WorkflowOptions::default().queue("remote")).await,
                Err(Error::UnknownWorkflow(name)) if name == "typo"));
            ctx.step("after-refusals", || async {
                effect_counter.fetch_add(1, Ordering::SeqCst);
                Ok(42_i64)
            })
            .await
        }
    });
    let legacy_effects = Arc::new(AtomicUsize::new(0));
    let legacy_counter = legacy_effects.clone();
    engine.register("legacy", move |ctx: DurableContext, _: ()| {
        let legacy_counter = legacy_counter.clone();
        async move {
            assert!(matches!(
                ctx.start_workflow::<_, ()>(
                    "not-started",
                    Unserializable,
                    WorkflowOptions::default()
                )
                .await,
                Err(Error::Serde(_))
            ));
            ctx.step("effect", || async {
                legacy_counter.fetch_add(1, Ordering::SeqCst);
                Ok(42_i64)
            })
            .await
        }
    });
    let handle = engine
        .start::<_, Vec<String>>(
            "parent",
            (),
            WorkflowOptions::with_id("parent")
                .authenticated_user("alice")
                .assumed_role("caller")
                .authenticated_roles(["reader", "writer"]),
        )
        .await?;
    let expected = vec!["parent-0".to_string(), "parent-1".to_string()];
    assert_eq!(handle.result().await?, expected);
    let a = provider.get_workflow_status("parent-0").await?.unwrap();
    assert_eq!(a.name, "external");
    assert_eq!(a.input, json!(0));
    assert_eq!(a.status, "DELAYED");
    assert_eq!(a.parent_workflow_id.as_deref(), Some("parent"));
    assert_eq!(a.queue_name.as_deref(), Some("remote"));
    assert_eq!(a.app_version, "target-version");
    assert!(a.executor_id.is_empty());
    assert_eq!(a.timeout_ms, Some(5000));
    assert!(a.deadline_ms.is_none());
    assert_eq!(a.priority, 7);
    assert_eq!(a.config_name.as_deref(), Some("instance"));
    assert_eq!(a.class_name.as_deref(), Some("Orders"));
    assert_eq!(a.authenticated_user.as_deref(), Some("alice"));
    assert_eq!(a.assumed_role.as_deref(), Some("worker"));
    assert_eq!(a.authenticated_roles, vec!["reader", "writer"]);
    assert_eq!(a.attributes, Some(json!({"tag":"remote"})));
    let b = provider.get_workflow_status("parent-1").await?.unwrap();
    assert_eq!(b.status, "ENQUEUED");
    assert_eq!(b.app_version, "target-version");
    let steps = engine.get_workflow_steps("parent").await?;
    assert_eq!(
        steps
            .iter()
            .map(|s| (s.step_id, s.name.as_str()))
            .collect::<Vec<_>>(),
        vec![(0, "external"), (1, "external"), (2, "after")]
    );
    assert_eq!(steps[0].child_workflow_id.as_deref(), Some("parent-0"));

    // The original owner finished before recovery. Force a recovery fixture to
    // exercise normal execution replay, then also the read-only verifier.
    assert!(
        provider
            .set_workflow_status("parent", STATUS_PENDING, None, None)
            .await?
    );
    assert_eq!(engine.recover().await?, 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), handle.result())
            .await
            .unwrap()?,
        expected
    );
    assert_eq!(engine.verify_replay("parent").await?.divergence, None);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let after = provider.get_workflow_status("parent-0").await?.unwrap();
    assert_eq!(after.input, a.input);
    assert_eq!(after.delay_until_ms, a.delay_until_ms);
    assert_eq!(after.created_at, a.created_at);
    assert_eq!(
        provider
            .list_workflows(&ListFilter {
                parent_workflow_ids: vec!["parent".into()],
                ..Default::default()
            })
            .await?
            .len(),
        2
    );

    // A repeated enqueue may explicitly resolve the existing deduplication
    // slot. Checkpoint the actual returned ID, including across recovery.
    let existing = Client::new(provider.clone())
        .enqueue::<_, ()>(
            "remote",
            "external",
            (),
            WorkflowOptions::with_id("existing").dedup_id("key"),
        )
        .await?;
    let dedup = engine
        .start::<_, String>("dedup", (), WorkflowOptions::with_id("dedup"))
        .await?;
    assert_eq!(dedup.result().await?, existing.id());
    assert_eq!(
        engine.get_workflow_steps("dedup").await?[0]
            .child_workflow_id
            .as_deref(),
        Some("existing")
    );
    assert!(provider.get_workflow_status("dedup-0").await?.is_none());
    assert_eq!(engine.verify_replay("dedup").await?.divergence, None);
    engine
        .start::<_, ()>("mismatched", (), WorkflowOptions::with_id("mismatched"))
        .await?
        .result()
        .await?;
    let mismatch_steps = engine.get_workflow_steps("mismatched").await?;
    assert_eq!(mismatch_steps.len(), 4);
    assert_eq!(mismatch_steps[3].step_id, 3);
    assert!(mismatch_steps
        .iter()
        .all(|step| step.child_workflow_id.is_none()));
    assert_eq!(engine.verify_replay("mismatched").await?.divergence, None);
    // Simulate the crash window: the queued row committed, but its parent
    // relationship checkpoint did not. Retrying must observe the canonical
    // row and write only the missing relationship.
    let mut inserted = durare::WorkflowStatus::new(
        "gap-0",
        "external",
        json!("original-input"),
        "ENQUEUED",
        "",
        "target-version",
    );
    inserted.queue_name = Some("remote".into());
    inserted.parent_workflow_id = Some("gap".into());
    provider.insert_workflow_status(inserted).await?;
    let gap = engine
        .start::<_, String>("gap", (), WorkflowOptions::with_id("gap"))
        .await?;
    assert_eq!(gap.result().await?, "gap-0");
    let original = provider.get_workflow_status("gap-0").await?.unwrap();
    assert_eq!(original.input, json!("original-input"));
    assert_eq!(original.app_version, "target-version");
    assert_eq!(
        engine.get_workflow_steps("gap").await?[0]
            .child_workflow_id
            .as_deref(),
        Some("gap-0")
    );
    assert_eq!(engine.verify_replay("gap").await?.divergence, None);
    let refused = engine
        .start::<_, i64>("refusals", (), WorkflowOptions::with_id("refusals"))
        .await?;
    assert_eq!(refused.result().await?, 42);
    assert_eq!(engine.verify_replay("refusals").await?.divergence, None);
    assert!(
        provider
            .set_workflow_status("refusals", STATUS_PENDING, None, None)
            .await?
    );
    assert_eq!(engine.recover().await?, 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), refused.result())
            .await
            .unwrap()?,
        42
    );
    assert_eq!(refusal_effects.load(Ordering::SeqCst), 1);
    assert_eq!(engine.get_workflow_steps("refusals").await?.len(), 4);

    // Released 0.5.0 wrote no position for the rejected input, so its next
    // side effect was checkpointed at zero. Seed that exact history layout.
    provider
        .insert_workflow_status(durare::WorkflowStatus::new(
            "legacy",
            "legacy",
            json!(null),
            STATUS_PENDING,
            "stopped-0.5.0",
            engine.app_version(),
        ))
        .await?;
    provider
        .record_step_result("legacy", 0, "effect", json!(42), None, None, None)
        .await?;
    assert_eq!(engine.recover().await?, 1);
    let old = engine.retrieve_workflow::<i64>("legacy").await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), old.result())
            .await
            .unwrap()?,
        42
    );
    assert_eq!(
        legacy_effects.load(Ordering::SeqCst),
        0,
        "released side effect ran twice"
    );
    assert_eq!(engine.verify_replay("legacy").await?.divergence, None);
    assert_eq!(engine.get_workflow_steps("legacy").await?[0].step_id, 0);
    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

#[tokio::test]
async fn named_enqueue_memory() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn named_enqueue_sqlite() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn named_enqueue_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "named_enqueue").await;
    let result = contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

async fn worker_launch_order_contract(provider: Arc<dyn StateProvider>) -> Result<()> {
    for worker_first in [true, false] {
        let mut parent_builder = DurableEngine::builder(provider.clone());
        parent_builder.app_version("parent-version");
        let mut parent = parent_builder.build().await?;
        parent.register_queue(WorkflowQueue::new("remote"));
        parent.listen_queues(Vec::<String>::new());
        parent.register("parent", |ctx: DurableContext, n: i64| async move {
            ctx.enqueue_workflow::<_, i64>(
                "remote",
                "external",
                n,
                WorkflowOptions::default().app_version("worker-version"),
            )
            .await?
            .result()
            .await
        });
        let mut worker_builder = DurableEngine::builder(provider.clone());
        worker_builder.app_version("worker-version");
        let mut worker = worker_builder.build().await?;
        worker.register_queue(
            WorkflowQueue::new("remote").base_polling_interval(Duration::from_millis(10)),
        );
        worker.register("external", |_ctx: DurableContext, n: i64| async move {
            Ok::<_, Error>(n * 3)
        });
        if worker_first {
            worker.launch().await?;
            parent.launch().await?;
            // Ensure the producer really is latest, including coarse clocks.
            tokio::time::sleep(Duration::from_millis(2)).await;
            provider
                .set_latest_application_version("parent-version")
                .await?;
        } else {
            parent.launch().await?;
            worker.launch().await?;
            tokio::time::sleep(Duration::from_millis(2)).await;
            provider
                .set_latest_application_version("worker-version")
                .await?;
        }
        assert_eq!(
            provider
                .get_latest_application_version()
                .await?
                .unwrap()
                .version_name,
            if worker_first {
                "parent-version"
            } else {
                "worker-version"
            }
        );
        let id = format!("launch-order-{worker_first}");
        let output = tokio::time::timeout(Duration::from_secs(5), async {
            parent
                .start::<_, i64>("parent", 7, WorkflowOptions::with_id(&id))
                .await?
                .result()
                .await
        })
        .await
        .unwrap()?;
        assert_eq!(output, 21);
        let row = provider
            .get_workflow_status(&format!("{id}-0"))
            .await?
            .unwrap();
        assert_eq!(row.app_version, "worker-version");
        assert_eq!(row.executor_id, worker.executor_id());
        parent.shutdown(Duration::from_secs(1)).await?;
        worker.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn worker_launch_order_memory() -> Result<()> {
    worker_launch_order_contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn worker_launch_order_sqlite() -> Result<()> {
    worker_launch_order_contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn worker_launch_order_postgres() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "enqueue_launch_order").await;
    let result =
        worker_launch_order_contract(Arc::new(durare::PostgresProvider::connect(&url).await?))
            .await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[tokio::test]
async fn invalid_and_nested_enqueues_have_no_side_effect() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register_queue(WorkflowQueue::new("remote"));
    engine.listen_queues(Vec::<String>::new());
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        assert!(ctx
            .enqueue_workflow::<_, ()>("", "external", (), WorkflowOptions::default())
            .await
            .is_err());
        assert!(ctx
            .enqueue_workflow::<_, ()>("remote", "", (), WorkflowOptions::default())
            .await
            .is_err());
        assert!(matches!(
            ctx.start_workflow::<_, ()>("external", (), WorkflowOptions::default())
                .await,
            Err(Error::UnknownWorkflow(_))
        ));
        assert!(matches!(
            ctx.enqueue_workflow::<_, ()>("missing", "external", (), WorkflowOptions::default())
                .await,
            Err(Error::UnknownQueue(_))
        ));
        let nested = ctx.clone();
        ctx.step("nested", || async move {
            assert!(matches!(
                nested
                    .enqueue_workflow::<_, ()>("remote", "external", (), WorkflowOptions::default())
                    .await,
                Err(Error::NestedDurableCall { .. })
            ));
            Ok(())
        })
        .await?;
        Ok::<_, Error>(())
    });
    engine
        .start::<_, ()>("parent", (), WorkflowOptions::with_id("invalid"))
        .await?
        .result()
        .await?;
    let steps = engine.get_workflow_steps("invalid").await?;
    assert_eq!(steps.len(), 5);
    assert_eq!(steps[4].step_id, 4);
    assert_eq!(engine.verify_replay("invalid").await?.divergence, None);
    assert_eq!(
        provider.list_workflows(&ListFilter::default()).await?.len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn remote_routing_is_rejected_before_insertion() -> Result<()> {
    for producer_only in [false, true] {
        let provider = Arc::new(InMemoryProvider::new());
        let mut builder = DurableEngine::builder(provider.clone());
        builder.app_version("producer-version");
        let mut engine = builder.build().await?;
        engine.register_queue(
            WorkflowQueue::new("remote").base_polling_interval(Duration::from_millis(5)),
        );
        if producer_only {
            engine.listen_queues(Vec::<String>::new());
        }
        engine.register("parent", move |ctx: DurableContext, _: ()| async move {
            for version in [None, Some(""), Some("producer-version")] {
                // A version matching the producer would make the default
                // dispatcher claim and damage the child in the old design.
                // Only an explicit version AND a producer-only queue is valid.
                if producer_only && version == Some("producer-version") {
                    continue;
                }
                let error = ctx
                    .enqueue_workflow::<_, ()>(
                        "remote",
                        "external",
                        (),
                        WorkflowOptions {
                            app_version: version.map(str::to_owned),
                            ..Default::default()
                        },
                    )
                    .await
                    .err()
                    .expect("unsafe route was accepted");
                if version.is_none_or(str::is_empty) {
                    assert!(matches!(error, Error::UnknownWorkflow(_)));
                } else {
                    assert!(matches!(error, Error::App { .. }));
                }
            }
            ctx.step("after-errors", || async { Ok(()) }).await
        });
        engine.launch().await?;
        engine
            .start::<_, ()>("parent", (), WorkflowOptions::with_id("guard"))
            .await?
            .result()
            .await?;
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            provider.list_workflows(&ListFilter::default()).await?.len(),
            1
        );
        let steps = engine.get_workflow_steps("guard").await?;
        assert_eq!(steps.len(), if producer_only { 3 } else { 4 });
        assert_eq!(
            steps.last().unwrap().step_id,
            if producer_only { 2 } else { 3 }
        );
        assert_eq!(engine.verify_replay("guard").await?.divergence, None);
        engine.shutdown(Duration::from_secs(1)).await?;
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_parent_cannot_enqueue_new_children() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register_queue(WorkflowQueue::new("remote"));
    engine.listen_queues(Vec::<String>::new());
    let db = provider.clone();
    engine.register("parent", move |ctx: DurableContext, _: ()| {
        let db = db.clone();
        async move {
            db.set_workflow_status("cancelled-parent", durare::STATUS_CANCELLED, None, None)
                .await?;
            assert!(
                matches!(ctx.enqueue_workflow::<_, ()>("remote", "external", (),
                WorkflowOptions::default().app_version("worker")).await,
                Err(Error::Cancelled(id)) if id == "cancelled-parent")
            );
            Ok(())
        }
    });
    let result = engine
        .start::<_, ()>("parent", (), WorkflowOptions::with_id("cancelled-parent"))
        .await?
        .result()
        .await;
    assert!(matches!(result, Err(Error::Cancelled(_))));
    assert!(provider
        .get_workflow_status("cancelled-parent-0")
        .await?
        .is_none());
    assert!(provider
        .get_workflow_steps("cancelled-parent")
        .await?
        .is_empty());
    Ok(())
}
