//! Schedule definition patches preserve runtime state and are observed by live loops.
use chrono::{TimeZone, Utc};
use durare::{
    Client, DurableContext, DurableEngine, InMemoryProvider, Result, ScheduleOptions,
    ScheduleStatus, ScheduleUpdate, ScheduledInput, StateProvider,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
mod common;

async fn contract(provider: Arc<dyn StateProvider>) -> Result<()> {
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("tick", |_ctx: DurableContext, _: ScheduledInput| async {
        Ok::<_, durare::Error>(())
    });
    let client = Client::new(provider.clone());
    engine
        .create_schedule(
            "s",
            "tick",
            "0 * * * * *",
            ScheduleOptions::new()
                .context(&json!({"v":1}))
                .automatic_backfill(true)
                .cron_timezone("America/Los_Angeles")
                .queue_name("q"),
        )
        .await?;
    engine.pause_schedule("s").await?;
    let last = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    provider
        .set_schedule_last_fired("s", last.timestamp_millis())
        .await?;
    let before = engine.get_schedule("s").await?.unwrap();
    engine
        .update_schedule(
            "s",
            ScheduleUpdate {
                schedule: Some("*/10 * * * * *".into()),
                ..Default::default()
            },
        )
        .await?;
    let row = client.get_schedule("s").await?.unwrap();
    assert_eq!(row.schedule, "*/10 * * * * *");
    assert_eq!(row.context, before.context);
    assert_eq!(row.automatic_backfill, before.automatic_backfill);
    assert_eq!(row.cron_timezone, before.cron_timezone);
    assert_eq!(row.queue_name, before.queue_name);
    assert_eq!(row.schedule_id, before.schedule_id);
    assert_eq!(row.last_fired_at, before.last_fired_at);
    assert_eq!(row.status, ScheduleStatus::Paused);
    assert_eq!(row.workflow_name, before.workflow_name);

    client
        .update_schedule(
            "s",
            ScheduleUpdate {
                context: Some(Some(json!({"v":2}))),
                automatic_backfill: Some(false),
                cron_timezone: Some(Some("UTC".into())),
                queue_name: Some(Some("other".into())),
                ..Default::default()
            },
        )
        .await?;
    let row = engine.get_schedule("s").await?.unwrap();
    assert_eq!(row.context, Some(json!({"v":2})));
    assert!(!row.automatic_backfill);
    assert_eq!(row.cron_timezone.as_deref(), Some("UTC"));
    assert_eq!(row.queue_name.as_deref(), Some("other"));
    client
        .update_schedule(
            "s",
            ScheduleUpdate {
                context: Some(None),
                cron_timezone: Some(None),
                queue_name: Some(None),
                ..Default::default()
            },
        )
        .await?;
    let row = engine.get_schedule("s").await?.unwrap();
    assert!(row.context.is_none() && row.cron_timezone.is_none() && row.queue_name.is_none());
    assert_eq!(row.schedule_id, before.schedule_id);
    assert_eq!(row.last_fired_at, Some(last));
    assert_eq!(row.status, ScheduleStatus::Paused);
    engine
        .update_schedule("s", ScheduleUpdate::default())
        .await?;
    assert!(client
        .update_schedule("missing", ScheduleUpdate::default())
        .await
        .is_err());
    assert!(engine
        .update_schedule(
            "missing",
            ScheduleUpdate {
                context: Some(Some(json!(1))),
                ..Default::default()
            }
        )
        .await
        .is_err());
    assert!(engine
        .update_schedule("", ScheduleUpdate::default())
        .await
        .is_err());
    for invalid in [
        ScheduleUpdate {
            schedule: Some("invalid".into()),
            context: Some(Some(json!("must not land"))),
            ..Default::default()
        },
        ScheduleUpdate {
            cron_timezone: Some(Some("not/a/timezone".into())),
            context: Some(Some(json!("must not land"))),
            ..Default::default()
        },
    ] {
        assert!(engine.update_schedule("s", invalid.clone()).await.is_err());
        assert!(client.update_schedule("s", invalid.clone()).await.is_err());
        assert!(provider.update_schedule("s", &invalid).await.is_err());
        assert!(engine.get_schedule("s").await?.unwrap().context.is_none());
    }
    // Disjoint concurrent patches must not lose each other's fields or rewind
    // a concurrently advanced runtime cursor.
    let cron = ScheduleUpdate {
        schedule: Some("*/5 * * * * *".into()),
        ..Default::default()
    };
    let context = ScheduleUpdate {
        context: Some(Some(json!(3))),
        ..Default::default()
    };
    let next = last + chrono::Duration::seconds(5);
    let (a, b, c) = tokio::join!(
        provider.update_schedule("s", &cron),
        provider.update_schedule("s", &context),
        provider.set_schedule_last_fired("s", next.timestamp_millis())
    );
    a?;
    b?;
    c?;
    let row = engine.get_schedule("s").await?.unwrap();
    assert_eq!(row.schedule, "*/5 * * * * *");
    assert_eq!(row.context, Some(json!(3)));
    assert_eq!(row.last_fired_at, Some(next));
    assert_eq!(row.schedule_id, before.schedule_id);
    assert_eq!(row.status, ScheduleStatus::Paused);
    client
        .update_schedule(
            "s",
            ScheduleUpdate {
                context: Some(Some(json!(null))),
                ..Default::default()
            },
        )
        .await?;
    assert!(engine.get_schedule("s").await?.unwrap().context.is_none());
    assert!(client
        .update_schedule(
            "s' OR 1=1 --",
            ScheduleUpdate {
                context: Some(Some(json!("bad"))),
                ..Default::default()
            }
        )
        .await
        .is_err());
    assert!(engine.get_schedule("s").await?.unwrap().context.is_none());
    Ok(())
}

#[tokio::test]
async fn memory_update() -> Result<()> {
    contract(Arc::new(InMemoryProvider::new())).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_update() -> Result<()> {
    contract(Arc::new(
        durare::SqliteProvider::connect("sqlite::memory:").await?,
    ))
    .await
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_update() -> Result<()> {
    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "schedule_update").await;
    let result =
        async { contract(Arc::new(durare::PostgresProvider::connect(&url).await?)).await }.await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[tokio::test]
async fn live_loop_observes_definition_changes_without_replaying_history() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let (sent, mut received) = tokio::sync::mpsc::unbounded_channel();
    engine.register("tick", move |_ctx: DurableContext, tick: ScheduledInput| {
        let sent = sent.clone();
        async move {
            sent.send(tick).unwrap();
            Ok::<_, durare::Error>(())
        }
    });
    engine.register_queue(durare::WorkflowQueue::new("updated-q"));
    // A far-future cron installs an idle loop. Updating its stable row must
    // replace that loop; identity-only reconciliation would never fire.
    engine
        .create_schedule(
            "live",
            "tick",
            "0 0 0 1 1 * 2099",
            ScheduleOptions::new()
                .context(&"old")
                .automatic_backfill(true),
        )
        .await?;
    let last = Utc::now() - chrono::Duration::seconds(2);
    provider
        .set_schedule_last_fired("live", last.timestamp_millis())
        .await?;
    let original_id = engine.get_schedule("live").await?.unwrap().schedule_id;
    engine.launch().await?;
    tokio::time::sleep(Duration::from_millis(700)).await;
    Client::new(provider.clone())
        .update_schedule(
            "live",
            ScheduleUpdate {
                schedule: Some("* * * * * *".into()),
                context: Some(Some(json!("new"))),
                ..Default::default()
            },
        )
        .await?;
    let result = async {
        let first = tokio::time::timeout(Duration::from_secs(5), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(first.scheduled_time > last);
        assert_eq!(first.context, Some(json!("new")));
        // A context-only patch must also refresh an installed loop. Already
        // processed tick ids remain the same, so backfill cannot re-execute them.
        engine
            .update_schedule(
                "live",
                ScheduleUpdate {
                    context: Some(Some(json!("latest"))),
                    ..Default::default()
                },
            )
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let mut seen = std::collections::HashSet::from([first.scheduled_time]);
        loop {
            let tick = tokio::time::timeout_at(deadline, received.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(seen.insert(tick.scheduled_time), "history tick replayed");
            assert!(tick.scheduled_time > last);
            if tick.context == Some(json!("latest")) {
                break;
            }
        }
        engine
            .update_schedule(
                "live",
                ScheduleUpdate {
                    queue_name: Some(Some("updated-q".into())),
                    ..Default::default()
                },
            )
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let queued = engine
                .list_workflows(&durare::ListFilter {
                    queue_name: vec!["updated-q".into()],
                    ..Default::default()
                })
                .await?;
            if queued
                .iter()
                .any(|row| row.input["context"] == json!("latest"))
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "updated queue was not used"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        engine
            .update_schedule(
                "live",
                ScheduleUpdate {
                    context: Some(Some(json!("direct"))),
                    queue_name: Some(None),
                    ..Default::default()
                },
            )
            .await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let tick = tokio::time::timeout_at(deadline, received.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(seen.insert(tick.scheduled_time), "history tick replayed");
            if tick.context == Some(json!("direct")) {
                break;
            }
        }
        let all = engine
            .list_workflows(&durare::ListFilter::default())
            .await?;
        assert!(all
            .iter()
            .any(|row| row.input["context"] == json!("direct") && row.queue_name.is_none()));
        let row = engine.get_schedule("live").await?.unwrap();
        assert_eq!(row.schedule_id, original_id);
        assert!(row.last_fired_at.is_some_and(|at| at >= last));
        Ok(())
    }
    .await;
    engine.shutdown(Duration::from_secs(1)).await?;
    result
}
