//! Workflow communication tests: send/recv messaging (FIFO, timeouts, replay
//! safety) and set_event/get_event, on the in-memory provider.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
    WorkflowStatus, STATUS_PENDING,
};
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// An external send unblocks a workflow waiting in recv.
#[tokio::test]
async fn send_unblocks_waiting_recv() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("waiter", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("greetings", Duration::from_secs(5)).await?;
        Ok::<_, Error>(msg.unwrap_or_default())
    });

    let handle = engine
        .start::<_, String>("waiter", (), WorkflowOptions::with_id("wf-recv"))
        .await?;
    engine
        .send("wf-recv", "hello".to_string(), "greetings")
        .await?;
    assert_eq!(handle.result().await?, "hello");
    Ok(())
}

/// Messages on a topic are consumed in FIFO order, including across workflows
/// exchanging messages via ctx.send.
#[tokio::test]
async fn recv_is_fifo() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("take_two", |ctx: DurableContext, _: ()| async move {
        let a: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        let b: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        Ok::<_, Error>(format!(
            "{},{}",
            a.unwrap_or_default(),
            b.unwrap_or_default()
        ))
    });
    engine.register("producer", |ctx: DurableContext, dest: String| async move {
        ctx.send(&dest, "m1".to_string(), "t").await?;
        ctx.send(&dest, "m2".to_string(), "t").await?;
        Ok::<_, Error>(())
    });

    let consumer = engine
        .start::<_, String>("take_two", (), WorkflowOptions::with_id("wf-fifo"))
        .await?;
    let producer = engine
        .start::<_, ()>(
            "producer",
            "wf-fifo".to_string(),
            WorkflowOptions::with_id("wf-producer"),
        )
        .await?;
    producer.result().await?;
    assert_eq!(consumer.result().await?, "m1,m2");
    Ok(())
}

/// recv returns None once its (durable) timeout expires.
#[tokio::test]
async fn recv_times_out_to_none() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("impatient", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("silence", Duration::from_millis(100)).await?;
        Ok::<_, Error>(msg.is_none())
    });

    let started = Instant::now();
    let timed_out: bool = engine
        .start("impatient", (), WorkflowOptions::with_id("wf-timeout"))
        .await?
        .result()
        .await?;
    assert!(timed_out, "recv with no sender must return None");
    assert!(started.elapsed() >= Duration::from_millis(80));
    Ok(())
}

/// A replayed recv returns its checkpointed message without consuming another:
/// re-executing the workflow body (via recover) yields the same message, and
/// the second message is still in the mailbox afterwards.
#[tokio::test]
async fn recv_replay_does_not_double_consume() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("take_one", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        Ok::<_, Error>(msg.unwrap_or_default())
    });

    // Create the workflow row directly in PENDING so recover() executes it.
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "wf-replay",
            "take_one",
            Value::Null,
            STATUS_PENDING,
            "",
            engine.app_version(),
        ))
        .await?;
    engine.send("wf-replay", "m1".to_string(), "t").await?;
    engine.send("wf-replay", "m2".to_string(), "t").await?;

    // recover() dispatches the run to the background and returns; each
    // execution is awaited by polling for a terminal status.
    let settled = |provider: Arc<InMemoryProvider>| async move {
        for _ in 0..250 {
            let s = provider.get_workflow_status("wf-replay").await?.unwrap();
            if s.status != STATUS_PENDING {
                return Ok::<_, Error>(s);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("recovered run never settled");
    };

    // First execution consumes m1 and completes.
    assert_eq!(engine.recover().await?, 1);
    let first = settled(provider.clone()).await?;
    assert_eq!(first.output, Some(Value::String("m1".into())));

    // Force a re-execution of the body: the recv must replay its checkpoint
    // (m1), not consume m2.
    provider
        .set_workflow_status("wf-replay", STATUS_PENDING, None, None)
        .await?;
    assert_eq!(engine.recover().await?, 1);
    let second = settled(provider.clone()).await?;
    assert_eq!(second.output, Some(Value::String("m1".into())));

    // m2 must still be unconsumed.
    let leftover = provider
        .consume_notification("wf-replay", "t", 999, "test-probe")
        .await?;
    assert_eq!(leftover, Some(Value::String("m2".into())));
    Ok(())
}

/// Sending to a workflow id that does not exist is an error.
#[tokio::test]
async fn send_to_missing_workflow_errors() -> Result<()> {
    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    let res = engine.send("ghost", "boo".to_string(), "t").await;
    assert!(res.is_err());
    Ok(())
}

/// set_event publishes a value readable from outside the workflow (and after
/// it completes); get_event from another workflow sees it too.
#[tokio::test]
async fn set_event_and_get_event() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("publisher", |ctx: DurableContext, _: ()| async move {
        ctx.set_event("status", "ready").await?;
        Ok::<_, Error>(())
    });
    engine.register(
        "subscriber",
        |ctx: DurableContext, target: String| async move {
            let v: Option<String> = ctx
                .get_event(&target, "status", Duration::from_secs(5))
                .await?;
            Ok::<_, Error>(v.unwrap_or_default())
        },
    );

    engine
        .start::<_, ()>("publisher", (), WorkflowOptions::with_id("wf-pub"))
        .await?
        .result()
        .await?;

    // External read.
    let v: Option<String> = engine
        .get_event("wf-pub", "status", Duration::from_secs(1))
        .await?;
    assert_eq!(v.as_deref(), Some("ready"));

    // Cross-workflow durable read.
    let got: String = engine
        .start(
            "subscriber",
            "wf-pub".to_string(),
            WorkflowOptions::with_id("wf-sub"),
        )
        .await?
        .result()
        .await?;
    assert_eq!(got, "ready");
    Ok(())
}

/// Distinct event keys coexist, and re-setting a key overwrites it: a reader sees
/// the latest value for an updated key and the independent value for another —
/// last-write-wins per key (mirrors the other SDKs' set/get-event semantics).
#[tokio::test]
async fn set_event_keys_are_independent_and_last_write_wins() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("multi_event", |ctx: DurableContext, _: ()| async move {
        ctx.set_event("phase", "start").await?;
        ctx.set_event("progress", 10_i64).await?;
        // Overwrite one key; the other must be untouched.
        ctx.set_event("phase", "done").await?;
        Ok::<_, Error>(())
    });

    engine
        .start::<_, ()>("multi_event", (), WorkflowOptions::with_id("wf-ev"))
        .await?
        .result()
        .await?;

    // The overwritten key reads back its latest value.
    let phase: Option<String> = engine
        .get_event("wf-ev", "phase", Duration::from_secs(1))
        .await?;
    assert_eq!(phase.as_deref(), Some("done"), "last write wins for a key");

    // The independent key keeps its own value.
    let progress: Option<i64> = engine
        .get_event("wf-ev", "progress", Duration::from_secs(1))
        .await?;
    assert_eq!(progress, Some(10), "a distinct key is unaffected");
    Ok(())
}

/// get_event on a key that is never set returns None after the timeout, both
/// from outside and inside a workflow.
#[tokio::test]
async fn get_event_times_out_to_none() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("noop", |_ctx: DurableContext, _: ()| async move {
        Ok::<_, Error>(())
    });
    engine
        .start::<_, ()>("noop", (), WorkflowOptions::with_id("wf-empty"))
        .await?
        .result()
        .await?;

    let v: Option<String> = engine
        .get_event("wf-empty", "missing", Duration::from_millis(80))
        .await?;
    assert_eq!(v, None);
    Ok(())
}

/// An idempotent send delivers at most once per key: two sends sharing a key
/// collapse to a single message (a retry never double-delivers), while a distinct
/// key delivers independently.
#[tokio::test]
async fn send_with_idempotency_key_delivers_at_most_once() -> Result<()> {
    let provider = Arc::new(InMemoryProvider::new());
    let engine = DurableEngine::new(provider.clone()).await?;

    // A destination workflow to receive the messages.
    provider
        .insert_workflow_status(WorkflowStatus::new(
            "dest",
            "sink",
            Value::Null,
            STATUS_PENDING,
            "",
            engine.app_version(),
        ))
        .await?;

    // Same key twice → the retry is dropped; a different key delivers.
    engine
        .send_with_idempotency_key("dest", "a".to_string(), "t", "k1")
        .await?;
    engine
        .send_with_idempotency_key("dest", "a-again".to_string(), "t", "k1")
        .await?;
    engine
        .send_with_idempotency_key("dest", "b".to_string(), "t", "k2")
        .await?;

    // The mailbox holds exactly two messages, in send order.
    let m1 = provider
        .consume_notification("dest", "t", 0, "probe")
        .await?;
    let m2 = provider
        .consume_notification("dest", "t", 1, "probe")
        .await?;
    let m3 = provider
        .consume_notification("dest", "t", 2, "probe")
        .await?;
    assert_eq!(m1, Some(Value::String("a".into())));
    assert_eq!(m2, Some(Value::String("b".into())));
    assert_eq!(m3, None, "the duplicate keyed send was not delivered");
    Ok(())
}

/// send_bulk fans one call out to many workflows: each destination's recv
/// gets its own payload on its own topic, from a single engine call.
#[tokio::test]
async fn send_bulk_fans_out_to_many_workflows() -> Result<()> {
    use durare::SendMessage;

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("waiter", |ctx: DurableContext, topic: String| async move {
        let msg: Option<String> = ctx.recv(&topic, Duration::from_secs(5)).await?;
        Ok::<_, Error>(msg.unwrap_or_default())
    });

    let mut handles = Vec::new();
    for n in 0..3 {
        handles.push(
            engine
                .start::<_, String>(
                    "waiter",
                    format!("topic-{n}"),
                    WorkflowOptions::with_id(format!("bulk-dest-{n}")),
                )
                .await?,
        );
    }

    engine
        .send_bulk(&[
            SendMessage::new("bulk-dest-0", "zero".to_string(), "topic-0"),
            SendMessage::new("bulk-dest-1", "one".to_string(), "topic-1"),
            SendMessage::new("bulk-dest-2", "two".to_string(), "topic-2"),
        ])
        .await?;

    let mut got = Vec::new();
    for h in handles {
        got.push(h.result().await?);
    }
    assert_eq!(got, vec!["zero", "one", "two"]);
    Ok(())
}

/// A repeated idempotency key within one bulk call is a caller bug, rejected
/// up front; distinct keys are at-most-once across repeated calls.
#[tokio::test]
async fn send_bulk_idempotency_keys() -> Result<()> {
    use durare::SendMessage;

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("sink", |ctx: DurableContext, _: ()| async move {
        // Park so the mailbox can be inspected while the workflow is live.
        ctx.recv::<String>("done", Duration::from_secs(5)).await?;
        Ok::<_, Error>(())
    });
    let h = engine
        .start::<_, ()>("sink", (), WorkflowOptions::with_id("bulk-sink"))
        .await?;

    // Duplicate key inside one call: rejected, nothing delivered.
    let err = engine
        .send_bulk(&[
            SendMessage::new("bulk-sink", "a".to_string(), "t").idempotency_key("k1"),
            SendMessage::new("bulk-sink", "b".to_string(), "t").idempotency_key("k1"),
        ])
        .await
        .expect_err("duplicate keys must be rejected");
    assert!(
        err.to_string().contains("duplicate idempotency keys"),
        "{err}"
    );

    // The same keyed batch sent twice delivers once.
    let batch = [SendMessage::new("bulk-sink", "once".to_string(), "t").idempotency_key("k2")];
    engine.send_bulk(&batch).await?;
    engine.send_bulk(&batch).await?;
    let notifications = engine.list_workflow_notifications("bulk-sink").await?;
    let on_topic = notifications
        .iter()
        .filter(|n| n.topic.as_deref() == Some("t"))
        .count();
    assert_eq!(on_topic, 1, "keyed batch delivered at most once");

    engine.send("bulk-sink", "fin".to_string(), "done").await?;
    h.result().await?;
    Ok(())
}

/// One nonexistent destination rejects the whole batch — nothing is
/// delivered to the valid destinations either (all-or-nothing).
#[tokio::test]
async fn send_bulk_is_all_or_nothing_on_a_missing_destination() -> Result<()> {
    use durare::SendMessage;

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("sink", |ctx: DurableContext, _: ()| async move {
        ctx.recv::<String>("done", Duration::from_secs(5)).await?;
        Ok::<_, Error>(())
    });
    let h = engine
        .start::<_, ()>("sink", (), WorkflowOptions::with_id("bulk-real"))
        .await?;

    let err = engine
        .send_bulk(&[
            SendMessage::new("bulk-real", "hi".to_string(), "t"),
            SendMessage::new("no-such-workflow", "hi".to_string(), "t"),
        ])
        .await
        .expect_err("missing destination must reject the batch");
    assert!(matches!(err, Error::NonExistentWorkflow(_)), "{err}");
    let delivered = engine.list_workflow_notifications("bulk-real").await?;
    assert!(
        delivered.iter().all(|n| n.topic.as_deref() != Some("t")),
        "nothing delivered from the failed batch: {delivered:?}"
    );

    engine.send("bulk-real", "fin".to_string(), "done").await?;
    h.result().await?;
    Ok(())
}

/// ctx.send_bulk is one durable step: the batch lands, exactly one
/// `DBOS.send_bulk` checkpoint is recorded, and a replay does not re-deliver.
#[tokio::test]
async fn ctx_send_bulk_records_one_step() -> Result<()> {
    use durare::SendMessage;

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("fan", |ctx: DurableContext, _: ()| async move {
        ctx.send_bulk(&[
            SendMessage::new("bulk-rx-0", "a".to_string(), "t"),
            SendMessage::new("bulk-rx-1", "b".to_string(), "t"),
        ])
        .await?;
        Ok::<_, Error>(())
    });
    engine.register("rx", |ctx: DurableContext, _: ()| async move {
        let msg: Option<String> = ctx.recv("t", Duration::from_secs(5)).await?;
        Ok::<_, Error>(msg.unwrap_or_default())
    });

    let mut receivers = Vec::new();
    for n in 0..2 {
        receivers.push(
            engine
                .start::<_, String>("rx", (), WorkflowOptions::with_id(format!("bulk-rx-{n}")))
                .await?,
        );
    }
    engine
        .start::<_, ()>("fan", (), WorkflowOptions::with_id("bulk-fan"))
        .await?
        .result()
        .await?;

    assert_eq!(receivers.remove(0).result().await?, "a");
    assert_eq!(receivers.remove(0).result().await?, "b");

    let steps = engine.get_workflow_steps("bulk-fan").await?;
    let bulk_steps: Vec<_> = steps
        .iter()
        .filter(|s| s.name == "DBOS.send_bulk")
        .collect();
    assert_eq!(bulk_steps.len(), 1, "one checkpoint for the whole batch");
    Ok(())
}
