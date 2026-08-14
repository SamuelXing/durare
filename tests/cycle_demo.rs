//! Demonstrations of the two "cycle" dangers the engine does NOT guard against
//! (matching Go/Python, which don't either), plus the prevention patterns.
//!
//! Danger 1 — same-queue parent/child deadlock: a parent claimed from a
//!   concurrency-limited queue holds its slot while blocked on a child enqueued
//!   on the SAME queue; the child can never be claimed.
//! Danger 2 — unbounded recursion: nothing caps a workflow spawning itself;
//!   instances grow without bound.
//!
//! Each danger test proves the bad behavior under a tokio timeout so the suite
//! itself never hangs. The prevention tests show the same shapes made safe.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions, WorkflowQueue,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn fast_queue(name: &str) -> WorkflowQueue {
    WorkflowQueue::new(name).base_polling_interval(Duration::from_millis(10))
}

/// DANGER 1: parent and child share a worker_concurrency(1) queue.
/// The parent occupies the only slot while awaiting the child, which needs
/// that slot to run. Neither ever completes — a wait-for cycle through the
/// queue's capacity.
#[tokio::test]
async fn danger_same_queue_parent_child_deadlocks() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("leaf", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        // Child on the SAME queue the parent was claimed from.
        let child = ctx
            .start_workflow::<_, i64>("leaf", 7_i64, WorkflowOptions::default().queue("only"))
            .await?;
        let v = child.result().await?; // blocks forever: child can't get the slot
        Ok::<_, Error>(v)
    });
    engine.register_queue(fast_queue("only").worker_concurrency(1));
    engine.launch().await?;

    let handle = engine
        .start::<_, i64>(
            "parent",
            (),
            WorkflowOptions::with_id("dead-parent").queue("only"),
        )
        .await?;

    // Give it far longer than a healthy run would need. It must NOT finish.
    let outcome = tokio::time::timeout(Duration::from_secs(2), handle.result()).await;
    assert!(
        outcome.is_err(),
        "expected deadlock: parent holds the only slot while waiting for a \
         child that needs it — but it completed"
    );

    engine.shutdown(Duration::from_millis(100)).await?;
    Ok(())
}

/// DANGER 2: a workflow that spawns itself with no base case. The engine
/// imposes no depth cap, so instances keep multiplying until something
/// external stops them. We watch the spawn counter blow past a threshold,
/// then shut down.
#[tokio::test]
async fn danger_unbounded_recursion_grows_without_limit() -> Result<()> {
    static SPAWNS: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("fork_bomb", |ctx: DurableContext, depth: i64| async move {
        SPAWNS.fetch_add(1, Ordering::SeqCst);
        // No base case: always spawn the next generation (direct run, no queue).
        let child = ctx
            .start_workflow::<_, ()>("fork_bomb", depth + 1, WorkflowOptions::default())
            .await?;
        child.result().await?; // chain: each generation waits on the next
        Ok::<_, Error>(())
    });
    engine.launch().await?;

    let _ = engine
        .start::<_, ()>("fork_bomb", 0_i64, WorkflowOptions::with_id("bomb-0"))
        .await?;

    // Within a short window the chain should already be dozens of generations
    // deep — nothing in the engine is slowing or capping it.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let spawned = SPAWNS.load(Ordering::SeqCst);
    assert!(
        spawned > 50,
        "expected unbounded growth; only {spawned} generations spawned"
    );
    eprintln!("fork_bomb reached {spawned} generations in 500ms — no engine cap");

    engine.shutdown(Duration::from_millis(100)).await?;
    Ok(())
}

/// PREVENTION 1: put children on a different queue (or run them direct).
/// Identical shape to danger 1, but the child's queue isn't capacity-coupled
/// to the parent's — completes immediately.
#[tokio::test]
async fn prevention_separate_queues_break_the_wait_cycle() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("leaf", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        let child = ctx
            .start_workflow::<_, i64>("leaf", 7_i64, WorkflowOptions::default().queue("kids"))
            .await?;
        Ok::<_, Error>(child.result().await?)
    });
    engine.register_queue(fast_queue("parents").worker_concurrency(1));
    engine.register_queue(fast_queue("kids").worker_concurrency(1));
    engine.launch().await?;

    let handle = engine
        .start::<_, i64>(
            "parent",
            (),
            WorkflowOptions::with_id("ok-parent").queue("parents"),
        )
        .await?;
    let v = tokio::time::timeout(Duration::from_secs(5), handle.result())
        .await
        .expect("separate queues must not deadlock")?;
    assert_eq!(v, 7);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// PREVENTION 2: thread a depth counter through the input and refuse to spawn
/// past a cap — recursion with a base case. The chain terminates and the root
/// completes.
#[tokio::test]
async fn prevention_depth_guard_bounds_recursion() -> Result<()> {
    const MAX_DEPTH: i64 = 5;
    static SPAWNS: AtomicUsize = AtomicUsize::new(0);

    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("bounded", |ctx: DurableContext, depth: i64| async move {
        SPAWNS.fetch_add(1, Ordering::SeqCst);
        if depth >= MAX_DEPTH {
            return Ok::<_, Error>(depth); // base case: stop the cycle here
        }
        let child = ctx
            .start_workflow::<_, i64>("bounded", depth + 1, WorkflowOptions::default())
            .await?;
        Ok::<_, Error>(child.result().await?)
    });
    engine.launch().await?;

    let handle = engine
        .start::<_, i64>("bounded", 0_i64, WorkflowOptions::with_id("bounded-0"))
        .await?;
    let deepest = tokio::time::timeout(Duration::from_secs(5), handle.result())
        .await
        .expect("bounded recursion must terminate")?;

    assert_eq!(deepest, MAX_DEPTH);
    assert_eq!(SPAWNS.load(Ordering::SeqCst), (MAX_DEPTH + 1) as usize);

    engine.shutdown(Duration::from_secs(1)).await?;
    Ok(())
}

/// PREVENTION 3: a workflow timeout turns a would-be-forever wait into a
/// bounded failure. Same deadlock shape as danger 1, but the parent carries a
/// deadline, so instead of hanging indefinitely it is cancelled.
#[tokio::test]
async fn prevention_timeout_bounds_the_deadlock() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("leaf", |_ctx: DurableContext, n: i64| async move {
        Ok::<_, Error>(n)
    });
    engine.register("parent", |ctx: DurableContext, _: ()| async move {
        let child = ctx
            .start_workflow::<_, i64>("leaf", 7_i64, WorkflowOptions::default().queue("only"))
            .await?;
        Ok::<_, Error>(child.result().await?)
    });
    engine.register_queue(fast_queue("only").worker_concurrency(1));
    engine.launch().await?;

    let mut opts = WorkflowOptions::with_id("timed-parent").queue("only");
    opts.timeout = Some(Duration::from_millis(300));
    let handle = engine.start::<_, i64>("parent", (), opts).await?;

    // Not a hang: the deadline fires and the workflow resolves as an error
    // (cancelled) well before our outer guard.
    let outcome = tokio::time::timeout(Duration::from_secs(5), handle.result()).await;
    let inner = outcome.expect("the workflow deadline must fire, not hang");
    assert!(
        inner.is_err(),
        "the deadlocked parent must be cancelled by its timeout, not succeed"
    );

    engine.shutdown(Duration::from_millis(100)).await?;
    Ok(())
}
