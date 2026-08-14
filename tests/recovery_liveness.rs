//! Recovery liveness: a run parked on `recv` must not stall `recover()`.
//!
//! Regression for the inline-dispatch bug where recovery ran each pending
//! workflow to completion before returning: one run waiting on a message
//! blocked the caller of `recover()` — and every pending run behind it in
//! the dispatch loop — for its entire wait. The DBOS SDKs return handles
//! and resume execution in the background; so do we now.

use durare::{DurableContext, DurableEngine, SqliteProvider, WorkflowOptions};
use std::sync::Arc;
use std::time::Duration;

async fn engine_over(url: &str) -> DurableEngine {
    let provider = Arc::new(SqliteProvider::connect(url).await.expect("connect"));
    let mut b = DurableEngine::builder(provider);
    b.register("parked", |ctx: DurableContext, _input: i32| async move {
        // Parks for up to ten minutes; the property under test is that
        // nobody is made to wait alongside it.
        let got: Option<String> = ctx.recv("go", Duration::from_secs(600)).await?;
        Ok::<String, durare::Error>(got.unwrap_or_default())
    });
    b.build().await.expect("engine builds")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_parked_on_recv_does_not_stall_recover() {
    let db = std::env::temp_dir().join(format!(
        "durare-recovery-liveness-{}.db",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after 1970")
            .as_nanos()
    ));
    let url = format!("sqlite://{}?mode=rwc", db.display());

    // "Process 1": start a run, let it reach the recv, then go away without
    // completing it — the crash this recovery exists for.
    let first = engine_over(&url).await;
    let _handle: durare::WorkflowHandle<String> = first
        .start("parked", 7, WorkflowOptions::with_id("parked-1"))
        .await
        .expect("start");
    tokio::time::sleep(Duration::from_millis(300)).await;
    let _ = first.shutdown(Duration::from_millis(200)).await;
    drop(first);

    // "Process 2": recovery must return promptly even though the run it
    // re-dispatches will sit on `recv` until a message arrives. Before the
    // fix this timed out: recover() awaited the full recv wait inline.
    let second = engine_over(&url).await;
    let recovered = tokio::time::timeout(Duration::from_secs(5), second.recover())
        .await
        .expect("recover() must not wait for the parked run")
        .expect("recover succeeds");
    assert_eq!(recovered, 1, "the parked run is re-dispatched");

    // The recovered run is live in the background: one send completes it.
    second
        .send("parked-1", "approved".to_string(), "go")
        .await
        .expect("send");
    let handle: durare::WorkflowHandle<String> = second
        .start("parked", 7, WorkflowOptions::with_id("parked-1"))
        .await
        .expect("same-id start adopts the existing run");
    let out = tokio::time::timeout(Duration::from_secs(10), handle)
        .await
        .expect("completes once the message lands")
        .expect("workflow result");
    assert_eq!(out, "approved");

    let _ = second.shutdown(Duration::from_secs(2)).await;
    let _ = std::fs::remove_file(&db);
}
