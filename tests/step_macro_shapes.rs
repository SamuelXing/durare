//! The signatures `#[durare::step]` and `#[durare::transaction]` accept.
//!
//! Both emit a plain `fn` returning a `PendingStep`, which borrows the context
//! and therefore needs a lifetime the signature can name. Leaning on elision
//! for that would quietly narrow what the macros take — a second reference
//! stops compiling, and a generic step cannot be written at all, because the
//! bound it would need is on an anonymous lifetime. The macro introduces its
//! own lifetime instead, so these shapes go on compiling; most of this file is
//! the test.

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

/// A second borrow alongside the context.
#[durare::step]
async fn count_chars(ctx: &DurableContext, s: &str) -> Result<usize> {
    Ok(s.len())
}

/// A lifetime the caller named.
#[durare::step]
async fn first_word<'a>(ctx: &DurableContext, s: &'a str) -> Result<String> {
    Ok(s.split(' ').next().unwrap_or_default().to_string())
}

/// A generic step: the type parameter has to outlive the borrow, which only the
/// macro can express.
#[durare::step]
async fn echo<T: Serialize + DeserializeOwned + Send>(ctx: &DurableContext, v: T) -> Result<T> {
    Ok(v)
}

/// The same for a transactional step, whose arguments the runtime re-clones per
/// attempt (hence `Clone`, and the `Sync + 'static` the body closure needs).
#[cfg(feature = "sqlite")]
#[durare::transaction]
async fn echo_txn<T: Serialize + DeserializeOwned + Send + Sync + Clone + 'static>(
    ctx: &DurableContext,
    tx: &mut durare::Tx<'_>,
    v: T,
) -> Result<T> {
    tx.execute("SELECT 1", &durare::params![]).await?;
    Ok(v)
}

/// The shapes above compile, and still record in the order they are written.
#[tokio::test]
async fn the_accepted_shapes_run_and_keep_their_positions() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("probe", |ctx: DurableContext, _: ()| async move {
        let owned = String::from("hello world");
        // Built in this order, awaited in the reverse.
        let counted = count_chars(&ctx, &owned);
        let worded = first_word(&ctx, &owned);
        let echoed = echo(&ctx, 7_i64);
        assert_eq!(echoed.await?, 7);
        assert_eq!(worded.await?, "hello");
        assert_eq!(counted.await?, 11);
        Ok::<_, durare::Error>(0_i64)
    });
    engine
        .start::<_, i64>("probe", (), WorkflowOptions::with_id("wf"))
        .await?
        .result()
        .await?;

    let seen: Vec<(i32, String)> = engine
        .get_workflow_steps("wf")
        .await?
        .into_iter()
        .map(|step| (step.step_id, step.name))
        .collect();
    assert_eq!(
        seen,
        [
            (0, "count_chars".to_string()),
            (1, "first_word".to_string()),
            (2, "echo".to_string())
        ]
    );
    Ok(())
}
