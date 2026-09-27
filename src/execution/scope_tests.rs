//! Private execution identities let these tests move an in-flight call into a
//! different scope without requiring it to outlive its borrowed context.
use super::Execution;
use crate::Error;
use futures_util::FutureExt;
use std::future::{poll_fn, Future};
use std::panic::AssertUnwindSafe;
use std::task::Poll;

#[tokio::test]
async fn scope_identity_and_restoration_survive_pending_and_panics() {
    let owner = Execution::new("same-id");
    let other = Execution::new("same-id");
    owner
        .scope(|| async {
            assert!(owner.check_placement("step").is_ok());
            other
                .scope(|| async {
                    assert!(matches!(
                        owner.check_placement("step"),
                        Err(Error::DurableCallOutsideExecution { .. })
                    ));
                })
                .await;
            assert!(owner.check_placement("step").is_ok());
            let mut suspended = Box::pin(other.scope(std::future::pending::<()>));
            poll_fn(|cx| {
                assert!(suspended.as_mut().poll(cx).is_pending());
                assert!(owner.check_placement("step").is_ok());
                Poll::Ready(())
            })
            .await;
            drop(suspended);
            let panic = AssertUnwindSafe(other.scope(|| async { panic!("body panic") }))
                .catch_unwind()
                .await;
            assert!(panic.is_err());
            assert!(owner.check_placement("step").is_ok());
            let panic = std::panic::catch_unwind(AssertUnwindSafe(|| {
                other.scope(|| -> std::future::Ready<()> { panic!("construction panic") })
            }));
            assert!(panic.is_err());
            assert!(owner.check_placement("step").is_ok());
        })
        .await;
    assert!(owner.check_placement("step").is_err());
}

#[cfg(feature = "sqlite")]
async fn move_async_call_after_first_poll(kind: u8) -> crate::Result<()> {
    use crate::{DurableContext, DurableEngine, ErrorCode, SqliteProvider, WorkflowOptions};
    use std::{pin::Pin, sync::Arc};
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let provider = Arc::new(SqliteProvider::from_pool(pool.clone()));
    let ds = provider.system_datasource();
    let mut engine = DurableEngine::new(provider.clone()).await?;
    let captured_pool = pool.clone();
    engine.register("move", move |ctx: DurableContext, _: ()| {
        let pool = captured_pool.clone();
        let ds = ds.clone();
        async move {
            let lease = pool.acquire().await?;
            let mut call: Pin<Box<dyn Future<Output = crate::Result<()>> + Send + '_>> = match kind
            {
                0 => Box::pin(ctx.transaction_on(&ds, "native", async |_conn| Ok(()))),
                1 => Box::pin(async { ctx.patch("marker").await.map(|_| ()) }),
                _ => Box::pin(ctx.deprecate_patch("marker")),
            };
            // The one connection is held: the call passes its first check and
            // then suspends in the provider. No timing-based sleep is needed.
            poll_fn(|cx| {
                assert!(call.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(lease);
            let another_run = Execution::new(ctx.workflow_id());
            let error = another_run.scope(|| call).await.unwrap_err();
            assert_eq!(error.code(), ErrorCode::DurableCallOutsideExecution);
            assert_eq!(ctx.current_step_id(), if kind == 0 { 1 } else { 0 });
            Ok(())
        }
    });
    engine
        .start::<_, ()>("move", (), WorkflowOptions::with_id("same-id"))
        .await?
        .result()
        .await?;
    assert!(engine.get_workflow_steps("same-id").await?.is_empty());
    pool.close().await;
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn native_transaction_checks_scope_after_its_first_poll() -> crate::Result<()> {
    move_async_call_after_first_poll(0).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn patch_checks_scope_after_its_first_poll() -> crate::Result<()> {
    move_async_call_after_first_poll(1).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn deprecated_patch_checks_scope_after_its_first_poll() -> crate::Result<()> {
    move_async_call_after_first_poll(2).await
}
