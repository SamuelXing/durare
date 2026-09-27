//! Native transactions keep their identities when polling order changes.
#![cfg(feature = "sqlite")]

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, SqliteDataSource,
    SqliteProvider, StateProvider, TransactionOptions, WorkflowOptions,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};

mod common;

/// Both the single-commit and separate-database paths use native connections.
async fn setup(external: bool) -> Result<(DurableEngine, SqliteDataSource, sqlx::SqlitePool)> {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    let (provider, ds): (Arc<dyn StateProvider>, _) = if external {
        (
            Arc::new(InMemoryProvider::new()),
            SqliteDataSource::new(pool.clone()).await?,
        )
    } else {
        let provider = Arc::new(SqliteProvider::from_pool(pool.clone()));
        let ds = provider.system_datasource();
        (provider, ds)
    };
    Ok((DurableEngine::new(provider).await?, ds, pool))
}

#[tokio::test]
async fn native_transactions_keep_positions_when_awaited_in_reverse_order() -> Result<()> {
    for external in [false, true] {
        let (mut engine, ds, pool) = setup(external).await?;
        engine.register("reverse", move |ctx: DurableContext, _: ()| {
            let ds = ds.clone();
            async move {
                let metadata = ctx.clone();
                let first = ctx.transaction_on(&ds, "first", async move |conn| {
                    tokio::task::yield_now().await;
                    let id = metadata.workflow_id();
                    let value: String = sqlx::query_scalar("SELECT ?")
                        .bind(id)
                        .fetch_one(&mut *conn)
                        .await?;
                    Ok(value)
                });
                let middle = ctx.step("middle", || async { Ok(()) });
                let last =
                    ctx.transaction_on_with(&ds, TransactionOptions::new("last"), async |conn| {
                        let n: i64 = sqlx::query_scalar("SELECT 42")
                            .fetch_one(&mut *conn)
                            .await?;
                        Ok(n)
                    });
                assert_eq!(last.await?, 42);
                middle.await?;
                assert_eq!(first.await?, "reverse");
                Ok(())
            }
        });
        engine
            .start::<_, ()>("reverse", (), WorkflowOptions::with_id("reverse"))
            .await?
            .result()
            .await?;
        assert_eq!(
            common::recorded(&engine, "reverse").await?,
            [
                (0, "first".into()),
                (1, "middle".into()),
                (2, "last".into())
            ]
        );
        assert_eq!(engine.verify_replay("reverse").await?.divergence, None);
        pool.close().await;
    }
    Ok(())
}

/// A name check cannot detect two same-named transactions exchanging outputs.
#[tokio::test]
async fn native_transaction_values_survive_a_different_replay_poll_order() -> Result<()> {
    for external in [false, true] {
        let (mut engine, ds, pool) = setup(external).await?;
        let reversed = Arc::new(AtomicBool::new(true));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicUsize::new(0));
        let order = reversed.clone();
        let outputs = observed.clone();
        let count = calls.clone();
        engine.register("values", move |ctx: DurableContext, _: ()| {
            let (ds, order, outputs, count) =
                (ds.clone(), order.clone(), outputs.clone(), count.clone());
            async move {
                let first_count = count.clone();
                let first = ctx.transaction_on(&ds, "same", async move |_conn| {
                    first_count.fetch_add(1, Ordering::SeqCst);
                    Ok(11)
                });
                let second = ctx.transaction_on_with(
                    &ds,
                    TransactionOptions::new("same"),
                    async move |_conn| {
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(22)
                    },
                );
                // Only execution order changes; construction order is fixed.
                let pair = if order.load(Ordering::SeqCst) {
                    let b = second.await?;
                    (first.await?, b)
                } else {
                    let a = first.await?;
                    (a, second.await?)
                };
                outputs.lock().unwrap().push(pair);
                Ok(pair)
            }
        });
        assert_eq!(
            engine
                .start::<_, (i32, i32)>("values", (), WorkflowOptions::with_id("values"))
                .await?
                .result()
                .await?,
            (11, 22)
        );
        reversed.store(false, Ordering::SeqCst);
        assert_eq!(engine.verify_replay("values").await?.divergence, None);
        assert_eq!(*observed.lock().unwrap(), [(11, 22), (11, 22)]);
        assert_eq!(calls.load(Ordering::SeqCst), 2, "replay ran a body");
        pool.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn dropped_native_transactions_spend_positions_without_running() -> Result<()> {
    let (mut engine, ds, pool) = setup(true).await?;
    engine.register("drop", move |ctx: DurableContext, _: ()| {
        let ds = ds.clone();
        async move {
            let first = ctx.transaction_on(&ds, "dropped", async |_conn| {
                panic!("a dropped native body ran");
                #[allow(unreachable_code)]
                Ok(())
            });
            let second = ctx.transaction_on_with(
                &ds,
                TransactionOptions::new("dropped-with"),
                async |_conn| {
                    panic!("a dropped native body ran");
                    #[allow(unreachable_code)]
                    Ok(())
                },
            );
            drop((first, second));
            ctx.step("after", || async { Ok(()) }).await
        }
    });
    engine
        .start::<_, ()>("drop", (), WorkflowOptions::with_id("drop"))
        .await?
        .result()
        .await?;
    assert_eq!(
        common::recorded(&engine, "drop").await?,
        [(2, "after".into())]
    );
    let witnesses: i64 = sqlx::query_scalar("SELECT count(*) FROM transaction_completion")
        .fetch_one(&pool)
        .await?;
    assert_eq!(witnesses, 0);
    assert_eq!(engine.verify_replay("drop").await?.divergence, None);
    pool.close().await;
    Ok(())
}

/// Deferring a nested call's first poll must not turn it into legal work.
#[tokio::test]
async fn a_native_transaction_constructed_in_a_body_stays_refused_outside_it() -> Result<()> {
    let (mut engine, ds, pool) = setup(false).await?;
    engine.register("nested", move |ctx: DurableContext, _: ()| {
        let ds = ds.clone();
        async move {
            let mut escaped = None;
            ctx.step("outer", || {
                escaped = Some(ctx.transaction_on(&ds, "inner", async |_conn| Ok(())));
                async { Ok(()) }
            })
            .await?;
            let outcome = escaped.unwrap().await;
            assert!(
                matches!(outcome, Err(Error::NestedDurableCall { .. })),
                "{outcome:?}"
            );
            assert_eq!(ctx.current_step_id(), 1);
            ctx.step("after", || async { Ok(()) }).await
        }
    });
    engine
        .start::<_, ()>("nested", (), WorkflowOptions::with_id("nested"))
        .await?
        .result()
        .await?;
    assert_eq!(
        common::recorded(&engine, "nested").await?,
        [(0, "outer".into()), (1, "after".into())]
    );
    pool.close().await;
    Ok(())
}
