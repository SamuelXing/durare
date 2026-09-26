//! Known ergonomic limitation, not a safety benefit: a boxed HRTB callback cannot
//! keep the workflow borrow across an await. Copy metadata before boxing instead.
use durare::{workflow_fn, DurableEngine, SqliteDataSource};
fn register(engine: &mut DurableEngine, ds: SqliteDataSource) {
    engine.register("metadata", workflow_fn(move |ctx, (): ()| {
        let ds = ds.clone();
        Box::pin(async move {
            ctx.transaction_on(&ds, "tx", |_conn| Box::pin(async move {
                tokio::task::yield_now().await;
                Ok(ctx.workflow_id().to_owned())
            })).await
        })
    }));
}
fn main() { let _ = register; }
