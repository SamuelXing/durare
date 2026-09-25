//! A `transaction_on` body that borrows the context. The body is an `AsyncFn`;
//! rustc cannot prove its future `Send` for every argument lifetime once it
//! holds a `&DurableContext`, so the workflow does not compile. The message is
//! rustc's ("implementation of `Send` is not general enough"), and it fires
//! for any use of the context in the body, nesting or not.
use durare::{workflow_fn, DurableEngine, SqliteDataSource};

fn register(engine: &mut DurableEngine, ds: SqliteDataSource) {
    engine.register(
        "nested",
        workflow_fn(move |ctx, (): ()| {
            let ds = ds.clone();
            Box::pin(async move {
                let inner_ds = ds.clone();
                ctx.transaction_on(&ds, "outer", async move |_conn| {
                    ctx.transaction_on(&inner_ds, "inner", async |_conn| Ok(())).await
                })
                .await
            })
        }),
    );
}

fn main() {
    let _ = register;
}
