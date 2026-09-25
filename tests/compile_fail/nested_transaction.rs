//! A transaction inside a transaction: `transaction`'s body must be `'static`,
//! and a workflow only ever holds `&DurableContext`, so the outer body cannot
//! capture the context to nest the inner call.
use durare::{DurableContext, Result};

async fn wf(ctx: &DurableContext, _: ()) -> Result<()> {
    ctx.transaction::<(), _>("outer", move |_tx| {
        Box::pin(async move {
            ctx.transaction::<(), _>("inner", |_tx| Box::pin(async { Ok(()) }))
                .await
        })
    })
    .await
}

fn main() {
    let _ = durare::erase(wf);
}
