use durare::{DurableContext, Result};
async fn workflow(ctx: &DurableContext, _: ()) -> Result<()> {
    ctx.transaction("tx", move |_tx| {
        drop(ctx.step("hidden", |_| async { Ok(()) }));
        Box::pin(async { Ok(()) })
    }).await
}
fn main() { let _ = workflow; }
