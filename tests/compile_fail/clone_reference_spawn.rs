use durare::{DurableContext, Result};
async fn workflow(ctx: &DurableContext, _: ()) -> Result<()> {
    let copy = Clone::clone(&ctx);
    tokio::spawn(async move { copy.step("escaped", |_| async { Ok(()) }).await }).await.unwrap()
}
fn main() { let _ = workflow; }
