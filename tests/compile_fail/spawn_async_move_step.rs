//! (i) A step inside `tokio::spawn(async move { .. })`: the task must be
//! `'static`, the context is only borrowed.
use durare::{DurableContext, Error, Result};

async fn wf(ctx: &DurableContext, _: ()) -> Result<i64> {
    tokio::spawn(async move { ctx.step("x", |_| async { Ok::<_, Error>(1) }).await })
        .await
        .unwrap()
}

fn main() {
    let _ = durare::erase(wf);
}
