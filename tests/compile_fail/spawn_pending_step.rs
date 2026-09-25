//! (ii) The `PendingStep` itself handed to `tokio::spawn`: it borrows the
//! context for its whole life, so it is not `'static` either.
use durare::{DurableContext, Error, Result};

async fn wf(ctx: &DurableContext, _: ()) -> Result<i64> {
    tokio::spawn(ctx.step("x", |_| async { Ok::<_, Error>(1) }))
        .await
        .unwrap()
}

fn main() {
    let _ = durare::erase(wf);
}
