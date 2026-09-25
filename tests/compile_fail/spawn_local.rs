//! (iii) `spawn_local` drops the `Send` requirement but keeps `'static`, so a
//! borrowed context cannot escape into a local task either.
use durare::{DurableContext, Error, Result};

async fn wf(ctx: &DurableContext, _: ()) -> Result<i64> {
    tokio::task::spawn_local(async move { ctx.step("x", |_| async { Ok::<_, Error>(1) }).await })
        .await
        .unwrap()
}

fn main() {
    let _ = durare::erase(wf);
}
