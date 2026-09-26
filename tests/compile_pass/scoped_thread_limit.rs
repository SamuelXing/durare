//! A deliberate limit of the guarantee: scoped threads can borrow the context.
//! This compiles; it is not a recommended durable-concurrency pattern. Borrowing
//! does not enforce deterministic construction order or propagate task-local guards.
use durare::{DurableContext, Result};
async fn workflow(ctx: &DurableContext, _: ()) -> Result<()> {
    let runtime = tokio::runtime::Handle::current();
    std::thread::scope(|scope| {
        scope.spawn(|| runtime.block_on(ctx.step("scoped", |_| async { Ok(()) })))
            .join().unwrap()
    })
}
fn main() { let _ = workflow; }
