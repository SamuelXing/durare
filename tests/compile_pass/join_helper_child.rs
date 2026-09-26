//! In-scope concurrency stays ordinary: `join!` over a step, a helper that
//! takes `&DurableContext`, and a child workflow, all borrowing the one lent
//! context. A closure handler through `workflow_fn` and a capture-free async
//! closure register alongside the `async fn`.
use durare::{workflow_fn, DurableContext, DurableEngine, Error, Result, WorkflowOptions};

async fn helper(ctx: &DurableContext) -> Result<i64> {
    ctx.step("h", |step| async move {
        let _ = step.attempt;
        Ok::<_, Error>(2)
    })
    .await
}

async fn wf(ctx: &DurableContext, _: ()) -> Result<i64> {
    let (a, b, child) = tokio::join!(
        ctx.step("a", |_| async { Ok::<_, Error>(1) }),
        helper(ctx),
        ctx.start_workflow::<(), i64>("child", (), WorkflowOptions::with_id("c")),
    );
    let c = child?.result().await?;
    Ok(a? + b? + c)
}

fn register(engine: &mut DurableEngine) {
    engine.register("wf", wf);
    let greeting = String::from("hi");
    engine.register(
        "closure",
        workflow_fn(move |ctx, n: i64| {
            let greeting = greeting.clone();
            Box::pin(async move {
                let s = ctx.step("s", |_| async move { Ok::<_, Error>(greeting) }).await?;
                Ok::<_, Error>(format!("{s} {n}"))
            })
        }),
    );
    engine.register("capture_free", async |ctx: &DurableContext, n: i64| {
        ctx.step("s", |_| async move { Ok::<_, Error>(n + 1) }).await
    });
}

fn main() {
    let _ = register;
}
