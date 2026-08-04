//! Trigger durable workflows over HTTP: an axum handler starts a workflow and
//! the DBOS idempotency-key header makes retried requests attach to the same
//! run instead of repeating its effects.
//!
//! The whole integration is two lines of glue — there is no framework
//! adapter to configure, because durability lives in the engine, not the
//! transport:
//!   * read the `dbos-idempotency-key` header (the cross-SDK name; any
//!     caller-supplied stable id works) and make it the workflow id;
//!   * `engine.start(...)` — a repeated id attaches to the existing run, so a
//!     client or proxy retrying the POST cannot double-charge.
//!
//! The same shape works in any HTTP framework: extract a stable id, pass it
//! as `WorkflowOptions::with_id`. Responses can be immediate (return the
//! workflow id, poll later — shown here) or synchronous (await the handle).
//!
//! ```text
//! cargo run --example http_trigger --features admin
//! # then, in another terminal (same key twice — one workflow):
//! curl -X POST localhost:8080/orders -H 'dbos-idempotency-key: order-1001' -d '1001'
//! curl -X POST localhost:8080/orders -H 'dbos-idempotency-key: order-1001' -d '1001'
//! ```
//!
//! (Requires the `admin` feature only because that is what pulls axum into
//! this crate's dev graph — your application depends on axum directly.)

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::Router;
use durare::{DurableContext, DurableEngine, Error, InMemoryProvider, Result, WorkflowOptions};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Stand-in for a payment API that must never run twice for one order.
static CHARGES: AtomicU32 = AtomicU32::new(0);

async fn process_order(ctx: DurableContext, order_id: String) -> Result<String> {
    let charge = ctx
        .step("charge", || async {
            CHARGES.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(format!("ch_{order_id}"))
        })
        .await?;
    Ok(charge)
}

async fn create_order(
    State(engine): State<Arc<DurableEngine>>,
    headers: HeaderMap,
    body: String,
) -> std::result::Result<String, (axum::http::StatusCode, String)> {
    // The caller's idempotency key becomes the workflow id: a retried request
    // (same key) attaches to the same run — started exactly once.
    let opts = match headers
        .get("dbos-idempotency-key")
        .and_then(|v| v.to_str().ok())
    {
        Some(key) => WorkflowOptions::with_id(key),
        None => WorkflowOptions::default(), // no key: every request is a fresh run
    };
    let handle = engine
        .start::<String, String>("process_order", body, opts)
        .await
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
    let id = handle.id().to_string();
    // Respond immediately; the run continues durably. (Awaiting `handle`
    // instead gives a synchronous response.)
    Ok(id)
}

async fn charges() -> String {
    format!("charges: {}\n", CHARGES.load(Ordering::SeqCst))
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
    engine.register("process_order", process_order);
    let engine = Arc::new(engine);
    engine.launch().await?;

    let app = Router::new()
        .route("/orders", post(create_order))
        .route("/charges", get(charges))
        .with_state(engine);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .map_err(|e| Error::app(format!("bind failed: {e}")))?;
    println!("POST an order:   curl -X POST localhost:8080/orders -H 'dbos-idempotency-key: order-1001' -d '1001'");
    println!("check the count: curl localhost:8080/charges   (stays 1 however often you retry)");
    axum::serve(listener, app)
        .await
        .map_err(|e| Error::app(format!("serve failed: {e}")))?;
    Ok(())
}
