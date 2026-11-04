//! Export durare's spans over OTLP to Grafana Tempo.
//!
//! durare instruments every workflow and step with `tracing` spans carrying
//! the cross-SDK `dbos.*` attributes (see the `observability` module docs).
//! Nothing in the engine knows about OpenTelemetry: a `tracing-opentelemetry`
//! layer on the subscriber forwards the spans to an OTLP endpoint, and from
//! there a Collector routes them to Tempo, Datadog, or anything else.
//!
//! Start the local stack (Collector, Tempo, Grafana), run the example, then
//! open Grafana and search for the trace id it prints:
//!
//! ```text
//! docker compose -f examples/telemetry/docker-compose.yml up -d
//! cargo run --example telemetry
//! open http://localhost:3000/explore          # Tempo data source is provisioned
//! ```
//!
//! Without the stack the example still runs; the exporter warns that the
//! spans were dropped. The endpoint comes from `OTEL_EXPORTER_OTLP_ENDPOINT`
//! (default `http://localhost:4318`). For Datadog see the `observability`
//! module docs.

use durare::{DurableContext, DurableEngine, InMemoryProvider, Result, WorkflowOptions};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[durare::step]
async fn reserve_inventory(ctx: &DurableContext, sku: String) -> Result<u32> {
    tokio::time::sleep(Duration::from_millis(40)).await; // pretend to call the warehouse
    Ok(sku.len() as u32 * 100)
}

#[durare::step]
async fn charge_card(ctx: &DurableContext, cents: u32) -> Result<String> {
    tokio::time::sleep(Duration::from_millis(60)).await; // pretend to call the processor
    Ok(format!("charge-{cents}"))
}

#[durare::step]
async fn book_courier(ctx: &DurableContext, charge: String) -> Result<String> {
    tokio::time::sleep(Duration::from_millis(30)).await;
    Ok(format!("courier for {charge}"))
}

// A child workflow: its span nests under the parent's in the exported trace.
#[durare::workflow]
async fn dispatch_order(ctx: DurableContext, charge: String) -> Result<()> {
    book_courier(&ctx, charge).await?;
    Ok(())
}

#[durare::workflow]
async fn checkout(ctx: DurableContext, sku: String) -> Result<String> {
    let cents = reserve_inventory(&ctx, sku).await?;
    let charge = charge_card(&ctx, cents).await?;
    ctx.start_workflow::<_, ()>("dispatch_order", charge.clone(), WorkflowOptions::default())
        .await?
        .result()
        .await?;
    Ok(charge)
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    // Spans leave in batches over OTLP/HTTP; the SDK reads the endpoint from
    // the standard environment variables.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()?;
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name("durare-telemetry")
                .build(),
        )
        .build();

    // The bridge layer turns durare's `tracing` spans into OTel spans named by
    // `otel.name` and carrying `otel.status_code`.
    tracing_subscriber::registry()
        .with(LevelFilter::INFO)
        .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("durare-telemetry")))
        .init();

    let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;

    // A workflow started inside an instrumented span parents under it. This
    // stand-in for the HTTP handler also gives us a trace id to print.
    let request = tracing::info_span!("http.request", http.route = "/checkout");
    let trace_id = request.context().span().span_context().trace_id();
    let charge: String = async {
        engine
            .start_with(
                Checkout,
                "sku-4711".into(),
                WorkflowOptions::with_id("order-1001"),
            )
            .await?
            .await
    }
    .instrument(request)
    .await?;
    println!("[done] {charge}");
    println!("[trace] {trace_id}  <- search this id in Grafana Explore (Tempo)");

    // Flush the batch before exiting, or the last spans never leave the process.
    engine.shutdown(Duration::from_secs(2)).await?;
    provider.shutdown()?;
    Ok(())
}
