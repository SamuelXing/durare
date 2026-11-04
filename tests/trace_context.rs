//! Trace-context propagation (`otel` feature): a run that starts elsewhere
//! joins the trace of whatever started it.

use durare::{
    DurableContext, DurableEngine, Error, InMemoryProvider, Result, StateProvider, WorkflowOptions,
    WorkflowQueue,
};
use opentelemetry::trace::{SpanId, TraceContextExt, TraceId, TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;

const CARRIER: &str = "dbos.otelContext";

/// An OpenTelemetry pipeline exporting into memory, installed for this test's
/// thread; keep the guard and the provider alive for the test's whole body.
fn pipeline() -> (
    InMemorySpanExporter,
    SdkTracerProvider,
    tracing::subscriber::DefaultGuard,
) {
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("test"));
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    (exporter, provider, guard)
}

fn attr(span: &SpanData, key: &str) -> Option<String> {
    span.attributes
        .iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().into_owned())
}

/// The exported `workflow` span for `id`; call after the engine is shut down,
/// which is when the last span has closed.
fn workflow_span(exporter: &InMemorySpanExporter, id: &str) -> SpanData {
    exporter
        .get_finished_spans()
        .expect("exporter is live")
        .into_iter()
        .find(|span| {
            attr(span, "dbos.operation.workflow_id").as_deref() == Some(id)
                && attr(span, "dbos.operation.type").as_deref() == Some("workflow")
        })
        .unwrap_or_else(|| panic!("no workflow span exported for {id}"))
}

async fn engine() -> Result<(DurableEngine, Arc<InMemoryProvider>)> {
    let provider = Arc::new(InMemoryProvider::new());
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register("echo", |_ctx: DurableContext, input: String| async move {
        Ok::<_, Error>(input)
    });
    engine.register_queue(WorkflowQueue::new("q").base_polling_interval(Duration::from_millis(10)));
    Ok((engine, provider))
}

/// Started under a caller span, a queued run — whose dispatcher has no live
/// parent — joins the caller's trace when asked to, and stays a root when not.
#[tokio::test]
async fn a_queued_run_joins_the_callers_trace_when_asked() -> Result<()> {
    let (exporter, _provider, _guard) = pipeline();
    let (engine, provider) = engine().await?;
    engine.launch().await?;

    let caller = tracing::info_span!("caller");
    let caller_context = caller.context().span().span_context().clone();
    assert!(
        caller_context.is_valid(),
        "the layer tracks the caller span"
    );
    async {
        for (id, opts) in [
            (
                "propagating",
                WorkflowOptions::with_id("propagating").propagate_trace_context(),
            ),
            ("plain", WorkflowOptions::with_id("plain")),
        ] {
            let out: String = engine
                .start::<_, String>("echo", id.to_string(), opts.queue("q"))
                .await?
                .result()
                .await?;
            assert_eq!(out, id);
        }
        Ok::<_, Error>(())
    }
    .instrument(caller)
    .await?;
    engine.shutdown(Duration::from_secs(2)).await?;

    // The carrier is persisted with the propagating workflow only ...
    let status = provider
        .get_workflow_status("propagating")
        .await?
        .expect("row");
    let traceparent = status.attributes.as_ref().unwrap()[CARRIER]["traceparent"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(traceparent.starts_with(&format!("00-{}-", caller_context.trace_id())));
    let plain = provider.get_workflow_status("plain").await?.expect("row");
    assert!(plain.attributes.is_none());

    // ... and only that run joins the caller's trace.
    let span = workflow_span(&exporter, "propagating");
    assert_eq!(span.span_context.trace_id(), caller_context.trace_id());
    assert_eq!(span.parent_span_id, caller_context.span_id());
    let span = workflow_span(&exporter, "plain");
    assert_ne!(span.span_context.trace_id(), caller_context.trace_id());
    assert_eq!(span.parent_span_id, SpanId::INVALID);
    Ok(())
}

/// A carrier another SDK's enqueuer wrote into the attributes is honored.
#[tokio::test]
async fn a_carrier_written_by_another_sdk_is_honored() -> Result<()> {
    let (exporter, _provider, _guard) = pipeline();
    let (engine, _db) = engine().await?;
    let trace_id = TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap();
    let span_id = SpanId::from_hex("b7ad6b7169203331").unwrap();
    let mut attributes = serde_json::Map::new();
    attributes.insert(
        CARRIER.into(),
        json!({ "traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" }),
    );
    engine
        .start::<_, String>(
            "echo",
            "hi".to_string(),
            WorkflowOptions::with_id("foreign").attributes(attributes),
        )
        .await?
        .result()
        .await?;
    engine.shutdown(Duration::from_secs(2)).await?;

    let span = workflow_span(&exporter, "foreign");
    assert_eq!(span.span_context.trace_id(), trace_id);
    assert_eq!(span.parent_span_id, span_id);
    Ok(())
}
