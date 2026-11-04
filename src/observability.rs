//! Seeing what the engine is doing: spans, their attributes, and how to
//! export them.
//!
//! durare instruments itself with the [`tracing`] facade — the engine emits
//! structured **spans** around every workflow and step execution, plus the
//! log events it always emitted. With no subscriber installed all of it
//! compiles to near-nothing; install one and you get a live execution tree:
//!
//! ```text
//! workflow{otel.name=checkout dbos.operation.workflow_id=order-1001 …}
//! ├── step{otel.name=reserve_inventory dbos.step.id=0 …}
//! ├── step{otel.name=charge_card dbos.step.id=1 …}
//! └── workflow{otel.name=dispatch_order …}          // a child workflow
//! ```
//!
//! # The spans
//!
//! **Workflow span** — one per workflow execution, covering the body *and*
//! the terminal status write. Every path that runs a workflow gets one:
//! direct starts, queued runs claimed by a dispatcher, scheduled ticks,
//! child workflows, and recovery re-runs.
//!
//! | Field | Value |
//! |---|---|
//! | `otel.name` | the workflow's registered name |
//! | `dbos.operation.type` | `workflow` |
//! | `dbos.operation.workflow_id` | the workflow id |
//! | `dbos.application.version` | the engine's application version |
//! | `dbos.executor.id` | the executor running it |
//! | `dbos.queue.name` | the queue it was claimed from (queued runs only) |
//! | `dbos.user.name`, `dbos.user.roles`, `dbos.user.assumed_role` | the [`AuthContext`], when one is attached |
//! | `dbos.workflow.status` | recorded at completion: `SUCCESS`, `ERROR`, or `CANCELLED` |
//! | `otel.status_code` | recorded at completion: `OK` on success, `ERROR` otherwise |
//!
//! A body panic records `otel.status_code = ERROR` but **no**
//! `dbos.workflow.status`: the row keeps its non-terminal state so recovery
//! can re-run it (see [`durability`](crate::durability)).
//!
//! **Step span** — one per durable operation inside a workflow: a
//! [`step`](crate::DurableContext::step) /
//! [`step_with`](crate::DurableContext::step_with) call (all retry attempts
//! included) or a [`transaction`](crate::DurableContext::transaction).
//!
//! | Field | Value |
//! |---|---|
//! | `otel.name` | the step name |
//! | `dbos.operation.type` | `step` or `transaction` |
//! | `dbos.operation.workflow_id` | the owning workflow id |
//! | `dbos.step.id` | the checkpoint sequence number (matches [`get_workflow_steps`](crate::DurableEngine::get_workflow_steps)) |
//! | `dbos.step.replayed` | `true` when the recorded outcome was served and the body did not run |
//! | `otel.status_code` | `OK` / `ERROR` by the step's final outcome |
//!
//! `dbos.step.replayed` is the recovery story made visible: after a crash,
//! the re-run's trace shows every already-checkpointed step as a short
//! replayed span and only the frontier step doing real work.
//!
//! # Parenting
//!
//! Step spans nest under their workflow span, and a child workflow's span
//! nests under the parent workflow's — the tree above. A workflow started
//! from an already-instrumented context (say, an HTTP handler span) parents
//! under it. Queued, scheduled, and recovered runs have no live parent and
//! form roots — unless the start carried its trace context along.
//!
//! With the `otel` feature,
//! [`WorkflowOptions::propagate_trace_context`](crate::WorkflowOptions::propagate_trace_context)
//! stores the current span's W3C `traceparent` (and `tracestate`) in the
//! workflow's attributes under `dbos.otelContext`, the key the Python SDK
//! uses, and every run of that workflow — inline, after a queue handoff, or
//! on recovery — parents its span under it. A carrier a Python enqueuer wrote
//! is honored the same way. Only the trace context travels, never baggage,
//! and children do not inherit it: set the option again on a child start to
//! keep it on the trace. A scheduled tick has no caller and stays a root; its
//! child starts can opt in. Without a valid span context at the call site
//! nothing is stored.
//!
//! The attribute names are the `dbos.*` ones the other DBOS SDKs emit in
//! their OpenTelemetry semantic-convention mode, so dashboards keyed on
//! them work across languages.
//!
//! # Subscribing
//!
//! Any [`tracing`] subscriber works. The spans are `INFO`-level under the
//! `durare` crate target:
//!
//! ```
//! # use durare::{DurableEngine, InMemoryProvider, Result, WorkflowOptions};
//! # use std::sync::Arc;
//! # #[durare::workflow]
//! # async fn hello(ctx: durare::DurableContext, name: String) -> Result<String> {
//! #     ctx.step("greet", || async { Ok(format!("hello, {name}")) }).await
//! # }
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! tracing_subscriber::fmt()
//!     .with_max_level(tracing::Level::INFO)
//!     .init();
//!
//! let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
//! let handle = engine
//!     .start_with(Hello, "world".into(), WorkflowOptions::default())
//!     .await?;
//! let out: String = handle.await?; // spans print to stderr as they close
//! # assert_eq!(out, "hello, world");
//! # Ok(())
//! # }
//! ```
//!
//! # Exporting over OpenTelemetry
//!
//! For OpenTelemetry export, bridge the same spans with
//! [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) — no
//! engine configuration involved; the layer picks the spans up from the
//! subscriber like any other. An OTLP exporter to a local Collector:
//!
//! ```no_run
//! use opentelemetry::trace::TracerProvider as _;
//! use opentelemetry_sdk::trace::SdkTracerProvider;
//! use opentelemetry_otlp::WithExportConfig;
//! use tracing_subscriber::layer::SubscriberExt;
//! use tracing_subscriber::util::SubscriberInitExt;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let exporter = opentelemetry_otlp::SpanExporter::builder()
//!     .with_http()
//!     .with_endpoint("http://localhost:4318/v1/traces")
//!     .build()?;
//! let provider = SdkTracerProvider::builder()
//!     .with_batch_exporter(exporter)
//!     .build();
//! tracing_subscriber::registry()
//!     .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("my-app")))
//!     .init();
//! // ... run the engine; call `provider.shutdown()` before exiting to flush.
//! # Ok(())
//! # }
//! ```
//!
//! The bridge understands the `otel.name` and `otel.status_code` fields:
//! exported spans are named after the workflow or step (not the literal
//! `workflow` / `step` span names) and carry OTel status. Under a plain
//! [`fmt`](https://docs.rs/tracing-subscriber) subscriber the two appear as
//! ordinary fields.
//!
//! From the Collector the spans go anywhere OTLP goes. The `telemetry`
//! example ships a compose stack — Collector, Grafana Tempo, and Grafana with
//! Tempo provisioned — and prints the trace id to search for. Datadog takes
//! the same spans through a Datadog Agent with OTLP ingestion enabled or the
//! Collector's `datadog` exporter; no vendor client is involved.
//!
//! Two things the export does not give you. durare emits spans and log
//! events, not metrics: throughput, failure rate and step latency are either
//! derived from the spans by the backend or polled from
//! [`DurableEngine::metrics`] and published by the application. And, as noted
//! under Parenting, queued and recovered runs need the `otel` feature and a
//! persisted trace context to continue the caller's trace; scheduled ticks
//! without a caller remain roots.
//!
//! # Probes
//!
//! Traces tell you what the engine *did*; a probe tells an orchestrator
//! whether to send it work at all. [`DurableEngine::health`] returns a
//! [`HealthReport`] with one entry per axis — the state backend (reachable,
//! dbos schema present and current) and dispatch (launched, not deactivated,
//! not shut down, every dispatcher task alive) — each `None` when healthy or
//! carrying the reason when not. It never fails: failures are the report's
//! content.
//!
//! With the `admin` feature, the admin server serves it as `GET /readyz` —
//! `200` when ready, `503` with the failing axes otherwise — alongside the
//! cross-SDK `GET /dbos-healthz` liveness probe (which stays unconditionally
//! healthy, matching the other DBOS SDKs, so a deactivated process can drain
//! without being restarted). Without the feature, wire
//! [`health`](crate::DurableEngine::health) into any HTTP handler.
//!
//! # Metrics
//!
//! [`DurableEngine::metrics`] returns a point-in-time [`EngineMetrics`]
//! snapshot — the poll-style shape, like tokio's runtime metrics, so durare
//! makes no metrics-system choice for you and adds no dependency. Gauges are
//! instantaneous (in-flight runs on this process; `ENQUEUED` depth per
//! registered queue, fleet-wide); the `*_total` counters are process-lifetime
//! and monotonic (workflows recovered, step retries, dead-lettered workflows,
//! failed dequeue polls).
//!
//! ```
//! # use durare::{DurableEngine, InMemoryProvider, Result};
//! # use std::sync::Arc;
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<()> {
//! # let engine = DurableEngine::new(Arc::new(InMemoryProvider::new())).await?;
//! # engine.launch().await?;
//! let m = engine.metrics().await?;
//! assert_eq!(m.workflows_in_flight, 0);
//! assert_eq!(m.dead_lettered_total, 0);
//! # Ok(())
//! # }
//! ```
//!
//! Poll it on your exporter's scrape interval and map the fields onto
//! whatever system you run — e.g. with a Prometheus registry:
//!
//! ```ignore
//! let m = engine.metrics().await?;
//! in_flight_gauge.set(m.workflows_in_flight as i64);
//! for (queue, depth) in &m.queue_depth {
//!     queue_depth_gauge.with_label_values(&[queue]).set(*depth);
//! }
//! recovered_counter.absolute(m.workflows_recovered_total);
//! ```
//!
//! Two readings a scrape apart give rates; the counters reset only with the
//! process, so exporters treat them like any process-lifetime total.
//!
//! [`tracing`]: https://docs.rs/tracing
//! [`AuthContext`]: crate::AuthContext
//! [`DurableEngine::health`]: crate::DurableEngine::health
//! [`HealthReport`]: crate::HealthReport
//! [`DurableEngine::metrics`]: crate::DurableEngine::metrics
//! [`EngineMetrics`]: crate::EngineMetrics

// This module is documentation only; the instrumentation lives in the engine
// and context implementations.
