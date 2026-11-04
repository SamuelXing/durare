//! The W3C trace context a workflow carries in its attributes; see the
//! Parenting section of [`observability`](crate::observability).

use opentelemetry::trace::{SpanContext, TraceContextExt, TraceState};
use opentelemetry::{Context, SpanId, TraceFlags, TraceId};
use serde_json::{Map, Value};
use tracing_opentelemetry::{OpenTelemetrySpanExt, SetParentError};

/// Reserved workflow attribute holding the trace carrier, the key the Python
/// SDK uses.
pub(crate) const CARRIER_ATTRIBUTE: &str = "dbos.otelContext";

/// `attributes` with the current span's trace context folded in. Unchanged
/// when no valid span context is current, so the option is harmless without
/// an OpenTelemetry layer installed.
pub(crate) fn with_current_carrier(
    mut attributes: Option<Map<String, Value>>,
) -> Option<Map<String, Value>> {
    let context = tracing::Span::current().context();
    if let Some(carrier) = carrier_from(context.span().span_context()) {
        attributes
            .get_or_insert_with(Map::new)
            .insert(CARRIER_ATTRIBUTE.to_string(), Value::Object(carrier));
    }
    attributes
}

fn carrier_from(span_context: &SpanContext) -> Option<Map<String, Value>> {
    if !span_context.is_valid() {
        return None;
    }
    let mut carrier = Map::new();
    // Trace and span ids display as zero-padded lowercase hex, the W3C form.
    carrier.insert(
        "traceparent".to_string(),
        Value::String(format!(
            "00-{}-{}-{:02x}",
            span_context.trace_id(),
            span_context.span_id(),
            span_context.trace_flags().to_u8()
        )),
    );
    let state = span_context.trace_state().header();
    if !state.is_empty() {
        carrier.insert("tracestate".to_string(), Value::String(state));
    }
    Some(carrier)
}

/// The remote parent named by a carrier in `attributes`, if it holds a usable
/// W3C trace context. Never fails: the attributes are user-writable, so a bad
/// value means "no parent", not a broken run.
fn parent_from_attributes(attributes: Option<&Value>) -> Option<Context> {
    let carrier = attributes?.get(CARRIER_ATTRIBUTE)?.as_object()?;
    let mut parts = carrier.get("traceparent")?.as_str()?.split('-');
    let (Some("00"), Some(trace_id), Some(span_id), Some(flags), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return None;
    };
    if trace_id.len() != 32 || span_id.len() != 16 || flags.len() != 2 {
        return None;
    }
    let trace_id = TraceId::from_hex(trace_id).ok()?;
    let span_id = SpanId::from_hex(span_id).ok()?;
    let flags = TraceFlags::new(u8::from_str_radix(flags, 16).ok()?);
    let state = carrier
        .get("tracestate")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<TraceState>().ok())
        .unwrap_or_default();
    let span_context = SpanContext::new(trace_id, span_id, flags, true, state);
    span_context
        .is_valid()
        .then(|| Context::new().with_remote_span_context(span_context))
}

/// Parent `span` under the carrier in `attributes`, when there is one. The
/// carrier wins over whatever span is current, so the run lands on the same
/// trace whether it executes inline, after a queue handoff, or on recovery.
pub(crate) fn parent_span(span: &tracing::Span, attributes: Option<&Value>) {
    if let Some(context) = parent_from_attributes(attributes) {
        // Fails when no OpenTelemetry layer tracks the span, which is not an
        // error, or when the span was already entered, which would be a bug
        // in the caller's ordering.
        let result = span.set_parent(context);
        debug_assert!(!matches!(result, Err(SetParentError::AlreadyStarted)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn attributes(traceparent: &str, tracestate: Option<&str>) -> Value {
        let mut carrier = json!({ "traceparent": traceparent });
        if let Some(state) = tracestate {
            carrier["tracestate"] = json!(state);
        }
        json!({ CARRIER_ATTRIBUTE: carrier })
    }

    #[test]
    fn a_carrier_round_trips() {
        let original = SpanContext::new(
            TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap(),
            SpanId::from_hex("b7ad6b7169203331").unwrap(),
            TraceFlags::SAMPLED,
            false,
            "vendor=opaque".parse().unwrap(),
        );
        let carrier = Value::Object(carrier_from(&original).unwrap());
        assert_eq!(
            carrier["traceparent"],
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        );
        assert_eq!(carrier["tracestate"], "vendor=opaque");

        let parent = parent_from_attributes(Some(&json!({ CARRIER_ATTRIBUTE: carrier }))).unwrap();
        let restored = parent.span().span_context().clone();
        assert_eq!(restored.trace_id(), original.trace_id());
        assert_eq!(restored.span_id(), original.span_id());
        assert_eq!(restored.trace_flags(), original.trace_flags());
        assert_eq!(restored.trace_state().header(), "vendor=opaque");
        assert!(restored.is_remote());
    }

    #[test]
    fn an_invalid_span_context_writes_nothing() {
        assert!(carrier_from(&SpanContext::empty_context()).is_none());
        assert!(
            with_current_carrier(None).is_none(),
            "no span is current in a plain test"
        );
    }

    #[test]
    fn a_bad_carrier_means_no_parent() {
        for traceparent in [
            "",
            "garbage",
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-zz",
        ] {
            assert!(
                parent_from_attributes(Some(&attributes(traceparent, None))).is_none(),
                "{traceparent:?}"
            );
        }
        assert!(parent_from_attributes(None).is_none());
        assert!(parent_from_attributes(Some(&json!({ "other": 1 }))).is_none());
        assert!(
            parent_from_attributes(Some(&json!({ CARRIER_ATTRIBUTE: "not an object" }))).is_none()
        );
    }

    #[test]
    fn a_bad_tracestate_is_dropped_not_fatal() {
        let parent = parent_from_attributes(Some(&attributes(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            Some("=== not a trace state ==="),
        )))
        .unwrap();
        assert_eq!(parent.span().span_context().trace_state().header(), "");
    }
}
