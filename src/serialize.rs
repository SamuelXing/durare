//! Workflow-data serialization formats, mirroring the Go SDK's `Serializer`.
//!
//! Every serialized value (workflow inputs/outputs, step outputs, messages,
//! event values) is stored as TEXT alongside a **format name** in the row's
//! `serialization` column. Recording the format lets any DBOS SDK decode a value
//! another wrote — the basis for cross-language interop via [`Serializer::Portable`].
//!
//! Formats (matching `dbos-transact-golang/dbos/serialization.go`):
//!
//! | Rust                  | Go name        | Wire form                | nil      |
//! |-----------------------|----------------|--------------------------|----------|
//! | [`Serializer::Json`]  | `DBOS_JSON`    | base64(JSON) — default   | `__DBOS_NIL` |
//! | [`Serializer::Portable`] | `portable_json` | plain JSON (cross-lang) | `null` |
//! | (read-only)           | `DBOS_GOB`     | Go gob — unsupported here | — |
//!
//! Encoding uses the provider's configured serializer; **decoding always
//! dispatches on the stored format name** (Go's `resolveDecoder`), so a Rust
//! worker can read rows written by a default-config Go worker (`DBOS_JSON`) or by
//! any SDK using `portable_json`. A `DBOS_GOB` value yields a clear error rather
//! than silent corruption.

use crate::error::{Error, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::Value;

/// Go's `PortableSerializerName` — the cross-language format.
pub const PORTABLE: &str = "portable_json";
/// Go's default serializer name.
pub const DBOS_JSON: &str = "DBOS_JSON";
/// Go's gob serializer name (Rust can encode/decode neither).
pub const DBOS_GOB: &str = "DBOS_GOB";
/// Sentinel Go's `DBOS_JSON` writes for a nil value.
const NIL_MARKER: &str = "__DBOS_NIL";

/// A serialization format for workflow data. Cheap to copy; held by each
/// provider as the format it *encodes* with (decoding is format-directed).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Serializer {
    /// `DBOS_JSON`: base64-encoded JSON. The default, matching the Go SDK.
    #[default]
    Json,
    /// `portable_json`: plain JSON, readable across DBOS languages.
    Portable,
}

impl Serializer {
    /// The format name stored in the `serialization` column.
    pub fn name(self) -> &'static str {
        match self {
            Serializer::Json => DBOS_JSON,
            Serializer::Portable => PORTABLE,
        }
    }

    /// Encode a JSON value to its stored TEXT form.
    pub fn encode(self, value: &Value) -> Result<String> {
        match self {
            Serializer::Portable => {
                if value.is_null() {
                    return Ok("null".to_string());
                }
                Ok(serde_json::to_string(value)?)
            }
            Serializer::Json => {
                if value.is_null() {
                    return Ok(NIL_MARKER.to_string());
                }
                Ok(STANDARD.encode(serde_json::to_vec(value)?))
            }
        }
    }
}

/// Decode a stored TEXT value using the format recorded in its `serialization`
/// column. `None`, `""`, and `DBOS_JSON` all select the default (base64 JSON);
/// `portable_json` selects plain JSON. `DBOS_GOB` and unknown names error.
pub fn decode(format: Option<&str>, stored: &str) -> Result<Value> {
    match format.unwrap_or("") {
        PORTABLE => {
            if stored == "null" {
                return Ok(Value::Null);
            }
            Ok(serde_json::from_str(stored)?)
        }
        "" | DBOS_JSON => {
            if stored == NIL_MARKER {
                return Ok(Value::Null);
            }
            let bytes = STANDARD.decode(stored).map_err(|e| {
                Error::Serialization(format!("invalid base64 in DBOS_JSON value: {e}"))
            })?;
            Ok(serde_json::from_slice(&bytes)?)
        }
        DBOS_GOB => Err(Error::Serialization(
            "value was serialized with Go's DBOS_GOB format, which the Rust SDK cannot decode; \
             configure the producing app to use portable_json for cross-language interop"
                .to_string(),
        )),
        other => Err(Error::Serialization(format!(
            "unknown serialization format {other:?}"
        ))),
    }
}

/// Decode an optional stored value, defaulting absent/undecodable rows to `Null`
/// is *not* done here — callers that want lenient behavior handle the `Err`.
pub fn decode_opt(format: Option<&str>, stored: Option<&str>) -> Result<Option<Value>> {
    match stored {
        Some(s) => Ok(Some(decode(format, s)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_roundtrip_is_base64() {
        let v = json!({"a": 1, "b": "x"});
        let enc = Serializer::Json.encode(&v).unwrap();
        // base64, not plain JSON.
        assert!(!enc.starts_with('{'));
        assert_eq!(decode(Some(DBOS_JSON), &enc).unwrap(), v);
        // Empty/None format name falls back to DBOS_JSON.
        assert_eq!(decode(None, &enc).unwrap(), v);
    }

    #[test]
    fn portable_roundtrip_is_plain_json() {
        let v = json!({"a": 1});
        let enc = Serializer::Portable.encode(&v).unwrap();
        assert_eq!(enc, r#"{"a":1}"#);
        assert_eq!(decode(Some(PORTABLE), &enc).unwrap(), v);
    }

    #[test]
    fn nil_markers_match_go() {
        assert_eq!(Serializer::Json.encode(&Value::Null).unwrap(), NIL_MARKER);
        assert_eq!(Serializer::Portable.encode(&Value::Null).unwrap(), "null");
        assert_eq!(decode(Some(DBOS_JSON), NIL_MARKER).unwrap(), Value::Null);
        assert_eq!(decode(Some(PORTABLE), "null").unwrap(), Value::Null);
    }

    #[test]
    fn gob_and_unknown_error() {
        assert!(decode(Some(DBOS_GOB), "abc").is_err());
        assert!(decode(Some("DBOS_PICKLE"), "abc").is_err());
    }
}
