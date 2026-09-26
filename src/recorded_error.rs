//! Versioned error records, shared by step and workflow result readers.

use crate::{Divergence, Error, PortableWorkflowError, RecordedError, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::json;

// The default format historically stored arbitrary display text. New writes
// escape application messages beginning with this reserved prefix. Old readers
// cannot recover these records' types; see the durability guide's rollout rules.
pub(crate) const PREFIX: &str = "__DURARE_ERROR__:";
pub(crate) const NAME: &str = "durare.RecordedError";

// A remote derive keeps the persisted representation private and exhaustively
// matched to Error, without promising that arbitrary live Error values support
// serde. Driver objects must be captured as RecordedError before serialization.
#[derive(Serialize, Deserialize)]
#[serde(
    remote = "Error",
    tag = "kind",
    content = "data",
    rename_all = "snake_case"
)]
#[allow(dead_code)]
enum ErrorWire {
    #[serde(skip)]
    Db(sqlx::Error),
    #[serde(skip)]
    Migrate(sqlx::migrate::MigrateError),
    #[serde(skip)]
    Serde(serde_json::Error),
    Recorded(Box<RecordedError>),
    Serialization(String),
    UnknownWorkflow(String),
    UnknownQueue(String),
    NonExistentWorkflow(String),
    NotAuthorized(String),
    QueueDeduplicated {
        queue_name: String,
        dedup_id: String,
    },
    Cancelled(String),
    MaxRecoveryAttemptsExceeded(String),
    ConflictingRegistration(String),
    Timeout,
    UnexpectedStep {
        workflow_id: String,
        step_id: i32,
        expected: String,
        recorded: String,
    },
    NestedDurableCall {
        workflow_id: String,
        operation: String,
    },
    DurableCallCrossedBody(String),
    WorkflowConflict(String),
    ReplayDiverged {
        workflow_id: String,
        #[serde(with = "DivergenceWire")]
        divergence: Divergence,
    },
    App {
        message: String,
        #[serde(skip)]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },
    Portable(Box<PortableWorkflowError>),
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "Divergence", tag = "kind", rename_all = "snake_case")]
#[allow(dead_code)]
enum DivergenceWire {
    Mismatch {
        position: i32,
        expected: String,
        recorded: String,
    },
    Extra {
        position: i32,
        operation: String,
    },
    Missing {
        position: i32,
        recorded: String,
    },
    Failed {
        error: String,
    },
}

#[derive(Serialize)]
struct BorrowedError<'a>(#[serde(with = "ErrorWire")] &'a Error);

#[derive(Deserialize)]
struct OwnedError(#[serde(with = "ErrorWire")] Error);

#[derive(Deserialize)]
struct Payload {
    version: u32,
    #[serde(default)]
    error: serde_json::Value,
}

/// Return a new-format record where the legacy format would lose information,
/// or where an application value must be escaped to avoid the reserved marker.
pub(crate) fn encode(serializer: &Serializer, error: &Error) -> Option<String> {
    match error {
        Error::App { message, .. } if !message.starts_with(PREFIX) => return None,
        Error::Portable(info)
            if matches!(serializer, Serializer::Portable)
                && info.name != NAME
                && !(info.name == "DBOSNotAuthorizedError"
                    && info.code.is_none()
                    && info.data.is_none()) =>
        {
            return None
        }
        // Preserve the established cross-SDK authorization envelope. Its exact
        // fieldless form has a typed decoder below; structured foreign payloads
        // with the same class name remain Portable.
        Error::NotAuthorized(_) if matches!(serializer, Serializer::Portable) => return None,
        _ => {}
    }
    let captured;
    let recordable = match error {
        Error::Db(_) | Error::Migrate(_) | Error::Serde(_) => {
            captured = Error::Recorded(Box::new(RecordedError::capture(error)));
            &captured
        }
        other => other,
    };
    let envelope = PortableWorkflowError {
        name: NAME.into(),
        message: error.to_string(),
        code: None,
        data: Some(json!({ "version": 1, "error": BorrowedError(recordable) })),
    };
    let encoded =
        serde_json::to_string(&envelope).expect("a recorded error contains only JSON-safe fields");
    Some(if matches!(serializer, Serializer::Portable) {
        encoded
    } else {
        format!("{PREFIX}{encoded}")
    })
}

/// Decode a stored failure only once: escaped application errors can themselves
/// contain the prefix or reserved portable name, without recursive interpretation.
pub(crate) fn from_parts(message: String, info: Option<PortableWorkflowError>) -> Error {
    match info {
        Some(info) if info.name == NAME => {
            let decode = || -> crate::Result<Error> {
                let payload: Payload = serde_json::from_value(info.data.unwrap_or_default())?;
                if payload.version != 1 {
                    return Err(Error::Serialization(format!(
                        "unsupported recorded error version {}",
                        payload.version
                    )));
                }
                Ok(serde_json::from_value::<OwnedError>(payload.error)?.0)
            };
            decode().unwrap_or_else(|error| {
                Error::Serialization(format!("cannot decode recorded error: {error}"))
            })
        }
        Some(info)
            if info.name == "DBOSNotAuthorizedError"
                && info.code.is_none()
                && info.data.is_none() =>
        {
            Error::NotAuthorized(info.message)
        }
        Some(info) => Error::Portable(Box::new(info)),
        None if message.starts_with(PREFIX) => {
            match serde_json::from_str::<PortableWorkflowError>(&message[PREFIX.len()..]) {
                Ok(info) if info.name == NAME => from_parts(info.message.clone(), Some(info)),
                _ => Error::Serialization("cannot decode recorded error envelope".into()),
            }
        }
        None => Error::app(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::{encode_error, restore_error};
    use crate::ErrorCode;

    fn roundtrip(serializer: &Serializer, error: &Error) -> Error {
        restore_error(Some(serializer.name()), &encode_error(serializer, error))
    }

    #[test]
    fn version_one_timeout_wire_format_is_stable() {
        let fixture = r#"{"name":"durare.RecordedError","message":"operation timed out","data":{"version":1,"error":{"kind":"timeout"}}}"#;
        for (serializer, stored) in [
            (Serializer::Portable, fixture.to_string()),
            (Serializer::Json, format!("{PREFIX}{fixture}")),
        ] {
            assert_eq!(encode_error(&serializer, &Error::Timeout), stored);
            assert!(matches!(
                restore_error(Some(serializer.name()), &stored),
                Error::Timeout
            ));
        }
    }

    #[test]
    fn builtin_variants_keep_their_fields_and_classification() {
        let errors = [
            Error::Timeout,
            Error::Serialization("unknown format".into()),
            Error::UnknownWorkflow("missing".into()),
            Error::UnknownQueue("queue".into()),
            Error::NonExistentWorkflow("wf".into()),
            Error::NotAuthorized("role".into()),
            Error::queue_deduplicated("queue", "key"),
            Error::Cancelled("wf".into()),
            Error::MaxRecoveryAttemptsExceeded("wf".into()),
            Error::ConflictingRegistration("name".into()),
            Error::unexpected_step("wf", 2, "wanted", "stored"),
            Error::NestedDurableCall {
                workflow_id: "wf".into(),
                operation: "step".into(),
            },
            Error::DurableCallCrossedBody("step".into()),
            Error::WorkflowConflict("wf".into()),
            Error::ReplayDiverged {
                workflow_id: "wf".into(),
                divergence: Divergence::Missing {
                    position: 3,
                    recorded: "step".into(),
                },
            },
        ];
        for serializer in [Serializer::Json, Serializer::Portable] {
            for error in &errors {
                let restored = roundtrip(&serializer, error);
                assert_eq!(restored.code(), error.code());
                assert_eq!(format!("{restored:?}"), format!("{error:?}"));
                assert_eq!(restored.to_string(), error.to_string());
                assert_eq!(
                    encode_error(&serializer, &restored),
                    encode_error(&serializer, error)
                );
            }
        }
    }

    #[test]
    fn driver_diagnostics_preserve_code_message_and_retryability() {
        let errors = [
            Error::Db(sqlx::Error::PoolTimedOut),
            Error::Db(sqlx::Error::PoolClosed),
            Error::Db(sqlx::Error::Io(std::io::Error::other("connection lost"))),
            Error::Migrate(sqlx::migrate::MigrateError::VersionMissing(7)),
            Error::Serde(serde_json::from_str::<u8>("false").unwrap_err()),
        ];
        for serializer in [Serializer::Json, Serializer::Portable] {
            for error in &errors {
                let restored = roundtrip(&serializer, error);
                assert!(matches!(restored, Error::Recorded(_)));
                assert_eq!(restored.code(), error.code());
                assert_eq!(restored.to_string(), error.to_string());
                assert_eq!(restored.is_retryable(), error.is_retryable());
                assert_eq!(restored.is_tx_conflict(), error.is_tx_conflict());
                assert!(std::error::Error::source(&restored).is_none());
            }
        }
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn recorded_constraint_errors_keep_database_predicates() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for query in [
            "PRAGMA foreign_keys = ON",
            "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
            "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id))",
            "INSERT INTO parent VALUES (1)",
        ] {
            sqlx::query(query).execute(&pool).await.unwrap();
        }
        for (query, unique, foreign) in [
            ("INSERT INTO parent VALUES (1)", true, false),
            ("INSERT INTO child VALUES (2)", false, true),
        ] {
            let error = Error::Db(sqlx::query(query).execute(&pool).await.unwrap_err());
            for serializer in [Serializer::Json, Serializer::Portable] {
                let restored = roundtrip(&serializer, &error);
                assert_eq!(restored.code(), ErrorCode::Database);
                assert_eq!(restored.is_unique_violation(), unique);
                assert_eq!(restored.is_foreign_key_violation(), foreign);
                assert_eq!(restored.to_string(), error.to_string());
            }
        }
    }

    #[test]
    fn application_envelopes_survive_both_formats_and_reserved_names() {
        for name in ["ValidationError", NAME, "DBOSNotAuthorizedError"] {
            for data in [None, Some(json!({"version": 999, "field": "email"}))] {
                let info = PortableWorkflowError {
                    name: name.into(),
                    message: "bad email".into(),
                    code: None,
                    data,
                };
                for serializer in [Serializer::Json, Serializer::Portable] {
                    let restored = roundtrip(&serializer, &Error::Portable(Box::new(info.clone())));
                    assert!(matches!(restored, Error::Portable(pe) if *pe == info));
                }
            }
        }
    }

    #[test]
    fn reserved_prefix_in_application_text_is_escaped_once() {
        for message in [
            format!("{PREFIX}not json"),
            encode_error(&Serializer::Json, &Error::Timeout),
        ] {
            for serializer in [Serializer::Json, Serializer::Portable] {
                let error = roundtrip(&serializer, &Error::app(&message));
                assert!(matches!(error, Error::App { .. }));
                assert_eq!(error.to_string(), message);
            }
        }
    }

    #[test]
    fn old_text_and_foreign_envelopes_are_not_guessed_from_messages() {
        let error = restore_error(None, "operation timed out");
        assert!(matches!(error, Error::App { .. }));
        let foreign = r#"{"name":"ValidationError","message":"operation timed out","code":400,"data":{"field":"email"}}"#;
        let error = restore_error(Some(crate::serialize::PORTABLE), foreign);
        assert!(
            matches!(error, Error::Portable(pe) if pe.code == Some(json!(400)) && pe.data == Some(json!({"field":"email"})))
        );
    }

    #[test]
    fn legacy_fieldless_authorization_records_use_the_documented_typed_decode() {
        let stored = r#"{"name":"DBOSNotAuthorizedError","message":"denied"}"#;
        let error = restore_error(Some(crate::serialize::PORTABLE), stored);
        assert!(matches!(error, Error::NotAuthorized(ref message) if message == "denied"));
        assert_eq!(error.code(), ErrorCode::NotAuthorized);
        let structured = r#"{"name":"DBOSNotAuthorizedError","message":"denied","code":403}"#;
        assert!(matches!(
            restore_error(Some(crate::serialize::PORTABLE), structured),
            Error::Portable(_)
        ));
    }

    #[test]
    fn unknown_codes_are_rejected_instead_of_changing_workflow_branches() {
        let error = Error::Db(sqlx::Error::PoolTimedOut);
        let mut record: serde_json::Value =
            serde_json::from_str(&encode_error(&Serializer::Portable, &error)).unwrap();
        record["data"]["error"]["data"]["code"] = json!("future_code");
        let restored = restore_error(Some(crate::serialize::PORTABLE), &record.to_string());
        assert_eq!(restored.code(), ErrorCode::Serialization);
        assert!(restored.to_string().contains("future_code"));
    }

    #[test]
    fn version_is_checked_before_the_version_specific_shape() {
        let record = json!({"name": NAME, "message": "future", "data": {"version": 2}});
        let error = restore_error(Some(crate::serialize::PORTABLE), &record.to_string());
        assert!(
            error
                .to_string()
                .contains("unsupported recorded error version 2"),
            "{error}"
        );
    }

    #[test]
    fn corrupt_and_unknown_records_report_decoding_errors() {
        let good = encode_error(&Serializer::Portable, &Error::Timeout);
        let mut unknown: serde_json::Value = serde_json::from_str(&good).unwrap();
        unknown["data"]["version"] = json!(2);
        let mut bad_kind: serde_json::Value = serde_json::from_str(&good).unwrap();
        bad_kind["data"]["error"]["kind"] = json!("future_variant");
        for value in [
            unknown,
            bad_kind,
            json!({"name": NAME, "message": "broken"}),
            json!({"name": NAME}),
        ] {
            let text = value.to_string();
            for (format, stored) in [
                (Some(crate::serialize::PORTABLE), text.clone()),
                (None, format!("{PREFIX}{text}")),
            ] {
                let error = restore_error(format, &stored);
                assert_eq!(error.code(), ErrorCode::Serialization);
            }
        }
        assert_eq!(
            restore_error(None, &format!("{PREFIX}{{")).code(),
            ErrorCode::Serialization
        );
    }
}
