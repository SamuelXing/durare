//! Per-execution storage-failure channel. It is deliberately separate from the
//! workflow's Result: catching an error must not authorize a terminal write.
use crate::{Error, Result};
use std::sync::{Arc, OnceLock};
use tokio::sync::Notify;

#[derive(Clone, Default)]
pub(crate) struct Execution(Arc<Inner>);

#[derive(Default)]
struct Inner {
    failure: OnceLock<Arc<Error>>,
    changed: Notify,
}

impl Execution {
    pub(crate) fn check(&self) -> Result<()> {
        match self.0.failure.get() {
            Some(error) => Err(Error::RecoveryRequired(error.clone())),
            None => Ok(()),
        }
    }

    /// Classify only a provider/storage result, never a user body result.
    /// Providers also return semantic rejections (missing destination, closed
    /// stream, unsupported operation); those remain ordinary catchable errors.
    pub(crate) fn record(&self, error: Error) -> Error {
        match provider_error(error) {
            interruption @ Error::RecoveryRequired(_) => self.interrupt(interruption),
            rejection => rejection,
        }
    }

    /// A body may return another execution's interruption (for example from
    /// a child handle). Adopt that control signal before rollback or retry can
    /// replace it. Ordinary body errors, including Db, remain business errors.
    pub(crate) fn body_error(&self, error: Error) -> Error {
        if matches!(
            error,
            Error::RecoveryRequired(_) | Error::ObservationFailed(_)
        ) {
            self.interrupt(error)
        } else {
            error
        }
    }

    /// A known infrastructure failure, including an inconsistent stored row
    /// whose diagnostic uses App. Do not use for user-value serialization.
    pub(crate) fn interrupt(&self, error: Error) -> Error {
        let first = self
            .0
            .failure
            .get_or_init(|| match error {
                Error::RecoveryRequired(cause) => cause,
                error => Arc::new(error),
            })
            .clone();
        self.0.changed.notify_one();
        Error::RecoveryRequired(first)
    }

    pub(crate) fn storage<T>(&self, result: Result<T>) -> Result<T> {
        result.map_err(|error| self.record(error))
    }

    pub(crate) async fn failed(&self) -> Error {
        loop {
            let changed = self.0.changed.notified();
            if let Err(error) = self.check() {
                return error;
            }
            changed.await;
        }
    }
}

/// Normalize an infrastructure failure once; callers already carrying the
/// recovery channel retain the original cause and wrapper.
pub(crate) fn recovery_error(error: Error) -> Error {
    match error {
        Error::RecoveryRequired(_) => error,
        other => Error::RecoveryRequired(Arc::new(other)),
    }
}

/// Preserve infrastructure origin at a provider read/write boundary, including
/// observers with no local execution latch. Never apply this to user-body errors.
pub(crate) fn provider_error(error: Error) -> Error {
    if is_storage_failure(&error) {
        recovery_error(error)
    } else {
        error
    }
}

/// Failure to observe an operation does not establish that its target stopped.
/// Keep the cause for retry diagnostics without making it recordable business data.
pub(crate) fn observation_error(error: Error) -> Error {
    match error {
        Error::ObservationFailed(_) => error,
        Error::RecoveryRequired(cause) => Error::ObservationFailed(cause),
        error if is_storage_failure(&error) => Error::ObservationFailed(Arc::new(error)),
        error => error,
    }
}

/// The provider boundary's error contract, not a global classification of an
/// arbitrary Error: a Db/Serde returned by application code is a business result.
/// Recorded driver errors are snapshots of business failures, not live failures.
pub(crate) fn is_storage_failure(error: &Error) -> bool {
    matches!(
        error,
        Error::Db(_)
            | Error::Migrate(_)
            | Error::Serde(_)
            | Error::Serialization(_)
            | Error::RecoveryRequired(_)
            | Error::ObservationFailed(_)
    )
}

#[cfg(test)]
pub(crate) mod test_provider;
