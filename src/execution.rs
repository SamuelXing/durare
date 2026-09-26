//! Per-execution storage-failure channel. It is deliberately separate from the
//! workflow's Result: catching an error must not authorize a terminal write.
use crate::{Error, Result};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

#[derive(Clone, Default)]
pub(crate) struct Execution(Arc<Inner>);

#[derive(Default)]
struct Inner {
    failure: Mutex<Option<Arc<Error>>>,
    changed: Notify,
}

impl Execution {
    pub(crate) fn check(&self) -> Result<()> {
        match self
            .0
            .failure
            .lock()
            .expect("execution failure lock poisoned")
            .as_ref()
        {
            Some(error) => Err(Error::RecoveryRequired(error.clone())),
            None => Ok(()),
        }
    }

    /// Classify only a provider/storage result, never a user body result.
    /// Providers also return semantic rejections (missing destination, closed
    /// stream, unsupported operation); those remain ordinary catchable errors.
    pub(crate) fn record(&self, error: Error) -> Error {
        if !is_storage_failure(&error) {
            return error;
        }
        self.interrupt(error)
    }

    /// A known infrastructure failure, including an inconsistent stored row
    /// whose diagnostic uses App. Do not use for user-value serialization.
    pub(crate) fn interrupt(&self, error: Error) -> Error {
        let mut failure = self
            .0
            .failure
            .lock()
            .expect("execution failure lock poisoned");
        let first = failure
            .get_or_insert_with(|| match error {
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
    )
}
