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

    /// Only call at a storage/decoding boundary, never around user code or a
    /// recorded business failure. Ownership and cancellation keep their existing
    /// control paths; an ordinary provider failure halts this execution.
    pub(crate) fn record(&self, error: Error) -> Error {
        if matches!(
            error,
            Error::Cancelled(_) | Error::WorkflowConflict(_) | Error::UnexpectedStep { .. }
        ) {
            return error;
        }
        let mut failure = self
            .0
            .failure
            .lock()
            .expect("execution failure lock poisoned");
        let first = failure.get_or_insert_with(|| Arc::new(error)).clone();
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
