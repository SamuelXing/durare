//! What [`verify_replay`](crate::DurableEngine::verify_replay) reports: a
//! recorded workflow re-run against the current code, and the first place the
//! two disagree.
//!
//! A replay serves each durable operation from the record at its position, so a
//! workflow function is pinned to the sequence of operations it issued when it
//! first ran. Editing that function while runs are in flight is the one change
//! a test suite does not catch: the new code passes its own tests, and the old
//! run fails on the first position whose recorded name no longer matches — in
//! production, at recovery time, on a workflow that was already half-finished.
//!
//! [`ReplayReport`] answers that question before the deploy instead of after
//! it: it is the outcome of re-running one recorded workflow's function against
//! the code in the binary you are about to ship.

use crate::error::{Error, Result};
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Mutex, OnceLock};

/// The outcome of re-running one recorded workflow against the current code —
/// see [`DurableEngine::verify_replay`](crate::DurableEngine::verify_replay),
/// which documents what the check does and does not catch.
///
/// The counts are there to tell a clean pass from a vacuous one: a report with
/// `recorded: 0` is deterministic in the same sense an empty test suite is
/// green, and one whose `matched` stops well short of `recorded` says the
/// verification did not get far, whatever the divergence field holds.
#[derive(Clone, Debug, PartialEq)]
pub struct ReplayReport {
    /// The workflow that was verified.
    pub workflow_id: String,
    /// The registered name its function is looked up under.
    pub workflow_name: String,
    /// Durable operations in the recorded history.
    pub recorded: usize,
    /// Recorded operations the re-run reached and matched, in order.
    pub matched: usize,
    /// Whether the recorded workflow had finished. A workflow still running has
    /// a history that legitimately ends early, so reaching its end is not a
    /// divergence.
    ///
    /// "Finished" means the run ended on its own — `SUCCESS` or `ERROR`. A
    /// cancelled or dead-lettered run is stopped from the outside, wherever it
    /// happened to be, so its history ends early too and it is reported here as
    /// `false` like a running one.
    pub terminal: bool,
    /// The first place the re-run and the history disagreed, if they did.
    pub divergence: Option<Divergence>,
}

/// The first disagreement between a recorded history and a re-run of the
/// current code.
///
/// Only the *first* one is reported. Once a position has shifted, every later
/// position is suspect, so a list of them would be noise: fix the first and run
/// the check again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Divergence {
    /// The re-run asked for a different operation than the one recorded here.
    Mismatch {
        /// The step position the two disagree at.
        position: i32,
        /// The operation the current code issues there.
        expected: String,
        /// The operation the recorded run issued there.
        recorded: String,
    },
    /// The re-run issued a durable operation the completed history does not have.
    Extra {
        /// The step position with nothing recorded at it.
        position: i32,
        /// The operation the current code issues there.
        operation: String,
    },
    /// The re-run finished without reaching operations the history holds.
    Missing {
        /// The first recorded position the re-run never reached.
        position: i32,
        /// The operation recorded there.
        recorded: String,
    },
    /// The recorded run succeeded, but the re-run failed or panicked without
    /// any operation disagreeing.
    ///
    /// No body ran, so the failure came from the re-run itself: most often a
    /// recorded value that no longer decodes as the type the code now expects
    /// at the same name, or code between operations that now fails. A history
    /// that ended in an error is expected to replay as that error and is not
    /// reported here.
    Failed {
        /// What the re-run failed with.
        error: String,
    },
}

impl fmt::Display for Divergence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Divergence::Mismatch {
                position,
                expected,
                recorded,
            } => write!(
                f,
                "step {position}: the code now issues `{expected}`, but `{recorded}` is recorded there"
            ),
            Divergence::Extra {
                position,
                operation,
            } => write!(
                f,
                "step {position}: the code now issues `{operation}`, which the recorded history does not have"
            ),
            Divergence::Missing {
                position,
                recorded,
            } => write!(
                f,
                "step {position}: the code no longer reaches `{recorded}`, which is recorded there"
            ),
            Divergence::Failed { error } => write!(
                f,
                "the recorded run succeeded, but the re-run failed: {error}"
            ),
        }
    }
}

impl ReplayReport {
    /// `true` when the re-run issued the recorded operations, in order, and
    /// nothing else.
    pub fn is_deterministic(&self) -> bool {
        self.divergence.is_none()
    }

    /// `Ok(())` when deterministic, otherwise a descriptive error — for `?` in a
    /// test or a CI step:
    ///
    /// ```no_run
    /// # use durare::DurableEngine;
    /// # async fn check(engine: &DurableEngine, id: &str) -> durare::Result<()> {
    /// engine.verify_replay(id).await?.into_result()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_result(self) -> Result<()> {
        match self.divergence {
            None => Ok(()),
            Some(divergence) => Err(Error::app(format!(
                "workflow `{}` (`{}`) no longer replays its recorded history: {divergence} \
                 ({} of {} recorded operations matched)",
                self.workflow_id, self.workflow_name, self.matched, self.recorded
            ))),
        }
    }
}

/// Where a verification run books what it saw, shared by every clone of the
/// context it was given to.
///
/// It is the authoritative channel, not the errors the refused calls return: a
/// workflow body may write `let _ = ctx.step(..).await;` or `.ok()` and carry
/// on, so nothing guarantees a refusal reaches the caller. Whatever the body
/// does with the error, the divergence is already recorded here.
///
/// A body can also reach the same position from several tasks, so first write
/// wins and later ones are dropped: [`OnceLock::set`] is exactly that, and the
/// first divergence is the one worth reporting anyway.
#[derive(Default)]
pub(crate) struct Verification {
    divergence: OnceLock<Divergence>,
    /// The recorded positions the re-run reached and matched by name.
    ///
    /// A set of positions, not a count and not the position counter: a call
    /// that is built and dropped claims a position without ever asking for the
    /// record at it, so the counter moves past history the re-run never
    /// verified. What was *served* is the only honest measure of what was
    /// checked.
    served: Mutex<BTreeSet<i32>>,
}

impl Verification {
    /// Book a divergence, if it is the first.
    pub(crate) fn saw(&self, divergence: Divergence) {
        let _ = self.divergence.set(divergence);
    }

    /// Book one recorded operation served to the re-run at `seq`.
    pub(crate) fn served_record(&self, seq: i32) {
        self.served
            .lock()
            .expect("verification served-set mutex poisoned")
            .insert(seq);
    }

    /// The recorded positions the re-run reached and matched.
    pub(crate) fn served(&self) -> BTreeSet<i32> {
        self.served
            .lock()
            .expect("verification served-set mutex poisoned")
            .clone()
    }

    /// The first divergence, if the run found one.
    pub(crate) fn divergence(&self) -> Option<Divergence> {
        self.divergence.get().cloned()
    }
}
