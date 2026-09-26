//! What [`verify_replay`](crate::DurableEngine::verify_replay) reports: a
//! recorded workflow re-run against the current code, and the first place the
//! two disagree. The argument for the check is on that method.

use crate::error::{panic_message, Error, Result};
use crate::provider::StepInfo;
use crate::STATUS_ERROR;
use serde_json::Value;
use std::any::Any;
use std::collections::BTreeSet;
use std::fmt;
use std::sync::{Mutex, MutexGuard};

/// The outcome of re-running one recorded workflow against the current code —
/// see [`DurableEngine::verify_replay`](crate::DurableEngine::verify_replay),
/// which documents what the check does and does not catch.
///
/// The counts are there to tell a clean pass from a vacuous one: a report with
/// `recorded: 0` passes in the same sense an empty test suite is green, and one
/// whose `matched` stops well short of `recorded` says the verification did not
/// get far, whatever the divergence field holds.
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
    /// Whether the recorded run ended on its own (`SUCCESS` or `ERROR`). A
    /// running, cancelled or dead-lettered workflow has a history that
    /// legitimately ends early, so only a [`Mismatch`](Divergence::Mismatch),
    /// [`Missing`](Divergence::Missing) or [`Failed`](Divergence::Failed) is
    /// reported against it, never [`Extra`](Divergence::Extra).
    pub complete: bool,
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
    /// The re-run failed or panicked without any operation disagreeing, where
    /// the recorded run had not failed.
    ///
    /// No body ran, so the failure came from the re-run itself: most often a
    /// recorded value that no longer decodes as the type the code now expects
    /// at the same name, or code between operations that now fails. A history
    /// that ended in `ERROR` is expected to replay as that error, so nothing is
    /// reported against one whichever way its re-run ends — a run that no longer
    /// fails is not a recovery hazard.
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
            Divergence::Failed { error } => {
                write!(f, "the recorded run did not fail, but the re-run did — {error}")
            }
        }
    }
}

impl ReplayReport {
    /// `true` when nothing diverged: the re-run issued the recorded operations,
    /// in order, nothing else, and did not fail where the recorded run had not.
    pub fn passes(&self) -> bool {
        self.divergence.is_none()
    }

    /// `Ok(())` when the report [`passes`](Self::passes), otherwise
    /// [`Error::ReplayDiverged`] — for `?` in a test or a CI step:
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
            Some(divergence) => Err(Error::ReplayDiverged {
                workflow_id: self.workflow_id,
                divergence,
            }),
        }
    }
}

/// The recorded run a verification is judged against.
pub(crate) struct History<'a> {
    /// The workflow's id.
    pub(crate) id: &'a str,
    /// The registered name its function is looked up under.
    pub(crate) name: &'a str,
    /// The status row's `status`.
    pub(crate) status: &'a str,
    /// Its recorded durable operations, in position order.
    pub(crate) recorded: &'a [StepInfo],
}

/// How the re-run of the workflow body ended: what it returned, or the panic
/// payload `catch_unwind` handed back.
pub(crate) type RunOutcome = std::result::Result<Result<Value>, Box<dyn Any + Send>>;

/// What a verification run has seen so far, under one lock so a divergence,
/// the stop at the frontier and the served set cannot disagree with each other.
#[derive(Default)]
struct Seen {
    /// The first divergence, if the run found one.
    divergence: Option<Divergence>,
    /// Where the run reached the end of an incomplete history: the position and
    /// the operation the code issued there. A fact about how far the re-run got,
    /// not a divergence — the recorded run had not got there either.
    stopped_at: Option<(i32, String)>,
    /// The recorded positions the re-run reached and matched by name.
    ///
    /// A set of positions, not a count and not the position counter: a call
    /// that is built and dropped claims a position without ever asking for the
    /// record at it, so the counter moves past history the re-run never
    /// verified. What was *served* is the only honest measure of what was
    /// checked.
    served: BTreeSet<i32>,
}

/// Where a verification run books what it saw, shared by every clone of the
/// context it was given to.
///
/// It is the authoritative channel, not the errors the refused calls return: a
/// workflow body may write `let _ = ctx.step(..).await;` or `.ok()` and carry
/// on, so nothing guarantees a refusal reaches the caller. Whatever the body
/// does with the error, the divergence is already recorded here — the cell is
/// the channel, the error is a courtesy.
///
/// A body can also reach the same position from several tasks, so first write
/// wins and later ones are dropped: the first divergence is the one worth
/// reporting anyway.
pub(crate) struct Verification {
    /// Whether the history is complete — the recorded run ended on its own. An
    /// unrecorded operation is [`Divergence::Extra`] against a complete history
    /// and the frontier of an incomplete one.
    complete: bool,
    seen: Mutex<Seen>,
}

impl Verification {
    /// A fresh cell for a history that is `complete` or not (see
    /// [`ReplayReport::complete`]).
    pub(crate) fn new(complete: bool) -> Self {
        Self {
            complete,
            seen: Mutex::new(Seen::default()),
        }
    }

    fn seen(&self) -> MutexGuard<'_, Seen> {
        self.seen.lock().expect("verification state mutex poisoned")
    }

    /// Whether the history being verified is complete.
    pub(crate) fn complete(&self) -> bool {
        self.complete
    }

    /// Book a divergence, if it is the first.
    pub(crate) fn saw(&self, divergence: Divergence) {
        self.seen().divergence.get_or_insert(divergence);
    }

    /// Book that the re-run reached the end of an incomplete history at `seq`,
    /// where the code issues `operation`, if it is the first time.
    pub(crate) fn stopped(&self, seq: i32, operation: &str) {
        self.seen()
            .stopped_at
            .get_or_insert_with(|| (seq, operation.to_owned()));
    }

    /// Book one recorded operation served to the re-run at `seq`.
    pub(crate) fn served_record(&self, seq: i32) {
        self.seen().served.insert(seq);
    }

    /// The report for a re-run of `history` that ended in `outcome`.
    ///
    /// One decision, in this order: a divergence the run booked wins; a run the
    /// verifier stopped at the frontier of an incomplete history is clean; a run
    /// that ended on its own is judged first by how it ended ([`failed`]) and
    /// then by what it never asked for ([`missing`]).
    pub(crate) fn report(&self, history: History<'_>, outcome: RunOutcome) -> ReplayReport {
        let seen = self.seen();
        let divergence = seen.divergence.clone().or_else(|| {
            if seen.stopped_at.is_some() {
                return None;
            }
            failed(history.status, &outcome).or_else(|| missing(history.recorded, &seen.served))
        });
        ReplayReport {
            workflow_id: history.id.to_owned(),
            workflow_name: history.name.to_owned(),
            recorded: history.recorded.len(),
            matched: seen.served.len(),
            complete: self.complete,
            divergence,
        }
    }
}

/// A run that agreed with its history at every operation and still did not end
/// the way the recorded run did.
///
/// No body ran, so the difference is in the re-run itself: a recorded value
/// that no longer decodes as the type now expected under the same name, code
/// between operations that now fails, or a panic. A history that ended in
/// `ERROR` is expected to replay as that error, so an `Err` against one is not
/// reported; nor is an `Ok`, since a run that no longer fails is not a
/// recovery hazard. Against any other status — a completed success as much as
/// a run still in flight — a failing re-run is a failure.
fn failed(status: &str, outcome: &RunOutcome) -> Option<Divergence> {
    if status == STATUS_ERROR {
        return None;
    }
    let error = match outcome {
        Err(payload) => format!("workflow panicked: {}", panic_message(&**payload)),
        Ok(Err(e)) => e.to_string(),
        Ok(Ok(_)) => return None,
    };
    Some(Divergence::Failed { error })
}

/// A history holding an operation the re-run never asked for.
///
/// Asked for, not reached: a call that is built and dropped claims its position
/// without ever consulting the record there, so the position counter is no
/// evidence that anything was verified. The set of positions actually served
/// is. This holds against a prefix as much as against a complete history — a
/// recorded position the re-run walked past without asking is not one it
/// verified, whatever comes after it.
fn missing(recorded: &[StepInfo], served: &BTreeSet<i32>) -> Option<Divergence> {
    recorded
        .iter()
        .find(|op| !served.contains(&op.step_id))
        .map(|op| Divergence::Missing {
            position: op.step_id,
            recorded: op.name.clone(),
        })
}
