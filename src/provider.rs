use crate::error::{Error, Result};
use crate::schedule::{ScheduleFilter, ScheduleStatus, WorkflowSchedule};
use crate::tx::{TransactionOptions, TxBody};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::sync::Arc;
use std::time::Duration;

/// Map a `workflow_status` insert failure to a typed deduplication error when it
/// is a unique-constraint violation — the queue-scoped dedup index. A primary
/// key conflict never reaches here; the inserts use `ON CONFLICT DO NOTHING`.
pub(crate) fn dedup_or(e: sqlx::Error, s: &WorkflowStatus) -> Error {
    let err = Error::from(e);
    if err.is_unique_violation() {
        return Error::queue_deduplicated(
            s.queue_name.clone().unwrap_or_default(),
            s.dedup_id.clone().unwrap_or_default(),
        );
    }
    err
}

/// Map a notification insert failure to a typed "no such workflow" error when
/// the destination foreign key is violated.
pub(crate) fn nonexistent_or(e: sqlx::Error, destination_id: &str) -> Error {
    let err = Error::from(e);
    if err.is_foreign_key_violation() {
        return Error::nonexistent_workflow(destination_id);
    }
    err
}

/// Encode the authenticated-roles list for storage in the single nullable
/// `authenticated_roles` text column: a JSON array, or `NULL` when empty. This
/// is the cross-SDK on-disk shape, so workers in other languages read it back.
pub(crate) fn encode_roles(roles: &[String]) -> Option<String> {
    if roles.is_empty() {
        None
    } else {
        serde_json::to_string(roles).ok()
    }
}

/// Decode the `authenticated_roles` column written by [`encode_roles`] (or by
/// another SDK): a JSON array of strings, with `NULL`/unparseable → empty.
pub(crate) fn decode_roles(stored: Option<&str>) -> Vec<String> {
    stored
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or_default()
}

// The workflow lifecycle states stored in the `status` column. These string
// constants are the on-disk values, shared verbatim with the other DBOS SDKs.

/// Enqueued and waiting to be claimed by a dispatcher.
pub const STATUS_ENQUEUED: &str = "ENQUEUED";
/// Enqueued with a delay; transitions to [`STATUS_ENQUEUED`] once it is due.
pub const STATUS_DELAYED: &str = "DELAYED";
/// Claimed by an executor and currently running.
pub const STATUS_PENDING: &str = "PENDING";
/// Terminal: the workflow completed and its output is recorded.
pub const STATUS_SUCCESS: &str = "SUCCESS";
/// Terminal: the workflow failed and its error is recorded.
pub const STATUS_ERROR: &str = "ERROR";
/// Terminated by an operator; replay is refused.
pub const STATUS_CANCELLED: &str = "CANCELLED";
/// Recovered too many times; parked until manually resumed.
pub const STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED: &str = "MAX_RECOVERY_ATTEMPTS_EXCEEDED";

/// Value written into a stream's `value` column to mark it closed. Stored
/// verbatim (no serialization), so a reader in any language recognizes the
/// close the same way — a shared on-disk identifier, like the `DBOS.*` step
/// names. User values are serializer-encoded, so they never collide with it.
pub(crate) const STREAM_CLOSED_SENTINEL: &str = "__DBOS_STREAM_CLOSED__";

/// `LISTEN`/`NOTIFY` channel a new `notifications` row is announced on (the
/// `dbos_notifications_trigger` payload is `destination_uuid::topic`). Shared
/// verbatim with the other SDKs and the schema trigger.
#[cfg(feature = "postgres")]
pub(crate) const NOTIFICATIONS_CHANNEL: &str = "dbos_notifications_channel";
/// `LISTEN`/`NOTIFY` channel a new `workflow_events` row is announced on (the
/// `dbos_workflow_events_trigger` payload is `workflow_uuid::key`).
#[cfg(feature = "postgres")]
pub(crate) const WORKFLOW_EVENTS_CHANNEL: &str = "dbos_workflow_events_channel";

/// A condition a blocked `recv`/`get_event` wants to be nudged about, so it can
/// re-check the database promptly instead of waiting out its poll interval. A
/// backend with push signalling (Postgres `LISTEN`/`NOTIFY`) maps each variant to
/// its channel + payload; others ignore it and simply sleep.
#[derive(Clone, Copy, Debug)]
pub enum ChangeWait<'a> {
    /// A notification delivered to `workflow_id`'s mailbox on `topic`.
    Notification {
        /// Recipient workflow whose mailbox is being watched.
        workflow_id: &'a str,
        /// Topic the awaited message is sent on.
        topic: &'a str,
    },
    /// Event `key` set on `workflow_id`.
    Event {
        /// Workflow whose event is being watched.
        workflow_id: &'a str,
        /// Event key being awaited.
        key: &'a str,
    },
}

impl ChangeWait<'_> {
    /// The `LISTEN`/`NOTIFY` channel this condition is announced on.
    #[cfg(feature = "postgres")]
    pub(crate) fn channel(&self) -> &'static str {
        match self {
            ChangeWait::Notification { .. } => NOTIFICATIONS_CHANNEL,
            ChangeWait::Event { .. } => WORKFLOW_EVENTS_CHANNEL,
        }
    }

    /// The `NOTIFY` payload the schema trigger emits for this condition
    /// (`workflow_uuid::topic` / `workflow_uuid::key`).
    #[cfg(feature = "postgres")]
    pub(crate) fn payload(&self) -> String {
        match self {
            ChangeWait::Notification { workflow_id, topic } => format!("{workflow_id}::{topic}"),
            ChangeWait::Event { workflow_id, key } => format!("{workflow_id}::{key}"),
        }
    }
}

/// Group ordered `(key, value, serialization)` stream rows — sorted by key then
/// offset — into one `(key, decoded values)` entry per key, decoding each value
/// and dropping the close sentinel (a key present only via its sentinel still
/// appears, with an empty value list). Shared by the SQL backends'
/// `list_workflow_streams`.
pub(crate) fn group_stream_rows(
    serializer: &crate::serialize::Serializer,
    rows: Vec<(String, String, Option<String>)>,
) -> Result<Vec<(String, Vec<Value>)>> {
    let mut out: Vec<(String, Vec<Value>)> = Vec::new();
    for (key, value, fmt) in rows {
        if value == STREAM_CLOSED_SENTINEL {
            if out.last().map(|(k, _)| k != &key).unwrap_or(true) {
                out.push((key, Vec::new()));
            }
            continue;
        }
        let decoded = crate::serialize::decode(serializer, fmt.as_deref(), &value)?;
        match out.last_mut() {
            Some((k, vals)) if *k == key => vals.push(decoded),
            _ => out.push((key, vec![decoded])),
        }
    }
    Ok(out)
}

/// How often [`drain_stream`] re-polls the backend for new stream values while
/// the producer is still active.
const STREAM_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How many workflows one garbage-collection `DELETE` may remove: collection
/// over a large backlog runs as a sequence of short transactions of this size,
/// so vacuum keeps up and the hot `workflow_status` table is never pinned by
/// one long delete.
pub(crate) const GC_BATCH: i64 = 10_000;

/// Whether a workflow's stored `attributes` contain every key-value pair of
/// `filter` — the in-memory equivalent of Postgres JSONB `@>` containment at
/// the top level: each filter key must exist with an equal value (equality is
/// deep for nested values, matching how `@>` treats a filter whose values are
/// scalars or exact sub-objects).
pub(crate) fn attributes_contain(stored: Option<&Value>, filter: &Map<String, Value>) -> bool {
    let Some(Value::Object(stored)) = stored else {
        return filter.is_empty();
    };
    filter
        .iter()
        .all(|(k, v)| stored.get(k).is_some_and(|s| contains_value(s, v)))
}

/// JSONB `@>` containment between two values: objects contain a subset of
/// keys (recursively), arrays contain every element of the filter array, and
/// scalars compare by equality.
fn contains_value(stored: &Value, filter: &Value) -> bool {
    match (stored, filter) {
        (Value::Object(s), Value::Object(f)) => f
            .iter()
            .all(|(k, v)| s.get(k).is_some_and(|sv| contains_value(sv, v))),
        (Value::Array(s), Value::Array(f)) => {
            f.iter().all(|fv| s.iter().any(|sv| contains_value(sv, fv)))
        }
        (s, f) => s == f,
    }
}

/// Resolve the effective garbage-collection cutoff from the two bounds — the
/// newer of the absolute `cutoff_epoch_ms` and the `created_at` of the
/// `rows_threshold`-th-newest workflow. `None` means nothing to collect (no
/// bound given, or fewer than `rows_threshold` workflows exist and no absolute
/// cutoff). Validates `rows_threshold > 0`. Shared by the trait's default
/// [`garbage_collect`](StateProvider::garbage_collect) and the SQL overrides.
pub(crate) async fn resolve_gc_cutoff<P: StateProvider + ?Sized>(
    provider: &P,
    cutoff_epoch_ms: Option<i64>,
    rows_threshold: Option<i64>,
) -> Result<Option<i64>> {
    if let Some(t) = rows_threshold {
        if t <= 0 {
            return Err(Error::app(format!(
                "rows_threshold must be positive, got {t}"
            )));
        }
    }
    let mut cutoff = cutoff_epoch_ms;
    if let Some(threshold) = rows_threshold {
        let nth_newest = provider
            .list_workflows(&ListFilter {
                sort_desc: true,
                limit: Some(1),
                offset: Some(threshold - 1),
                load_input: false,
                load_output: false,
                ..Default::default()
            })
            .await?;
        if let Some(w) = nth_newest.first() {
            let rows_cutoff = w.created_at.timestamp_millis();
            // The more restrictive (newer) bound wins.
            if cutoff.is_none_or(|c| rows_cutoff > c) {
                cutoff = Some(rows_cutoff);
            }
        }
    }
    Ok(cutoff)
}

/// Read stream `key` on `workflow_id` in order, blocking until it is closed (a
/// producer called `close_stream`) or the producing workflow goes inactive (no
/// longer `PENDING`/`ENQUEUED`) — after which no more values can arrive. Returns
/// every value written, in order, and whether the stream is closed. Polls the
/// backend at [`STREAM_POLL_INTERVAL`]; errors if the workflow does not exist.
///
/// This is a *live* read, not a durable step — it is never checkpointed, so a
/// reader (the engine, the client, or a workflow via `ctx.read_stream`) re-reads
/// from the start on replay. Shared by all three.
pub(crate) async fn drain_stream<T: DeserializeOwned>(
    provider: &dyn StateProvider,
    workflow_id: &str,
    key: &str,
) -> Result<(Vec<T>, bool)> {
    drain_stream_from(provider, workflow_id, key).await
}

/// The two backend reads [`drain_stream`] performs, factored into a narrow seam
/// so the drain loop — including the subtle drain-on-inactive ordering — can be
/// unit-tested against a scripted backend without standing up a whole
/// [`StateProvider`]. Blanket-implemented for every provider, so the public
/// entry point and its callers pass their `&dyn StateProvider` unchanged.
#[async_trait]
trait StreamBackend {
    /// Stream `(workflow_id, key)` entries at `from_offset`, and whether the
    /// close sentinel has been reached.
    async fn stream_entries(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)>;

    /// The producing workflow's current status, or `None` if it does not exist.
    async fn producer_status(&self, workflow_id: &str) -> Result<Option<String>>;
}

#[async_trait]
impl<T: StateProvider + ?Sized> StreamBackend for T {
    async fn stream_entries(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)> {
        self.read_stream(workflow_id, key, from_offset).await
    }

    async fn producer_status(&self, workflow_id: &str) -> Result<Option<String>> {
        Ok(self
            .get_workflow_status(workflow_id)
            .await?
            .map(|s| s.status))
    }
}

/// The drain loop itself, generic over the [`StreamBackend`] seam.
async fn drain_stream_from<T: DeserializeOwned, S: StreamBackend + ?Sized>(
    source: &S,
    workflow_id: &str,
    key: &str,
) -> Result<(Vec<T>, bool)> {
    let mut all = Vec::new();
    let mut offset = 0_i32;
    // Set once the producer is observed inactive; the loop then makes one more
    // read pass to drain anything it committed just before terminating, and only
    // then closes the stream.
    let mut final_read = false;
    loop {
        let (values, closed) = source.stream_entries(workflow_id, key, offset).await?;
        offset += values.len() as i32;
        for v in values {
            all.push(serde_json::from_value(v)?);
        }
        if closed {
            return Ok((all, true));
        }
        // A previous pass saw the producer inactive; this pass has now drained
        // whatever it committed in the meantime, so the stream is complete.
        if final_read {
            return Ok((all, true));
        }
        // No close sentinel yet: keep reading only while the producer is still
        // active.
        match source.producer_status(workflow_id).await? {
            None => return Err(Error::nonexistent_workflow(workflow_id)),
            Some(s) if s != STATUS_PENDING && s != STATUS_ENQUEUED => {
                // The producer is inactive, but it may have committed values
                // between the read above and this status check. Once it is
                // terminal all of its writes are committed, so make one more read
                // pass to drain to the end of the stream before closing, rather
                // than dropping a value written just before completion.
                final_read = true;
                continue;
            }
            _ => {}
        }
        tokio::time::sleep(STREAM_POLL_INTERVAL).await;
    }
}

/// Read the currently-available values of stream `key` on `workflow_id` from
/// `from_offset`, without blocking. Returns the values in order and whether the
/// close sentinel has been reached. Pass the count read so far as the next
/// `from_offset` to poll incrementally.
pub(crate) async fn snapshot_stream<T: DeserializeOwned>(
    provider: &dyn StateProvider,
    workflow_id: &str,
    key: &str,
    from_offset: i32,
) -> Result<(Vec<T>, bool)> {
    let (values, closed) = provider.read_stream(workflow_id, key, from_offset).await?;
    let out = values
        .into_iter()
        .map(serde_json::from_value)
        .collect::<std::result::Result<Vec<T>, _>>()?;
    Ok((out, closed))
}

/// Read stream `key` on `workflow_id` as an asynchronous [`Stream`], yielding
/// each value in order as it is committed — the incremental counterpart to
/// [`drain_stream`], which instead blocks and returns the whole stream at once.
/// The stream ends (`None`) when the producer closes the stream or goes inactive
/// (the same termination [`drain_stream`] uses, including the final drain pass);
/// a decode failure, a backend error, or a missing workflow is yielded as a
/// single terminal `Err`, after which the stream ends.
///
/// Like [`drain_stream`] this is a *live* read, never checkpointed: a workflow
/// reader re-reads from the start on replay. The returned stream borrows
/// `source`, and is lazy — it polls the backend only as the consumer pulls.
pub(crate) fn stream_values<'a, T>(
    source: &'a dyn StateProvider,
    workflow_id: &str,
    key: &str,
) -> std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<T>> + 'a>>
where
    T: DeserializeOwned + 'a,
{
    /// Drives the incremental drain across `unfold` steps. `buffer` holds values
    /// read but not yet yielded; `final_read` mirrors [`drain_stream`]'s
    /// one-more-pass after the producer is seen inactive; `done` latches the end
    /// after a terminal `Err`. The backend is reached through the blanket
    /// [`StreamBackend`] impl on `dyn StateProvider`.
    struct State<'a> {
        source: &'a dyn StateProvider,
        workflow_id: String,
        key: String,
        offset: i32,
        buffer: std::collections::VecDeque<Value>,
        final_read: bool,
        done: bool,
    }

    let init = State {
        source,
        workflow_id: workflow_id.to_string(),
        key: key.to_string(),
        offset: 0,
        buffer: std::collections::VecDeque::new(),
        final_read: false,
        done: false,
    };

    Box::pin(futures_util::stream::unfold(init, |mut st| async move {
        if st.done {
            return None;
        }
        loop {
            // Emit anything already read before touching the backend again.
            if let Some(v) = st.buffer.pop_front() {
                return match serde_json::from_value::<T>(v) {
                    Ok(t) => Some((Ok(t), st)),
                    Err(e) => {
                        st.done = true;
                        Some((Err(Error::from(e)), st))
                    }
                };
            }
            // Buffer drained: read the next batch from the current offset.
            let (values, closed) = match st
                .source
                .stream_entries(&st.workflow_id, &st.key, st.offset)
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    st.done = true;
                    return Some((Err(e), st));
                }
            };
            st.offset += values.len() as i32;
            st.buffer.extend(values);
            if !st.buffer.is_empty() {
                continue; // hand the freshly-read values to the buffer arm above
            }
            if closed || st.final_read {
                return None;
            }
            // No values and no close yet: keep going only while the producer is
            // active, with a final drain pass once it is observed inactive.
            match st.source.producer_status(&st.workflow_id).await {
                Err(e) => {
                    st.done = true;
                    return Some((Err(e), st));
                }
                Ok(None) => {
                    st.done = true;
                    return Some((Err(Error::nonexistent_workflow(&st.workflow_id)), st));
                }
                Ok(Some(s)) if s != STATUS_PENDING && s != STATUS_ENQUEUED => {
                    st.final_read = true;
                    continue;
                }
                Ok(_) => {}
            }
            tokio::time::sleep(STREAM_POLL_INTERVAL).await;
        }
    }))
}

/// `true` if `status` is terminal (no further execution will occur).
pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        STATUS_SUCCESS | STATUS_ERROR | STATUS_CANCELLED | STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED
    )
}

/// A persisted workflow instance.
///
/// Carries everything the engine, queues, and management APIs need. Fields for
/// features that are not implemented yet are present anyway so the storage
/// schema stays stable as those features land.
#[derive(Clone, Debug)]
pub struct WorkflowStatus {
    /// Unique workflow id (the `workflow_uuid` primary key).
    pub id: String,
    /// Registered name of the workflow function.
    pub name: String,
    /// Current lifecycle state — one of the `STATUS_*` constants.
    pub status: String,
    /// The workflow's input, serialized as stored.
    pub input: Value,
    /// Present once the workflow reaches `SUCCESS`.
    pub output: Option<Value>,
    /// Present once the workflow reaches `ERROR`: the human-readable message.
    /// For a `portable_json` row this is the envelope's `message` field.
    pub error: Option<String>,
    /// The structured error for a workflow that failed under portable
    /// serialization — `name`/`code`/`data` as written by any SDK (a Rust error
    /// carries the generic name [`crate::PortableWorkflowError`] documents).
    /// `None` for a non-portable row or a workflow that did not fail.
    pub error_info: Option<crate::PortableWorkflowError>,
    /// The executor (process) that owns this run; empty until claimed.
    pub executor_id: String,
    /// Application version that produced this row — recovery is version-gated.
    /// Empty means "unset", and is persisted as SQL NULL, never as `''`: every
    /// SDK's dequeue gate admits an unversioned row with
    /// `application_version = $n OR application_version IS NULL`, which an
    /// empty string would not match.
    pub app_version: String,
    /// Queue this workflow was enqueued on, if any.
    pub queue_name: Option<String>,
    /// Partition key within a partitioned queue, if any.
    pub queue_partition_key: Option<String>,
    /// Dispatch priority within a queue; lower runs first.
    pub priority: i32,
    /// Deduplication key, unique per queue among active workflows.
    pub dedup_id: Option<String>,
    /// How many times recovery has re-dispatched this workflow. Incremented on
    /// each recovery pass; once it exceeds the engine's `max_recovery_attempts`
    /// the workflow is parked in `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
    pub recovery_attempts: i32,
    /// Parent workflow id (reserved for child workflows; not yet populated).
    pub parent_workflow_id: Option<String>,
    /// Wall-clock timeout for the whole workflow, if one was requested.
    /// For queued workflows the deadline is computed from this at claim time.
    pub timeout_ms: Option<i64>,
    /// Absolute deadline in epoch millis, fixed once the workflow starts.
    pub deadline_ms: Option<i64>,
    /// When the workflow was claimed and started (ENQUEUED→PENDING), epoch ms.
    pub started_at_ms: Option<i64>,
    /// `true` when dequeued from a rate-limited queue, so the rate limiter only
    /// counts starts it governs.
    pub rate_limited: bool,
    /// For `DELAYED` workflows: when to transition to `ENQUEUED`, epoch ms.
    pub delay_until_ms: Option<i64>,
    /// When the workflow reached a terminal state, epoch ms.
    pub completed_at_ms: Option<i64>,
    /// On a forked workflow, the id it was forked from.
    pub forked_from: Option<String>,
    /// User on whose behalf the workflow was started, if any.
    pub authenticated_user: Option<String>,
    /// Role the workflow assumed for this run, if any.
    pub assumed_role: Option<String>,
    /// Full set of roles available to the authenticated user.
    pub authenticated_roles: Vec<String>,
    /// Class / namespace name (e.g. a Python class whose method is the workflow).
    /// Passive metadata in Rust — persisted and round-tripped for cross-SDK
    /// compatibility, not itself used to route dispatch.
    pub class_name: Option<String>,
    /// Config / instance name: selects among multiple handlers registered under
    /// the *same* workflow name (one per configured instance), and is durably
    /// recorded so recovery re-dispatches to the same instance.
    pub config_name: Option<String>,
    /// When the row was first created.
    pub created_at: DateTime<Utc>,
    /// When the row was last modified.
    pub updated_at: DateTime<Utc>,
    /// Custom user-defined attributes attached to this workflow (a JSON
    /// object), searchable via [`ListFilter::attributes`] containment on
    /// Postgres. `None` when no attributes are set.
    pub attributes: Option<Value>,
}

impl WorkflowStatus {
    /// The application version as it must be **persisted**: `None` when unset,
    /// so the column holds SQL NULL rather than `''`.
    ///
    /// Every SDK's dequeue gate admits an unversioned row with
    /// `application_version = $n OR application_version IS NULL`. An empty
    /// string satisfies neither predicate, so a row written as `''` is
    /// invisible to Go, Python, and TypeScript executors and sits `ENQUEUED`
    /// forever. Normalizing here keeps that impossible to reintroduce at any
    /// single call site.
    pub(crate) fn app_version_opt(&self) -> Option<&str> {
        Some(self.app_version.as_str()).filter(|v| !v.is_empty())
    }

    /// A fresh row for `id`/`name`/`input` in the given non-terminal `status`,
    /// stamped with the owning executor and app version. Optional fields default
    /// to empty; callers set queue/priority/etc. as needed.
    pub fn new(
        id: impl Into<String>,
        name: impl Into<String>,
        input: Value,
        status: impl Into<String>,
        executor_id: impl Into<String>,
        app_version: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            name: name.into(),
            status: status.into(),
            input,
            output: None,
            error: None,
            error_info: None,
            executor_id: executor_id.into(),
            app_version: app_version.into(),
            queue_name: None,
            queue_partition_key: None,
            priority: 0,
            dedup_id: None,
            recovery_attempts: 0,
            parent_workflow_id: None,
            timeout_ms: None,
            deadline_ms: None,
            started_at_ms: None,
            rate_limited: false,
            delay_until_ms: None,
            completed_at_ms: None,
            forked_from: None,
            authenticated_user: None,
            assumed_role: None,
            authenticated_roles: Vec::new(),
            class_name: None,
            config_name: None,
            created_at: now,
            updated_at: now,
            attributes: None,
        }
    }
}

/// Filter for [`StateProvider::list_workflows`]. All fields are ANDed;
/// empty/`None` fields are ignored. Times are epoch milliseconds.
///
/// `start_time_ms`/`end_time_ms` bound `created_at`; the dedicated
/// `completed_*`/`dequeued_*` bounds match `completed_at`/`started_at`.
#[derive(Clone)]
pub struct ListFilter {
    /// Match any of these exact workflow ids (OR).
    pub workflow_ids: Vec<String>,
    /// Match any workflow whose id starts with one of these prefixes (OR).
    pub workflow_id_prefix: Vec<String>,
    /// Match any of these workflow names (OR).
    pub name: Vec<String>,
    /// Match any of these statuses.
    pub status: Vec<String>,
    /// Match any of these queue names (OR).
    pub queue_name: Vec<String>,
    /// Match any of these application versions (OR).
    pub app_version: Vec<String>,
    /// Match any of these executor ids (OR).
    pub executor_ids: Vec<String>,
    /// Match any of these authenticated users (OR).
    pub authenticated_users: Vec<String>,
    /// Match any workflow forked from one of these source ids (OR).
    pub forked_from: Vec<String>,
    /// Match any workflow whose parent is one of these ids (OR).
    pub parent_workflow_ids: Vec<String>,
    /// `Some(true)` keeps only workflows that were themselves created by a fork;
    /// `Some(false)` only those that were not; `None` does not filter on it.
    pub was_forked_from: Option<bool>,
    /// Lower bound (inclusive) on `created_at`, epoch ms.
    pub start_time_ms: Option<i64>,
    /// Upper bound (inclusive) on `created_at`, epoch ms.
    pub end_time_ms: Option<i64>,
    /// Lower bound on `completed_at` (epoch ms).
    pub completed_after_ms: Option<i64>,
    /// Upper bound on `completed_at` (epoch ms).
    pub completed_before_ms: Option<i64>,
    /// Lower bound on `started_at` — when the workflow was dequeued/started
    /// (epoch ms).
    pub dequeued_after_ms: Option<i64>,
    /// Upper bound on `started_at` — when the workflow was dequeued/started
    /// (epoch ms).
    pub dequeued_before_ms: Option<i64>,
    /// `Some(true)` keeps only workflows that have a parent; `Some(false)` only
    /// those that don't; `None` does not filter on parentage.
    pub has_parent: Option<bool>,
    /// Keep only workflows whose attributes **contain** all of these key-value
    /// pairs (JSONB `@>` containment, served by a GIN index). Requires a
    /// Postgres backend: SQLite errors on an attribute filter, matching the
    /// reference SDKs; the in-memory backend emulates containment.
    pub attributes: Option<Map<String, Value>>,
    /// Maximum number of rows to return; `None` for no limit.
    pub limit: Option<i64>,
    /// Number of matching rows to skip before returning (for pagination).
    pub offset: Option<i64>,
    /// Sort by `created_at` descending instead of ascending.
    pub sort_desc: bool,
    /// Return only workflows that are (or were) on a queue — those with a
    /// non-null `queue_name`.
    pub queues_only: bool,
    /// When `false`, the `input` field is omitted from results (returned as
    /// `Null`) and not read from the database. Defaults to `true`.
    pub load_input: bool,
    /// When `false`, the `output` field is omitted from results (returned as
    /// `None`) and not read from the database. Defaults to `true`.
    pub load_output: bool,
}

impl Default for ListFilter {
    fn default() -> Self {
        Self {
            workflow_ids: Vec::new(),
            workflow_id_prefix: Vec::new(),
            name: Vec::new(),
            status: Vec::new(),
            queue_name: Vec::new(),
            app_version: Vec::new(),
            executor_ids: Vec::new(),
            authenticated_users: Vec::new(),
            forked_from: Vec::new(),
            parent_workflow_ids: Vec::new(),
            was_forked_from: None,
            start_time_ms: None,
            end_time_ms: None,
            completed_after_ms: None,
            completed_before_ms: None,
            dequeued_after_ms: None,
            dequeued_before_ms: None,
            has_parent: None,
            attributes: None,
            limit: None,
            offset: None,
            sort_desc: false,
            queues_only: false,
            // Loading input/output is the default; callers opt out for cheaper scans.
            load_input: true,
            load_output: true,
        }
    }
}

/// Grouping and filters for [`StateProvider::get_workflow_aggregates`]: count
/// workflows grouped by one or more `workflow_status` columns and/or a
/// `created_at` time bucket.
///
/// At least one `by_*` flag must be set, or `time_bucket_ms` must be `Some`;
/// the filter fields narrow which workflows are counted before grouping.
#[derive(Clone, Default)]
pub struct WorkflowAggregateQuery {
    /// Group by workflow `status`.
    pub by_status: bool,
    /// Group by workflow `name`.
    pub by_name: bool,
    /// Group by `queue_name`.
    pub by_queue_name: bool,
    /// Group by `executor_id`.
    pub by_executor_id: bool,
    /// Group by `application_version`.
    pub by_app_version: bool,
    /// Select the per-group row count.
    pub select_count: bool,
    /// Select the earliest `created_at` in the group (epoch ms).
    pub select_min_created_at: bool,
    /// Select the longest queue wait in the group: `MAX(started_at - created_at)`
    /// in ms. Workflows that never started (no `started_at`) are ignored.
    pub select_max_queue_wait_ms: bool,
    /// Select the longest end-to-end latency in the group:
    /// `MAX(completed_at - created_at)` in ms. Unfinished workflows are ignored.
    pub select_max_total_latency_ms: bool,
    /// Also group by `created_at` bucket of this size in milliseconds.
    pub time_bucket_ms: Option<i64>,
    // Filters (all ANDed; empty/`None` ignored).
    /// Keep only these statuses.
    pub status: Vec<String>,
    /// Keep only these workflow names.
    pub name: Vec<String>,
    /// Keep only these application versions.
    pub app_version: Vec<String>,
    /// Keep only these executor ids.
    pub executor_ids: Vec<String>,
    /// Keep only these queue names.
    pub queue_names: Vec<String>,
    /// Keep only workflows whose id starts with this prefix.
    pub workflow_id_prefix: Option<String>,
    /// Lower bound on `created_at` (epoch ms).
    pub start_time_ms: Option<i64>,
    /// Upper bound on `created_at` (epoch ms).
    pub end_time_ms: Option<i64>,
    /// Lower bound on `completed_at` (epoch ms).
    pub completed_after_ms: Option<i64>,
    /// Upper bound on `completed_at` (epoch ms).
    pub completed_before_ms: Option<i64>,
    /// Lower bound on `started_at` — when the workflow was dequeued/started
    /// (epoch ms).
    pub dequeued_after_ms: Option<i64>,
    /// Upper bound on `started_at` — when the workflow was dequeued/started
    /// (epoch ms).
    pub dequeued_before_ms: Option<i64>,
    /// Cap on the number of group rows returned.
    pub limit: Option<i64>,
}

/// The grouping-dimension keys used in [`WorkflowAggregate::group`], in a stable
/// order. Shared identifiers, matching the `workflow_status` column names.
pub(crate) const AGG_DIMENSIONS: &[(&str, &str)] = &[
    ("status", "status"),
    ("name", "name"),
    ("queue_name", "queue_name"),
    ("executor_id", "executor_id"),
    ("application_version", "application_version"),
];

impl WorkflowAggregateQuery {
    /// The enabled grouping dimensions as `(group_key, column)` pairs, in stable
    /// order; the `time_bucket` dimension (if any) is handled separately by each
    /// backend since it is a computed expression.
    pub(crate) fn enabled_columns(&self) -> Vec<(&'static str, &'static str)> {
        let flags = [
            self.by_status,
            self.by_name,
            self.by_queue_name,
            self.by_executor_id,
            self.by_app_version,
        ];
        AGG_DIMENSIONS
            .iter()
            .zip(flags)
            .filter(|(_, on)| *on)
            .map(|(d, _)| *d)
            .collect()
    }

    /// `true` when nothing to group by — an invalid query.
    pub fn is_empty(&self) -> bool {
        self.enabled_columns().is_empty() && self.time_bucket_ms.is_none()
    }

    /// `true` when no aggregate is selected — an invalid query.
    pub fn no_select(&self) -> bool {
        !self.select_count
            && !self.select_min_created_at
            && !self.select_max_queue_wait_ms
            && !self.select_max_total_latency_ms
    }
}

/// The selected aggregate expressions for `get_workflow_aggregates`, each as
/// `EXPR AS alias`, in a stable order (the aliases are read back by the SQL
/// backends' `row_to_aggregate`). The engine guarantees at least one is selected.
/// The column names are identical on SQLite and Postgres, so this is shared.
pub(crate) fn workflow_agg_selects(q: &WorkflowAggregateQuery) -> Vec<&'static str> {
    let mut sel = Vec::new();
    if q.select_count {
        sel.push("COUNT(*) AS cnt");
    }
    if q.select_min_created_at {
        sel.push("MIN(created_at) AS min_created_at");
    }
    if q.select_max_queue_wait_ms {
        sel.push("MAX(started_at_epoch_ms - created_at) AS max_queue_wait_ms");
    }
    if q.select_max_total_latency_ms {
        sel.push("MAX(completed_at - created_at) AS max_total_latency_ms");
    }
    sel
}

/// One aggregate group from [`StateProvider::get_workflow_aggregates`]. Each
/// aggregate is `Some` only when the query selected it (an unselected aggregate
/// is `None`, serialized as `null`, matching the other SDKs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowAggregate {
    /// Each enabled grouping dimension → its value for this group. `None` is a
    /// NULL grouped value (e.g. a workflow with no `queue_name`). The
    /// `time_bucket` value, when present, is the bucket's start in epoch ms.
    pub group: std::collections::BTreeMap<String, Option<String>>,
    /// How many workflows fell into this group.
    pub count: Option<i64>,
    /// Earliest `created_at` in the group (epoch ms).
    pub min_created_at: Option<i64>,
    /// Longest queue wait in the group: `MAX(started_at - created_at)` in ms.
    pub max_queue_wait_ms: Option<i64>,
    /// Longest end-to-end latency in the group: `MAX(completed_at - created_at)`
    /// in ms.
    pub max_total_latency_ms: Option<i64>,
}

/// A step's status derived from `operation_outputs`: a NULL `error` means the
/// step succeeded, otherwise it errored. There is no explicit status column, so
/// this SQL expression stands in for one wherever step status is grouped or
/// filtered.
pub(crate) const STEP_STATUS_EXPR: &str =
    "(CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END)";

/// Grouping, selected aggregates, and filters for
/// [`StateProvider::get_step_aggregates`]: aggregate `operation_outputs` rows
/// grouped by function name and/or derived status and/or a `completed_at` time
/// bucket.
///
/// At least one `by_*` flag must be set or `time_bucket_ms` must be `Some`, and
/// at least one `select_*` flag must be set.
#[derive(Clone, Default)]
pub struct StepAggregateQuery {
    /// Group by step `function_name`.
    pub by_function_name: bool,
    /// Group by derived step status: `SUCCESS` when the step's `error` is null,
    /// else `ERROR`.
    pub by_status: bool,
    /// Select the per-group row count.
    pub select_count: bool,
    /// Select the per-group maximum step duration (`completed_at - started_at`).
    /// Rows with no recorded timing (instantaneous markers) are ignored.
    pub select_max_duration_ms: bool,
    /// Also group by `completed_at` bucket of this size in milliseconds.
    pub time_bucket_ms: Option<i64>,
    // Filters (all ANDed; empty/`None` ignored).
    /// Keep only these derived statuses (`SUCCESS`/`ERROR`).
    pub status: Vec<String>,
    /// Keep only these step function names.
    pub function_name: Vec<String>,
    /// Keep only steps of workflows whose id starts with this prefix.
    pub workflow_id_prefix: Option<String>,
    /// Lower bound on `completed_at` (epoch ms).
    pub completed_after_ms: Option<i64>,
    /// Upper bound on `completed_at` (epoch ms).
    pub completed_before_ms: Option<i64>,
    /// Cap on the number of group rows returned.
    pub limit: Option<i64>,
}

impl StepAggregateQuery {
    /// The enabled grouping dimensions as `(group_key, sql_expr)` pairs, in
    /// stable order. `status` maps to [`STEP_STATUS_EXPR`] rather than a column;
    /// `time_bucket` is a computed expression handled separately per backend.
    pub(crate) fn group_exprs(&self) -> Vec<(&'static str, &'static str)> {
        let mut v = Vec::new();
        if self.by_function_name {
            v.push(("function_name", "function_name"));
        }
        if self.by_status {
            v.push(("status", STEP_STATUS_EXPR));
        }
        v
    }

    /// `true` when nothing to group by — an invalid query.
    pub fn no_grouping(&self) -> bool {
        !self.by_function_name && !self.by_status && self.time_bucket_ms.is_none()
    }

    /// `true` when no aggregate is selected — an invalid query.
    pub fn no_select(&self) -> bool {
        !self.select_count && !self.select_max_duration_ms
    }
}

/// One aggregate group from [`StateProvider::get_step_aggregates`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepAggregate {
    /// Each enabled grouping dimension → its value (`function_name`, `status`,
    /// and/or `time_bucket` as the bucket start in epoch ms).
    pub group: std::collections::BTreeMap<String, Option<String>>,
    /// Row count for this group, if `select_count` was set.
    pub count: Option<i64>,
    /// Maximum step duration in ms for this group, if `select_max_duration_ms`
    /// was set; `None` when no row in the group had recorded timing.
    pub max_duration_ms: Option<i64>,
}

/// One notification in a workflow's `send`/`recv` mailbox, surfaced by
/// [`StateProvider::list_workflow_notifications`].
#[derive(Clone, Debug)]
pub struct NotificationInfo {
    /// The topic it was sent on, or `None` when sent without one.
    pub topic: Option<String>,
    /// The decoded message payload.
    pub message: Value,
    /// When it was enqueued, epoch ms.
    pub created_at_ms: i64,
    /// Whether a `recv` has already consumed it.
    pub consumed: bool,
}

/// One prepared notification row for
/// [`StateProvider::insert_notifications`] — the provider-level form of a
/// bulk-send message, with the payload already serialized to JSON.
#[derive(Clone, Debug)]
pub struct NotificationInsert {
    /// Destination workflow id.
    pub destination_id: String,
    /// Topic the destination's `recv` listens on.
    pub topic: String,
    /// The serialized message payload.
    pub message: Value,
    /// Optional at-most-once key, scoped per destination (see
    /// [`StateProvider::insert_notification`]).
    pub idempotency_key: Option<String>,
}

/// One recorded operation of a workflow.
///
/// Materialized from an `operation_outputs` row by
/// [`StateProvider::get_workflow_steps`]; durable steps, sleeps, sends, and
/// child-workflow invocations all surface here, ordered by [`step_id`](Self::step_id).
#[derive(Clone, Debug)]
pub struct StepInfo {
    /// Sequence index of the operation within the workflow (its `function_id`).
    pub step_id: i32,
    /// The step's recorded name (e.g. a step name, or `DBOS.sleep`/`DBOS.send`).
    pub name: String,
    /// The decoded output, if any (`None` for operations that record no value).
    pub output: Option<Value>,
    /// The recorded error string, if the operation failed.
    pub error: Option<String>,
    /// The child workflow this operation started, if it was a child-workflow call.
    pub child_workflow_id: Option<String>,
    /// When the operation started, if step timing was recorded.
    pub started_at: Option<DateTime<Utc>>,
    /// When the operation completed, if step timing was recorded.
    pub completed_at: Option<DateTime<Utc>>,
}

/// The recorded outcome of a durable step: a successful output value, or a
/// failure. A step's outcome is checkpointed exactly once — on replay it is
/// returned without re-running, so a step that failed stays failed (and a
/// non-deterministic step does not silently succeed on a later attempt).
#[derive(Clone, Debug)]
pub enum StepOutcome {
    /// The step succeeded; carries its decoded output.
    Output(Value),
    /// The step failed; carries the human message and — for a portable row — the
    /// structured error, mirroring [`WorkflowStatus::error`]/`error_info`.
    Failure {
        /// Human-readable error message.
        message: String,
        /// Structured error, present when the row used portable serialization.
        info: Option<crate::PortableWorkflowError>,
    },
}

impl StepOutcome {
    /// The value this outcome represents: a recorded `Output` is returned as
    /// `Ok`, a recorded `Failure` as the reconstructed `Err` — the structured
    /// [`Error::Portable`] when the row carried one, else a plain application
    /// error. Used to surface a replayed step result (output or error) to its
    /// caller.
    pub(crate) fn into_value_result(self) -> Result<Value> {
        match self {
            StepOutcome::Output(v) => Ok(v),
            StepOutcome::Failure { message, info } => Err(match info {
                Some(pe) => Error::Portable(Box::new(pe)),
                None => Error::app(message),
            }),
        }
    }
}

/// A previously checkpointed step as read back on replay: the recorded
/// function name plus its outcome. The name lets the replayer detect a
/// non-deterministic workflow — a different operation recorded at this step
/// position than the one now executing (see [`Error::UnexpectedStep`]).
#[derive(Clone, Debug)]
pub struct RecordedStep {
    /// The operation name recorded by the original execution.
    pub name: String,
    /// The recorded outcome (output or failure).
    pub outcome: StepOutcome,
}

/// Build a [`StepOutcome`] from an `operation_outputs` row's `output`/`error`
/// columns and recorded serialization format. A non-null `error` is a failure
/// (decoded with [`crate::serialize::decode_error`]); otherwise the `output` is
/// the success value. Both null (an impossible row) yields `None`. Shared by the
/// SQL backends' `get_step_result`/`record_step_result`.
pub(crate) fn step_outcome_from(
    serializer: &crate::serialize::Serializer,
    fmt: Option<&str>,
    output: Option<&str>,
    error: Option<&str>,
) -> Result<Option<StepOutcome>> {
    if let Some(err) = error {
        let (message, info) = crate::serialize::decode_error(fmt, err);
        return Ok(Some(StepOutcome::Failure { message, info }));
    }
    match output {
        Some(o) => Ok(Some(StepOutcome::Output(crate::serialize::decode(
            serializer, fmt, o,
        )?))),
        None => Ok(None),
    }
}

/// A registered application version (a row of `application_versions`). The
/// "latest" version is the one with the most recent [`version_timestamp`](Self::version_timestamp).
#[derive(Clone, Debug)]
pub struct VersionInfo {
    /// Stable unique id for this version row.
    pub version_id: String,
    /// The application version string (e.g. `0.1.0`).
    pub version_name: String,
    /// Recency marker; bumped by `set_latest_application_version` so the version
    /// sorts to the top. Versions are ordered newest-first by this.
    pub version_timestamp: DateTime<Utc>,
    /// When the version was first registered.
    pub created_at: DateTime<Utc>,
}

/// One workflow's full durable state in a portable, backend-agnostic form: the
/// `workflow_status` row plus every dependent `operation_outputs`,
/// `workflow_events`, `workflow_events_history`, and `streams` row, each kept as
/// a column-keyed JSON object. Produced by [`StateProvider::export_workflow`] and
/// consumed by [`StateProvider::import_workflow`]; the conductor ships it between
/// environments as gzipped, base64-encoded JSON. The keys match the other DBOS
/// SDKs' portable schema, so a workflow exported by one can be imported by
/// another.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ExportedWorkflow {
    /// The `workflow_status` row as a column-keyed JSON object.
    #[serde(default)]
    pub workflow_status: Map<String, Value>,
    /// The workflow's `operation_outputs` (step checkpoint) rows.
    #[serde(default, deserialize_with = "null_seq")]
    pub operation_outputs: Vec<Map<String, Value>>,
    /// The workflow's current `workflow_events` rows.
    #[serde(default, deserialize_with = "null_seq")]
    pub workflow_events: Vec<Map<String, Value>>,
    /// The workflow's `workflow_events_history` rows.
    #[serde(default, deserialize_with = "null_seq")]
    pub workflow_events_history: Vec<Map<String, Value>>,
    /// The workflow's `streams` rows.
    #[serde(default, deserialize_with = "null_seq")]
    pub streams: Vec<Map<String, Value>>,
}

/// Deserialize a JSON array that the producer may have rendered as `null` (some
/// SDKs marshal an empty list as null rather than `[]`) into an empty `Vec`.
fn null_seq<'de, D, T>(d: D) -> std::result::Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(d)?.unwrap_or_default())
}

/// The text columns carried in an exported `workflow_status` row — the cross-SDK
/// portable set. String and integer columns are listed separately so each is read
/// (and re-bound) with the right type. Together with [`EXPORT_STATUS_INT_COLS`]
/// these are exactly the columns the other SDKs export.
pub(crate) const EXPORT_STATUS_STR_COLS: &[&str] = &[
    "workflow_uuid",
    "status",
    "name",
    "authenticated_user",
    "assumed_role",
    "authenticated_roles",
    "output",
    "error",
    "executor_id",
    "application_version",
    "application_id",
    "class_name",
    "config_name",
    "queue_name",
    "deduplication_id",
    "inputs",
    "queue_partition_key",
    "forked_from",
    "parent_workflow_id",
    "serialization",
];
/// The integer columns of an exported `workflow_status` row (see
/// [`EXPORT_STATUS_STR_COLS`]).
#[cfg(feature = "sqlite")]
pub(crate) const EXPORT_STATUS_INT_COLS: &[&str] = &[
    "created_at",
    "updated_at",
    "recovery_attempts",
    "workflow_timeout_ms",
    "workflow_deadline_epoch_ms",
    "started_at_epoch_ms",
    "priority",
    "delay_until_epoch_ms",
];

/// A column's value pulled from an exported row as an owned `String` (`None` for
/// JSON null or a missing/non-string key). Shared by the SQL providers' import.
pub(crate) fn col_str(m: &Map<String, Value>, key: &str) -> Option<String> {
    m.get(key).and_then(|v| v.as_str()).map(str::to_string)
}

/// A column's value pulled from an exported row as an `i64` (`None` for JSON null
/// or a missing/non-integer key). Shared by the SQL providers' import.
pub(crate) fn col_i64(m: &Map<String, Value>, key: &str) -> Option<i64> {
    m.get(key).and_then(Value::as_i64)
}

/// A column's value pulled from an exported row as a `bool` (`None` for JSON null
/// or a missing/non-bool key). Shared by the SQL providers' import.
pub(crate) fn col_bool(m: &Map<String, Value>, key: &str) -> Option<bool> {
    m.get(key).and_then(Value::as_bool)
}

/// Parameters for [`StateProvider::fork_workflow`]. The fork is created
/// directly `ENQUEUED` on `queue_name`, in the same transaction that copies the
/// original's checkpoints, so a fork is never observable half-made.
#[derive(Clone, Debug)]
pub struct ForkParams {
    /// The workflow being forked.
    pub original_id: String,
    /// Id of the fork.
    pub new_id: String,
    /// First step to re-execute; checkpoints below it are copied over.
    pub start_step: i32,
    /// Version to stamp on the fork; `None` inherits the original's (so the
    /// fork stays runnable by the executors that could run the original).
    pub app_version: Option<String>,
    /// Queue the fork is enqueued on.
    /// Queue the fork is enqueued on.
    pub queue_name: String,
    /// Partition key when `queue_name` is a partitioned queue.
    pub partition_key: Option<String>,
}

/// Parameters for one dequeue iteration, computed by the engine's dispatcher
/// from a [`crate::WorkflowQueue`]'s configuration. Plain scalars so the storage
/// layer stays decoupled from the queue type.
#[derive(Clone, Debug)]
pub struct DequeueRequest {
    /// Queue to claim workflows from.
    pub queue_name: String,
    /// Executor claiming the workflows.
    pub executor_id: String,
    /// Only workflows of this application version (or none) are claimed.
    pub app_version: String,
    /// For a partitioned queue, restrict the claim to this partition and scope
    /// the concurrency / rate-limit counts to it. `None` for a non-partitioned
    /// queue (matches the queue's rows regardless of partition key).
    pub partition_key: Option<String>,
    /// Upper bound for this iteration, already adjusted for worker concurrency
    /// (`worker_concurrency - locally running`).
    pub max_tasks: i64,
    /// If set, cap claims so queue-wide PENDING never exceeds this.
    pub global_concurrency: Option<i64>,
    /// If set with `rate_limit_period_ms`: cap claims so the number of
    /// rate-limited starts within the trailing period stays under this.
    pub rate_limit_max: Option<i64>,
    /// Trailing window (epoch ms) the `rate_limit_max` cap is measured over.
    pub rate_limit_period_ms: Option<i64>,
}

/// The pluggable durable-state backend.
///
/// This is the single seam that decouples the runtime from storage. v0.1 ships a
/// An opaque identity for one provider *instance*: two handles match only if
/// they were cloned from the same original. Compared by pointer, so a match is
/// unforgeable — there is no way to construct an identity equal to another's.
///
/// Used to bind a system data source (`system_datasource`) to the provider
/// that minted it, so
/// [`transaction_on`](crate::DurableContext::transaction_on) takes its
/// single-commit fast path only against that provider's own database and
/// rejects a system data source from a different engine.
#[derive(Clone)]
pub struct ProviderIdentity(Arc<IdentityMarker>);

/// INVARIANT: this must stay behind an `Arc`. Identity is the address of the
/// per-instance `ArcInner` allocation — `Arc::new` allocates one even for a
/// zero-sized value, so every identity is distinct and clones compare equal.
/// A `Box`/`Rc`-of-static "simplification" would give every zero-sized
/// instance the same dangling address and make ALL identities match.
struct IdentityMarker;

// No `Default`: `default()` conventionally yields one canonical value, but a
// fresh identity is unique by design — two `default()` calls would not match.
#[allow(clippy::new_without_default)]
impl ProviderIdentity {
    /// A fresh identity, equal only to its own clones.
    pub fn new() -> Self {
        Self(Arc::new(IdentityMarker))
    }

    /// Whether `other` was cloned from the same original as `self`.
    pub fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl std::fmt::Debug for ProviderIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProviderIdentity({:p})", Arc::as_ptr(&self.0))
    }
}

/// One recovery sweep's atomic claim on one `PENDING` workflow row, expressed
/// as a compare-and-set against the row as the sweep observed it. See
/// [`StateProvider::claim_for_recovery`].
#[derive(Clone, Copy, Debug)]
pub struct RecoveryClaimRequest<'a> {
    /// The workflow to claim.
    pub workflow_id: &'a str,
    /// `executor_id` as observed when the sweep listed the row — the executor
    /// being declared dead.
    pub expected_executor: &'a str,
    /// `recovery_attempts` as observed when the sweep listed the row. Doubles
    /// as the fencing counter: every claim increments it, so a rival sweep's
    /// claim (even one recovering the *same* executor id) invalidates this
    /// request.
    pub expected_attempts: i32,
    /// The claiming executor, stamped on the row when the claim succeeds.
    pub new_executor: &'a str,
    /// The recovery-attempt cap; a claim that would exceed it parks the row.
    pub max_attempts: i32,
    /// `true` for a workflow that was claimed off a queue before the crash:
    /// release it back to `ENQUEUED` (the queue dispatcher re-runs it under its
    /// concurrency limits) instead of claiming it for direct re-dispatch.
    pub requeue: bool,
}

/// The outcome of [`StateProvider::claim_for_recovery`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryClaim {
    /// This sweep owns the run: the row is `PENDING` under the claimant's
    /// executor id at the returned attempt count. Dispatch it.
    Claimed {
        /// `recovery_attempts` after the claim's increment.
        attempts: i32,
    },
    /// The row went back to its queue as `ENQUEUED`; the queue's own atomic
    /// claim will admit exactly one runner. Nothing to dispatch here.
    Requeued,
    /// The claim would have exceeded the attempt cap; the row is now parked in
    /// `MAX_RECOVERY_ATTEMPTS_EXCEEDED`. Nothing to dispatch.
    Parked {
        /// `recovery_attempts` after the parking increment.
        attempts: i32,
    },
    /// The row no longer matches what the sweep observed — a rival sweep
    /// claimed it, or it completed, was cancelled, or was resumed. Another
    /// process is responsible for it now; do nothing.
    Lost,
}

/// Postgres implementation and an in-memory one; a DynamoDB / Aurora DSQL
/// implementation can be added later **without touching the engine**.
///
/// Every method must be **idempotent** with respect to its keys, because the
/// engine may re-run a workflow after a crash and replay completed steps.
#[async_trait]
pub trait StateProvider: Send + Sync {
    /// Create tables / indexes if they do not yet exist.
    async fn init(&self) -> Result<()>;

    /// Probe the backend for a readiness check: is it reachable, and is its
    /// system schema present and at least as new as this binary expects? One
    /// cheap round trip — suitable for a load-balancer probe interval. The
    /// default is unconditionally healthy (right for the in-memory provider,
    /// which cannot lose its own state); the SQL providers verify their
    /// applied migrations. Surfaced through
    /// [`DurableEngine::health`](crate::DurableEngine::health).
    async fn ping(&self) -> Result<()> {
        Ok(())
    }

    /// The serialization format this provider stores values in. The engine reads
    /// it to encode a failed workflow's error in that same format — errors are
    /// encoded at the engine because they carry a structured type the
    /// [`set_workflow_status`](Self::set_workflow_status) `&str` channel cannot —
    /// so a portable provider writes the cross-language error envelope. Defaults
    /// to [`Serializer::Json`](crate::Serializer::Json) (bare error strings); the SQL providers return
    /// their configured serializer.
    fn serializer(&self) -> crate::serialize::Serializer {
        crate::serialize::Serializer::Json
    }

    /// This provider instance's [`ProviderIdentity`], if it issues one. Used to
    /// verify that a system data source was minted by *this* provider before
    /// the single-commit fast path is taken. The default (`None`) is fail-safe:
    /// a provider without an identity never matches, so the fast path is never
    /// wrongly taken against it.
    fn provider_identity(&self) -> Option<&ProviderIdentity> {
        None
    }

    /// Whether this backend pushes change signals (Postgres `LISTEN`/`NOTIFY`),
    /// so a blocked `recv`/`get_event` is woken as soon as the row it waits for
    /// is written rather than only by polling. Callers that get `true` can wait
    /// on a long backstop interval and rely on [`await_change`](Self::await_change)
    /// for promptness; `false` means they must poll at a short interval.
    fn supports_listen_notify(&self) -> bool {
        false
    }

    /// Wait up to `within` for a hint that `wait`'s condition may have changed,
    /// returning early when a matching change is signalled. The wake is only a
    /// hint: the caller must re-check the database (a signal can be missed in the
    /// gap between the caller's last check and subscribing — the bounded `within`
    /// is the backstop). Backends without push signalling just sleep.
    async fn await_change(&self, wait: ChangeWait<'_>, within: std::time::Duration) {
        let _ = wait;
        tokio::time::sleep(within).await;
    }

    /// Idempotently insert a workflow row. If `status.id` already exists, the
    /// existing row is returned unchanged (so a re-submitted id is a no-op, not a
    /// duplicate). This is the single creation path for both direct runs and
    /// enqueues.
    ///
    /// The returned flag is `true` iff **this call** created the row — the
    /// atomic arbiter for "who runs it" when several executors race the same
    /// deterministic id (e.g. a scheduled tick). Executor ids cannot serve as
    /// that arbiter: they are not unique across processes (`"local"` is the
    /// default for every process not told otherwise).
    async fn insert_workflow_status(
        &self,
        status: WorkflowStatus,
    ) -> Result<(WorkflowStatus, bool)>;

    /// The id of the workflow currently holding the deduplication slot
    /// `(queue_name, dedup_id)`, if any. Backs
    /// [`DeduplicationPolicy::ReturnExisting`](crate::DeduplicationPolicy::ReturnExisting):
    /// on a dedup collision the enqueue returns a handle to this workflow.
    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        dedup_id: &str,
    ) -> Result<Option<String>>;

    /// Fetch a workflow row by id, if it exists.
    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>>;

    /// Transition a workflow to a new status, optionally writing its terminal
    /// `output` or `error`. Bumps `updated_at`. Returns whether the write
    /// landed.
    ///
    /// Terminal targets are guarded — a run owns its workflow's outcome exactly
    /// as long as the row says that run is what the workflow is doing:
    ///
    /// - `SUCCESS` / `ERROR` apply only to a `PENDING` row. `false` means the
    ///   row was cancelled, parked past its recovery cap, already terminal,
    ///   handed to another execution (e.g. `ENQUEUED` by a concurrent resume),
    ///   or gone entirely — the caller must adopt the recorded outcome instead
    ///   of reporting its own.
    /// - `CANCELLED` applies to any row not already `SUCCESS`, `ERROR`, or
    ///   `CANCELLED` (a completed workflow cannot be retroactively cancelled).
    /// - `MAX_RECOVERY_ATTEMPTS_EXCEEDED` applies only to a `PENDING` row.
    ///
    /// Non-terminal targets are unguarded and return `true` iff the row exists.
    ///
    /// (This mirrors the DBOS SDKs' outcome guard; note it does not detect two
    /// live executions both at `PENDING` — the step-checkpoint conflict in
    /// [`record_step_result`](Self::record_step_result) covers that window.)
    async fn set_workflow_status(
        &self,
        id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<bool>;

    /// Return a previously checkpointed step — its recorded name plus outcome
    /// (output or recorded failure) — or `None` if the step has not run. The
    /// name lets the replayer detect a non-deterministic workflow (a different
    /// operation recorded at this position than the one now executing).
    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<RecordedStep>>;

    /// Idempotently record a step's outcome keyed by `(workflow_id, seq)`: its
    /// success `value`, or — when `error` is set — its already-encoded failure
    /// (stored in the `error` column, output left null). A step records exactly
    /// one of the two.
    ///
    /// Returns the **canonical** stored outcome. If a row is already recorded
    /// at this position, what happens depends on how it compares to this write:
    ///
    /// - **Identical** (same name, same output/error, same `started_at_ms`):
    ///   this is a replay or a retry of the same logical write — the stored
    ///   outcome is returned, so every caller observes the same result.
    /// - **Different name**: the workflow function is non-deterministic —
    ///   [`Error::UnexpectedStep`](crate::Error::UnexpectedStep).
    /// - **Same name, different content or start time**: another *live*
    ///   execution of this workflow checkpointed the step first —
    ///   [`Error::WorkflowConflict`](crate::Error::WorkflowConflict). The
    ///   caller must stop executing and adopt the recorded workflow outcome;
    ///   continuing would double every remaining step's side effects.
    ///
    /// When this call **wins** the checkpoint (its insert lands) and
    /// `executor_id` is set, the workflow row's `executor_id` is refreshed to
    /// it, best-effort — checkpointing proves this executor is the one actually
    /// running the workflow, and the row should report that. The refresh grants
    /// no exclusivity and never fails the checkpoint.
    ///
    /// `started_at_ms` is when the step's work began (epoch ms); the
    /// implementation stamps `completed_at` itself as the time of the write.
    /// `None` records no start time — used for instantaneous operations (sends,
    /// event sets, sleep markers) that have no duration; such rows are excluded
    /// from step duration aggregates. (With no start time, the conflict
    /// comparison above falls back to name + content alone: identical
    /// instantaneous writes converge rather than conflict.)
    ///
    /// Durable sleep is built on this too: the wake instant is recorded as an
    /// ordinary step (`DBOS.sleep`) in `operation_outputs` — there is no
    /// separate timers table.
    #[allow(clippy::too_many_arguments)] // one argument per checkpoint column
    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
        error: Option<&str>,
        started_at_ms: Option<i64>,
        executor_id: Option<&str>,
    ) -> Result<StepOutcome>;

    /// Run a transactional step: `body`'s SQL writes and this step's
    /// `operation_outputs` checkpoint commit in **one** database transaction, so
    /// the writes happen exactly once. Returns the step's recorded outcome — its
    /// output as `Ok` (`body`'s on the first run, the stored one on replay), or a
    /// recorded failure as `Err`.
    ///
    /// On a `body` error the body's writes **roll back** (the step stays atomic),
    /// but the error is then recorded *outside* the aborted transaction, so the
    /// failed step is durable: a replay returns the recorded error without
    /// re-running `body` (like an ordinary step). A transaction-level conflict
    /// (serialization failure / deadlock) is *not* recorded — it restarts the
    /// whole transaction on a fresh one, re-running `body`. SQL backends only; the
    /// in-memory provider returns an error.
    async fn run_transaction_step(
        &self,
        workflow_id: &str,
        seq: i32,
        started_at_ms: i64,
        opts: &TransactionOptions,
        body: TxBody<'_>,
    ) -> Result<Value>;

    /// Atomically claim up to `req.max_tasks` `ENQUEUED` workflows from a queue,
    /// transitioning them to `PENDING` stamped with the claiming executor, the
    /// app version, and `started_at`. Candidates are ordered by
    /// `(priority, created_at)`. Honors `global_concurrency` (queue-wide PENDING
    /// cap) and the rate-limit window if set; for workflows with a stored
    /// `timeout_ms`, the absolute deadline is fixed at claim time.
    ///
    /// Must be safe under concurrent dispatchers: a workflow is claimed by
    /// exactly one caller (Postgres uses `FOR UPDATE SKIP LOCKED` / `NOWAIT`).
    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>>;

    /// Transition every `DELAYED` workflow whose `delay_until_ms <= now_ms` to
    /// `ENQUEUED`. Returns how many were transitioned. Called by the dispatcher
    /// at the top of each polling iteration.
    async fn transition_delayed_workflows(&self, now_ms: i64) -> Result<u64>;

    /// Distinct non-null partition keys among the `ENQUEUED` workflows on
    /// `queue_name`. The dispatcher of a partitioned queue iterates these and
    /// dequeues each partition independently.
    async fn queue_partitions(&self, queue_name: &str) -> Result<Vec<String>>;

    /// Append a message for `destination_id` on `topic`. Errors if the
    /// destination workflow does not exist (FK violation in the SQL backends).
    ///
    /// When `idempotency_key` is `Some`, the row's primary key is derived from it
    /// (`{key}::{destination_id}`) and a repeated insert is a silent no-op, so a
    /// caller that retries the send delivers the message **at most once**; `None`
    /// assigns a fresh id, so every send delivers.
    async fn insert_notification(
        &self,
        destination_id: &str,
        topic: &str,
        message: Value,
        idempotency_key: Option<&str>,
    ) -> Result<()>;

    /// Append many messages in one operation — the bulk counterpart of
    /// [`insert_notification`](Self::insert_notification), with the same
    /// per-row semantics (FK-checked destinations; a keyed row's id derives
    /// from `{key}::{destination_id}`, so a repeat is a silent no-op).
    ///
    /// The SQL backends deliver the whole batch **atomically** in a single
    /// multi-row statement: one nonexistent destination rejects the entire
    /// batch and nothing is delivered. The default implementation inserts
    /// sequentially — same per-row semantics but no all-or-nothing guarantee;
    /// providers that can should override atomically. An empty slice is a
    /// no-op.
    async fn insert_notifications(&self, rows: &[NotificationInsert]) -> Result<()> {
        for r in rows {
            self.insert_notification(
                &r.destination_id,
                &r.topic,
                r.message.clone(),
                r.idempotency_key.as_deref(),
            )
            .await?;
        }
        Ok(())
    }

    /// Atomically claim the **oldest unconsumed** message for
    /// `(workflow_id, topic)` and record it as the step checkpoint
    /// `(workflow_id, seq)` in the same transaction — if claiming and
    /// checkpointing were separate, a crash between them would lose the
    /// message. Returns the message, or `None` when the mailbox is empty
    /// (nothing is recorded in that case).
    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
        seq: i32,
        step_name: &str,
    ) -> Result<Option<Value>>;

    /// Set (or overwrite) the value of event `key` on `workflow_id`.
    async fn upsert_event(&self, workflow_id: &str, key: &str, value: Value) -> Result<()>;

    /// Read the current value of event `key` on `workflow_id`, if set.
    async fn get_event_value(&self, workflow_id: &str, key: &str) -> Result<Option<Value>>;

    /// List workflows matching `filter`, newest- or oldest-first per
    /// `filter.sort_desc`.
    async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>>;

    /// Count workflows grouped per `query` (one [`WorkflowAggregate`] per
    /// non-empty group). The engine validates that the query groups by at least
    /// one dimension before calling this.
    async fn get_workflow_aggregates(
        &self,
        query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>>;

    /// Aggregate step (`operation_outputs`) rows grouped per `query`, selecting
    /// count and/or max duration. The engine validates that the query groups by
    /// at least one dimension and selects at least one aggregate before calling.
    async fn get_step_aggregates(&self, query: &StepAggregateQuery) -> Result<Vec<StepAggregate>>;

    /// Cancel a workflow: if it is not already terminal, set it `CANCELLED`,
    /// stamp `completed_at`, and clear queue assignment / dedup so it leaves any
    /// queue. A running workflow stops cooperatively at its next step.
    async fn cancel_workflow(&self, id: &str) -> Result<()>;

    /// Resume a `CANCELLED` (or otherwise non-terminal) workflow by returning it
    /// to `PENDING`, resetting `recovery_attempts` and clearing deadline / dedup
    /// / started / completed. Returns `true` if a row was actually transitioned
    /// (i.e. it existed and was not already `SUCCESS`/`ERROR`). The caller
    /// re-dispatches it.
    async fn resume_workflow(&self, id: &str) -> Result<bool>;

    /// Route an existing row to a queue: set it `ENQUEUED` on `queue`, clearing
    /// the owning executor and start time so a dispatcher claims it fresh. Used
    /// to re-execute a resumed workflow on a running engine without
    /// re-running it locally. A no-op if the id is gone.
    async fn enqueue_existing(&self, id: &str, queue: &str) -> Result<()>;

    /// Cancel many workflows in one round-trip. Each existing, non-terminal id is
    /// set `CANCELLED` (same effect as [`cancel_workflow`](Self::cancel_workflow));
    /// missing or already-terminal ids are silently skipped (no error). An empty
    /// slice is a no-op.
    async fn cancel_workflows(&self, ids: &[String]) -> Result<()>;

    /// Resume many workflows in one round-trip. Each existing id that is not
    /// `SUCCESS`/`ERROR` returns to `PENDING` (same effect as
    /// [`resume_workflow`](Self::resume_workflow)). Returns the ids actually
    /// transitioned, so the caller can re-dispatch exactly those. An empty slice
    /// returns an empty vec.
    async fn resume_workflows(&self, ids: &[String]) -> Result<Vec<String>>;

    /// Delete workflows and (via `ON DELETE CASCADE`) their step / event / stream
    /// rows, regardless of state. When `delete_children`, every descendant by
    /// `parent_workflow_id` (transitively) is deleted too. Missing ids are
    /// skipped. An empty slice is a no-op.
    async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()>;

    /// Garbage-collect workflow history: delete every workflow **not** in
    /// `PENDING`/`ENQUEUED`/`DELAYED` created strictly before a cutoff, along
    /// with its step / event / stream rows. Returns how many workflows were
    /// deleted.
    ///
    /// The cutoff is the more restrictive (newer) of the two bounds, matching
    /// the other DBOS SDKs:
    ///
    /// - `cutoff_epoch_ms` — an absolute epoch-milliseconds threshold;
    /// - `rows_threshold` — keep (at most) the newest N workflows: the
    ///   `created_at` of the Nth-newest becomes the cutoff. Must be positive.
    ///
    /// With both `None` the call is a no-op returning `0`. In-flight work is
    /// never collected: `PENDING`/`ENQUEUED`/`DELAYED` rows survive regardless
    /// of age.
    ///
    /// Deletion happens in bounded batches (10,000 rows at a time), so a
    /// first sweep over a large backlog is many short transactions instead of
    /// one long one — a long-running delete would pin the MVCC horizon and
    /// bloat the hottest table in the system. The default implementation
    /// composes [`list_workflows`](Self::list_workflows) and
    /// [`delete_workflows`](Self::delete_workflows); the SQL backends override
    /// it with a direct batched `DELETE`.
    async fn garbage_collect(
        &self,
        cutoff_epoch_ms: Option<i64>,
        rows_threshold: Option<i64>,
    ) -> Result<u64> {
        let Some(cutoff) = resolve_gc_cutoff(self, cutoff_epoch_ms, rows_threshold).await? else {
            return Ok(0);
        };
        let filter = ListFilter {
            status: vec![
                STATUS_SUCCESS.to_string(),
                STATUS_ERROR.to_string(),
                STATUS_CANCELLED.to_string(),
                STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED.to_string(),
            ],
            // The delete bound is *strictly* before the cutoff; the filter's
            // bound is inclusive, so step one millisecond back.
            end_time_ms: Some(cutoff - 1),
            limit: Some(GC_BATCH),
            load_input: false,
            load_output: false,
            ..Default::default()
        };
        let mut total = 0u64;
        loop {
            let ids: Vec<String> = self
                .list_workflows(&filter)
                .await?
                .into_iter()
                .map(|w| w.id)
                .collect();
            let batch = ids.len() as u64;
            self.delete_workflows(&ids, false).await?;
            total += batch;
            if batch < GC_BATCH as u64 {
                return Ok(total);
            }
        }
    }

    /// Reschedule a `DELAYED` workflow: set its `delay_until` to
    /// `delay_until_ms`. Only affects a row currently in `DELAYED` (a queue
    /// dispatcher promotes it to `ENQUEUED` once due); anything else is a no-op.
    /// Returns whether a row was updated.
    async fn set_workflow_delay(&self, id: &str, delay_until_ms: i64) -> Result<bool>;

    /// **Replace** the custom attributes attached to workflow `id` (`None` or
    /// an empty map clears them) and bump `updated_at`. Errors with
    /// [`Error::NonExistentWorkflow`](crate::Error::NonExistentWorkflow) if
    /// the workflow does not exist.
    async fn set_workflow_attributes(
        &self,
        id: &str,
        attributes: Option<&Map<String, Value>>,
    ) -> Result<()>;

    /// Create `params.new_id` as a fork of `params.original_id`: a fresh
    /// `ENQUEUED` workflow on `params.queue_name` with the same
    /// name/input/auth/class/config/app-id, `forked_from = original_id`, and the
    /// original's step checkpoints with `seq < start_step` copied in so
    /// execution resumes from that step. Marks the original `was_forked_from`.
    /// Errors if the original does not exist.
    async fn fork_workflow(&self, params: &ForkParams) -> Result<()>;

    /// Atomically claim one `PENDING` workflow for a recovery sweep — or report
    /// that another process got there first.
    ///
    /// The claim is a compare-and-set against the row *as the sweep observed
    /// it*: it applies only while the row is still `PENDING`, still owned by
    /// [`expected_executor`](RecoveryClaimRequest::expected_executor), and still
    /// at [`expected_attempts`](RecoveryClaimRequest::expected_attempts). Any
    /// interleaved transition — a rival sweep's claim (which bumps the attempt
    /// count), a completion, a cancellation, a resume — makes the predicate
    /// miss, and the caller gets [`RecoveryClaim::Lost`]: at most one process
    /// dispatches each pending workflow, no matter how many recover the same
    /// dead executor at once.
    ///
    /// One caveat: a resume *resets* the attempt counter, so a cancel-then-
    /// resume can reconstruct the exact triple a sweep observed before either
    /// happened, and a claim that stayed in flight across both would land on
    /// the resumed run. The window requires a sweep stalled across two operator
    /// actions; the terminal-write guard and the step-checkpoint conflict in
    /// [`record_step_result`](Self::record_step_result) contain the doubled
    /// execution if it ever occurs.
    ///
    /// A successful claim increments `recovery_attempts` and, depending on the
    /// request, either re-stamps `executor_id` with the claimant
    /// ([`RecoveryClaim::Claimed`] — the caller re-runs the workflow), or
    /// releases the row back to its queue as `ENQUEUED`
    /// ([`RecoveryClaim::Requeued`] — the queue's own atomic
    /// `ENQUEUED → PENDING` claim admits exactly one runner). A claim that
    /// would push the attempt count past
    /// [`max_attempts`](RecoveryClaimRequest::max_attempts) instead parks the
    /// row in `MAX_RECOVERY_ATTEMPTS_EXCEEDED` ([`RecoveryClaim::Parked`]).
    async fn claim_for_recovery(&self, req: &RecoveryClaimRequest<'_>) -> Result<RecoveryClaim>;

    /// Idempotently record that `parent_id`'s step `seq` started child workflow
    /// `child_id`. Stored as an `operation_outputs` checkpoint carrying the child
    /// id, so a replay of the parent re-attaches to the same child instead of
    /// starting a new one. A second record for the same `(parent_id, seq)` is a
    /// no-op.
    async fn record_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
        name: &str,
        child_id: &str,
    ) -> Result<()>;

    /// Return `(child_id, recorded_name)` for the child workflow `parent_id`
    /// started at step `seq`, if one was recorded by
    /// [`record_child_workflow`](Self::record_child_workflow). The recorded
    /// name lets the replayer detect a non-deterministic parent (a different
    /// child recorded at this position than the one now being started).
    async fn check_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
    ) -> Result<Option<(String, String)>>;

    /// List a workflow's recorded operations (its `operation_outputs` rows) as
    /// [`StepInfo`], ordered by `step_id`. Outputs are decoded per each row's
    /// recorded serialization format. Returns an empty list for an unknown
    /// workflow or one that has run no steps.
    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>>;

    /// The `function_name` recorded at `(workflow_id, seq)`, if a row exists.
    /// Used by the patch system to tell a marker from a pre-patch step.
    async fn get_step_name(&self, workflow_id: &str, seq: i32) -> Result<Option<String>>;

    /// Idempotently record a name-only marker row at `(workflow_id, seq)` (no
    /// output) — the checkpoint the patch system writes so a replay observes the
    /// same patch decision. A second record for the same key is a no-op.
    async fn record_patch(&self, workflow_id: &str, seq: i32, name: &str) -> Result<()>;

    /// Append one entry to the append-only stream `(workflow_id, key)` at the
    /// next offset (`MAX(offset) + 1`, starting at 0), stamped with the producing
    /// step's `function_id`. `value` is the user value to encode and store;
    /// `None` writes the close sentinel instead, which seals the stream. Errors
    /// if the stream is already closed. The destination workflow's existence is
    /// enforced by the streams foreign key.
    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: Option<Value>,
        function_id: i32,
    ) -> Result<()>;

    /// Read stream `(workflow_id, key)` entries with `offset >= from_offset` in
    /// order, decoding each per its stored serialization. Returns the decoded
    /// values and whether the close sentinel was reached (the sentinel itself is
    /// not included). Reading never blocks — the caller polls.
    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)>;

    /// All `(key, value)` events set on a workflow (`set_event`), decoded per
    /// their stored serialization, ordered by key. For observability/control
    /// planes that surface a workflow's events.
    async fn list_workflow_events(&self, workflow_id: &str) -> Result<Vec<(String, Value)>>;

    /// All notifications sent to a workflow (its `send`/`recv` mailbox), oldest
    /// first, with each message decoded. Includes already-consumed entries.
    async fn list_workflow_notifications(&self, workflow_id: &str)
        -> Result<Vec<NotificationInfo>>;

    /// All of a workflow's streams, grouped by key and ordered by write offset,
    /// with values decoded and the close sentinel excluded.
    async fn list_workflow_streams(&self, workflow_id: &str) -> Result<Vec<(String, Vec<Value>)>>;

    /// Insert a schedule row. The `schedule_name` is unique, so creating one that
    /// already exists is a unique violation.
    async fn create_schedule(&self, schedule: &WorkflowSchedule) -> Result<()>;

    /// Atomically replace each named schedule (delete-by-name then insert) in a
    /// single transaction, so the whole batch is all-or-nothing: a failure on
    /// any entry leaves every prior entry — and any pre-existing rows the batch
    /// would have replaced — untouched. The caller validates the entries and
    /// mints a fresh `schedule_id` for each beforehand.
    async fn apply_schedules(&self, schedules: &[WorkflowSchedule]) -> Result<()>;

    /// All schedules matching `filter` (empty filter returns every schedule),
    /// ordered by `schedule_name`.
    async fn list_schedules(&self, filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>>;

    /// Set a schedule's status. Returns whether a row matched.
    async fn set_schedule_status(&self, name: &str, status: ScheduleStatus) -> Result<bool>;

    /// Stamp `last_fired_at` (epoch ms) on a schedule. A no-op if it is gone.
    async fn set_schedule_last_fired(&self, name: &str, at_ms: i64) -> Result<()>;

    /// Delete a schedule by name. Returns whether a row was removed.
    async fn delete_schedule(&self, name: &str) -> Result<bool>;

    /// Register an application version, idempotently (no-op if `version_name`
    /// already exists). Stamps both timestamps with now.
    async fn create_application_version(&self, version_name: &str) -> Result<()>;

    /// All registered application versions, newest `version_timestamp` first.
    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>>;

    /// The version with the most recent `version_timestamp`, or `None` if none
    /// are registered.
    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>>;

    /// Mark a version as latest by bumping its `version_timestamp` to now.
    /// Returns whether a row matched (no-op if the name is unknown).
    async fn set_latest_application_version(&self, version_name: &str) -> Result<bool>;

    /// Persist a queue's configuration into the `queues` table — the
    /// database-backed registry the conductor reads fleet-wide, distinct from the
    /// engine's in-process registry. Keyed by `name`: a first write inserts;
    /// a name collision does nothing unless `update_existing`, which overwrites
    /// the stored configuration. Called once per registered queue on `launch`.
    async fn upsert_queue(&self, queue: &crate::WorkflowQueue, update_existing: bool)
        -> Result<()>;

    /// Every queue persisted in the `queues` table, sorted by name — the
    /// database-backed (fleet-wide) counterpart to the engine's in-process
    /// `list_registered_queues`. Fields not stored in the table
    /// (`max_tasks_per_iteration`, `max_polling_interval`) come back at their
    /// defaults.
    async fn list_queues(&self) -> Result<Vec<crate::WorkflowQueue>>;

    /// Export a workflow and (when `export_children`) all of its transitive
    /// children into the portable [`ExportedWorkflow`] form. The root workflow is
    /// first in the returned list, followed by descendants discovered through
    /// `parent_workflow_id`. Errors if the root workflow does not exist.
    async fn export_workflow(
        &self,
        workflow_id: &str,
        export_children: bool,
    ) -> Result<Vec<ExportedWorkflow>>;

    /// Import previously [`export_workflow`](Self::export_workflow)ed workflows,
    /// re-inserting each one's `workflow_status` row and dependent rows. Atomic:
    /// either every workflow is imported or none is. A workflow whose id already
    /// exists is an error (import does not overwrite).
    async fn import_workflow(&self, workflows: &[ExportedWorkflow]) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::{decode_roles, drain_stream_from, encode_roles, StreamBackend, STATUS_SUCCESS};
    use crate::error::Result;
    use async_trait::async_trait;
    use serde_json::{json, Value};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A backend that scripts the lost-value interleaving: the producer's final
    /// value is invisible on the first stream read but committed by the second,
    /// while the status is already terminal. With the drain-on-inactive fix the
    /// loop makes that second read pass; without it the value is dropped.
    struct ScriptedStream {
        reads: AtomicUsize,
    }

    #[async_trait]
    impl StreamBackend for ScriptedStream {
        async fn stream_entries(
            &self,
            _workflow_id: &str,
            _key: &str,
            _from_offset: i32,
        ) -> Result<(Vec<Value>, bool)> {
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                // First read: the producer commits its final value in the window
                // between here and the status check below, so it is not visible.
                Ok((vec![], false))
            } else {
                // The post-inactive read pass drains the value the producer
                // committed just before completing. Still no close sentinel — the
                // producer finished without calling `close_stream`.
                Ok((vec![json!("final")], false))
            }
        }

        async fn producer_status(&self, _workflow_id: &str) -> Result<Option<String>> {
            // Terminal: every write the producer made is committed by now.
            Ok(Some(STATUS_SUCCESS.to_string()))
        }
    }

    #[tokio::test]
    async fn drain_stream_drains_value_committed_before_producer_inactive() {
        let source = ScriptedStream {
            reads: AtomicUsize::new(0),
        };
        let (values, closed): (Vec<String>, bool) =
            drain_stream_from(&source, "wf", "stream").await.unwrap();

        assert!(
            closed,
            "stream is reported closed once the producer is terminal"
        );
        assert_eq!(
            values,
            vec!["final".to_string()],
            "the value committed just before the producer went inactive must not be dropped",
        );
    }

    #[test]
    fn roles_round_trip_as_json_array() {
        // Empty maps to NULL (no column value) and back to an empty list.
        assert_eq!(encode_roles(&[]), None);
        assert!(decode_roles(None).is_empty());

        // A populated list is stored as a JSON array string — the shared on-disk
        // shape other SDKs read — and decodes back unchanged.
        let roles = vec!["admin".to_string(), "user".to_string()];
        let stored = encode_roles(&roles).expect("non-empty roles encode to Some");
        assert_eq!(stored, r#"["admin","user"]"#);
        assert_eq!(decode_roles(Some(&stored)), roles);
    }

    #[test]
    fn decode_roles_tolerates_garbage() {
        // A malformed column never panics; it degrades to no roles.
        assert!(decode_roles(Some("not json")).is_empty());
    }
}
