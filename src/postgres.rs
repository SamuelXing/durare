use crate::error::{Error, Result};
use crate::provider::{
    col_bool, col_i64, col_str, decode_roles, dedup_or, encode_roles, group_stream_rows,
    is_terminal, nonexistent_or, step_outcome_from, workflow_agg_selects, ChangeWait,
    DequeueRequest, ExportedWorkflow, ForkParams, ListFilter, NotificationInfo, NotificationInsert,
    RecordedStep, RecoveryClaim, RecoveryClaimRequest, StateProvider, StepAggregate,
    StepAggregateQuery, StepInfo, StepOutcome, VersionInfo, WorkflowAggregate,
    WorkflowAggregateQuery, WorkflowStatus, EXPORT_STATUS_STR_COLS, NOTIFICATIONS_CHANNEL,
    STATUS_CANCELLED, STATUS_DELAYED, STATUS_ENQUEUED, STATUS_ERROR,
    STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED, STATUS_PENDING, STATUS_SUCCESS, STEP_STATUS_EXPR,
    STREAM_CLOSED_SENTINEL, WORKFLOW_EVENTS_CHANNEL,
};
use crate::schedule::{ScheduleFilter, ScheduleStatus, WorkflowSchedule};
use crate::serialize::{self, Serializer};
use crate::tx::{IsolationLevel, TransactionOptions, Tx, TxBody};
use crate::{RateLimiter, WorkflowQueue};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use sqlx::postgres::{PgListener, PgPool, PgPoolOptions, Postgres};
use sqlx::{QueryBuilder, Row};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// Columns selected when materializing a [`WorkflowStatus`] from `workflow_status`.
/// `serialization` drives how `inputs`/`output` are decoded (see [`crate::Serializer`]).
const SELECT_COLS: &str = "workflow_uuid, name, inputs, output, status, error, executor_id, \
     application_version, queue_name, queue_partition_key, priority, deduplication_id, recovery_attempts, \
     parent_workflow_id, workflow_timeout_ms, workflow_deadline_epoch_ms, \
     started_at_epoch_ms, rate_limited, delay_until_epoch_ms, completed_at, forked_from, \
     authenticated_user, assumed_role, authenticated_roles, class_name, config_name, \
     serialization, created_at, updated_at, attributes";

/// Routes inbound `LISTEN`/`NOTIFY` notifications to the `recv`/`get_event`
/// waiters parked on the matching payload. Waiters subscribe by payload; the
/// listener task wakes them when a `NOTIFY` for that payload arrives.
#[derive(Default)]
struct NotifyHub {
    waiters: Mutex<HashMap<String, WaitEntry>>,
}

/// The shared wake handle for everyone waiting on one payload, plus a refcount so
/// the entry is dropped when the last waiter leaves.
struct WaitEntry {
    notify: Arc<Notify>,
    count: usize,
}

/// An active subscription on one payload; deregisters on drop.
struct Subscription {
    hub: Arc<NotifyHub>,
    payload: String,
    notify: Arc<Notify>,
}

impl NotifyHub {
    /// Register interest in `payload`, returning a [`Subscription`] whose
    /// `notify` fires when the listener sees a matching `NOTIFY` (or a reconnect).
    fn subscribe(self: &Arc<Self>, payload: String) -> Subscription {
        let mut waiters = self.waiters.lock().unwrap();
        let entry = waiters.entry(payload.clone()).or_insert_with(|| WaitEntry {
            notify: Arc::new(Notify::new()),
            count: 0,
        });
        entry.count += 1;
        let notify = entry.notify.clone();
        Subscription {
            hub: Arc::clone(self),
            payload,
            notify,
        }
    }

    /// Wake everyone waiting on exactly `payload`.
    fn signal(&self, payload: &str) {
        if let Some(entry) = self.waiters.lock().unwrap().get(payload) {
            entry.notify.notify_waiters();
        }
    }

    /// Wake every waiter — used after a listener reconnect, when a notification
    /// may have been missed and all waiters should re-check the database.
    fn signal_all(&self) {
        for entry in self.waiters.lock().unwrap().values() {
            entry.notify.notify_waiters();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut waiters = self.hub.waiters.lock().unwrap();
        if let Some(entry) = waiters.get_mut(&self.payload) {
            entry.count -= 1;
            if entry.count == 0 {
                waiters.remove(&self.payload);
            }
        }
    }
}

/// The schema every DBOS SDK keeps its system tables in by default. Sharing it
/// means a Rust engine pointed at a Go/Python system database reads the same
/// tables they write.
const DEFAULT_SCHEMA: &str = "dbos";

/// A schema name safe to interpolate into `CREATE SCHEMA` and `search_path`
/// without quoting games: `[A-Za-z_][A-Za-z0-9_]*`.
pub(crate) fn is_plain_identifier(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Schema-qualified system-table names, computed once at construction. Every
/// query names its table through these instead of relying on the connection's
/// `search_path` — mutable session state that SQL in a transactional step can
/// change out from under a pooled connection. An empty schema
/// ([`from_pool`](PostgresProvider::from_pool)) leaves the names unqualified:
/// the caller's pool decides where they resolve.
///
/// The schema is interpolated *unquoted*, deliberately: `CREATE SCHEMA` and
/// `search_path` see it unquoted too, so all three case-fold the same way.
struct SystemTables {
    workflow_status: String,
    operation_outputs: String,
    notifications: String,
    workflow_events: String,
    workflow_events_history: String,
    streams: String,
    workflow_schedules: String,
    application_versions: String,
    queues: String,
    /// sqlx's own migration-bookkeeping table (`_sqlx_migrations`).
    sqlx_migrations: String,
}

impl SystemTables {
    fn new(schema: &str) -> Self {
        let name = |table: &str| {
            if schema.is_empty() {
                table.to_string()
            } else {
                format!("{schema}.{table}")
            }
        };
        Self {
            workflow_status: name("workflow_status"),
            operation_outputs: name("operation_outputs"),
            notifications: name("notifications"),
            workflow_events: name("workflow_events"),
            workflow_events_history: name("workflow_events_history"),
            streams: name("streams"),
            workflow_schedules: name("workflow_schedules"),
            application_versions: name("application_versions"),
            queues: name("queues"),
            sqlx_migrations: name("_sqlx_migrations"),
        }
    }
}

/// Postgres-backed [`StateProvider`], built on sqlx and the canonical DBOS
/// schema (`workflow_status` / `operation_outputs`).
pub struct PostgresProvider {
    pool: PgPool,
    /// System-tables schema this provider owns; created (if missing) and
    /// migrated on `init`. Empty for [`from_pool`](Self::from_pool), where the
    /// caller's pool controls the search path.
    schema: String,
    /// Format used when *encoding* stored values. Decoding always follows each
    /// row's recorded format, so this only sets what new rows are written as.
    serializer: Serializer,
    /// Wakes parked `recv`/`get_event` calls from the `LISTEN`/`NOTIFY` listener.
    /// Note: the listener (started in `init`) holds one pool connection for its
    /// lifetime, so the app effectively has `max_connections - 1` available.
    notify_hub: Arc<NotifyHub>,
    /// Cancels the background listener task when the provider is dropped.
    listener_token: CancellationToken,
    /// This instance's identity; binds system data sources to it.
    identity: crate::provider::ProviderIdentity,
    /// Qualified system-table names (see [`SystemTables`]).
    tables: SystemTables,
    /// Ensures the listener task is spawned at most once (on the first `init`).
    listener_started: AtomicBool,
}

impl PostgresProvider {
    /// Connect to Postgres using a standard connection URL, e.g.
    /// `postgres://user:pass@localhost:5432/durare`.
    ///
    /// System tables live in the `dbos` schema — the same one every DBOS SDK
    /// defaults to, so a database shared with Go/Python workers just works. Use
    /// [`connect_with_schema`](Self::connect_with_schema) to isolate a tenant
    /// under a different schema.
    pub async fn connect(database_url: &str) -> Result<Self> {
        Self::connect_with_schema(database_url, DEFAULT_SCHEMA).await
    }

    /// Like [`connect`](Self::connect) but with an explicit system-tables
    /// `schema` — e.g. one schema per tenant sharing a database. The schema is
    /// created if missing and migrated independently on `init` (each schema has
    /// its own migration history). The name must be a plain identifier
    /// (`[A-Za-z_][A-Za-z0-9_]*`).
    ///
    /// Implementation: the provider's own queries name the schema explicitly
    /// (`<schema>.workflow_status`), so they cannot be redirected by anything
    /// that changes a pooled connection's `search_path`. Every pooled
    /// connection additionally starts with `search_path` set to the schema, so
    /// a transactional step's *user* SQL also resolves inside it by default.
    /// One caveat: `LISTEN`/`NOTIFY` channel names are shared database-wide,
    /// so tenants in one database may wake each other spuriously; wakeups are
    /// only hints (the state is always re-read), so this affects latency
    /// noise, not correctness.
    pub async fn connect_with_schema(database_url: &str, schema: &str) -> Result<Self> {
        if !is_plain_identifier(schema) {
            return Err(Error::app(format!(
                "invalid schema name {schema:?}: use letters, digits, and underscores, \
                 not starting with a digit"
            )));
        }
        use std::str::FromStr as _;
        let opts = sqlx::postgres::PgConnectOptions::from_str(database_url)?
            .options([("search_path", schema)]);
        // One of these is held by the LISTEN/NOTIFY listener for its lifetime
        // (see `notify_hub`), leaving the rest for workflow/app queries.
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await?;
        let mut provider = Self::from_pool(pool);
        provider.schema = schema.to_string();
        provider.tables = SystemTables::new(schema);
        Ok(provider)
    }

    /// Build a provider from an existing pool (useful if your app already owns
    /// one). The pool's own configuration controls the search path, so system
    /// tables live wherever it resolves unqualified names (no schema is created
    /// on `init`).
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            schema: String::new(),
            serializer: Serializer::default(),
            notify_hub: Arc::new(NotifyHub::default()),
            listener_token: CancellationToken::new(),
            listener_started: AtomicBool::new(false),
            identity: crate::provider::ProviderIdentity::new(),
            tables: SystemTables::new(""),
        }
    }

    /// The underlying connection pool (e.g. for application queries that should
    /// share the provider's connections and search path).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// A [`PgDataSource`](crate::PgDataSource) over this provider's own pool,
    /// for application tables that live **in the system database**.
    ///
    /// Because the pool is known to be the system database's,
    /// [`transaction_on`](crate::DurableContext::transaction_on) takes a
    /// **single-commit fast path**: the body's writes and the step checkpoint
    /// commit in one transaction — the same guarantee as
    /// [`transaction`](crate::DurableContext::transaction), but the body gets
    /// the native `&mut sqlx::PgConnection` (full Postgres type support:
    /// `jsonb`, arrays, `uuid`, …) instead of the `Param`-limited [`Tx`]
    /// wrapper. No completion table is used or created.
    ///
    /// A **durare extension**.
    ///
    /// # Trust note
    ///
    /// The connection handed to a transaction body is unrestricted by design:
    /// it can address the system tables, because the application owns the
    /// database and the credential — not the SDK — is the real protection
    /// boundary (the same is true of [`Tx`], whose SQL text is equally
    /// unfiltered). What *is* guarded is the silent accident: a stray
    /// `COMMIT`/`ROLLBACK` in the body is detected and fails the step. Also
    /// note these connections start with `search_path` set to the system
    /// schema, so **unqualified** DDL in a body (`CREATE TABLE orders …`)
    /// lands in that schema next to the system tables — qualify your table
    /// names if you care where they live.
    ///
    /// [`Tx`]: crate::Tx
    pub fn system_datasource(&self) -> crate::PgDataSource {
        crate::PgDataSource::system(self.pool.clone(), self.identity.clone(), &self.schema)
    }

    /// Choose the format new values are encoded with. Use [`Serializer::Portable`]
    /// when this database is shared with DBOS workers in other languages.
    pub fn with_serializer(mut self, serializer: Serializer) -> Self {
        self.serializer = serializer;
        self
    }

    /// Run the embedded migrations on one held connection. The migrations'
    /// DDL is unqualified, and a pooled connection's session `search_path` can
    /// have been changed by earlier SQL — so pin it to the system schema
    /// explicitly first. (The SET matches the pool's connect-time default, so
    /// releasing the connection afterwards changes nothing. `from_pool` has no
    /// schema and keeps the caller's resolution untouched.)
    async fn run_migrations(&self) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        if !self.schema.is_empty() {
            sqlx::query(&format!("SET search_path TO {}", self.schema))
                .execute(&mut *conn)
                .await?;
        }
        // `run_direct` rather than `run`: sqlx provides it precisely to avoid
        // rustc's "implementation of `Acquire` is not general enough" error
        // when migrating over a plain `&mut` connection.
        sqlx::migrate!("./migrations/postgres")
            .run_direct(&mut *conn)
            .await?;
        Ok(())
    }

    /// Back off before the next attempt of the unbounded transaction-conflict
    /// retry loop, unless the workflow has been cancelled — in which case return
    /// [`Error::Cancelled`] so an operator can stop a transaction wedged on a
    /// conflict (the loop is otherwise unbounded, matching Go/Python). The backoff
    /// is exponential, capped at 1s. A failure to read the status (e.g. the
    /// database is momentarily unreachable) is treated as not-cancelled so a
    /// transient outage keeps being retried until it clears or the workflow is
    /// actually cancelled.
    async fn conflict_retry_wait(&self, workflow_id: &str, attempt: u32) -> Result<()> {
        let status: std::result::Result<Option<String>, _> = sqlx::query_scalar(&format!(
            "SELECT status FROM {workflow_status} WHERE workflow_uuid = $1",
            workflow_status = self.tables.workflow_status
        ))
        .bind(workflow_id)
        .fetch_optional(&self.pool)
        .await;
        if matches!(status, Ok(Some(s)) if s == STATUS_CANCELLED) {
            return Err(Error::Cancelled(workflow_id.to_string()));
        }
        let ms = (1u64 << attempt.min(10)).min(1000);
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        Ok(())
    }
}

impl Drop for PostgresProvider {
    fn drop(&mut self) {
        // Stop the background listener task (it holds a pooled connection).
        self.listener_token.cancel();
    }
}

/// Initial (and post-recovery) delay between failed listener (re)connect/recv
/// attempts; doubles up to [`LISTENER_BACKOFF_MAX`].
const LISTENER_BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Ceiling on the listener reconnect backoff.
const LISTENER_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Background task: hold a dedicated connection `LISTEN`ing on the notification
/// and event channels, and wake the matching waiters as notifications arrive.
/// Reconnects on failure (waking all waiters to re-poll, in case one was missed),
/// and exits when `token` is cancelled (provider dropped).
async fn run_listener(pool: PgPool, hub: Arc<NotifyHub>, token: CancellationToken) {
    let mut backoff = LISTENER_BACKOFF_MIN;
    loop {
        if token.is_cancelled() {
            return;
        }
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                tracing::debug!(error = %e, "notification listener: connect failed");
                if !sleep_or_cancel(backoff, &token).await {
                    return;
                }
                backoff = (backoff * 2).min(LISTENER_BACKOFF_MAX);
                continue;
            }
        };
        if let Err(e) = listener
            .listen_all([NOTIFICATIONS_CHANNEL, WORKFLOW_EVENTS_CHANNEL])
            .await
        {
            tracing::debug!(error = %e, "notification listener: LISTEN failed");
            if !sleep_or_cancel(backoff, &token).await {
                return;
            }
            backoff = (backoff * 2).min(LISTENER_BACKOFF_MAX);
            continue;
        }
        // Freshly (re)connected: a notification may have been missed while
        // disconnected, so nudge every waiter to re-check the database.
        hub.signal_all();

        loop {
            tokio::select! {
                _ = token.cancelled() => return,
                res = listener.try_recv() => match res {
                    // A notification: wake exactly the channel+payload's waiters.
                    // Receiving anything proves the link works, so relax the backoff.
                    Ok(Some(n)) => {
                        backoff = LISTENER_BACKOFF_MIN;
                        hub.signal(&hub_key(n.channel(), n.payload()));
                    }
                    // try_recv returns None when it had to reconnect; re-poll all.
                    Ok(None) => {
                        backoff = LISTENER_BACKOFF_MIN;
                        hub.signal_all();
                    }
                    // Hard error: back off before rebuilding so a recv that keeps
                    // failing while connect succeeds can't spin (matches Go's loop,
                    // which sleeps on a notification error rather than retrying hot).
                    Err(e) => {
                        tracing::debug!(error = %e, "notification listener: recv failed");
                        if !sleep_or_cancel(backoff, &token).await {
                            return;
                        }
                        backoff = (backoff * 2).min(LISTENER_BACKOFF_MAX);
                        break; // rebuild the listener
                    }
                }
            }
        }
    }
}

/// Hub key for one waiter: the channel and payload joined by a NUL (so a
/// notification payload never collides with an identical-looking event payload).
fn hub_key(channel: &str, payload: &str) -> String {
    format!("{channel}\u{0}{payload}")
}

/// Sleep for `dur`, returning `false` if cancelled first.
async fn sleep_or_cancel(dur: Duration, token: &CancellationToken) -> bool {
    tokio::select! {
        _ = token.cancelled() => false,
        _ = tokio::time::sleep(dur) => true,
    }
}

/// Epoch-millis (as stored) → `DateTime<Utc>`.
fn ms_to_dt(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

/// Map a `workflow_status` row to a [`WorkflowStatus`], decoding `inputs` and
/// `output` per the row's recorded serialization format.
fn row_to_status(serializer: &Serializer, row: &sqlx::postgres::PgRow) -> WorkflowStatus {
    let fmt: Option<String> = row.try_get("serialization").ok().flatten();
    let fmt = fmt.as_deref();
    let inputs: Option<String> = row.try_get("inputs").ok().flatten();
    let output: Option<String> = row.try_get("output").ok().flatten();
    let stored_error: Option<String> = row.try_get("error").ok().flatten();
    let (error, error_info) = serialize::decode_error_opt(fmt, stored_error.as_deref());
    WorkflowStatus {
        id: row.get("workflow_uuid"),
        name: row.get("name"),
        status: row.get("status"),
        input: serialize::decode_input_opt(serializer, fmt, inputs.as_deref())
            .ok()
            .flatten()
            .unwrap_or(Value::Null),
        output: serialize::decode_opt(serializer, fmt, output.as_deref())
            .ok()
            .flatten(),
        error,
        error_info,
        executor_id: row.get("executor_id"),
        app_version: row.get("application_version"),
        queue_name: row.try_get("queue_name").ok().flatten(),
        attributes: row.try_get("attributes").ok().flatten(),
        queue_partition_key: row.try_get("queue_partition_key").ok().flatten(),
        priority: row.get("priority"),
        dedup_id: row.try_get("deduplication_id").ok().flatten(),
        recovery_attempts: row.get::<i64, _>("recovery_attempts") as i32,
        parent_workflow_id: row.try_get("parent_workflow_id").ok().flatten(),
        timeout_ms: row.try_get("workflow_timeout_ms").ok().flatten(),
        deadline_ms: row.try_get("workflow_deadline_epoch_ms").ok().flatten(),
        started_at_ms: row.try_get("started_at_epoch_ms").ok().flatten(),
        rate_limited: row.get("rate_limited"),
        delay_until_ms: row.try_get("delay_until_epoch_ms").ok().flatten(),
        completed_at_ms: row.try_get("completed_at").ok().flatten(),
        forked_from: row.try_get("forked_from").ok().flatten(),
        authenticated_user: row.try_get("authenticated_user").ok().flatten(),
        assumed_role: row.try_get("assumed_role").ok().flatten(),
        authenticated_roles: decode_roles(
            row.try_get::<Option<String>, _>("authenticated_roles")
                .ok()
                .flatten()
                .as_deref(),
        ),
        class_name: row.try_get("class_name").ok().flatten(),
        config_name: row.try_get("config_name").ok().flatten(),
        created_at: ms_to_dt(row.get("created_at")),
        updated_at: ms_to_dt(row.get("updated_at")),
    }
}

#[async_trait]
impl StateProvider for PostgresProvider {
    fn provider_identity(&self) -> Option<&crate::provider::ProviderIdentity> {
        Some(&self.identity)
    }

    async fn ping(&self) -> Result<()> {
        // One round trip proves reachability and that the dbos system schema
        // is migrated: `_sqlx_migrations` (in the system schema) must exist
        // and hold every version this binary embeds. A *newer*
        // schema is healthy — migrations are additive, and an older binary
        // against an upgraded database is the normal rolling-deploy state.
        let expected = sqlx::migrate!("./migrations/postgres")
            .migrations
            .iter()
            .map(|m| m.version)
            .max()
            .unwrap_or(0);
        let applied: Option<i64> = sqlx::query_scalar(&format!(
            "SELECT max(version) FROM {sqlx_migrations}",
            sqlx_migrations = self.tables.sqlx_migrations
        ))
        .fetch_one(&self.pool)
        .await?;
        match applied {
            Some(v) if v >= expected => Ok(()),
            Some(v) => Err(crate::error::Error::app(format!(
                "dbos schema is behind: migration {v} applied, this binary expects {expected}"
            ))),
            None => Err(crate::error::Error::app(
                "dbos schema is not migrated: no applied migrations recorded",
            )),
        }
    }

    async fn init(&self) -> Result<()> {
        // Make sure the system-tables schema exists before migrating into it.
        // `from_pool` leaves `schema` empty and skips this — the caller's pool
        // decides where names resolve.
        if !self.schema.is_empty() {
            if let Err(e) = sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema))
                .execute(&self.pool)
                .await
            {
                // `IF NOT EXISTS` is not race-safe: two concurrent creators can
                // both pass the existence check, and the loser fails on the
                // catalog's unique constraint (23505) or `duplicate_schema`
                // (42P06). Either way the schema exists now — proceed.
                let benign = matches!(
                    &e,
                    sqlx::Error::Database(db)
                        if matches!(db.code().as_deref(), Some("23505") | Some("42P06"))
                );
                if !benign {
                    return Err(e.into());
                }
            }
        }
        // Embedded migrations (baked in at compile time from ./migrations/postgres).
        // sqlx tracks applied versions in `_sqlx_migrations` (inside the schema,
        // so each schema migrates independently) and applies only what is
        // pending, so this is safe on every startup and upgrades existing DBs.
        // The migrations' DDL is unqualified, so it lands wherever the
        // connection's `search_path` points — and a pooled connection's session
        // value can have been changed by earlier SQL. Pin it explicitly on one
        // held connection before running them (the SET also matches the pool's
        // connect-time default, so releasing the connection changes nothing).
        self.run_migrations().await?;
        // Start the LISTEN/NOTIFY listener once (it powers await_change). Spawned
        // here so it only runs for a provider that has been brought up; cancelled
        // when the provider is dropped. `Relaxed` suffices: this is purely a
        // spawn-once guard — the task's inputs are moved in via `spawn` (which
        // carries its own happens-before), so the flag publishes no other memory.
        if !self.listener_started.swap(true, Ordering::Relaxed) {
            tokio::spawn(run_listener(
                self.pool.clone(),
                self.notify_hub.clone(),
                self.listener_token.clone(),
            ));
        }
        Ok(())
    }

    fn supports_listen_notify(&self) -> bool {
        true
    }

    fn serializer(&self) -> serialize::Serializer {
        self.serializer.clone()
    }

    async fn await_change(&self, wait: ChangeWait<'_>, within: Duration) {
        // Subscribe before awaiting so a NOTIFY arriving during the wait wakes us;
        // a NOTIFY in the gap before subscribing is covered by `within` (the
        // caller re-checks the database on return).
        let sub = self
            .notify_hub
            .subscribe(hub_key(wait.channel(), &wait.payload()));
        let _ = tokio::time::timeout(within, sub.notify.notified()).await;
    }

    async fn insert_workflow_status(&self, s: WorkflowStatus) -> Result<(WorkflowStatus, bool)> {
        // Idempotent create: an existing id is left untouched.
        //
        // `owner_xid` is a write-once creation token (the DBOS SDKs mint one
        // per insert and never update it); it is stamped for cross-SDK schema
        // parity but plays no part in this SDK's arbitration — the returned
        // `created` flag is the equivalent "did this call create the row" test.
        let created = sqlx::query(&format!(
            "INSERT INTO {workflow_status}
                 (workflow_uuid, name, inputs, status, executor_id, application_version,
                  queue_name, queue_partition_key, priority, deduplication_id, parent_workflow_id,
                  workflow_timeout_ms, workflow_deadline_epoch_ms, delay_until_epoch_ms,
                  authenticated_user, assumed_role, authenticated_roles, class_name, config_name,
                  serialization, created_at, updated_at, attributes, owner_xid)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                     $17, $18, $19, $20, $21, $22, $23, gen_random_uuid()::TEXT)
             ON CONFLICT (workflow_uuid) DO NOTHING",
            workflow_status = self.tables.workflow_status
        ))
        .bind(&s.id)
        .bind(&s.name)
        .bind(serialize::encode_input(&self.serializer, &s.input)?)
        .bind(&s.status)
        .bind(&s.executor_id)
        .bind(&s.app_version)
        .bind(&s.queue_name)
        .bind(&s.queue_partition_key)
        .bind(s.priority)
        .bind(&s.dedup_id)
        .bind(&s.parent_workflow_id)
        .bind(s.timeout_ms)
        .bind(s.deadline_ms)
        .bind(s.delay_until_ms)
        .bind(&s.authenticated_user)
        .bind(&s.assumed_role)
        .bind(encode_roles(&s.authenticated_roles))
        .bind(&s.class_name)
        .bind(&s.config_name)
        .bind(self.serializer.name())
        .bind(s.created_at.timestamp_millis())
        .bind(s.updated_at.timestamp_millis())
        .bind(&s.attributes)
        .execute(&self.pool)
        .await
        .map_err(|e| dedup_or(e, &s))?
        // `ON CONFLICT DO NOTHING`: 1 row iff this call created it.
        .rows_affected()
            == 1;

        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM {workflow_status} WHERE workflow_uuid = $1",
            workflow_status = self.tables.workflow_status
        ))
        .bind(&s.id)
        .fetch_one(&self.pool)
        .await?;
        Ok((row_to_status(&self.serializer, &row), created))
    }

    async fn get_deduplicated_workflow(
        &self,
        queue_name: &str,
        dedup_id: &str,
    ) -> Result<Option<String>> {
        let row = sqlx::query(&format!(
            "SELECT workflow_uuid FROM {workflow_status} \
             WHERE queue_name = $1 AND deduplication_id = $2 LIMIT 1",
            workflow_status = self.tables.workflow_status
        ))
        .bind(queue_name)
        .bind(dedup_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| r.get("workflow_uuid")))
    }

    async fn get_workflow_status(&self, id: &str) -> Result<Option<WorkflowStatus>> {
        let row = sqlx::query(&format!(
            "SELECT {SELECT_COLS} FROM {workflow_status} WHERE workflow_uuid = $1",
            workflow_status = self.tables.workflow_status
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| row_to_status(&self.serializer, &r)))
    }

    async fn set_workflow_status(
        &self,
        id: &str,
        status: &str,
        output: Option<&Value>,
        error: Option<&str>,
    ) -> Result<bool> {
        let output_str = output.map(|v| self.serializer.encode(v)).transpose()?;
        // The error column is stored verbatim: the engine has already encoded a
        // failed workflow's error (as the portable envelope when portable), since
        // only it sees the structured error type. Cancellation/timeout reasons
        // arrive here as the plain strings their call sites pass.
        let now = Utc::now().timestamp_millis();
        let terminal = is_terminal(status);
        let completed = terminal.then_some(now);
        // Terminal writes are guarded (see the trait doc): an outcome or a
        // recovery parking lands only on a PENDING row, and a cancellation
        // never overwrites a completed one. The guard is expressed as the set
        // of prior states the target status may transition from.
        let guard = match status {
            STATUS_SUCCESS | STATUS_ERROR | STATUS_MAX_RECOVERY_ATTEMPTS_EXCEEDED => {
                " AND status = 'PENDING'"
            }
            STATUS_CANCELLED => " AND status NOT IN ('SUCCESS', 'ERROR', 'CANCELLED')",
            _ => "",
        };
        // Reaching a terminal state frees the queue-scoped deduplication slot so
        // the same deduplication id can be enqueued again.
        let res = sqlx::query(&format!(
            "UPDATE {workflow_status}
             SET status = $2,
                 output = COALESCE($3, output),
                 error  = COALESCE($4, error),
                 completed_at = COALESCE($5, completed_at),
                 deduplication_id = CASE WHEN $7 THEN NULL ELSE deduplication_id END,
                 updated_at = $6
             WHERE workflow_uuid = $1{guard}",
            workflow_status = self.tables.workflow_status
        ))
        .bind(id)
        .bind(status)
        .bind(output_str)
        .bind(error)
        .bind(completed)
        .bind(now)
        .bind(terminal)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<RecordedStep>> {
        let row = sqlx::query(&format!(
            "SELECT function_name, output, error, serialization FROM {operation_outputs}
             WHERE workflow_uuid = $1 AND function_id = $2",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(workflow_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(r) => Ok(step_outcome_from(
                &self.serializer,
                r.get::<Option<String>, _>("serialization").as_deref(),
                r.get::<Option<String>, _>("output").as_deref(),
                r.get::<Option<String>, _>("error").as_deref(),
            )?
            .map(|outcome| RecordedStep {
                name: r.get::<String, _>("function_name"),
                outcome,
            })),
            None => Ok(None),
        }
    }

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
    ) -> Result<StepOutcome> {
        // A failure records its (already-encoded) error with a null output; a
        // success records the encoded output with a null error.
        let (output_col, error_col) = match error {
            Some(e) => (None, Some(e.to_string())),
            None => (Some(self.serializer.encode(&value)?), None),
        };
        // One round trip covers the whole common case: the checkpoint insert
        // and — when it wins and an executor is given — the executor refresh,
        // since winning the checkpoint proves this executor is the one
        // actually running the workflow. The refresh is gated on the insert
        // through the CTE (a data-modifying CTE's RETURNING is visible to its
        // siblings even though the table change is not), grants no
        // exclusivity, and updates zero rows when the insert lost or the id
        // already matches.
        let won = match executor_id {
            Some(executor) => {
                sqlx::query_scalar::<_, bool>(&format!(
                    "WITH ins AS (
                     INSERT INTO {operation_outputs}
                         (workflow_uuid, function_id, function_name, output, error, serialization,
                          started_at_epoch_ms, completed_at_epoch_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (workflow_uuid, function_id) DO NOTHING
                     RETURNING 1
                 ),
                 refresh AS (
                     UPDATE {workflow_status} SET executor_id = $9
                     WHERE workflow_uuid = $1 AND EXISTS (SELECT 1 FROM ins)
                       AND (executor_id IS NULL OR executor_id <> $9)
                     RETURNING 1
                 )
                 SELECT EXISTS (SELECT 1 FROM ins)",
                    operation_outputs = self.tables.operation_outputs,
                    workflow_status = self.tables.workflow_status
                ))
                .bind(workflow_id)
                .bind(seq)
                .bind(name)
                .bind(&output_col)
                .bind(&error_col)
                .bind(self.serializer.name())
                .bind(started_at_ms)
                .bind(Utc::now().timestamp_millis())
                .bind(executor)
                .fetch_one(&self.pool)
                .await?
            }
            None => {
                sqlx::query(&format!(
                    "INSERT INTO {operation_outputs}
                         (workflow_uuid, function_id, function_name, output, error, serialization,
                          started_at_epoch_ms, completed_at_epoch_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                     ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
                    operation_outputs = self.tables.operation_outputs
                ))
                .bind(workflow_id)
                .bind(seq)
                .bind(name)
                .bind(&output_col)
                .bind(&error_col)
                .bind(self.serializer.name())
                .bind(started_at_ms)
                .bind(Utc::now().timestamp_millis())
                .execute(&self.pool)
                .await?
                .rows_affected()
                    > 0
            }
        };

        // Winning means the stored row is exactly this write — no read-back.
        if won {
            return Ok(step_outcome_from(
                &self.serializer,
                Some(self.serializer.name()),
                output_col.as_deref(),
                error_col.as_deref(),
            )?
            .unwrap_or(StepOutcome::Output(Value::Null)));
        }

        // Someone else's row is already at this position: read it to classify.
        // Identical content and start instant means a replay or a retry of
        // this same logical write — adopt it. Anything else is a live rival
        // execution (or a non-deterministic function, when the name differs).
        let row = sqlx::query(&format!(
            "SELECT function_name, output, error, serialization, started_at_epoch_ms
             FROM {operation_outputs}
             WHERE workflow_uuid = $1 AND function_id = $2",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(workflow_id)
        .bind(seq)
        .fetch_one(&self.pool)
        .await?;
        let stored_name: String = row.get("function_name");
        if stored_name != name {
            return Err(Error::unexpected_step(workflow_id, seq, name, stored_name));
        }
        let same_write = row.get::<Option<String>, _>("output") == output_col
            && row.get::<Option<String>, _>("error") == error_col
            && row.get::<Option<i64>, _>("started_at_epoch_ms") == started_at_ms;
        if !same_write {
            return Err(Error::WorkflowConflict(workflow_id.to_string()));
        }
        Ok(step_outcome_from(
            &self.serializer,
            row.get::<Option<String>, _>("serialization").as_deref(),
            row.get::<Option<String>, _>("output").as_deref(),
            row.get::<Option<String>, _>("error").as_deref(),
        )?
        .unwrap_or(StepOutcome::Output(Value::Null)))
    }

    async fn run_transaction_step(
        &self,
        workflow_id: &str,
        seq: i32,
        started_at_ms: i64,
        opts: &TransactionOptions,
        body: TxBody<'_>,
    ) -> Result<Value> {
        let name = opts.name.as_str();
        // Replay: a previously recorded outcome — a committed success or a durable
        // failure written by an earlier exhausted run — is returned immediately,
        // without running the body or re-evaluating the retry policy. This must
        // sit *ahead* of the retry loops: a recorded failure is terminal, not a
        // fresh error to be retried (otherwise a replay would spin the user-retry
        // loop, sleeping through its whole backoff, before returning it). The
        // `ON CONFLICT DO NOTHING` inserts below guard the write side; there is a
        // single writer per (workflow_id, seq).
        if let Some(r) = sqlx::query(&format!(
            "SELECT function_name, output, error, serialization FROM {operation_outputs}
             WHERE workflow_uuid = $1 AND function_id = $2",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(workflow_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?
        {
            let fmt: Option<String> = r.get("serialization");
            if let Some(so) = step_outcome_from(
                &self.serializer,
                fmt.as_deref(),
                r.get::<Option<String>, _>("output").as_deref(),
                r.get::<Option<String>, _>("error").as_deref(),
            )? {
                // A different operation recorded at this position means the
                // workflow is non-deterministic — replaying the stored outcome
                // would return the wrong step's result.
                let recorded: String = r.get("function_name");
                if recorded != name {
                    return Err(crate::error::Error::unexpected_step(
                        workflow_id,
                        seq,
                        name,
                        recorded,
                    ));
                }
                tracing::Span::current().record("dbos.step.replayed", true);
                return so.into_value_result();
            }
        }
        // OUTER loop: the user-facing retry policy for application errors. A body
        // error is retried up to `opts.max_retries` (gated by `retry_if`), the
        // whole body re-running on a fresh transaction. Conflicts are handled by
        // the inner loop and don't count against this budget.
        let mut user_attempt: u32 = 0;
        loop {
            let mut conflict_attempt: u32 = 0;
            // INNER loop: one committed success, or an application error surfaced
            // to the outer loop. A serialization/deadlock conflict or transient DB
            // error aborts the tx and retries on a fresh one — unbounded (until it
            // clears or the workflow is cancelled), matching Go/Python. Nothing is
            // recorded on a conflict.
            let body_err = 'conflict: loop {
                let outcome = async {
                    let mut tx = self.pool.begin().await?;
                    // `SET TRANSACTION` must come before any query in the tx.
                    if opts.isolation != IsolationLevel::ReadCommitted || opts.read_only {
                        let mut stmt = format!(
                            "SET TRANSACTION ISOLATION LEVEL {}",
                            opts.isolation.pg_sql()
                        );
                        if opts.read_only {
                            stmt.push_str(" READ ONLY");
                        }
                        sqlx::query(&stmt).execute(&mut *tx).await?;
                    }
                    // Run the user's body against this transaction. (Replay is
                    // handled up front, before the retry loops.)
                    let body_result = {
                        let mut h = Tx::postgres(&mut tx);
                        body(&mut h).await
                    };
                    match body_result {
                        // Success: checkpoint the output in the same transaction, so
                        // the body's writes and the checkpoint commit atomically.
                        Ok(value) => {
                            sqlx::query(&format!("INSERT INTO {operation_outputs}
                                     (workflow_uuid, function_id, function_name, output, serialization,
                                      started_at_epoch_ms, completed_at_epoch_ms)
                                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                                 ON CONFLICT (workflow_uuid, function_id) DO NOTHING", operation_outputs = self.tables.operation_outputs),
                            )
                            .bind(workflow_id)
                            .bind(seq)
                            .bind(name)
                            .bind(self.serializer.encode(&value)?)
                            .bind(self.serializer.name())
                            .bind(started_at_ms)
                            .bind(Utc::now().timestamp_millis())
                            .execute(&mut *tx)
                            .await?;
                            tx.commit().await?;
                            Ok(value)
                        }
                        // Any error rolls back the body's writes (dropping `tx` on a
                        // conflict/transient error, an explicit rollback otherwise) so
                        // the step stays atomic. Nothing is recorded here: a conflict
                        // or transient DB error retries on a fresh tx, and an
                        // application error is left for the outer user-retry loop,
                        // which records it only once the budget is exhausted.
                        Err(e) if e.is_tx_conflict() || e.is_retryable() => Err(e),
                        Err(e) => {
                            tx.rollback().await?;
                            Err(e)
                        }
                    }
                }
                .await;

                match outcome {
                    Ok(v) => return Ok(v),
                    // A transaction conflict or transient DB error: retry on a fresh
                    // transaction, unbounded, backing off and bailing if the workflow
                    // is cancelled. Matches Go/Python, which retry these until they
                    // clear rather than surfacing a spurious failure under contention.
                    Err(e) if e.is_tx_conflict() || e.is_retryable() => {
                        self.conflict_retry_wait(workflow_id, conflict_attempt)
                            .await?;
                        conflict_attempt = conflict_attempt.saturating_add(1);
                    }
                    Err(e) => break 'conflict e,
                }
            };

            // A body error reached the user-retry policy. Retry the whole body if
            // the budget allows and the predicate accepts; otherwise record the
            // failure durably (outside any transaction) so a replay returns it
            // without re-running, and surface the original error.
            if opts.should_user_retry(&body_err, user_attempt) {
                let delay = opts.user_retry_backoff(user_attempt);
                tracing::warn!(
                    step = %name,
                    attempt = user_attempt + 1,
                    error = %body_err,
                    "transaction failed; retrying after backoff"
                );
                tokio::time::sleep(delay).await;
                user_attempt += 1;
                continue;
            }
            sqlx::query(&format!(
                "INSERT INTO {operation_outputs}
                     (workflow_uuid, function_id, function_name, error, serialization,
                      started_at_epoch_ms, completed_at_epoch_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
                operation_outputs = self.tables.operation_outputs
            ))
            .bind(workflow_id)
            .bind(seq)
            .bind(name)
            .bind(serialize::encode_error(&self.serializer, &body_err))
            .bind(self.serializer.name())
            .bind(started_at_ms)
            .bind(Utc::now().timestamp_millis())
            .execute(&self.pool)
            .await?;
            return Err(body_err);
        }
    }

    async fn dequeue_workflows(&self, req: &DequeueRequest) -> Result<Vec<WorkflowStatus>> {
        let now_ms = Utc::now().timestamp_millis();
        let mut tx = self.pool.begin().await?;

        // Snapshot isolation is only required for global concurrency or rate
        // limiting, where the COUNT and the candidate scan must see a consistent
        // view across concurrent dispatchers; worker concurrency alone is
        // enforced in-process, so READ COMMITTED suffices. This must run before
        // any query in the transaction. (Mirrors Go's QueueDequeueIsolation.)
        if req.global_concurrency.is_some() || req.rate_limit_max.is_some() {
            sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                .execute(&mut *tx)
                .await?;
        }

        let mut max_tasks = req.max_tasks;

        // A partitioned queue scopes every count and the candidate scan to one
        // partition key; a non-partitioned queue (`None`) leaves them unscoped.
        let part = req.partition_key.as_deref();

        if let (Some(limit), Some(period_ms)) = (req.rate_limit_max, req.rate_limit_period_ms) {
            let part_clause = if part.is_some() {
                " AND queue_partition_key = $5"
            } else {
                ""
            };
            let sql = format!(
                "SELECT COUNT(*) FROM {workflow_status}
                 WHERE queue_name = $1 AND rate_limited = TRUE
                   AND status NOT IN ($2, $3) AND started_at_epoch_ms > $4{part_clause}",
                workflow_status = self.tables.workflow_status
            );
            let mut q = sqlx::query_scalar(&sql)
                .bind(&req.queue_name)
                .bind(STATUS_ENQUEUED)
                .bind(STATUS_DELAYED)
                .bind(now_ms - period_ms);
            if let Some(p) = part {
                q = q.bind(p);
            }
            let recent: i64 = q.fetch_one(&mut *tx).await?;
            max_tasks = max_tasks.min((limit - recent).max(0));
        }

        if let Some(global) = req.global_concurrency {
            let part_clause = if part.is_some() {
                " AND queue_partition_key = $3"
            } else {
                ""
            };
            let sql = format!(
                "SELECT COUNT(*) FROM {workflow_status} WHERE queue_name = $1 AND status = $2{part_clause}",
                workflow_status = self.tables.workflow_status
            );
            let mut q = sqlx::query_scalar(&sql)
                .bind(&req.queue_name)
                .bind(STATUS_PENDING);
            if let Some(p) = part {
                q = q.bind(p);
            }
            let pending: i64 = q.fetch_one(&mut *tx).await?;
            max_tasks = max_tasks.min((global - pending).max(0));
        }

        if max_tasks <= 0 {
            return Ok(Vec::new());
        }

        // SKIP LOCKED lets concurrent dispatchers claim disjoint sets without
        // blocking; with a global concurrency cap, NOWAIT instead surfaces
        // contention so the counts above stay consistent.
        let lock = if req.global_concurrency.is_none() {
            "FOR UPDATE SKIP LOCKED"
        } else {
            "FOR UPDATE NOWAIT"
        };
        // With a partition key the clause takes $4 and LIMIT moves to $5.
        let (part_clause, limit_ph) = if part.is_some() {
            (" AND queue_partition_key = $4", "$5")
        } else {
            ("", "$4")
        };
        // A row's version must match this executor's exactly; unversioned rows
        // ('' or NULL, e.g. client-enqueued) are claimable only by the fleet
        // running the LATEST registered application version — otherwise a
        // stale-version executor could claim work whose handlers it no longer
        // has. No registered versions ⇒ treat this executor as latest.
        let is_latest = sqlx::query_scalar::<_, String>(&format!(
            "SELECT version_name FROM {application_versions}
             ORDER BY version_timestamp DESC LIMIT 1",
            application_versions = self.tables.application_versions
        ))
        .fetch_optional(&mut *tx)
        .await?
        .is_none_or(|latest| latest == req.app_version);
        let version_clause = if is_latest {
            "(application_version = $3 OR application_version = '' \
              OR application_version IS NULL)"
        } else {
            "application_version = $3"
        };
        let sql = format!(
            "SELECT workflow_uuid FROM {workflow_status}
             WHERE queue_name = $1 AND status = $2
               AND {version_clause}{part_clause}
             ORDER BY priority ASC, created_at ASC
             {lock} LIMIT {limit_ph}",
            workflow_status = self.tables.workflow_status
        );
        let mut q = sqlx::query_scalar(&sql)
            .bind(&req.queue_name)
            .bind(STATUS_ENQUEUED)
            .bind(&req.app_version);
        if let Some(p) = part {
            q = q.bind(p);
        }
        let ids: Vec<String> = q.bind(max_tasks).fetch_all(&mut *tx).await?;

        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let rows = sqlx::query(&format!(
            "UPDATE {workflow_status}
             SET status = $1, executor_id = $2, application_version = $3,
                 started_at_epoch_ms = $4, rate_limited = $5, updated_at = $4,
                 workflow_deadline_epoch_ms = CASE
                     WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                     THEN $4 + workflow_timeout_ms
                     ELSE workflow_deadline_epoch_ms
                 END
             WHERE workflow_uuid = ANY($6) AND status = $7
             RETURNING {SELECT_COLS}",
            workflow_status = self.tables.workflow_status
        ))
        .bind(STATUS_PENDING)
        .bind(&req.executor_id)
        .bind(&req.app_version)
        .bind(now_ms)
        .bind(req.rate_limit_max.is_some())
        .bind(&ids)
        .bind(STATUS_ENQUEUED)
        .fetch_all(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(rows
            .iter()
            .map(|r| row_to_status(&self.serializer, r))
            .collect())
    }

    async fn transition_delayed_workflows(&self, now_ms: i64) -> Result<u64> {
        let res = sqlx::query(&format!(
            "UPDATE {workflow_status}
             SET status = $1, delay_until_epoch_ms = NULL, updated_at = $2
             WHERE status = $3 AND delay_until_epoch_ms <= $2",
            workflow_status = self.tables.workflow_status
        ))
        .bind(STATUS_ENQUEUED)
        .bind(now_ms)
        .bind(STATUS_DELAYED)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    async fn queue_partitions(&self, queue_name: &str) -> Result<Vec<String>> {
        let keys: Vec<String> = sqlx::query_scalar(&format!(
            "SELECT DISTINCT queue_partition_key FROM {workflow_status}
             WHERE queue_name = $1 AND status = $2 AND queue_partition_key IS NOT NULL",
            workflow_status = self.tables.workflow_status
        ))
        .bind(queue_name)
        .bind(STATUS_ENQUEUED)
        .fetch_all(&self.pool)
        .await?;
        Ok(keys)
    }

    async fn insert_notification(
        &self,
        destination_id: &str,
        topic: &str,
        message: Value,
        idempotency_key: Option<&str>,
    ) -> Result<()> {
        // A keyed send derives its primary key so a retry conflicts and is
        // dropped (at-most-once); an unkeyed send gets a fresh id every time.
        let message_uuid = match idempotency_key {
            Some(k) => format!("{k}::{destination_id}"),
            None => uuid::Uuid::new_v4().to_string(),
        };
        // The FK on destination_uuid rejects sends to nonexistent workflows; a
        // duplicate keyed message_uuid is silently ignored.
        sqlx::query(&format!("INSERT INTO {notifications}
                 (message_uuid, destination_uuid, topic, message, serialization, created_at_epoch_ms)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (message_uuid) DO NOTHING", notifications = self.tables.notifications),
        )
        .bind(message_uuid)
        .bind(destination_id)
        .bind(topic)
        .bind(self.serializer.encode(&message)?)
        .bind(self.serializer.name())
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await
        .map_err(|e| nonexistent_or(e, destination_id))?;
        Ok(())
    }

    async fn insert_notifications(&self, rows: &[NotificationInsert]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        // Serialize up front (fallible), then build one multi-row INSERT — the
        // whole batch commits or fails together, and each keyed row keeps the
        // singular path's `{key}::{destination}` at-most-once id.
        let mut prepared = Vec::with_capacity(rows.len());
        for r in rows {
            let message_uuid = match &r.idempotency_key {
                Some(k) => format!("{k}::{}", r.destination_id),
                None => uuid::Uuid::new_v4().to_string(),
            };
            prepared.push((message_uuid, self.serializer.encode(&r.message)?));
        }
        let now = Utc::now().timestamp_millis();
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "INSERT INTO {notifications}
                 (message_uuid, destination_uuid, topic, message, serialization, created_at_epoch_ms) ",
            notifications = self.tables.notifications
        ));
        qb.push_values(rows.iter().zip(prepared), |mut b, (r, (uuid, encoded))| {
            b.push_bind(uuid)
                .push_bind(&r.destination_id)
                .push_bind(&r.topic)
                .push_bind(encoded)
                .push_bind(self.serializer.name())
                .push_bind(now);
        });
        qb.push(" ON CONFLICT (message_uuid) DO NOTHING");
        qb.build().execute(&self.pool).await.map_err(|e| {
            let mut dests: Vec<&str> = rows.iter().map(|r| r.destination_id.as_str()).collect();
            dests.sort_unstable();
            dests.dedup();
            nonexistent_or(e, &dests.join(", "))
        })?;
        Ok(())
    }

    async fn consume_notification(
        &self,
        workflow_id: &str,
        topic: &str,
        seq: i32,
        step_name: &str,
    ) -> Result<Option<Value>> {
        // Claim and checkpoint in one transaction: a crash between them would
        // otherwise lose the message. message_uuid pins exactly one row even
        // when several messages share a created_at millisecond.
        let mut tx = self.pool.begin().await?;

        let claimed: Option<(String, Option<String>)> = sqlx::query_as(&format!(
            "WITH oldest_entry AS (
                 SELECT message_uuid FROM {notifications}
                 WHERE destination_uuid = $1 AND topic = $2 AND consumed = FALSE
                 ORDER BY created_at_epoch_ms ASC
                 LIMIT 1
             )
             UPDATE {notifications} SET consumed = TRUE
             WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
             RETURNING message, serialization",
            notifications = self.tables.notifications
        ))
        .bind(workflow_id)
        .bind(topic)
        .fetch_optional(&mut *tx)
        .await?;

        let Some((message, fmt)) = claimed else {
            return Ok(None);
        };

        // Checkpoint the consumed message verbatim, keeping its format so a
        // replay decodes it the same way.
        sqlx::query(&format!(
            "INSERT INTO {operation_outputs}
                 (workflow_uuid, function_id, function_name, output, serialization)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(workflow_id)
        .bind(seq)
        .bind(step_name)
        .bind(&message)
        .bind(&fmt)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(Some(serialize::decode(
            &self.serializer,
            fmt.as_deref(),
            &message,
        )?))
    }

    async fn upsert_event(&self, workflow_id: &str, key: &str, value: Value) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {workflow_events} (workflow_uuid, key, value, serialization)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, key)
             DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization",
            workflow_events = self.tables.workflow_events
        ))
        .bind(workflow_id)
        .bind(key)
        .bind(self.serializer.encode(&value)?)
        .bind(self.serializer.name())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_event_value(&self, workflow_id: &str, key: &str) -> Result<Option<Value>> {
        let row: Option<(String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT value, serialization FROM {workflow_events}
             WHERE workflow_uuid = $1 AND key = $2",
            workflow_events = self.tables.workflow_events
        ))
        .bind(workflow_id)
        .bind(key)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some((value, fmt)) => Ok(Some(serialize::decode(
                &self.serializer,
                fmt.as_deref(),
                &value,
            )?)),
            None => Ok(None),
        }
    }

    async fn list_workflows(&self, filter: &ListFilter) -> Result<Vec<WorkflowStatus>> {
        let cols = list_select_cols(filter);
        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(format!(
            "SELECT {cols} FROM {workflow_status}",
            workflow_status = self.tables.workflow_status
        ));
        push_list_filters(&mut qb, filter);
        qb.push(if filter.sort_desc {
            " ORDER BY created_at DESC"
        } else {
            " ORDER BY created_at ASC"
        });
        if let Some(lim) = filter.limit {
            qb.push(" LIMIT ").push_bind(lim);
        }
        if let Some(off) = filter.offset {
            qb.push(" OFFSET ").push_bind(off);
        }
        let rows = qb.build().fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| row_to_status(&self.serializer, r))
            .collect())
    }

    async fn get_workflow_aggregates(
        &self,
        query: &WorkflowAggregateQuery,
    ) -> Result<Vec<WorkflowAggregate>> {
        let cols = query.enabled_columns();
        let bucket = query.time_bucket_ms.filter(|b| *b > 0);

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("SELECT ");
        for (_, col) in &cols {
            qb.push(*col).push(", ");
        }
        if let Some(b) = bucket {
            qb.push("(created_at / ")
                .push_bind(b)
                .push(") * ")
                .push_bind(b)
                .push(" AS time_bucket, ");
        }
        // At least one select is guaranteed by the engine; emit a stable order.
        qb.push(workflow_agg_selects(query).join(", "));
        qb.push(format!(" FROM {}", self.tables.workflow_status));
        push_agg_filters(&mut qb, query);
        qb.push(" GROUP BY ");
        let mut first = true;
        for (_, col) in &cols {
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push(*col);
        }
        if bucket.is_some() {
            if !first {
                qb.push(", ");
            }
            qb.push("time_bucket");
        }
        if let Some(lim) = query.limit {
            qb.push(" LIMIT ").push_bind(lim);
        }

        let rows = qb.build().fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| row_to_aggregate(r, &cols, bucket.is_some()))
            .collect())
    }

    async fn get_step_aggregates(&self, query: &StepAggregateQuery) -> Result<Vec<StepAggregate>> {
        let dims = query.group_exprs();
        let bucket = query.time_bucket_ms.filter(|b| *b > 0);

        let mut qb: QueryBuilder<Postgres> = QueryBuilder::new("SELECT ");
        for (key, expr) in &dims {
            qb.push(*expr).push(" AS ").push(*key).push(", ");
        }
        if let Some(b) = bucket {
            qb.push("(completed_at_epoch_ms / ")
                .push_bind(b)
                .push(") * ")
                .push_bind(b)
                .push(" AS time_bucket, ");
        }
        let mut sel = Vec::new();
        if query.select_count {
            sel.push("COUNT(*) AS cnt");
        }
        if query.select_max_duration_ms {
            sel.push("MAX(completed_at_epoch_ms - started_at_epoch_ms) AS max_dur");
        }
        qb.push(sel.join(", "));
        qb.push(format!(" FROM {}", self.tables.operation_outputs));
        push_step_agg_filters(&mut qb, query);
        qb.push(" GROUP BY ");
        let mut first = true;
        for (_, expr) in &dims {
            if !first {
                qb.push(", ");
            }
            first = false;
            qb.push(*expr);
        }
        if let Some(b) = bucket {
            if !first {
                qb.push(", ");
            }
            qb.push("(completed_at_epoch_ms / ")
                .push_bind(b)
                .push(") * ")
                .push_bind(b);
        }
        if let Some(lim) = query.limit {
            qb.push(" LIMIT ").push_bind(lim);
        }

        let rows = qb.build().fetch_all(&self.pool).await?;
        Ok(rows
            .iter()
            .map(|r| {
                row_to_step_aggregate(
                    r,
                    &dims,
                    bucket.is_some(),
                    query.select_count,
                    query.select_max_duration_ms,
                )
            })
            .collect())
    }

    async fn cancel_workflow(&self, id: &str) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(&format!(
            "UPDATE {workflow_status}
             SET status = $2, completed_at = $3, started_at_epoch_ms = NULL,
                 queue_name = NULL, deduplication_id = NULL, updated_at = $3
             WHERE workflow_uuid = $1 AND status NOT IN ($4, $5, $6)",
            workflow_status = self.tables.workflow_status
        ))
        .bind(id)
        .bind(STATUS_CANCELLED)
        .bind(now)
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .bind(STATUS_CANCELLED)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn resume_workflow(&self, id: &str) -> Result<bool> {
        let res = sqlx::query(&format!(
            "UPDATE {workflow_status}
             SET status = $2, recovery_attempts = 0, workflow_deadline_epoch_ms = NULL,
                 deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL,
                 updated_at = $3
             WHERE workflow_uuid = $1 AND status NOT IN ($4, $5)",
            workflow_status = self.tables.workflow_status
        ))
        .bind(id)
        .bind(STATUS_PENDING)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn enqueue_existing(&self, id: &str, queue: &str) -> Result<()> {
        sqlx::query(&format!(
            "UPDATE {workflow_status}
             SET status = $2, queue_name = $3, executor_id = '',
                 started_at_epoch_ms = NULL, updated_at = $4
             WHERE workflow_uuid = $1",
            workflow_status = self.tables.workflow_status
        ))
        .bind(id)
        .bind(STATUS_ENQUEUED)
        .bind(queue)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn cancel_workflows(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        sqlx::query(&format!(
            "UPDATE {workflow_status}
             SET status = $2, completed_at = $3, started_at_epoch_ms = NULL,
                 queue_name = NULL, deduplication_id = NULL, updated_at = $3
             WHERE workflow_uuid = ANY($1) AND status NOT IN ($4, $5, $6)",
            workflow_status = self.tables.workflow_status
        ))
        .bind(ids)
        .bind(STATUS_CANCELLED)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .bind(STATUS_CANCELLED)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn resume_workflows(&self, ids: &[String]) -> Result<Vec<String>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let resumed: Vec<String> = sqlx::query_scalar(&format!(
            "UPDATE {workflow_status}
             SET status = $2, recovery_attempts = 0, workflow_deadline_epoch_ms = NULL,
                 deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL,
                 updated_at = $3
             WHERE workflow_uuid = ANY($1) AND status NOT IN ($4, $5)
             RETURNING workflow_uuid",
            workflow_status = self.tables.workflow_status
        ))
        .bind(ids)
        .bind(STATUS_PENDING)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_SUCCESS)
        .bind(STATUS_ERROR)
        .fetch_all(&self.pool)
        .await?;
        Ok(resumed)
    }

    async fn delete_workflows(&self, ids: &[String], delete_children: bool) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        // ON DELETE CASCADE removes each workflow's step / event / stream rows.
        if delete_children {
            sqlx::query(&format!(
                "WITH RECURSIVE targets AS (
                     SELECT workflow_uuid FROM {workflow_status} WHERE workflow_uuid = ANY($1)
                     UNION
                     SELECT w.workflow_uuid FROM {workflow_status} w
                       JOIN targets t ON w.parent_workflow_id = t.workflow_uuid
                 )
                 DELETE FROM {workflow_status}
                 WHERE workflow_uuid IN (SELECT workflow_uuid FROM targets)",
                workflow_status = self.tables.workflow_status
            ))
            .bind(ids)
            .execute(&self.pool)
            .await?;
        } else {
            sqlx::query(&format!(
                "DELETE FROM {workflow_status} WHERE workflow_uuid = ANY($1)",
                workflow_status = self.tables.workflow_status
            ))
            .bind(ids)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn garbage_collect(
        &self,
        cutoff_epoch_ms: Option<i64>,
        rows_threshold: Option<i64>,
    ) -> Result<u64> {
        let Some(cutoff) =
            crate::provider::resolve_gc_cutoff(self, cutoff_epoch_ms, rows_threshold).await?
        else {
            return Ok(0);
        };
        // The canonical DBOS GC delete: strictly-older terminal (and dead-letter)
        // rows go; in-flight work survives regardless of age. Children cascade.
        // Batched so a large backlog is many short transactions, not one long
        // delete pinning the MVCC horizon of the hottest table.
        let mut total = 0u64;
        loop {
            let res = sqlx::query(&format!(
                "DELETE FROM {workflow_status}
                 WHERE workflow_uuid IN (
                     SELECT workflow_uuid FROM {workflow_status}
                     WHERE created_at < $1 AND status NOT IN ($2, $3, $4)
                     LIMIT $5)",
                workflow_status = self.tables.workflow_status
            ))
            .bind(cutoff)
            .bind(STATUS_PENDING)
            .bind(STATUS_ENQUEUED)
            .bind(STATUS_DELAYED)
            .bind(crate::provider::GC_BATCH)
            .execute(&self.pool)
            .await?;
            total += res.rows_affected();
            if res.rows_affected() < crate::provider::GC_BATCH as u64 {
                return Ok(total);
            }
        }
    }

    async fn set_workflow_delay(&self, id: &str, delay_until_ms: i64) -> Result<bool> {
        let res = sqlx::query(&format!(
            "UPDATE {workflow_status} SET delay_until_epoch_ms = $2, updated_at = $3
             WHERE workflow_uuid = $1 AND status = $4",
            workflow_status = self.tables.workflow_status
        ))
        .bind(id)
        .bind(delay_until_ms)
        .bind(Utc::now().timestamp_millis())
        .bind(STATUS_DELAYED)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn set_workflow_attributes(
        &self,
        id: &str,
        attributes: Option<&serde_json::Map<String, Value>>,
    ) -> Result<()> {
        // Replace-not-merge; an empty map clears to NULL (the reference shape).
        let value = attributes
            .filter(|m| !m.is_empty())
            .map(|m| Value::Object(m.clone()));
        let res = sqlx::query(&format!(
            "UPDATE {workflow_status} SET attributes = $2, updated_at = $3
             WHERE workflow_uuid = $1",
            workflow_status = self.tables.workflow_status
        ))
        .bind(id)
        .bind(value)
        .bind(Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(Error::nonexistent_workflow(id));
        }
        Ok(())
    }

    async fn fork_workflow(&self, params: &ForkParams) -> Result<()> {
        let original_id = params.original_id.as_str();
        let new_id = params.new_id.as_str();
        let start_step = params.start_step;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now().timestamp_millis();

        let inserted = sqlx::query(&format!(
            "INSERT INTO {workflow_status}
                 (workflow_uuid, status, name, inputs, serialization, executor_id,
                  application_version, application_id, forked_from, recovery_attempts,
                  authenticated_user, assumed_role, authenticated_roles,
                  class_name, config_name, queue_name, queue_partition_key,
                  created_at, updated_at)
             SELECT $1, $2, name, inputs, serialization, '',
                    COALESCE($3, application_version), application_id, $4, 0,
                    authenticated_user, assumed_role, authenticated_roles,
                    class_name, config_name, $5, $6, $7, $7
             FROM {workflow_status} WHERE workflow_uuid = $4",
            workflow_status = self.tables.workflow_status
        ))
        .bind(new_id)
        .bind(STATUS_ENQUEUED)
        .bind(params.app_version.as_deref())
        .bind(original_id)
        .bind(&params.queue_name)
        .bind(params.partition_key.as_deref())
        .bind(now)
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            return Err(crate::error::Error::nonexistent_workflow(original_id));
        }

        sqlx::query(&format!(
            "UPDATE {workflow_status} SET was_forked_from = TRUE WHERE workflow_uuid = $1",
            workflow_status = self.tables.workflow_status
        ))
        .bind(original_id)
        .execute(&mut *tx)
        .await?;

        if start_step > 0 {
            sqlx::query(&format!(
                "INSERT INTO {operation_outputs}
                     (workflow_uuid, function_id, function_name, output, error,
                      child_workflow_id, serialization)
                 SELECT $1, function_id, function_name, output, error,
                        child_workflow_id, serialization
                 FROM {operation_outputs} WHERE workflow_uuid = $2 AND function_id < $3",
                operation_outputs = self.tables.operation_outputs
            ))
            .bind(new_id)
            .bind(original_id)
            .bind(start_step)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    async fn claim_for_recovery(&self, req: &RecoveryClaimRequest<'_>) -> Result<RecoveryClaim> {
        let now = Utc::now().timestamp_millis();
        let attempts = req.expected_attempts + 1;
        // Every arm carries the same compare-and-set predicate: the row is
        // still PENDING, still owned by the executor the sweep observed, and
        // still at the attempt count it observed. One UPDATE per claim — the
        // predicate, not a transaction, is the arbiter.
        let cas = "WHERE workflow_uuid = $1 AND status = 'PENDING' AND executor_id = $2 \
                   AND recovery_attempts = $3";

        let (set, needs_executor) = if attempts > req.max_attempts {
            // This dispatch would exceed the cap: park the row instead.
            (
                "status = 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', recovery_attempts = recovery_attempts + 1,
                 deduplication_id = NULL, updated_at = $4",
                false,
            )
        } else if req.requeue {
            // Give the row back to its queue; the dispatcher's own atomic
            // ENQUEUED → PENDING claim admits exactly one runner.
            (
                "status = 'ENQUEUED', recovery_attempts = recovery_attempts + 1,
                 started_at_epoch_ms = NULL, updated_at = $4",
                false,
            )
        } else {
            // Claim it for direct re-dispatch by this executor.
            (
                "recovery_attempts = recovery_attempts + 1, executor_id = $5, updated_at = $4",
                true,
            )
        };
        let sql = format!(
            "UPDATE {workflow_status} SET {set} {cas}",
            workflow_status = self.tables.workflow_status
        );
        let mut q = sqlx::query(&sql)
            .bind(req.workflow_id)
            .bind(req.expected_executor)
            .bind(req.expected_attempts)
            .bind(now);
        if needs_executor {
            q = q.bind(req.new_executor);
        }
        let res = q.execute(&self.pool).await?;
        if res.rows_affected() == 0 {
            return Ok(RecoveryClaim::Lost);
        }
        Ok(if attempts > req.max_attempts {
            RecoveryClaim::Parked { attempts }
        } else if req.requeue {
            RecoveryClaim::Requeued
        } else {
            RecoveryClaim::Claimed { attempts }
        })
    }

    async fn record_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
        name: &str,
        child_id: &str,
    ) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {operation_outputs}
                 (workflow_uuid, function_id, function_name, child_workflow_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(parent_id)
        .bind(seq)
        .bind(name)
        .bind(child_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn check_child_workflow(
        &self,
        parent_id: &str,
        seq: i32,
    ) -> Result<Option<(String, String)>> {
        let row = sqlx::query(&format!(
            "SELECT child_workflow_id, function_name FROM {operation_outputs}
             WHERE workflow_uuid = $1 AND function_id = $2",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(parent_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|r| {
            r.get::<Option<String>, _>("child_workflow_id")
                .map(|id| (id, r.get::<String, _>("function_name")))
        }))
    }

    async fn get_workflow_steps(&self, workflow_id: &str) -> Result<Vec<StepInfo>> {
        let rows = sqlx::query(&format!(
            "SELECT function_id, function_name, output, error, child_workflow_id,
                    started_at_epoch_ms, completed_at_epoch_ms, serialization
             FROM {operation_outputs}
             WHERE workflow_uuid = $1
             ORDER BY function_id ASC",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| row_to_step(&self.serializer, r))
            .collect()
    }

    async fn get_step_name(&self, workflow_id: &str, seq: i32) -> Result<Option<String>> {
        let name: Option<String> = sqlx::query_scalar(&format!(
            "SELECT function_name FROM {operation_outputs}
             WHERE workflow_uuid = $1 AND function_id = $2",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(workflow_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;
        Ok(name)
    }

    async fn record_patch(&self, workflow_id: &str, seq: i32, name: &str) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {operation_outputs} (workflow_uuid, function_id, function_name)
             VALUES ($1, $2, $3)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
            operation_outputs = self.tables.operation_outputs
        ))
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn write_stream(
        &self,
        workflow_id: &str,
        key: &str,
        value: Option<Value>,
        function_id: i32,
    ) -> Result<()> {
        // A closed entry stores the sentinel verbatim with no serialization;
        // user values are encoded and tagged with the serializer name.
        let (stored, ser): (String, Option<String>) = match value {
            Some(v) => (
                self.serializer.encode(&v)?,
                Some(self.serializer.name().to_string()),
            ),
            None => (STREAM_CLOSED_SENTINEL.to_string(), None),
        };

        // Check-then-append in one transaction so the next offset cannot be
        // claimed by a concurrent writer.
        let mut tx = self.pool.begin().await?;

        let closed: Option<i32> = sqlx::query_scalar(&format!(
            "SELECT 1 FROM {streams} WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1",
            streams = self.tables.streams
        ))
        .bind(workflow_id)
        .bind(key)
        .bind(STREAM_CLOSED_SENTINEL)
        .fetch_optional(&mut *tx)
        .await?;
        if closed.is_some() {
            return Err(crate::error::Error::app(format!(
                "stream `{key}` is already closed"
            )));
        }

        sqlx::query(&format!("INSERT INTO {streams} (workflow_uuid, key, value, \"offset\", function_id, serialization)
             SELECT $1, $2, $3, COALESCE(
                 (SELECT MAX(\"offset\") FROM {streams} WHERE workflow_uuid = $1 AND key = $2), -1
             ) + 1, $4, $5", streams = self.tables.streams),
        )
        .bind(workflow_id)
        .bind(key)
        .bind(&stored)
        .bind(function_id)
        .bind(&ser)
        .execute(&mut *tx)
        .await
        .map_err(|e| nonexistent_or(e, workflow_id))?;

        tx.commit().await?;
        Ok(())
    }

    async fn read_stream(
        &self,
        workflow_id: &str,
        key: &str,
        from_offset: i32,
    ) -> Result<(Vec<Value>, bool)> {
        let rows: Vec<(String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT value, serialization FROM {streams}
             WHERE workflow_uuid = $1 AND key = $2 AND \"offset\" >= $3
             ORDER BY \"offset\" ASC",
            streams = self.tables.streams
        ))
        .bind(workflow_id)
        .bind(key)
        .bind(from_offset)
        .fetch_all(&self.pool)
        .await?;

        let mut values = Vec::with_capacity(rows.len());
        let mut closed = false;
        for (value, fmt) in rows {
            if value == STREAM_CLOSED_SENTINEL {
                closed = true;
                break;
            }
            values.push(serialize::decode(&self.serializer, fmt.as_deref(), &value)?);
        }
        Ok((values, closed))
    }

    async fn list_workflow_events(&self, workflow_id: &str) -> Result<Vec<(String, Value)>> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT key, value, serialization FROM {workflow_events}
             WHERE workflow_uuid = $1 ORDER BY key ASC",
            workflow_events = self.tables.workflow_events
        ))
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(key, value, fmt)| {
                Ok((
                    key,
                    serialize::decode(&self.serializer, fmt.as_deref(), &value)?,
                ))
            })
            .collect()
    }

    async fn list_workflow_notifications(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<NotificationInfo>> {
        let rows: Vec<(String, String, Option<String>, i64, bool)> = sqlx::query_as(&format!(
            "SELECT topic, message, serialization, created_at_epoch_ms, consumed
             FROM {notifications} WHERE destination_uuid = $1
             ORDER BY created_at_epoch_ms ASC",
            notifications = self.tables.notifications
        ))
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|(topic, message, fmt, created_at_ms, consumed)| {
                Ok(NotificationInfo {
                    topic: (!topic.is_empty()).then_some(topic),
                    message: serialize::decode(&self.serializer, fmt.as_deref(), &message)?,
                    created_at_ms,
                    consumed,
                })
            })
            .collect()
    }

    async fn list_workflow_streams(&self, workflow_id: &str) -> Result<Vec<(String, Vec<Value>)>> {
        let rows: Vec<(String, String, Option<String>)> = sqlx::query_as(&format!(
            "SELECT key, value, serialization FROM {streams}
             WHERE workflow_uuid = $1 ORDER BY key ASC, \"offset\" ASC",
            streams = self.tables.streams
        ))
        .bind(workflow_id)
        .fetch_all(&self.pool)
        .await?;
        group_stream_rows(&self.serializer, rows)
    }

    async fn create_schedule(&self, schedule: &WorkflowSchedule) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {workflow_schedules} (
                 schedule_id, schedule_name, workflow_name, schedule, status, context,
                 last_fired_at, automatic_backfill, cron_timezone, queue_name
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            workflow_schedules = self.tables.workflow_schedules
        ))
        .bind(&schedule.schedule_id)
        .bind(&schedule.schedule_name)
        .bind(&schedule.workflow_name)
        .bind(&schedule.schedule)
        .bind(schedule.status.as_str())
        .bind(encode_schedule_context(&schedule.context))
        .bind(schedule.last_fired_at.map(|t| t.to_rfc3339()))
        .bind(schedule.automatic_backfill)
        .bind(&schedule.cron_timezone)
        .bind(&schedule.queue_name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn apply_schedules(&self, schedules: &[WorkflowSchedule]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for s in schedules {
            sqlx::query(&format!(
                "DELETE FROM {workflow_schedules} WHERE schedule_name = $1",
                workflow_schedules = self.tables.workflow_schedules
            ))
            .bind(&s.schedule_name)
            .execute(&mut *tx)
            .await?;
            sqlx::query(&format!(
                "INSERT INTO {workflow_schedules} (
                     schedule_id, schedule_name, workflow_name, schedule, status, context,
                     last_fired_at, automatic_backfill, cron_timezone, queue_name
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                workflow_schedules = self.tables.workflow_schedules
            ))
            .bind(&s.schedule_id)
            .bind(&s.schedule_name)
            .bind(&s.workflow_name)
            .bind(&s.schedule)
            .bind(s.status.as_str())
            .bind(encode_schedule_context(&s.context))
            .bind(s.last_fired_at.map(|t| t.to_rfc3339()))
            .bind(s.automatic_backfill)
            .bind(&s.cron_timezone)
            .bind(&s.queue_name)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn list_schedules(&self, filter: &ScheduleFilter) -> Result<Vec<WorkflowSchedule>> {
        let mut qb = QueryBuilder::new(format!(
            "SELECT schedule_id, schedule_name, workflow_name, schedule, status, context, \
             last_fired_at, automatic_backfill, cron_timezone, queue_name FROM {workflow_schedules}",
            workflow_schedules = self.tables.workflow_schedules
        ));
        let mut sep = " WHERE ";
        if !filter.statuses.is_empty() {
            let statuses: Vec<String> = filter
                .statuses
                .iter()
                .map(|s| s.as_str().to_string())
                .collect();
            qb.push(sep)
                .push("status = ANY(")
                .push_bind(statuses)
                .push(")");
            sep = " AND ";
        }
        if !filter.workflow_names.is_empty() {
            qb.push(sep)
                .push("workflow_name = ANY(")
                .push_bind(filter.workflow_names.clone())
                .push(")");
            sep = " AND ";
        }
        if !filter.name_prefixes.is_empty() {
            let patterns: Vec<String> = filter
                .name_prefixes
                .iter()
                .map(|p| format!("{p}%"))
                .collect();
            qb.push(sep)
                .push("schedule_name LIKE ANY(")
                .push_bind(patterns)
                .push(")");
        }
        qb.push(" ORDER BY schedule_name ASC");

        let rows = qb.build().fetch_all(&self.pool).await?;
        rows.iter().map(row_to_schedule).collect()
    }

    async fn set_schedule_status(&self, name: &str, status: ScheduleStatus) -> Result<bool> {
        let res = sqlx::query(&format!(
            "UPDATE {workflow_schedules} SET status = $1 WHERE schedule_name = $2",
            workflow_schedules = self.tables.workflow_schedules
        ))
        .bind(status.as_str())
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn set_schedule_last_fired(&self, name: &str, at_ms: i64) -> Result<()> {
        let at = DateTime::from_timestamp_millis(at_ms).map(|t| t.to_rfc3339());
        sqlx::query(&format!(
            "UPDATE {workflow_schedules} SET last_fired_at = $1 WHERE schedule_name = $2",
            workflow_schedules = self.tables.workflow_schedules
        ))
        .bind(at)
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn delete_schedule(&self, name: &str) -> Result<bool> {
        let res = sqlx::query(&format!(
            "DELETE FROM {workflow_schedules} WHERE schedule_name = $1",
            workflow_schedules = self.tables.workflow_schedules
        ))
        .bind(name)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn create_application_version(&self, version_name: &str) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        sqlx::query(&format!(
            "INSERT INTO {application_versions} \
             (version_id, version_name, version_timestamp, created_at) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (version_name) DO NOTHING",
            application_versions = self.tables.application_versions
        ))
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(version_name)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_application_versions(&self) -> Result<Vec<VersionInfo>> {
        let rows = sqlx::query(&format!(
            "SELECT version_id, version_name, version_timestamp, created_at \
             FROM {application_versions} ORDER BY version_timestamp DESC",
            application_versions = self.tables.application_versions
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_version).collect())
    }

    async fn get_latest_application_version(&self) -> Result<Option<VersionInfo>> {
        let row = sqlx::query(&format!(
            "SELECT version_id, version_name, version_timestamp, created_at \
             FROM {application_versions} ORDER BY version_timestamp DESC LIMIT 1",
            application_versions = self.tables.application_versions
        ))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(row_to_version))
    }

    async fn set_latest_application_version(&self, version_name: &str) -> Result<bool> {
        let res = sqlx::query(&format!(
            "UPDATE {application_versions} SET version_timestamp = $1 WHERE version_name = $2",
            application_versions = self.tables.application_versions
        ))
        .bind(Utc::now().timestamp_millis())
        .bind(version_name)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn upsert_queue(&self, queue: &WorkflowQueue, update_existing: bool) -> Result<()> {
        let now = Utc::now().timestamp_millis();
        // A name collision does nothing unless the engine resolved that this
        // process may overwrite (it runs the latest application version).
        let conflict = if update_existing {
            "ON CONFLICT (name) DO UPDATE SET \
             concurrency = excluded.concurrency, \
             worker_concurrency = excluded.worker_concurrency, \
             rate_limit_max = excluded.rate_limit_max, \
             rate_limit_period_sec = excluded.rate_limit_period_sec, \
             priority_enabled = excluded.priority_enabled, \
             partition_queue = excluded.partition_queue, \
             polling_interval_sec = excluded.polling_interval_sec, \
             updated_at = excluded.updated_at"
        } else {
            "ON CONFLICT (name) DO NOTHING"
        };
        // The concurrency columns are `INTEGER` (int4) in the DBOS schema, so
        // bind them as i32 (SQLite is dynamically typed; Postgres is not).
        let sql = format!(
            "INSERT INTO {queues} \
             (queue_id, name, concurrency, worker_concurrency, rate_limit_max, \
              rate_limit_period_sec, priority_enabled, partition_queue, \
              polling_interval_sec, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) {conflict}",
            queues = self.tables.queues
        );
        sqlx::query(&sql)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(&queue.name)
            .bind(queue.global_concurrency.map(|n| n as i32))
            .bind(queue.worker_concurrency.map(|n| n as i32))
            .bind(queue.rate_limit.as_ref().map(|r| r.limit as i32))
            .bind(queue.rate_limit.as_ref().map(|r| r.period.as_secs_f64()))
            .bind(queue.priority_enabled)
            .bind(queue.partitioned)
            .bind(queue.base_polling_interval.as_secs_f64())
            .bind(now)
            .bind(now)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn list_queues(&self) -> Result<Vec<WorkflowQueue>> {
        let rows = sqlx::query(&format!(
            "SELECT name, concurrency, worker_concurrency, rate_limit_max, \
             rate_limit_period_sec, priority_enabled, partition_queue, polling_interval_sec \
             FROM {queues} ORDER BY name",
            queues = self.tables.queues
        ))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(row_to_queue).collect())
    }

    async fn export_workflow(
        &self,
        workflow_id: &str,
        export_children: bool,
    ) -> Result<Vec<ExportedWorkflow>> {
        let mut tx = self.pool.begin().await?;

        // Root first, then transitive children discovered through parent_workflow_id.
        let mut ids = vec![workflow_id.to_string()];
        if export_children {
            let mut queue = vec![workflow_id.to_string()];
            while let Some(parent) = queue.pop() {
                let children: Vec<(String,)> = sqlx::query_as(&format!(
                    "SELECT workflow_uuid FROM {workflow_status} \
                     WHERE parent_workflow_id = $1 ORDER BY workflow_uuid ASC",
                    workflow_status = self.tables.workflow_status
                ))
                .bind(&parent)
                .fetch_all(&mut *tx)
                .await?;
                for (id,) in children {
                    ids.push(id.clone());
                    queue.push(id);
                }
            }
        }

        let mut exported = Vec::with_capacity(ids.len());
        for id in &ids {
            let status_row = sqlx::query(&format!(
                "SELECT * FROM {workflow_status} WHERE workflow_uuid = $1",
                workflow_status = self.tables.workflow_status
            ))
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
            let Some(status_row) = status_row else {
                return Err(Error::nonexistent_workflow(id));
            };
            let workflow_status = export_status_map(&status_row);

            let op_rows = sqlx::query(&format!("SELECT * FROM {operation_outputs} WHERE workflow_uuid = $1 ORDER BY function_id ASC", operation_outputs = self.tables.operation_outputs),
            )
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let operation_outputs = op_rows.iter().map(export_op_map).collect();

            let event_rows = sqlx::query(&format!(
                "SELECT * FROM {workflow_events} WHERE workflow_uuid = $1 ORDER BY key ASC",
                workflow_events = self.tables.workflow_events
            ))
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let workflow_events = event_rows.iter().map(export_event_map).collect();

            let history_rows = sqlx::query(&format!(
                "SELECT * FROM {workflow_events_history} WHERE workflow_uuid = $1 \
                 ORDER BY function_id ASC, key ASC",
                workflow_events_history = self.tables.workflow_events_history
            ))
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let workflow_events_history = history_rows.iter().map(export_history_map).collect();

            let stream_rows = sqlx::query(&format!(
                "SELECT * FROM {streams} WHERE workflow_uuid = $1 ORDER BY key ASC, \"offset\" ASC",
                streams = self.tables.streams
            ))
            .bind(id)
            .fetch_all(&mut *tx)
            .await?;
            let streams = stream_rows.iter().map(export_stream_map).collect();

            exported.push(ExportedWorkflow {
                workflow_status,
                operation_outputs,
                workflow_events,
                workflow_events_history,
                streams,
            });
        }

        tx.commit().await?;
        Ok(exported)
    }

    async fn import_workflow(&self, workflows: &[ExportedWorkflow]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for wf in workflows {
            let s = &wf.workflow_status;
            sqlx::query(&format!(
                "INSERT INTO {workflow_status}
                     (workflow_uuid, status, name, authenticated_user, assumed_role,
                      authenticated_roles, output, error, executor_id, created_at, updated_at,
                      application_version, application_id, class_name, config_name,
                      recovery_attempts, queue_name, workflow_timeout_ms,
                      workflow_deadline_epoch_ms, started_at_epoch_ms, deduplication_id, inputs,
                      priority, queue_partition_key, forked_from, parent_workflow_id,
                      delay_until_epoch_ms, serialization, was_forked_from)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                         $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)",
                workflow_status = self.tables.workflow_status
            ))
            .bind(col_str(s, "workflow_uuid"))
            .bind(col_str(s, "status"))
            .bind(col_str(s, "name"))
            .bind(col_str(s, "authenticated_user"))
            .bind(col_str(s, "assumed_role"))
            .bind(col_str(s, "authenticated_roles"))
            .bind(col_str(s, "output"))
            .bind(col_str(s, "error"))
            .bind(col_str(s, "executor_id"))
            .bind(col_i64(s, "created_at"))
            .bind(col_i64(s, "updated_at"))
            .bind(col_str(s, "application_version"))
            .bind(col_str(s, "application_id"))
            .bind(col_str(s, "class_name"))
            .bind(col_str(s, "config_name"))
            .bind(col_i64(s, "recovery_attempts"))
            .bind(col_str(s, "queue_name"))
            .bind(col_i64(s, "workflow_timeout_ms"))
            .bind(col_i64(s, "workflow_deadline_epoch_ms"))
            .bind(col_i64(s, "started_at_epoch_ms"))
            .bind(col_str(s, "deduplication_id"))
            .bind(col_str(s, "inputs"))
            .bind(col_i32(s, "priority"))
            .bind(col_str(s, "queue_partition_key"))
            .bind(col_str(s, "forked_from"))
            .bind(col_str(s, "parent_workflow_id"))
            .bind(col_i64(s, "delay_until_epoch_ms"))
            .bind(col_str(s, "serialization"))
            // `was_forked_from` marks a fork *source*, not the fork. Carry over
            // the exported value (Python-style portable format); the fallback
            // reconstruction below covers payloads that omit it (Go exports, or
            // older Rust ones). Never derived from this row's own `forked_from`.
            .bind(col_bool(s, "was_forked_from").unwrap_or(false))
            .execute(&mut *tx)
            .await?;

            for op in &wf.operation_outputs {
                sqlx::query(&format!(
                    "INSERT INTO {operation_outputs}
                         (workflow_uuid, function_id, function_name, output, error,
                          child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    operation_outputs = self.tables.operation_outputs
                ))
                .bind(col_str(op, "workflow_uuid"))
                .bind(col_i32(op, "function_id"))
                .bind(col_str(op, "function_name"))
                .bind(col_str(op, "output"))
                .bind(col_str(op, "error"))
                .bind(col_str(op, "child_workflow_id"))
                .bind(col_i64(op, "started_at_epoch_ms"))
                .bind(col_i64(op, "completed_at_epoch_ms"))
                .execute(&mut *tx)
                .await?;
            }

            for ev in &wf.workflow_events {
                sqlx::query(&format!(
                    "INSERT INTO {workflow_events} (workflow_uuid, key, value) VALUES ($1, $2, $3)",
                    workflow_events = self.tables.workflow_events
                ))
                .bind(col_str(ev, "workflow_uuid"))
                .bind(col_str(ev, "key"))
                .bind(col_str(ev, "value"))
                .execute(&mut *tx)
                .await?;
            }

            for h in &wf.workflow_events_history {
                sqlx::query(&format!(
                    "INSERT INTO {workflow_events_history} (workflow_uuid, function_id, key, value)
                     VALUES ($1, $2, $3, $4)",
                    workflow_events_history = self.tables.workflow_events_history
                ))
                .bind(col_str(h, "workflow_uuid"))
                .bind(col_i32(h, "function_id"))
                .bind(col_str(h, "key"))
                .bind(col_str(h, "value"))
                .execute(&mut *tx)
                .await?;
            }

            for st in &wf.streams {
                sqlx::query(&format!(
                    "INSERT INTO {streams} (workflow_uuid, key, value, \"offset\", function_id)
                     VALUES ($1, $2, $3, $4, $5)",
                    streams = self.tables.streams
                ))
                .bind(col_str(st, "workflow_uuid"))
                .bind(col_str(st, "key"))
                .bind(col_str(st, "value"))
                .bind(col_i32(st, "offset"))
                .bind(col_i32(st, "function_id"))
                .execute(&mut *tx)
                .await?;
            }
        }
        // Reconstruct `was_forked_from`: any workflow an imported fork points at
        // (via `forked_from`) is a fork source, so mark it — mirroring what
        // `fork_workflow` stamps and what the in-memory backend derives.
        let sources: Vec<String> = workflows
            .iter()
            .filter_map(|wf| col_str(&wf.workflow_status, "forked_from"))
            .collect();
        if !sources.is_empty() {
            sqlx::query(&format!(
                "UPDATE {workflow_status} SET was_forked_from = TRUE WHERE workflow_uuid = ANY($1)",
                workflow_status = self.tables.workflow_status
            ))
            .bind(&sources)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

/// An exported column read as an `i32` (Postgres `INTEGER`), matching the
/// narrower columns (`priority`, `function_id`, `offset`).
fn col_i32(m: &Map<String, Value>, key: &str) -> Option<i32> {
    col_i64(m, key).map(|v| v as i32)
}

/// A `String` column of a Postgres row as a JSON value (`null` when SQL NULL).
fn s_col(row: &sqlx::postgres::PgRow, key: &str) -> Value {
    json!(row.try_get::<Option<String>, _>(key).ok().flatten())
}

/// A `BIGINT` column of a Postgres row as a JSON value (`null` when SQL NULL).
fn i64_col(row: &sqlx::postgres::PgRow, key: &str) -> Value {
    json!(row.try_get::<Option<i64>, _>(key).ok().flatten())
}

/// An `INTEGER` column of a Postgres row as a JSON value (`null` when SQL NULL).
fn i32_col(row: &sqlx::postgres::PgRow, key: &str) -> Value {
    json!(row.try_get::<Option<i32>, _>(key).ok().flatten())
}

/// A `BOOLEAN` column of a Postgres row as a JSON value (`false` when SQL NULL).
fn bool_col(row: &sqlx::postgres::PgRow, key: &str) -> Value {
    json!(row
        .try_get::<Option<bool>, _>(key)
        .ok()
        .flatten()
        .unwrap_or(false))
}

fn export_status_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in EXPORT_STATUS_STR_COLS {
        m.insert(c.to_string(), s_col(row, c));
    }
    // All exported status integers are BIGINT except `priority` (INTEGER).
    for &c in &[
        "created_at",
        "updated_at",
        "recovery_attempts",
        "workflow_timeout_ms",
        "workflow_deadline_epoch_ms",
        "started_at_epoch_ms",
        "delay_until_epoch_ms",
    ] {
        m.insert(c.to_string(), i64_col(row, c));
    }
    m.insert("priority".to_string(), i32_col(row, "priority"));
    // `was_forked_from` (BOOLEAN) — included so the flag round-trips across SDKs
    // the way Python's portable format does (Go omits it).
    m.insert(
        "was_forked_from".to_string(),
        bool_col(row, "was_forked_from"),
    );
    m
}

fn export_op_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &[
        "workflow_uuid",
        "function_name",
        "output",
        "error",
        "child_workflow_id",
    ] {
        m.insert(c.to_string(), s_col(row, c));
    }
    m.insert("function_id".to_string(), i32_col(row, "function_id"));
    for &c in &["started_at_epoch_ms", "completed_at_epoch_ms"] {
        m.insert(c.to_string(), i64_col(row, c));
    }
    m
}

fn export_event_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &["workflow_uuid", "key", "value"] {
        m.insert(c.to_string(), s_col(row, c));
    }
    m
}

fn export_history_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &["workflow_uuid", "key", "value"] {
        m.insert(c.to_string(), s_col(row, c));
    }
    m.insert("function_id".to_string(), i32_col(row, "function_id"));
    m
}

fn export_stream_map(row: &sqlx::postgres::PgRow) -> Map<String, Value> {
    let mut m = Map::new();
    for &c in &["workflow_uuid", "key", "value"] {
        m.insert(c.to_string(), s_col(row, c));
    }
    for &c in &["offset", "function_id"] {
        m.insert(c.to_string(), i32_col(row, c));
    }
    m
}

fn row_to_version(row: &sqlx::postgres::PgRow) -> VersionInfo {
    VersionInfo {
        version_id: row.get("version_id"),
        version_name: row.get("version_name"),
        version_timestamp: ms_to_dt(row.get("version_timestamp")),
        created_at: ms_to_dt(row.get("created_at")),
    }
}

fn row_to_queue(row: &sqlx::postgres::PgRow) -> WorkflowQueue {
    // Concurrency columns are `INTEGER` (int4); widen back to the queue's i64/usize.
    let rate_limit = match (
        row.get::<Option<i32>, _>("rate_limit_max"),
        row.get::<Option<f64>, _>("rate_limit_period_sec"),
    ) {
        (Some(limit), Some(period_sec)) => Some(RateLimiter {
            limit: limit as i64,
            period: Duration::from_secs_f64(period_sec),
        }),
        _ => None,
    };
    // Start from the defaults so the fields the table doesn't store
    // (`max_tasks_per_iteration`, `max_polling_interval`) are populated.
    let mut q = WorkflowQueue::new(row.get::<String, _>("name"));
    q.global_concurrency = row.get::<Option<i32>, _>("concurrency").map(|n| n as i64);
    q.worker_concurrency = row
        .get::<Option<i32>, _>("worker_concurrency")
        .map(|n| n as usize);
    q.rate_limit = rate_limit;
    q.priority_enabled = row.get::<bool, _>("priority_enabled");
    q.partitioned = row.get::<bool, _>("partition_queue");
    q.base_polling_interval = Duration::from_secs_f64(row.get::<f64, _>("polling_interval_sec"));
    q
}

/// Encode a schedule's optional context as the stored JSON text (`null` when
/// absent, matching the cross-SDK `context TEXT NOT NULL` column).
fn encode_schedule_context(context: &Option<Value>) -> String {
    context
        .as_ref()
        .and_then(|v| serde_json::to_string(v).ok())
        .unwrap_or_else(|| "null".to_string())
}

/// Decode the stored context text back to an optional value (`null` -> `None`).
fn decode_schedule_context(text: &str) -> Option<Value> {
    match serde_json::from_str::<Value>(text) {
        Ok(Value::Null) => None,
        Ok(v) => Some(v),
        Err(_) => None,
    }
}

/// Map a `workflow_schedules` row to a [`WorkflowSchedule`].
fn row_to_schedule(row: &sqlx::postgres::PgRow) -> Result<WorkflowSchedule> {
    let context: String = row
        .try_get("context")
        .unwrap_or_else(|_| "null".to_string());
    let last_fired: Option<String> = row.try_get("last_fired_at").ok().flatten();
    Ok(WorkflowSchedule {
        schedule_id: row.get("schedule_id"),
        schedule_name: row.get("schedule_name"),
        workflow_name: row.get("workflow_name"),
        schedule: row.get("schedule"),
        status: ScheduleStatus::parse(&row.get::<String, _>("status")),
        context: decode_schedule_context(&context),
        last_fired_at: last_fired
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|t| t.with_timezone(&Utc)),
        automatic_backfill: row.try_get("automatic_backfill").unwrap_or(false),
        cron_timezone: row.try_get("cron_timezone").ok().flatten(),
        queue_name: row.try_get("queue_name").ok().flatten(),
    })
}

/// Map an `operation_outputs` row to a [`StepInfo`], decoding `output` per the
/// row's recorded serialization format.
fn row_to_step(serializer: &Serializer, row: &sqlx::postgres::PgRow) -> Result<StepInfo> {
    let fmt: Option<String> = row.try_get("serialization").ok().flatten();
    let output: Option<String> = row.try_get("output").ok().flatten();
    let error: Option<String> = row.try_get("error").ok().flatten();
    Ok(StepInfo {
        step_id: row.get("function_id"),
        name: row.try_get("function_name").unwrap_or_default(),
        output: serialize::decode_opt(serializer, fmt.as_deref(), output.as_deref())?,
        // A recorded failure is decoded to its human message (the envelope's
        // `message` for a portable row), like `WorkflowStatus.error`.
        error: error.map(|e| serialize::decode_error(fmt.as_deref(), &e).0),
        child_workflow_id: row.try_get("child_workflow_id").ok().flatten(),
        started_at: row
            .try_get::<Option<i64>, _>("started_at_epoch_ms")
            .ok()
            .flatten()
            .map(ms_to_dt),
        completed_at: row
            .try_get::<Option<i64>, _>("completed_at_epoch_ms")
            .ok()
            .flatten()
            .map(ms_to_dt),
    })
}

/// Append the WHERE clause shared by `list_workflows` (Postgres dialect).
fn push_list_filters<'a>(qb: &mut QueryBuilder<'a, Postgres>, filter: &'a ListFilter) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Postgres>| {
        qb.push(sep);
        sep = " AND ";
    };
    // `col = ANY($n)` for a non-empty value set (OR-match).
    let mut push_in = |qb: &mut QueryBuilder<'a, Postgres>, col: &str, vals: &'a [String]| {
        if vals.is_empty() {
            return;
        }
        clause(qb);
        qb.push(col).push(" = ANY(").push_bind(vals).push(")");
    };
    push_in(qb, "workflow_uuid", &filter.workflow_ids);
    push_in(qb, "name", &filter.name);
    push_in(qb, "status", &filter.status);
    push_in(qb, "queue_name", &filter.queue_name);
    push_in(qb, "application_version", &filter.app_version);
    push_in(qb, "executor_id", &filter.executor_ids);
    push_in(qb, "authenticated_user", &filter.authenticated_users);
    push_in(qb, "forked_from", &filter.forked_from);
    push_in(qb, "parent_workflow_id", &filter.parent_workflow_ids);
    if !filter.workflow_id_prefix.is_empty() {
        clause(qb);
        let patterns: Vec<String> = filter
            .workflow_id_prefix
            .iter()
            .map(|p| format!("{p}%"))
            .collect();
        qb.push("workflow_uuid LIKE ANY(").push_bind(patterns);
        qb.push(")");
    }
    if let Some(wf) = filter.was_forked_from {
        // The column is NOT NULL DEFAULT FALSE, so plain equality suffices.
        clause(qb);
        qb.push(if wf {
            "was_forked_from = TRUE"
        } else {
            "was_forked_from = FALSE"
        });
    }
    if let Some(t) = filter.start_time_ms {
        clause(qb);
        qb.push("created_at >= ").push_bind(t);
    }
    if let Some(t) = filter.end_time_ms {
        clause(qb);
        qb.push("created_at <= ").push_bind(t);
    }
    if let Some(t) = filter.completed_after_ms {
        clause(qb);
        qb.push("completed_at >= ").push_bind(t);
    }
    if let Some(t) = filter.completed_before_ms {
        clause(qb);
        qb.push("completed_at <= ").push_bind(t);
    }
    if let Some(t) = filter.dequeued_after_ms {
        clause(qb);
        qb.push("started_at_epoch_ms >= ").push_bind(t);
    }
    if let Some(t) = filter.dequeued_before_ms {
        clause(qb);
        qb.push("started_at_epoch_ms <= ").push_bind(t);
    }
    if let Some(hp) = filter.has_parent {
        clause(qb);
        qb.push(if hp {
            "parent_workflow_id IS NOT NULL"
        } else {
            "parent_workflow_id IS NULL"
        });
    }
    if let Some(attrs) = &filter.attributes {
        // JSONB containment, served by the partial GIN index on `attributes`.
        clause(qb);
        qb.push("attributes @> ")
            .push_bind(serde_json::Value::Object(attrs.clone()));
    }
    if filter.queues_only {
        clause(qb);
        qb.push("queue_name IS NOT NULL");
    }
}

/// Append the WHERE clause for `get_workflow_aggregates` (Postgres dialect).
fn push_agg_filters<'a>(qb: &mut QueryBuilder<'a, Postgres>, q: &'a WorkflowAggregateQuery) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Postgres>| {
        qb.push(sep);
        sep = " AND ";
    };
    let mut push_in = |qb: &mut QueryBuilder<'a, Postgres>, col: &str, vals: &'a [String]| {
        if vals.is_empty() {
            return;
        }
        clause(qb);
        qb.push(col).push(" = ANY(").push_bind(vals).push(")");
    };
    push_in(qb, "status", &q.status);
    push_in(qb, "name", &q.name);
    push_in(qb, "application_version", &q.app_version);
    push_in(qb, "executor_id", &q.executor_ids);
    push_in(qb, "queue_name", &q.queue_names);
    if let Some(prefix) = &q.workflow_id_prefix {
        clause(qb);
        qb.push("workflow_uuid LIKE ")
            .push_bind(format!("{prefix}%"));
    }
    if let Some(t) = q.start_time_ms {
        clause(qb);
        qb.push("created_at >= ").push_bind(t);
    }
    if let Some(t) = q.end_time_ms {
        clause(qb);
        qb.push("created_at <= ").push_bind(t);
    }
    if let Some(t) = q.completed_after_ms {
        clause(qb);
        qb.push("completed_at >= ").push_bind(t);
    }
    if let Some(t) = q.completed_before_ms {
        clause(qb);
        qb.push("completed_at <= ").push_bind(t);
    }
    if let Some(t) = q.dequeued_after_ms {
        clause(qb);
        qb.push("started_at_epoch_ms >= ").push_bind(t);
    }
    if let Some(t) = q.dequeued_before_ms {
        clause(qb);
        qb.push("started_at_epoch_ms <= ").push_bind(t);
    }
}

/// Materialize one `get_workflow_aggregates` group row: read each enabled
/// dimension column (and the computed `time_bucket`) plus the `cnt`.
fn row_to_aggregate(
    row: &sqlx::postgres::PgRow,
    cols: &[(&str, &str)],
    has_bucket: bool,
) -> WorkflowAggregate {
    let mut group: BTreeMap<String, Option<String>> = BTreeMap::new();
    for (key, col) in cols {
        let v: Option<String> = row.try_get(*col).ok().flatten();
        group.insert(key.to_string(), v);
    }
    if has_bucket {
        let b: Option<i64> = row.try_get("time_bucket").ok().flatten();
        group.insert("time_bucket".to_string(), b.map(|x| x.to_string()));
    }
    WorkflowAggregate {
        group,
        count: row.try_get("cnt").ok(),
        min_created_at: row.try_get("min_created_at").ok().flatten(),
        max_queue_wait_ms: row.try_get("max_queue_wait_ms").ok().flatten(),
        max_total_latency_ms: row.try_get("max_total_latency_ms").ok().flatten(),
    }
}

/// Append the WHERE clause for `get_step_aggregates` (Postgres dialect).
fn push_step_agg_filters<'a>(qb: &mut QueryBuilder<'a, Postgres>, q: &'a StepAggregateQuery) {
    let mut sep = " WHERE ";
    let mut clause = |qb: &mut QueryBuilder<'a, Postgres>| {
        qb.push(sep);
        sep = " AND ";
    };
    if !q.status.is_empty() {
        clause(qb);
        qb.push(STEP_STATUS_EXPR)
            .push(" = ANY(")
            .push_bind(&q.status)
            .push(")");
    }
    if !q.function_name.is_empty() {
        clause(qb);
        qb.push("function_name = ANY(")
            .push_bind(&q.function_name)
            .push(")");
    }
    if let Some(prefix) = &q.workflow_id_prefix {
        clause(qb);
        qb.push("workflow_uuid LIKE ")
            .push_bind(format!("{prefix}%"));
    }
    if let Some(t) = q.completed_after_ms {
        clause(qb);
        qb.push("completed_at_epoch_ms >= ").push_bind(t);
    }
    if let Some(t) = q.completed_before_ms {
        clause(qb);
        qb.push("completed_at_epoch_ms <= ").push_bind(t);
    }
}

/// Materialize one `get_step_aggregates` group row.
fn row_to_step_aggregate(
    row: &sqlx::postgres::PgRow,
    dims: &[(&str, &str)],
    has_bucket: bool,
    want_count: bool,
    want_duration: bool,
) -> StepAggregate {
    let mut group: BTreeMap<String, Option<String>> = BTreeMap::new();
    for (key, _) in dims {
        let v: Option<String> = row.try_get(*key).ok().flatten();
        group.insert(key.to_string(), v);
    }
    if has_bucket {
        let b: Option<i64> = row.try_get("time_bucket").ok().flatten();
        group.insert("time_bucket".to_string(), b.map(|x| x.to_string()));
    }
    StepAggregate {
        group,
        count: want_count.then(|| row.try_get("cnt").unwrap_or(0)),
        max_duration_ms: want_duration
            .then(|| row.try_get("max_dur").ok().flatten())
            .flatten(),
    }
}

/// The column list for `list_workflows`, substituting `NULL` for `inputs` /
/// `output` the caller opted out of loading, so those payloads are never read.
fn list_select_cols(filter: &ListFilter) -> std::borrow::Cow<'static, str> {
    if filter.load_input && filter.load_output {
        return std::borrow::Cow::Borrowed(SELECT_COLS);
    }
    let mut cols = SELECT_COLS.to_string();
    if !filter.load_input {
        cols = cols.replacen("inputs,", "NULL AS inputs,", 1);
    }
    if !filter.load_output {
        cols = cols.replacen("output,", "NULL AS output,", 1);
    }
    std::borrow::Cow::Owned(cols)
}
