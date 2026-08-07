//! Data sources: durable transactions on a **separate application database**
//! (`DurableContext::transaction_on`). Not rendered — this module is private;
//! the user-facing docs live on [`PgDataSource`]/[`SqliteDataSource`],
//! `transaction_on`, and the `transactions` guide.
//!
//! The protocol splits durability into two commits with a witness row: the
//! application transaction commits the user's writes plus a
//! `transaction_completion` row atomically, then the ordinary step checkpoint
//! is written to the system database. Recovery replays in layers — checkpoint
//! first, completion row second (the crash window between the commits) — and
//! only runs the body when neither exists. The table shape matches the Go
//! SDK's (`transaction_completion`, `step_id`); Python (`datasource_outputs`)
//! and TypeScript (`function_num`) diverge from Go and from each other, so
//! there is no cross-SDK contract to hold — Go, our parity anchor, wins.

#[cfg(feature = "postgres")]
use crate::error::Error;
use crate::error::Result;
use crate::tx::IsolationLevel;
use async_trait::async_trait;
use std::ops::DerefMut;

/// One row of the `transaction_completion` table: the witness that the
/// application transaction for `(workflow_id, step_id)` committed (output set)
/// or permanently failed (error set).
pub struct CompletionRow {
    pub(crate) output: Option<String>,
    pub(crate) error: Option<String>,
    pub(crate) serialization: Option<String>,
}

pub(crate) mod sealed {
    use super::*;

    /// The per-backend surface of a data source. Sealed: the protocol in
    /// `DurableContext::transaction_on` is written once against this trait, and
    /// the set of backends is closed (like every DBOS SDK's), so it is not
    /// implementable outside the crate.
    #[async_trait]
    pub trait Backend: Send + Sync {
        /// The native connection type handed to a transaction body.
        type Conn: Send;
        /// The in-progress native transaction; derefs to [`Self::Conn`].
        type NativeTx: DerefMut<Target = Self::Conn> + Send;

        /// Begin a transaction on the application pool at `isolation`
        /// (advisory on SQLite, which is always serializable).
        async fn begin(&self, isolation: IsolationLevel, read_only: bool)
            -> Result<Self::NativeTx>;

        /// Commit `tx`.
        async fn commit(&self, tx: Self::NativeTx) -> Result<()>;

        /// Roll back `tx` (best-effort; dropping also rolls back).
        async fn rollback(&self, tx: Self::NativeTx) -> Result<()>;

        /// An identifier of the connection's *current* transaction, used to
        /// detect a body that terminated the surrounding transaction via raw
        /// SQL. `None` when the backend cannot cheaply provide one (SQLite).
        async fn tx_fingerprint(&self, conn: &mut Self::Conn) -> Result<Option<String>>;

        /// Read the completion row for `(workflow_id, step_id)`, if any.
        async fn fetch_completion(
            &self,
            workflow_id: &str,
            step_id: i32,
        ) -> Result<Option<CompletionRow>>;

        /// Insert the completion row inside the caller's transaction. Returns
        /// `false` when a row already exists (another execution committed this
        /// step first — the caller rolls back and replays the canonical row).
        #[allow(clippy::too_many_arguments)]
        async fn insert_completion(
            &self,
            conn: &mut Self::Conn,
            workflow_id: &str,
            step_id: i32,
            output: Option<&str>,
            error: Option<&str>,
            serialization: &str,
        ) -> Result<bool>;

        /// Mirror a permanent failure into the completion table, outside any
        /// transaction (the body's transaction rolled back). Idempotent: an
        /// existing row is left untouched.
        async fn insert_failure(
            &self,
            workflow_id: &str,
            step_id: i32,
            error: &str,
            serialization: &str,
        ) -> Result<()>;

        /// Whether this data source runs on the system database's own pool
        /// (built by a provider's `system_datasource`), enabling the
        /// single-commit fast path: the checkpoint commits with the body's
        /// writes, no completion row needed.
        fn is_system(&self) -> bool;

        /// Insert the step checkpoint into `operation_outputs` on the caller's
        /// transaction — the fast-path equivalent of the completion row plus
        /// the system commit, in one. Only valid on a system data source,
        /// whose pool resolves the unqualified system tables.
        #[allow(clippy::too_many_arguments)]
        async fn insert_checkpoint(
            &self,
            conn: &mut Self::Conn,
            workflow_id: &str,
            step_id: i32,
            name: &str,
            output: &str,
            serialization: &str,
            started_at_ms: i64,
        ) -> Result<()>;
    }
}

/// A handle to an application database that
/// [`transaction_on`](crate::DurableContext::transaction_on) runs durable
/// transactions against.
///
/// Implemented by [`PgDataSource`] and [`SqliteDataSource`]. The trait is
/// **sealed** — the backend set is closed, like every DBOS SDK's — but usable
/// as a bound for helpers generic over the backend. The associated `Conn` type
/// (from the sealed supertrait) is the native `sqlx` connection a transaction
/// body receives: `sqlx::PgConnection` for [`PgDataSource`],
/// `sqlx::SqliteConnection` for [`SqliteDataSource`].
pub trait DataSource: sealed::Backend {}

const COMPLETION_COLUMNS: &str = "workflow_id, step_id, output, error, serialization, created_at";

/// A [`DataSource`] over a Postgres application database.
///
/// Pass it to
/// [`transaction_on`](crate::DurableContext::transaction_on) to run a durable
/// transaction whose body receives the native `&mut sqlx::PgConnection` —
/// existing queries, `sqlx` macros, and data-access helpers work unchanged.
///
/// Constructing one ensures the completion table exists:
/// `"<schema>".transaction_completion` (schema `dbos` by default), creating
/// the schema and table if missing — so the pool's role needs `CREATE`
/// privileges once, or create the table ahead of time from your own
/// migrations:
///
/// ```sql
/// CREATE SCHEMA IF NOT EXISTS "dbos";
/// CREATE TABLE IF NOT EXISTS "dbos".transaction_completion (
///     workflow_id   TEXT NOT NULL,
///     step_id       INT  NOT NULL,
///     output        TEXT,
///     error         TEXT,
///     serialization TEXT,
///     created_at    BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
///     PRIMARY KEY (workflow_id, step_id)
/// );
/// ```
///
/// The shape matches the Go DBOS SDK's, so a Go worker sharing this
/// application database reads the same rows.
#[cfg(feature = "postgres")]
#[derive(Clone)]
pub struct PgDataSource {
    pool: sqlx::PgPool,
    table: String,
    /// True when built by `PostgresProvider::system_datasource` — the pool is
    /// the system database's own, so `transaction_on` takes the single-commit
    /// fast path and the completion table is never used (or created).
    system: bool,
}

#[cfg(feature = "postgres")]
impl PgDataSource {
    /// Create a data source over `pool`, keeping the completion table under
    /// the default `dbos` schema.
    pub async fn new(pool: sqlx::PgPool) -> Result<Self> {
        Self::with_schema(pool, "dbos").await
    }

    /// Like [`new`](Self::new), with the completion table under `schema`
    /// instead of `dbos`. The name must be a plain identifier
    /// (`[A-Za-z_][A-Za-z0-9_]*`).
    pub async fn with_schema(pool: sqlx::PgPool, schema: &str) -> Result<Self> {
        if !crate::postgres::is_plain_identifier(schema) {
            return Err(Error::app(format!(
                "invalid Postgres schema name {schema:?}: must match [A-Za-z_][A-Za-z0-9_]*"
            )));
        }
        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""))
            .execute(&pool)
            .await?;
        let table = format!("\"{schema}\".transaction_completion");
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                 workflow_id TEXT NOT NULL,
                 step_id INT NOT NULL,
                 output TEXT,
                 error TEXT,
                 serialization TEXT,
                 created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
                 PRIMARY KEY (workflow_id, step_id)
             )"
        ))
        .execute(&pool)
        .await?;
        Ok(Self {
            pool,
            table,
            system: false,
        })
    }

    /// A data source over the system database's own pool (see
    /// `PostgresProvider::system_datasource`). Creates nothing: the fast path
    /// never touches a completion table.
    pub(crate) fn system(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            table: "transaction_completion".to_string(),
            system: true,
        }
    }

    /// The pool this data source runs on.
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }
}

#[cfg(feature = "postgres")]
impl DataSource for PgDataSource {}

#[cfg(feature = "postgres")]
#[async_trait]
impl sealed::Backend for PgDataSource {
    type Conn = sqlx::PgConnection;
    type NativeTx = sqlx::Transaction<'static, sqlx::Postgres>;

    async fn begin(&self, isolation: IsolationLevel, read_only: bool) -> Result<Self::NativeTx> {
        let mut tx = self.pool.begin().await?;
        // `SET TRANSACTION` must come before any query in the tx.
        if isolation != IsolationLevel::ReadCommitted || read_only {
            let mut stmt = format!("SET TRANSACTION ISOLATION LEVEL {}", isolation.pg_sql());
            if read_only {
                stmt.push_str(" READ ONLY");
            }
            sqlx::query(&stmt).execute(&mut *tx).await?;
        }
        Ok(tx)
    }

    async fn commit(&self, tx: Self::NativeTx) -> Result<()> {
        tx.commit().await?;
        Ok(())
    }

    async fn rollback(&self, tx: Self::NativeTx) -> Result<()> {
        tx.rollback().await?;
        Ok(())
    }

    async fn tx_fingerprint(&self, conn: &mut Self::Conn) -> Result<Option<String>> {
        // txid_current() is stable for the life of one transaction, so a body
        // that ran COMMIT/ROLLBACK via raw SQL lands in a new transaction and
        // the fingerprint changes.
        let id: i64 = sqlx::query_scalar("SELECT txid_current()::bigint")
            .fetch_one(conn)
            .await?;
        Ok(Some(id.to_string()))
    }

    async fn fetch_completion(
        &self,
        workflow_id: &str,
        step_id: i32,
    ) -> Result<Option<CompletionRow>> {
        use sqlx::Row as _;
        let row = sqlx::query(&format!(
            "SELECT output, error, serialization FROM {} WHERE workflow_id = $1 AND step_id = $2",
            self.table
        ))
        .bind(workflow_id)
        .bind(step_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| CompletionRow {
            output: r.get("output"),
            error: r.get("error"),
            serialization: r.get("serialization"),
        }))
    }

    async fn insert_completion(
        &self,
        conn: &mut Self::Conn,
        workflow_id: &str,
        step_id: i32,
        output: Option<&str>,
        error: Option<&str>,
        serialization: &str,
    ) -> Result<bool> {
        let res = sqlx::query(&format!(
            "INSERT INTO {} ({COMPLETION_COLUMNS}) VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (workflow_id, step_id) DO NOTHING",
            self.table
        ))
        .bind(workflow_id)
        .bind(step_id)
        .bind(output)
        .bind(error)
        .bind(serialization)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(conn)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    async fn insert_failure(
        &self,
        workflow_id: &str,
        step_id: i32,
        error: &str,
        serialization: &str,
    ) -> Result<()> {
        sqlx::query(&format!(
            "INSERT INTO {} ({COMPLETION_COLUMNS}) VALUES ($1, $2, NULL, $3, $4, $5)
             ON CONFLICT (workflow_id, step_id) DO NOTHING",
            self.table
        ))
        .bind(workflow_id)
        .bind(step_id)
        .bind(error)
        .bind(serialization)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    fn is_system(&self) -> bool {
        self.system
    }

    async fn insert_checkpoint(
        &self,
        conn: &mut Self::Conn,
        workflow_id: &str,
        step_id: i32,
        name: &str,
        output: &str,
        serialization: &str,
        started_at_ms: i64,
    ) -> Result<()> {
        // Unqualified: the system pool's per-connection search_path resolves
        // the system schema, same as the provider's own queries.
        sqlx::query(
            "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, function_name, output, serialization,
                  started_at_epoch_ms, completed_at_epoch_ms)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(step_id)
        .bind(name)
        .bind(output)
        .bind(serialization)
        .bind(started_at_ms)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(conn)
        .await?;
        Ok(())
    }
}

/// A [`DataSource`] over a SQLite application database.
///
/// Pass it to
/// [`transaction_on`](crate::DurableContext::transaction_on) to run a durable
/// transaction whose body receives the native `&mut sqlx::SqliteConnection`.
///
/// Constructing one ensures the (unqualified) `transaction_completion` table
/// exists, with the same columns as [`PgDataSource`]'s.
///
/// SQLite runs every transaction serializably, so the isolation level in
/// [`TransactionOptions`](crate::TransactionOptions) is advisory here;
/// busy/locked contention is retried like a serialization conflict.
#[cfg(feature = "sqlite")]
#[derive(Clone)]
pub struct SqliteDataSource {
    pool: sqlx::SqlitePool,
    /// True when built by `SqliteProvider::system_datasource` — see
    /// [`PgDataSource`]'s `system` field.
    system: bool,
}

#[cfg(feature = "sqlite")]
impl SqliteDataSource {
    /// Create a data source over `pool`, creating the completion table if
    /// missing.
    pub async fn new(pool: sqlx::SqlitePool) -> Result<Self> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS transaction_completion (
                 workflow_id TEXT NOT NULL,
                 step_id INTEGER NOT NULL,
                 output TEXT,
                 error TEXT,
                 serialization TEXT,
                 created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s','now') AS INTEGER) * 1000),
                 PRIMARY KEY (workflow_id, step_id)
             )",
        )
        .execute(&pool)
        .await?;
        Ok(Self {
            pool,
            system: false,
        })
    }

    /// A data source over the system database's own pool (see
    /// `SqliteProvider::system_datasource`). Creates nothing.
    pub(crate) fn system(pool: sqlx::SqlitePool) -> Self {
        Self { pool, system: true }
    }

    /// The pool this data source runs on.
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }
}

#[cfg(feature = "sqlite")]
impl DataSource for SqliteDataSource {}

#[cfg(feature = "sqlite")]
#[async_trait]
impl sealed::Backend for SqliteDataSource {
    type Conn = sqlx::SqliteConnection;
    type NativeTx = sqlx::Transaction<'static, sqlx::Sqlite>;

    async fn begin(&self, _isolation: IsolationLevel, _read_only: bool) -> Result<Self::NativeTx> {
        // SQLite is always serializable; isolation/read-only are advisory.
        Ok(self.pool.begin().await?)
    }

    async fn commit(&self, tx: Self::NativeTx) -> Result<()> {
        tx.commit().await?;
        Ok(())
    }

    async fn rollback(&self, tx: Self::NativeTx) -> Result<()> {
        tx.rollback().await?;
        Ok(())
    }

    async fn tx_fingerprint(&self, _conn: &mut Self::Conn) -> Result<Option<String>> {
        // No cheap transaction identifier on SQLite; the guard is documented
        // as Postgres-only.
        Ok(None)
    }

    async fn fetch_completion(
        &self,
        workflow_id: &str,
        step_id: i32,
    ) -> Result<Option<CompletionRow>> {
        use sqlx::Row as _;
        let row = sqlx::query(
            "SELECT output, error, serialization FROM transaction_completion
             WHERE workflow_id = ? AND step_id = ?",
        )
        .bind(workflow_id)
        .bind(step_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| CompletionRow {
            output: r.get("output"),
            error: r.get("error"),
            serialization: r.get("serialization"),
        }))
    }

    async fn insert_completion(
        &self,
        conn: &mut Self::Conn,
        workflow_id: &str,
        step_id: i32,
        output: Option<&str>,
        error: Option<&str>,
        serialization: &str,
    ) -> Result<bool> {
        let res = sqlx::query(&format!(
            "INSERT OR IGNORE INTO transaction_completion ({COMPLETION_COLUMNS})
             VALUES (?, ?, ?, ?, ?, ?)"
        ))
        .bind(workflow_id)
        .bind(step_id)
        .bind(output)
        .bind(error)
        .bind(serialization)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(conn)
        .await?;
        Ok(res.rows_affected() == 1)
    }

    async fn insert_failure(
        &self,
        workflow_id: &str,
        step_id: i32,
        error: &str,
        serialization: &str,
    ) -> Result<()> {
        sqlx::query(&format!(
            "INSERT OR IGNORE INTO transaction_completion ({COMPLETION_COLUMNS})
             VALUES (?, ?, NULL, ?, ?, ?)"
        ))
        .bind(workflow_id)
        .bind(step_id)
        .bind(error)
        .bind(serialization)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    fn is_system(&self) -> bool {
        self.system
    }

    async fn insert_checkpoint(
        &self,
        conn: &mut Self::Conn,
        workflow_id: &str,
        step_id: i32,
        name: &str,
        output: &str,
        serialization: &str,
        started_at_ms: i64,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO operation_outputs
                 (workflow_uuid, function_id, function_name, output, serialization,
                  started_at_epoch_ms, completed_at_epoch_ms)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (workflow_uuid, function_id) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(step_id)
        .bind(name)
        .bind(output)
        .bind(serialization)
        .bind(started_at_ms)
        .bind(chrono::Utc::now().timestamp_millis())
        .execute(conn)
        .await?;
        Ok(())
    }
}
