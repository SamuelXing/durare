use crate::error::Result;
use crate::provider::{StateProvider, WorkflowRecord, STATUS_COMPLETED, STATUS_FAILED, STATUS_PENDING};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use std::time::Duration;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS durust_workflows (
    id          TEXT PRIMARY KEY,
    name        TEXT        NOT NULL,
    input       JSONB       NOT NULL,
    output      JSONB,
    status      TEXT        NOT NULL DEFAULT 'PENDING',
    error       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS durust_steps (
    workflow_id TEXT        NOT NULL REFERENCES durust_workflows(id),
    seq         INT         NOT NULL,
    name        TEXT        NOT NULL,
    result      JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, seq)
);

CREATE TABLE IF NOT EXISTS durust_timers (
    workflow_id TEXT        NOT NULL REFERENCES durust_workflows(id),
    seq         INT         NOT NULL,
    wake_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workflow_id, seq)
);

CREATE INDEX IF NOT EXISTS durust_workflows_status_idx
    ON durust_workflows(status);
"#;

/// Postgres-backed [`StateProvider`], built on sqlx.
pub struct PostgresProvider {
    pool: PgPool,
}

impl PostgresProvider {
    /// Connect to Postgres using a standard connection URL, e.g.
    /// `postgres://user:pass@localhost:5432/durust`.
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    /// Build a provider from an existing pool (useful if your app already owns one).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl StateProvider for PostgresProvider {
    async fn init(&self) -> Result<()> {
        // The schema is a multi-statement batch; `execute` on the pool runs it.
        sqlx::raw_sql(SCHEMA).execute(&self.pool).await?;
        Ok(())
    }

    async fn start_workflow(&self, id: &str, name: &str, input: &Value) -> Result<WorkflowRecord> {
        // Idempotent create: an existing id is left untouched.
        sqlx::query(
            "INSERT INTO durust_workflows (id, name, input) VALUES ($1, $2, $3::jsonb)
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(id)
        .bind(name)
        .bind(input.clone())
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT id, name, input, status FROM durust_workflows WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await?;

        Ok(WorkflowRecord {
            id: row.get("id"),
            name: row.get("name"),
            input: row.get("input"),
            status: row.get("status"),
        })
    }

    async fn get_step_result(&self, workflow_id: &str, seq: i32) -> Result<Option<Value>> {
        let row = sqlx::query(
            "SELECT result FROM durust_steps WHERE workflow_id = $1 AND seq = $2",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.get::<Value, _>("result")))
    }

    async fn record_step_result(
        &self,
        workflow_id: &str,
        seq: i32,
        name: &str,
        value: Value,
    ) -> Result<Value> {
        sqlx::query(
            "INSERT INTO durust_steps (workflow_id, seq, name, result) VALUES ($1, $2, $3, $4::jsonb)
             ON CONFLICT (workflow_id, seq) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(name)
        .bind(value)
        .execute(&self.pool)
        .await?;

        // Read back the canonical value (ours, or a racing writer's that won).
        let row = sqlx::query(
            "SELECT result FROM durust_steps WHERE workflow_id = $1 AND seq = $2",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<Value, _>("result"))
    }

    async fn get_or_set_wakeup(
        &self,
        workflow_id: &str,
        seq: i32,
        dur: Duration,
    ) -> Result<DateTime<Utc>> {
        let proposed: DateTime<Utc> =
            Utc::now() + chrono::Duration::from_std(dur).unwrap_or_else(|_| chrono::Duration::zero());

        sqlx::query(
            "INSERT INTO durust_timers (workflow_id, seq, wake_at) VALUES ($1, $2, $3)
             ON CONFLICT (workflow_id, seq) DO NOTHING",
        )
        .bind(workflow_id)
        .bind(seq)
        .bind(proposed)
        .execute(&self.pool)
        .await?;

        let row = sqlx::query(
            "SELECT wake_at FROM durust_timers WHERE workflow_id = $1 AND seq = $2",
        )
        .bind(workflow_id)
        .bind(seq)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.get::<DateTime<Utc>, _>("wake_at"))
    }

    async fn complete_workflow(&self, id: &str, output: &Value) -> Result<()> {
        sqlx::query(
            "UPDATE durust_workflows
             SET status = $2, output = $3::jsonb, updated_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(STATUS_COMPLETED)
        .bind(output.clone())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn fail_workflow(&self, id: &str, error: &str) -> Result<()> {
        sqlx::query(
            "UPDATE durust_workflows
             SET status = $2, error = $3, updated_at = now()
             WHERE id = $1",
        )
        .bind(id)
        .bind(STATUS_FAILED)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_incomplete_workflows(&self) -> Result<Vec<WorkflowRecord>> {
        let rows = sqlx::query(
            "SELECT id, name, input, status FROM durust_workflows WHERE status = $1",
        )
        .bind(STATUS_PENDING)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| WorkflowRecord {
                id: row.get("id"),
                name: row.get("name"),
                input: row.get("input"),
                status: row.get("status"),
            })
            .collect())
    }
}
