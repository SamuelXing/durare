//! The schema version lives in the shared `dbos_migrations` ledger: caught up
//! when behind, left alone when ahead, adopted once from sqlx's legacy ledger.
use durare::{Result, StateProvider};
mod common;

/// The newest migration this build embeds, on both dialects.
const LATEST: i64 = 41;

#[cfg(feature = "postgres")]
#[tokio::test]
async fn pg_schema_version_lives_in_the_shared_ledger() -> Result<()> {
    use durare::PostgresProvider;
    use sqlx::PgPool;

    let Ok(base) = std::env::var("DATABASE_URL") else {
        return Ok(());
    };
    let (admin, url, name) = common::hermetic_pg_db(&base, "schema_ledger").await;
    let result = async {
        let provider = PostgresProvider::connect(&url).await?;
        provider.init().await?;
        let pool = PgPool::connect(&url).await?;
        async fn ledger_rows(pool: &PgPool) -> sqlx::Result<Vec<i64>> {
            sqlx::query_scalar("SELECT version FROM dbos.dbos_migrations")
                .fetch_all(pool)
                .await
        }

        // One row, at the newest version; sqlx's ledger is no longer written.
        assert_eq!(ledger_rows(&pool).await?, vec![LATEST]);
        let legacy: bool =
            sqlx::query_scalar("SELECT to_regclass('dbos._sqlx_migrations') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        assert!(!legacy, "no sqlx ledger expected");
        provider.ping().await?;

        // A second initialisation changes nothing.
        provider.init().await?;
        assert_eq!(ledger_rows(&pool).await?, vec![LATEST]);

        // A database behind this binary is reported, then caught up: undo
        // migrations 39 to 41 and roll the ledger back to 38.
        sqlx::raw_sql(
            "DROP TRIGGER dbos_streams_trigger ON dbos.streams;
             DROP FUNCTION dbos.streams_function();
             ALTER TABLE dbos.workflow_status DROP COLUMN attributes;
             ALTER TABLE dbos.workflow_status DROP COLUMN schedule_name;
             UPDATE dbos.dbos_migrations SET version = 38",
        )
        .execute(&pool)
        .await?;
        let behind = provider.ping().await.expect_err("schema at 38");
        assert!(behind.to_string().contains("behind"), "{behind}");
        provider.init().await?;
        assert_eq!(ledger_rows(&pool).await?, vec![LATEST]);
        let attributes: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'dbos' AND table_name = 'workflow_status'
               AND column_name = 'attributes')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(attributes, "migration 40 re-applied");
        let schedule_name: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = 'dbos'
               AND indexname = 'idx_workflow_status_schedule_name')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(schedule_name, "migration 41 re-applied");

        // A database a newer peer moved ahead is healthy and left alone.
        sqlx::query("UPDATE dbos.dbos_migrations SET version = 125")
            .execute(&pool)
            .await?;
        provider.init().await?;
        provider.ping().await?;
        assert_eq!(ledger_rows(&pool).await?, vec![125]);

        // A database durare migrated before it kept the shared ledger: only
        // sqlx's table records the version. It is adopted, and nothing is
        // re-applied (migration 1 would fail on the existing tables).
        // Only the columns the adoption reads.
        sqlx::raw_sql(&format!(
            "DROP TABLE dbos.dbos_migrations;
             CREATE TABLE dbos._sqlx_migrations (version BIGINT PRIMARY KEY, success BOOLEAN NOT NULL);
             INSERT INTO dbos._sqlx_migrations VALUES ({LATEST}, TRUE)"
        ))
        .execute(&pool)
        .await?;
        provider.ping().await?;
        provider.init().await?;
        assert_eq!(ledger_rows(&pool).await?, vec![LATEST]);
        let legacy: bool =
            sqlx::query_scalar("SELECT to_regclass('dbos._sqlx_migrations') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        assert!(legacy, "the legacy ledger is left in place");

        // Tables with no ledger at all are refused rather than re-created.
        sqlx::raw_sql("DROP TABLE dbos.dbos_migrations; DROP TABLE dbos._sqlx_migrations")
            .execute(&pool)
            .await?;
        let refused = provider.init().await.expect_err("unrecorded schema");
        assert!(refused.to_string().contains("records no migration version"), "{refused}");
        pool.close().await;
        Ok(())
    }
    .await;
    common::drop_hermetic_pg_db(&admin, &name).await;
    result
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_schema_version_lives_in_the_shared_ledger() -> Result<()> {
    use durare::SqliteProvider;
    use sqlx::SqlitePool;

    let (url, path) = common::temp_db_url("schema_ledger");
    let provider = SqliteProvider::connect(&url).await?;
    provider.init().await?;
    let pool = SqlitePool::connect(&url).await?;
    async fn ledger_rows(pool: &SqlitePool) -> sqlx::Result<Vec<i64>> {
        sqlx::query_scalar("SELECT version FROM dbos_migrations")
            .fetch_all(pool)
            .await
    }
    assert_eq!(ledger_rows(&pool).await?, vec![LATEST]);

    // Behind, then caught up: undo migrations 40 and 41 and roll the ledger
    // back.
    sqlx::raw_sql(
        "DROP INDEX idx_workflow_status_schedule_name;
         ALTER TABLE workflow_status DROP COLUMN schedule_name;
         ALTER TABLE workflow_status DROP COLUMN attributes;
         UPDATE dbos_migrations SET version = 39",
    )
    .execute(&pool)
    .await?;
    provider.init().await?;
    assert_eq!(ledger_rows(&pool).await?, vec![LATEST]);

    // Ahead: left alone.
    sqlx::query("UPDATE dbos_migrations SET version = 125")
        .execute(&pool)
        .await?;
    provider.init().await?;
    assert_eq!(ledger_rows(&pool).await?, vec![125]);

    // Legacy sqlx ledger only: adopted, nothing re-applied.
    sqlx::raw_sql(&format!(
        "DROP TABLE dbos_migrations;
         CREATE TABLE _sqlx_migrations (version BIGINT PRIMARY KEY, success BOOLEAN NOT NULL);
         INSERT INTO _sqlx_migrations VALUES ({LATEST}, 1)"
    ))
    .execute(&pool)
    .await?;
    provider.init().await?;
    assert_eq!(ledger_rows(&pool).await?, vec![LATEST]);

    // Tables with no ledger at all are refused rather than re-created.
    sqlx::raw_sql("DROP TABLE dbos_migrations; DROP TABLE _sqlx_migrations")
        .execute(&pool)
        .await?;
    let refused = provider.init().await.expect_err("unrecorded schema");
    assert!(
        refused.to_string().contains("records no migration version"),
        "{refused}"
    );
    pool.close().await;
    drop(provider);
    common::remove_sqlite_files(&path);
    Ok(())
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_legacy_adoption_preserves_data_and_adds_peer_defaults() -> Result<()> {
    let owned_pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;
    let provider = durare::SqliteProvider::from_pool(owned_pool.clone());
    let pool = &owned_pool;
    sqlx::migrate!("./migrations/sqlite").run(pool).await?;
    sqlx::raw_sql("INSERT INTO workflow_status (workflow_uuid, name, status, created_at, updated_at)
        VALUES ('parent', 'wf', 'PENDING', 123, 456);
        INSERT INTO notifications (message_uuid, destination_uuid, topic, message, created_at_epoch_ms)
        VALUES ('message', 'parent', 'topic', 'payload', 789);
        CREATE VIEW workflow_names AS SELECT name FROM workflow_status;
        CREATE TABLE audit (id TEXT);
        CREATE TRIGGER version_audit AFTER INSERT ON application_versions
        BEGIN INSERT INTO audit VALUES (NEW.version_id); END;")
        .execute(pool).await?;
    provider.init().await?;
    provider.init().await?;
    let times: (i64, i64) = sqlx::query_as(
        "SELECT created_at, updated_at FROM workflow_status WHERE workflow_uuid='parent'",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(times, (123, 456));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM notifications")
            .fetch_one(pool)
            .await?,
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT name FROM workflow_names")
            .fetch_one(pool)
            .await?,
        "wf"
    );
    // Python's version registration omits these timestamps, relying on defaults.
    sqlx::query(
        "INSERT INTO application_versions (version_id, version_name) VALUES ('peer', 'peer')",
    )
    .execute(pool)
    .await?;
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT id FROM audit")
            .fetch_one(pool)
            .await?,
        "peer"
    );
    sqlx::query("INSERT INTO workflow_status (workflow_uuid, name, status) VALUES ('peer', 'wf', 'PENDING')").execute(pool).await?;
    sqlx::query("INSERT INTO notifications (message_uuid, destination_uuid, message) VALUES ('peer', 'peer', 'payload')").execute(pool).await?;
    sqlx::query("INSERT INTO queues (queue_id, name) VALUES ('peer', 'peer')")
        .execute(pool)
        .await?;
    assert!(sqlx::query("INSERT INTO notifications (message_uuid, destination_uuid, message) VALUES ('bad', 'missing', 'payload')").execute(pool).await.is_err());
    assert!(sqlx::query(
        "INSERT INTO application_versions (version_id, version_name) VALUES ('duplicate', 'peer')"
    )
    .execute(pool)
    .await
    .is_err());
    assert!(sqlx::query("PRAGMA foreign_key_check")
        .fetch_optional(pool)
        .await?
        .is_none());
    let ts: i64 = sqlx::query_scalar(
        "SELECT version_timestamp FROM application_versions WHERE version_id='peer'",
    )
    .fetch_one(pool)
    .await?;
    assert!(ts > 0);
    Ok(())
}
