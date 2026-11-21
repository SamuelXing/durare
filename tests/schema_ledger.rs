//! The schema version lives in the shared `dbos_migrations` ledger: caught up
//! when behind, left alone when ahead, adopted once from sqlx's legacy ledger.
use durare::{Result, StateProvider};
mod common;

/// The newest migration this build embeds. Both dialects end at 109; SQLite
/// skips 43, 44 and 105, which touch triggers and a function it never had, as
/// the Go and Python SDKs' SQLite chains do. Versions 48 to 99 are unused.
const PG_LATEST: i64 = 109;
const SQLITE_LATEST: i64 = 109;

/// The tables migrations 100 to 104 add `application_name` to.
const APPLICATION_NAME_TABLES: [&str; 5] = [
    "application_versions",
    "operation_outputs",
    "queues",
    "workflow_schedules",
    "workflow_status",
];
/// The keys migrations 106 and 107 put on `application_versions`.
const VERSION_KEYS: [&str; 2] = [
    "uq_application_versions_owner_version",
    "uq_application_versions_unclaimed_version",
];

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
        // The triggers migrations 1 and 39 create and migrations 43 and 44 drop.
        async fn trigger_exists(pool: &PgPool, name: &str) -> sqlx::Result<bool> {
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = $1)")
                .bind(name)
                .fetch_one(pool)
                .await
        }

        // One row, at the newest version; sqlx's ledger is no longer written.
        assert_eq!(ledger_rows(&pool).await?, vec![PG_LATEST]);
        let legacy: bool =
            sqlx::query_scalar("SELECT to_regclass('dbos._sqlx_migrations') IS NOT NULL")
                .fetch_one(&pool)
                .await?;
        assert!(!legacy, "no sqlx ledger expected");
        provider.ping().await?;

        // Migration 44 dropped the events trigger migration 1 created; a
        // rollback below never reaches 1, so this is the one place to see it.
        assert!(
            !trigger_exists(&pool, "dbos_workflow_events_trigger").await?,
            "migration 44 dropped the events trigger"
        );
        assert!(
            trigger_exists(&pool, "dbos_notifications_trigger").await?,
            "the notifications trigger stays"
        );

        // Migrations 45 to 47 leave exactly one partition dequeue index.
        let partition_indexes: Vec<String> = sqlx::query_scalar(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'dbos'
             AND indexname LIKE 'idx_workflow_status_partition_dequeue%'",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            partition_indexes,
            ["idx_workflow_status_partition_dequeue_v2"]
        );

        let owned: Vec<String> = sqlx::query_scalar(
            "SELECT table_name::text FROM information_schema.columns
             WHERE table_schema = 'dbos' AND column_name = 'application_name'
             ORDER BY table_name",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(owned, APPLICATION_NAME_TABLES);
        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT indexname::text FROM pg_indexes WHERE schemaname = 'dbos'
             AND indexname LIKE 'uq_application_versions_%' ORDER BY indexname",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(keys, VERSION_KEYS);

        // Migration 105 leaves one enqueue_workflow, taking a trailing
        // application_name: omitted, the run is unclaimed; given, it is owned.
        let arities: Vec<i16> = sqlx::query_scalar(
            "SELECT p.pronargs FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'dbos' AND p.proname = 'enqueue_workflow'",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(arities, [17]);
        sqlx::raw_sql(
            "SELECT dbos.enqueue_workflow('wf', 'q', workflow_id => 'unclaimed');
             SELECT dbos.enqueue_workflow('wf', 'q', workflow_id => 'owned', application_name => 'app')",
        )
        .execute(&pool)
        .await?;
        let owners: Vec<(String, Option<String>)> = sqlx::query_as(
            "SELECT workflow_uuid, application_name FROM dbos.workflow_status ORDER BY workflow_uuid",
        )
        .fetch_all(&pool)
        .await?;
        assert_eq!(
            owners,
            [
                ("owned".to_owned(), Some("app".to_owned())),
                ("unclaimed".to_owned(), None)
            ]
        );
        sqlx::query("DELETE FROM dbos.workflow_status")
            .execute(&pool)
            .await?;

        // A second initialisation changes nothing.
        provider.init().await?;
        assert_eq!(ledger_rows(&pool).await?, vec![PG_LATEST]);

        // A database behind this binary is reported, then caught up: undo
        // migrations 40 to 42 (43 left nothing to undo) and roll the ledger
        // back to 38, so 39 recreates the trigger and 43 drops it again.
        sqlx::raw_sql(
            "ALTER TABLE dbos.workflow_status DROP COLUMN attributes;
             ALTER TABLE dbos.workflow_status DROP COLUMN schedule_name;
             ALTER TABLE dbos.workflow_status DROP COLUMN debounce_deadline_epoch_ms;
             ALTER TABLE dbos.workflow_status DROP COLUMN is_debounced;
             UPDATE dbos.dbos_migrations SET version = 38",
        )
        .execute(&pool)
        .await?;
        let behind = provider.ping().await.expect_err("schema at 38");
        assert!(behind.to_string().contains("behind"), "{behind}");
        provider.init().await?;
        assert_eq!(ledger_rows(&pool).await?, vec![PG_LATEST]);
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
        let debounced: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns
             WHERE table_schema = 'dbos' AND table_name = 'workflow_status'
               AND column_name = 'is_debounced')",
        )
        .fetch_one(&pool)
        .await?;
        assert!(debounced, "migration 42 re-applied");
        assert!(
            !trigger_exists(&pool, "dbos_streams_trigger").await?,
            "migration 43 re-applied"
        );


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
             INSERT INTO dbos._sqlx_migrations VALUES ({PG_LATEST}, TRUE)"
        ))
        .execute(&pool)
        .await?;
        provider.ping().await?;
        provider.init().await?;
        assert_eq!(ledger_rows(&pool).await?, vec![PG_LATEST]);
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
    assert_eq!(ledger_rows(&pool).await?, vec![SQLITE_LATEST]);
    // Migrations 45 to 47 leave exactly one partition dequeue index.
    let partition_indexes: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'index'
         AND name LIKE 'idx_workflow_status_partition_dequeue%'",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        partition_indexes,
        ["idx_workflow_status_partition_dequeue_v2"]
    );

    let owned: Vec<String> = sqlx::query_scalar(
        "SELECT m.name FROM sqlite_master m JOIN pragma_table_info(m.name) c
         ON c.name = 'application_name' WHERE m.type = 'table' ORDER BY m.name",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(owned, APPLICATION_NAME_TABLES);
    let keys: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'index'
         AND name LIKE 'uq_application_versions_%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(keys, VERSION_KEYS);

    // Behind, then caught up: a database left at 47 crosses the unused
    // versions to the newest one.
    let behind = SqlitePool::connect("sqlite::memory:").await?;
    let embedded = sqlx::migrate!("./migrations/sqlite");
    for m in embedded.iter().filter(|m| m.version <= 47) {
        sqlx::raw_sql(&m.sql).execute(&behind).await?;
    }
    sqlx::raw_sql(
        "CREATE TABLE dbos_migrations (version BIGINT NOT NULL PRIMARY KEY);
         INSERT INTO dbos_migrations VALUES (47)",
    )
    .execute(&behind)
    .await?;
    SqliteProvider::from_pool(behind.clone()).init().await?;
    assert_eq!(ledger_rows(&behind).await?, vec![SQLITE_LATEST]);
    behind.close().await;

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
         INSERT INTO _sqlx_migrations VALUES ({SQLITE_LATEST}, 1)"
    ))
    .execute(&pool)
    .await?;
    provider.init().await?;
    assert_eq!(ledger_rows(&pool).await?, vec![SQLITE_LATEST]);

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
