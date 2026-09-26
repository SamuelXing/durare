//! Shared helpers for the integration tests.
#![allow(dead_code)] // each test binary uses a subset

use durare::{DurableContext, DurableEngine, Result, StateProvider, WorkflowOptions};
#[cfg(feature = "postgres")]
use sqlx::postgres::PgPool;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The `(position, operation)` pairs workflow `id` recorded, in position order.
/// A position nothing was written at is simply absent, which is how a call that
/// claimed one without running shows up.
pub async fn recorded(engine: &DurableEngine, id: &str) -> Result<Vec<(i32, String)>> {
    Ok(engine
        .get_workflow_steps(id)
        .await?
        .into_iter()
        .map(|step| (step.step_id, step.name))
        .collect())
}

/// Runs `body` to completion as workflow `name` under `id` on `provider`, and
/// hands back the engine it ran under so the caller can read what it recorded.
pub async fn run_body<F, Fut>(
    provider: &Arc<dyn StateProvider>,
    name: &str,
    id: &str,
    body: F,
) -> Result<DurableEngine>
where
    F: Fn(DurableContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<i64>> + Send + 'static,
{
    let mut engine = DurableEngine::new(provider.clone()).await?;
    engine.register(name, move |ctx: DurableContext, _: ()| body(ctx));
    engine
        .start::<_, i64>(name, (), WorkflowOptions::with_id(id))
        .await?
        .result()
        .await?;
    Ok(engine)
}

/// A private SQLite database file for one test, as `(url, path)`; `tag` names
/// the test in the file name. Remove it with [`remove_sqlite_files`].
pub fn temp_db_url(tag: &str) -> (String, PathBuf) {
    let path = std::env::temp_dir().join(format!("durare-{tag}-{}.db", uuid::Uuid::new_v4()));
    (format!("sqlite://{}", path.display()), path)
}

/// Best-effort removal of a SQLite database and its `-wal`/`-shm` companions.
pub fn remove_sqlite_files(path: &Path) {
    for ext in ["", "-wal", "-shm"] {
        std::fs::remove_file(format!("{}{ext}", path.display())).ok();
    }
}

/// Swap the database name in a Postgres URL (`…/olddb?params` → `…/newdb?params`).
pub fn with_database(url: &str, dbname: &str) -> String {
    let (head, query) = match url.split_once('?') {
        Some((h, q)) => (h, Some(q)),
        None => (url, None),
    };
    let (prefix, _) = head
        .rsplit_once('/')
        .expect("postgres URL should end in /dbname");
    match query {
        Some(q) => format!("{prefix}/{dbname}?{q}"),
        None => format!("{prefix}/{dbname}"),
    }
}

/// Create a private, per-run database and return `(admin pool, url, dbname)`.
///
/// Tests whose *subject* is global mutable state — the "latest registered
/// version" above all — use this: in the shared test database any
/// concurrently launching engine (or freshly rebuilt test binary) can steal
/// "latest", so such tests get a database of their own instead of racing it.
#[cfg(feature = "postgres")]
pub async fn hermetic_pg_db(base_url: &str, prefix: &str) -> (PgPool, String, String) {
    let dbname = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPool::connect(base_url)
        .await
        .expect("connect to the base database");
    sqlx::raw_sql(&format!("CREATE DATABASE {dbname}"))
        .execute(&admin)
        .await
        .expect("create the hermetic database");
    let url = with_database(base_url, &dbname);
    (admin, url, dbname)
}

/// Best-effort teardown; `FORCE` terminates any connection a pool has not
/// finished closing yet (Postgres 13+).
#[cfg(feature = "postgres")]
pub async fn drop_hermetic_pg_db(admin: &PgPool, dbname: &str) {
    if let Err(e) = sqlx::raw_sql(&format!("DROP DATABASE {dbname} WITH (FORCE)"))
        .execute(admin)
        .await
    {
        eprintln!("hermetic db cleanup: leaving {dbname} behind: {e}");
    }
}
