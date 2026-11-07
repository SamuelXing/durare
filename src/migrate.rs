//! The system schema's migration ledger, shared with the other DBOS SDKs.
//!
//! Every DBOS SDK records the schema version of a system database in one row
//! of a `dbos_migrations` table, and from index 100 on defines the same
//! migration at the same index. A process reads the row, applies what is newer
//! than it, and moves the row forward, so processes written in different
//! languages share one schema and each picks up where the last one left off.
//!
//! durare used to keep sqlx's own ledger, `_sqlx_migrations`, which no other
//! SDK reads. A database durare created that way is adopted once: the newest
//! version that ledger holds becomes the shared row, and the old table is left
//! in place.

use crate::error::{Error, Result};
use sqlx::migrate::{Migration, Migrator};

/// The shared ledger: one row, `version BIGINT`.
pub(crate) const LEDGER: &str = "dbos_migrations";

/// sqlx's ledger from before durare kept the shared one.
pub(crate) const LEGACY_LEDGER: &str = "_sqlx_migrations";

/// The refusal for a schema that has tables but no ledger row: migrating it
/// from scratch would fail on the first table, less clearly.
pub(crate) const UNRECORDED_SCHEMA: &str = "dbos schema exists but records no migration version: \
     it was created by something that kept no `dbos_migrations` ledger, so this \
     binary cannot tell what it holds";

/// Every migration is a plain up migration at its own version: the runner
/// records one version per file, so a reversible pair or two files at one
/// version (the layout the Go SDK uses for one of its migrations) would be
/// misrecorded. Checked by each backend's unit tests.
#[cfg(test)]
pub(crate) fn check_embedded(migrator: &Migrator) {
    let mut versions = std::collections::HashSet::new();
    for m in migrator.iter() {
        assert!(
            matches!(m.migration_type, sqlx::migrate::MigrationType::Simple),
            "migration {} is not a plain up migration",
            m.version
        );
        assert!(
            versions.insert(m.version),
            "migration {} is defined twice",
            m.version
        );
    }
}

/// The newest version `migrator` embeds.
pub(crate) fn latest(migrator: &Migrator) -> i64 {
    migrator.iter().map(|m| m.version).max().unwrap_or(0)
}

/// The migrations newer than `applied`, in order.
pub(crate) fn pending(migrator: &Migrator, applied: i64) -> impl Iterator<Item = &Migration> {
    migrator.iter().filter(move |m| m.version > applied)
}

/// Whether a database at `recorded` serves a binary that embeds `latest`. A
/// newer database is healthy: migrations are additive, and an older binary
/// against an upgraded database is the normal rolling-deploy state.
pub(crate) fn check_current(recorded: Option<i64>, latest: i64) -> Result<()> {
    match recorded {
        Some(v) if v >= latest => Ok(()),
        Some(v) => Err(Error::app(format!(
            "dbos schema is behind: migration {v} applied, this binary expects {latest}"
        ))),
        None => Err(Error::app(
            "dbos schema is not migrated: no applied migrations recorded",
        )),
    }
}

/// The advisory-lock key migrations of `schema` serialize on: one fixed
/// namespace, folded with a stable hash of the schema so two schemas in one
/// database migrate independently. Stable across builds, which `DefaultHasher`
/// is not, so processes of different versions contend for the same lock.
#[cfg(feature = "postgres")]
pub(crate) fn lock_key(schema: &str) -> i64 {
    const NAMESPACE: u64 = 0x6475_7261_7265_6d67; // "duraremg"
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in schema.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash ^ NAMESPACE) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_rule() {
        assert!(check_current(Some(40), 40).is_ok());
        assert!(check_current(Some(125), 40).is_ok());
        let behind = check_current(Some(38), 40).unwrap_err().to_string();
        assert!(
            behind.contains("behind") && behind.contains("38"),
            "{behind}"
        );
        assert!(check_current(None, 40).is_err());
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn lock_key_is_stable_and_schema_specific() {
        assert_eq!(lock_key("dbos"), lock_key("dbos"));
        assert_ne!(lock_key("dbos"), lock_key("other"));
    }
}
