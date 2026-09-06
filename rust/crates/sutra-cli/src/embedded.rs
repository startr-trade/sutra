//! The engine schema-migration SQL, embedded in the binary at build time so
//! `sutra migrate` needs no SQL mount — the same trees the engine image bakes under its
//! migrations root. `--migrations <dir>` overrides the embedded set.
//!
//! Four trees are baked in, all Rust-owned under `sutra-persistence/migrations`:
//! - the core persistence subsystems `shipped/core` (alias/inbox/instance/lease/outbox/
//!   channel/waitstate — including the channel V7xx family an earlier standalone migrator
//!   omitted),
//! - the audit subsystem `shipped/audit` (V2xx),
//! - the DB-backed deployment source `shipped/deploy` (V10xx — the `deployment_archive` store),
//! - the Rust-only addendum `native` (V803 timer wait-states, V804 timer-start schedules).
//!
//! `shipped/{core,audit}` are byte-identical copies of the frozen reference engine
//! migration trees, so the checksummed `sutra_schema_history` ledger stays interoperable.
//!
//! Coverage is deliberately NOT here. Its tables live
//! in the deployment's own declared `coverage` data store, so their engine-owned DDL ships with
//! `sutra-datastore` and is applied to that connection on first use — it is not engine-database
//! schema and never enters this ledger.

use std::path::PathBuf;

use include_dir::{include_dir, Dir, DirEntry};
use sutra_persistence::migrate::{order_scripts, parse_script_name, MigrationScript};
use sutra_persistence::Result;

static CORE_TREE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../sutra-persistence/migrations/shipped/core");
static AUDIT_TREE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../sutra-persistence/migrations/shipped/audit");
static DEPLOY_TREE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../sutra-persistence/migrations/shipped/deploy");
static ADDENDUM_TREE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../sutra-persistence/migrations/native");

/// All embedded scripts in apply order (ascending V-number, duplicate-free — the same
/// invariants as filesystem discovery, enforced by the persistence runner).
pub fn engine_scripts() -> Result<Vec<MigrationScript>> {
    let mut scripts = Vec::new();
    for tree in [&CORE_TREE, &AUDIT_TREE, &DEPLOY_TREE, &ADDENDUM_TREE] {
        collect(tree, &mut scripts);
    }
    order_scripts(scripts)
}

fn collect(dir: &Dir<'static>, out: &mut Vec<MigrationScript>) {
    for entry in dir.entries() {
        match entry {
            DirEntry::Dir(d) => collect(d, out),
            DirEntry::File(f) => {
                let Some(name) = f.path().file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                let Some((version, description)) = parse_script_name(name) else {
                    continue;
                };
                let Some(sql) = f.contents_utf8() else {
                    continue;
                };
                out.push(MigrationScript {
                    version,
                    description,
                    path: PathBuf::from(format!("embedded:{}", f.path().display())),
                    sql: sql.to_owned(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_set_is_the_full_engine_set_including_channel_and_timer() {
        let scripts = engine_scripts().unwrap();
        let versions: Vec<i64> = scripts.iter().map(|s| s.version).collect();
        assert_eq!(
            versions,
            vec![
                101, 201, 301, 401, 402, 403, 404, 501, 601, 602, 603, 604, 605, 606, 701, 702,
                801, 802, 803, 804, 1001, 1101, 1201, 1202, 1301
            ],
            "core subsystems + audit + channel V7xx + the V803/V804 addenda + deploy V10xx + subject-index V11xx (GDPR erasure) + dead-letter V12xx + data-key V13xx (no V9xx — coverage migrates the store the deployment declares, not the engine database); V606 is the outbox emitting-node column (channel-call <q:retry>)"
        );
        assert!(scripts.iter().all(|s| !s.sql.is_empty()));
        // The ledger `script` column records the file name from the synthetic path.
        assert_eq!(
            scripts[0].path.file_name().and_then(|n| n.to_str()),
            Some("V101__alias_index.sql")
        );
    }
}
