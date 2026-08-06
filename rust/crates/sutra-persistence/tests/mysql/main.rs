//! MySQL 8 integration suite — hermetic container-per-suite (mysql:8.0 via
//! testcontainers), one fresh database per test, migrated from the crate's
//! `migrations_mysql/**` dialect tree. Mirrors the PostgreSQL reference suite
//! (`tests/pg/`) behaviour-for-behaviour: the semantics proof for this dialect.
//! The PG-specific row-security tests are replaced by `posture` (the documented
//! enforced-bind-only posture plus the byte-wise-collation pins).
//!
//! The MariaDB suite (`tests/mariadb/`) compiles THESE SAME test sources against a
//! mariadb:11 container — one dialect module, two engines, both proven.

use std::any::Any;

use testcontainers::core::Mount;
use testcontainers::runners::SyncRunner;
use testcontainers::ImageExt;

mod fixture;

mod alias;
mod channel;
mod deployment_archive;
mod inbox;
mod instance;
mod lease;
mod migrations;
mod outbox;
mod posture;
mod step;
mod timer_wait;
mod wait_state;

/// Starts this suite's database container: mysql:8.0. Returns the container (kept alive
/// for the process lifetime) and the mapped 3306 port.
pub(crate) fn start_db() -> (Box<dyn Any + Send + Sync>, u16) {
    // Throwaway-DB durability off: each test does CREATE DATABASE + a full migration
    // apply, so per-commit innodb fsync (not container startup) dominates the suite time.
    // Disabling the redo-log fsync + doublewrite buffer is safe here (the container is
    // discarded) and is the single biggest lever on this suite's wall-clock.
    let container = testcontainers_modules::mysql::Mysql::default()
        .with_tag("8.0")
        .with_cmd([
            "mysqld",
            "--innodb-flush-log-at-trx-commit=0",
            "--innodb-doublewrite=0",
        ])
        // Datadir on tmpfs (RAM) — the ~84 per-test CREATE DATABASE + migration DDL are
        // disk-I/O bound on the data dictionary even with fsync off; a throwaway RAM
        // datadir removes that I/O entirely.
        .with_mount(Mount::tmpfs_mount("/var/lib/mysql"))
        .start()
        .expect("start mysql:8.0 (docker required)");
    sutra_testkit::reap_on_exit(container.id());
    let port = container.get_host_port_ipv4(3306).expect("mapped 3306");
    (Box::new(container), port)
}
