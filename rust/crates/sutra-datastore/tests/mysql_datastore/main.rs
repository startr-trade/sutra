//! MySQL 8 user-datastore suite — hermetic container-per-suite (mysql:8.0 via
//! testcontainers), one fresh database per test. Mirrors the PostgreSQL reference suite
//! (`tests/pg_datastore.rs`) behaviour-for-behaviour: the dialect-parity proof for the user
//! datastore SQL surface.
//!
//! The MariaDB suite (`tests/mariadb_datastore/`) compiles THESE SAME test sources against
//! a mariadb:11 container — one dialect module, two engines, both proven.

use std::any::Any;

use testcontainers::runners::SyncRunner;
use testcontainers::ImageExt;

mod fixture;
mod suite;

/// Starts this suite's database container: mysql:8.0. Returns the container (kept alive for
/// the process lifetime) and the mapped 3306 port.
pub(crate) fn start_db() -> (Box<dyn Any + Send + Sync>, u16) {
    let container = testcontainers_modules::mysql::Mysql::default()
        .with_tag("8.0")
        .start()
        .expect("start mysql:8.0 (docker required)");
    sutra_testkit::reap_on_exit(container.id());
    let port = container.get_host_port_ipv4(3306).expect("mapped 3306");
    (Box::new(container), port)
}
