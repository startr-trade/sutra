//! MariaDB 11 user-datastore suite — the SAME test sources as the MySQL suite
//! (`tests/mysql_datastore/*.rs`, included by path), run against a mariadb:11 container: the
//! proof that MariaDB genuinely rides the shared `mysql` dialect module. Any test
//! that passes on one engine and fails on the other is a dialect split waiting to be flagged.

use std::any::Any;

use testcontainers::runners::SyncRunner;
use testcontainers::ImageExt;

#[path = "../mysql_datastore/fixture.rs"]
mod fixture;
#[path = "../mysql_datastore/suite.rs"]
mod suite;

/// Starts this suite's database container: mariadb:11. Returns the container (kept alive for
/// the process lifetime) and the mapped 3306 port.
pub(crate) fn start_db() -> (Box<dyn Any + Send + Sync>, u16) {
    let container = testcontainers_modules::mariadb::Mariadb::default()
        .with_tag("11")
        .start()
        .expect("start mariadb:11 (docker required)");
    sutra_testkit::reap_on_exit(container.id());
    let port = container.get_host_port_ipv4(3306).expect("mapped 3306");
    (Box::new(container), port)
}
