//! MariaDB 11 integration suite — the SAME test sources as the MySQL suite
//! (`tests/mysql/*.rs`, included by path), run against a mariadb:11 container: the proof
//! that MariaDB genuinely rides the shared `mysql` dialect module. Any test that
//! passes on one engine and fails on the other is a dialect split waiting to be flagged.

use std::any::Any;

use testcontainers::core::Mount;
use testcontainers::runners::SyncRunner;
use testcontainers::ImageExt;

#[path = "../mysql/fixture.rs"]
mod fixture;

#[path = "../mysql/alias.rs"]
mod alias;
#[path = "../mysql/channel.rs"]
mod channel;
#[path = "../mysql/deployment_archive.rs"]
mod deployment_archive;
#[path = "../mysql/inbox.rs"]
mod inbox;
#[path = "../mysql/instance.rs"]
mod instance;
#[path = "../mysql/lease.rs"]
mod lease;
#[path = "../mysql/migrations.rs"]
mod migrations;
#[path = "../mysql/outbox.rs"]
mod outbox;
#[path = "../mysql/posture.rs"]
mod posture;
#[path = "../mysql/step.rs"]
mod step;
#[path = "../mysql/timer_wait.rs"]
mod timer_wait;
#[path = "../mysql/wait_state.rs"]
mod wait_state;

/// Starts this suite's database container: mariadb:11. Returns the container (kept alive
/// for the process lifetime) and the mapped 3306 port.
pub(crate) fn start_db() -> (Box<dyn Any + Send + Sync>, u16) {
    // Throwaway-DB durability off (see the mysql suite): each test does CREATE DATABASE +
    // a full migration apply, so per-commit innodb fsync dominates. Safe — discarded
    // container. MariaDB 11's daemon is `mariadbd`.
    let container = testcontainers_modules::mariadb::Mariadb::default()
        .with_tag("11")
        .with_cmd([
            "mariadbd",
            "--innodb-flush-log-at-trx-commit=0",
            "--innodb-doublewrite=0",
        ])
        // Datadir on tmpfs (RAM) — the per-test DDL is data-dictionary I/O bound; a
        // throwaway RAM datadir removes it (mysql suite: 288s → 11s).
        .with_mount(Mount::tmpfs_mount("/var/lib/mysql"))
        .start()
        .expect("start mariadb:11 (docker required)");
    sutra_testkit::reap_on_exit(container.id());
    let port = container.get_host_port_ipv4(3306).expect("mapped 3306");
    (Box::new(container), port)
}
