//! SQL Server 2022 integration suite — hermetic container-per-suite
//! (mcr.microsoft.com/mssql/server:2022-latest via testcontainers; EULA accepted, strong
//! SA password from the module default), one fresh database per test, migrated from the
//! crate's `migrations_mssql/**` dialect tree. Mirrors the PostgreSQL reference suite
//! (`tests/pg/`) behaviour-for-behaviour: the semantics proof for this dialect.
//! The PG-specific row-security tests are replaced by `posture` (the documented
//! enforced-bind-only posture plus the BIN2-collation pins).

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
