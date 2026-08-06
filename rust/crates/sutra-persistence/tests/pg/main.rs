//! PostgreSQL integration suite — hermetic container-per-suite (postgres:16-alpine via
//! testcontainers), one fresh database per test, migrated with the shipped migration SQL.
//! One module per store, plus the row-level-security policy checks in `rls`.

mod alias;
mod audit_event;
mod channel;
mod data_key;
mod dead_letter;
mod deployment_archive;
mod external_task;
mod fixture;
mod inbox;
mod instance;
mod instance_migration;
mod lease;
mod migrations;
mod outbox;
mod rls;
mod step;
mod subject_index;
mod timer_schedule;
mod timer_wait;
mod wait_state;
