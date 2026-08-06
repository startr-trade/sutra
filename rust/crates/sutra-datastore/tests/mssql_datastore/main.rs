//! SQL Server 2022 user-datastore suite — hermetic container-per-suite
//! (mcr.microsoft.com/mssql/server:2022-latest via testcontainers; EULA accepted), one
//! fresh database per test. Mirrors the PostgreSQL reference suite (`tests/pg_datastore.rs`)
//! behaviour-for-behaviour: the dialect-parity proof for the user datastore
//! SQL surface on SQL Server, which (having no sqlx driver) runs on the tiberius TDS stack.

mod fixture;
mod suite;
