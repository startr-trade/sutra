//! Microsoft SQL Server dialect (PostgreSQL is the reference dialect — semantics are
//! normative, syntax is not).
//!
//! sqlx has no SQL Server driver, so this dialect runs on its own TDS client stack
//! (`tiberius` + a small checkout/checkin connection pool defined here). Deliberate
//! duplication: the module restates the reference stores in T-SQL rather than abstracting
//! over dialects — the PostgreSQL code is frozen as the reference implementation.
//!
//! Dialect-wide notes (each pinned by the container suite):
//!
//! - **Atomic claim**: `SELECT TOP (n) ... WITH (UPDLOCK, ROWLOCK, READPAST)` inside a
//!   transaction — the SQL Server equivalent of `FOR UPDATE SKIP LOCKED` (concurrent
//!   claimers lock disjoint rows, never block, never double-claim).
//! - **Upserts** are `MERGE ... WITH (HOLDLOCK)` — atomic under concurrency, the
//!   equivalent of the reference's conflict-target upsert.
//! - **First-observer dedup** is a plain `INSERT` + duplicate-key rejection (server
//!   errors 2627/2601). Transactions run `SET XACT_ABORT OFF` so a constraint violation
//!   terminates the statement, not the transaction — the conflict path continues inside
//!   the same transaction like the reference dialect's conflict-ignoring insert.
//! - **Isolation posture**: enforced-bind-only. No security policy ships for
//!   this dialect (none exists in the shipped migration trees for any non-PG dialect);
//!   the explicit `deployment_id` bind on every statement is the isolation.
//! - **Timestamps** are `DATETIME2(6)` bound/read as UTC `PrimitiveDateTime`
//!   (`SYSUTCDATETIME()` on the database side); the public API stays `OffsetDateTime`.
//! - **Uncommitted transactions roll back by connection teardown**: a dropped [`MssqlTx`]
//!   discards its connection instead of returning it to the pool — the server aborts the
//!   open transaction, which is exactly the crash-injection semantics the strict
//!   transactional-step proofs drive.

pub mod migrate;
pub mod step;
pub mod stores;

use std::sync::{Arc, Mutex};

use time::{OffsetDateTime, PrimitiveDateTime};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::{PersistenceError, Result};

/// The concrete client type this dialect drives (TDS over tokio TCP).
pub type MssqlClient = tiberius::Client<Compat<tokio::net::TcpStream>>;

/// SQL Server duplicate-key server errors: 2627 (constraint violation — PRIMARY
/// KEY/UNIQUE constraint) and 2601 (duplicate key in a unique index — the filtered
/// unique-live alias index reports this one).
pub(crate) fn is_duplicate_key(err: &tiberius::error::Error) -> bool {
    matches!(err.code(), Some(2627) | Some(2601))
}

/// UTC [`OffsetDateTime`] → the `DATETIME2(6)` bind value.
pub(crate) fn to_db(t: OffsetDateTime) -> PrimitiveDateTime {
    let utc = t.to_offset(time::UtcOffset::UTC);
    PrimitiveDateTime::new(utc.date(), utc.time())
}

/// `DATETIME2(6)` column value → UTC [`OffsetDateTime`].
pub(crate) fn from_db(t: PrimitiveDateTime) -> OffsetDateTime {
    t.assume_utc()
}

/// Reads a non-nullable column, surfacing NULL as an error instead of a panic.
pub(crate) fn req<'a, T>(row: &'a tiberius::Row, col: &'static str) -> Result<T>
where
    T: tiberius::FromSql<'a>,
{
    row.try_get::<T, _>(col)
        .map_err(PersistenceError::mssql("read column"))?
        .ok_or_else(|| PersistenceError::InvalidArgument(format!("column {col} unexpectedly NULL")))
}

/// Reads a nullable column.
pub(crate) fn opt<'a, T>(row: &'a tiberius::Row, col: &'static str) -> Result<Option<T>>
where
    T: tiberius::FromSql<'a>,
{
    row.try_get::<T, _>(col)
        .map_err(PersistenceError::mssql("read column"))
}

/// Connection settings for one SQL Server database.
#[derive(Debug, Clone)]
pub struct MssqlConfig {
    /// Server host.
    pub host: String,
    /// TDS port (1433 by default deployments).
    pub port: u16,
    /// Database name.
    pub database: String,
    /// SQL authentication user.
    pub user: String,
    /// SQL authentication password.
    pub password: String,
    /// Accept the server certificate without CA validation (containerised/dev servers).
    pub trust_cert: bool,
}

impl MssqlConfig {
    fn to_tiberius(&self) -> tiberius::Config {
        let mut config = tiberius::Config::new();
        config.host(&self.host);
        config.port(self.port);
        config.database(&self.database);
        config.authentication(tiberius::AuthMethod::sql_server(&self.user, &self.password));
        if self.trust_cert {
            config.trust_cert();
        }
        config
    }
}

/// A small checkout/checkin pool over [`MssqlClient`] connections.
///
/// Boring by design: checkout pops an idle connection or dials a new one; checkin happens
/// on guard drop. There is no upper bound — the container suites and the engine's store
/// call patterns keep concurrency modest, and correctness (an uncommitted transaction
/// never reaches the idle list — see [`MssqlTx`]) is the property that matters here.
#[derive(Clone)]
pub struct MssqlPool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    config: MssqlConfig,
    idle: Mutex<Vec<MssqlClient>>,
}

impl MssqlPool {
    /// Creates a pool; nothing connects until first use.
    pub fn new(config: MssqlConfig) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                config,
                idle: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Checks out an idle connection or dials a new one.
    pub async fn acquire(&self) -> Result<PooledMssql> {
        let idle = {
            let mut guard = self
                .inner
                .idle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.pop()
        };
        let client = match idle {
            Some(client) => client,
            None => {
                let config = self.inner.config.to_tiberius();
                let tcp = tokio::net::TcpStream::connect((
                    self.inner.config.host.as_str(),
                    self.inner.config.port,
                ))
                .await
                .map_err(|e| PersistenceError::InvalidArgument(format!("tcp connect: {e}")))?;
                tcp.set_nodelay(true)
                    .map_err(|e| PersistenceError::InvalidArgument(format!("tcp nodelay: {e}")))?;
                tiberius::Client::connect(config, tcp.compat_write())
                    .await
                    .map_err(PersistenceError::mssql("connect"))?
            }
        };
        Ok(PooledMssql {
            client: Some(client),
            pool: Arc::clone(&self.inner),
            discard: false,
        })
    }
}

/// A checked-out connection; returns to the pool on drop unless marked for discard.
pub struct PooledMssql {
    client: Option<MssqlClient>,
    pool: Arc<PoolInner>,
    discard: bool,
}

impl PooledMssql {
    /// The underlying client.
    pub fn client(&mut self) -> &mut MssqlClient {
        self.client.as_mut().expect("client present until drop")
    }

    fn discard_on_drop(&mut self) {
        self.discard = true;
    }
}

impl Drop for PooledMssql {
    fn drop(&mut self) {
        if self.discard {
            return; // dropping the client closes the socket; the server rolls back
        }
        if let (Some(client), Ok(mut idle)) = (self.client.take(), self.pool.idle.lock()) {
            idle.push(client);
        }
    }
}

/// Runs a parameterless statement batch to completion.
pub(crate) async fn run_batch(client: &mut MssqlClient, sql: &str) -> Result<()> {
    client
        .simple_query(sql)
        .await
        .map_err(PersistenceError::mssql("batch"))?
        .into_results()
        .await
        .map_err(PersistenceError::mssql("batch drain"))?;
    Ok(())
}

/// An explicit transaction over a pooled connection.
///
/// `SET XACT_ABORT OFF` is established at begin so a constraint violation terminates only
/// the offending statement (the first-observer dedup path relies on continuing inside the
/// transaction). Dropping the value without [`commit`](Self::commit) discards the
/// connection, which rolls the transaction back server-side — commit-or-nothing.
pub struct MssqlTx {
    conn: PooledMssql,
    open: bool,
}

impl MssqlTx {
    /// Opens a transaction on a pooled connection.
    pub async fn begin(pool: &MssqlPool) -> Result<MssqlTx> {
        let mut conn = pool.acquire().await?;
        run_batch(conn.client(), "SET XACT_ABORT OFF; BEGIN TRANSACTION").await?;
        Ok(MssqlTx { conn, open: true })
    }

    /// The transaction's connection.
    pub fn client(&mut self) -> &mut MssqlClient {
        self.conn.client()
    }

    /// Commits; the connection returns to the pool.
    pub async fn commit(mut self) -> Result<()> {
        run_batch(self.conn.client(), "COMMIT TRANSACTION").await?;
        self.open = false;
        Ok(())
    }

    /// Rolls back explicitly; the connection returns to the pool.
    pub async fn rollback(mut self) -> Result<()> {
        run_batch(self.conn.client(), "ROLLBACK TRANSACTION").await?;
        self.open = false;
        Ok(())
    }
}

impl Drop for MssqlTx {
    fn drop(&mut self) {
        if self.open {
            self.conn.discard_on_drop();
        }
    }
}
