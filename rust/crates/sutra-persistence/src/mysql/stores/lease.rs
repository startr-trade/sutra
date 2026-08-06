//! Lease store, MySQL/MariaDB dialect (`lease`) — durable leader-election
//! fallback. NOT deployment-scoped (process-level primitive).
//!
//! Acquire-or-renew: the reference dialect resolves contention in one conditional-upsert
//! round trip that returns the row only to the winner. This dialect has no equivalent
//! returning-upsert, so the same guarantee is built from a conditional
//! `INSERT ... ON DUPLICATE KEY UPDATE` (assignments guarded by `IF(...)` so a loser's
//! attempt never overwrites an unexpired foreign hold) followed by a read of the row —
//! both inside one transaction whose row lock serialises concurrent acquirers: exactly
//! one winner, losers observe the winner's row and get `None`.

use sqlx::MySqlPool;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::mysql::scope::begin_tx;
use crate::mysql::{from_db, to_db};
use crate::stores::{Lease, LeaseStore};
use crate::{PersistenceError, Result};

/// MySQL/MariaDB implementation of [`LeaseStore`].
#[derive(Debug, Clone)]
pub struct MySqlLeaseStore {
    pool: MySqlPool,
}

/// The takeover condition — the row is expired or already ours — is evaluated per
/// assignment. Assignment order matters (each sees prior assignments): `acquired_at` and
/// `holder` are guarded by the condition over the OLD `expires_at`/`holder`; by the time
/// `expires_at` is assigned, `holder` is already the new value iff the condition held, so
/// `holder = VALUES(holder)` reproduces it exactly.
const SQL_TRY_ACQUIRE: &str = "INSERT INTO lease (name, holder, acquired_at, expires_at) \
     VALUES (?, ?, ?, ?) \
     ON DUPLICATE KEY UPDATE \
       acquired_at = IF(expires_at <= VALUES(acquired_at) OR holder = VALUES(holder), \
                        VALUES(acquired_at), acquired_at), \
       holder      = IF(expires_at <= VALUES(acquired_at) OR holder = VALUES(holder), \
                        VALUES(holder), holder), \
       expires_at  = IF(holder = VALUES(holder), VALUES(expires_at), expires_at)";

const SQL_RENEW: &str = "UPDATE lease SET expires_at = ? WHERE name = ? AND holder = ?";

const SQL_RELEASE: &str = "DELETE FROM lease WHERE name = ? AND holder = ?";

const SQL_CURRENT: &str = "SELECT name, holder, acquired_at, expires_at FROM lease WHERE name = ?";

fn positive_ttl(ttl: std::time::Duration) -> Result<time::Duration> {
    if ttl.is_zero() {
        return Err(PersistenceError::InvalidArgument(
            "ttl must be strictly positive".to_owned(),
        ));
    }
    time::Duration::try_from(ttl)
        .map_err(|_| PersistenceError::InvalidArgument("ttl out of range".to_owned()))
}

/// Text columns arrive as bytes (binary collation — see the dialect module docs).
type LeaseRow = (Vec<u8>, Vec<u8>, PrimitiveDateTime, PrimitiveDateTime);

fn lease_of(row: LeaseRow) -> Result<Lease> {
    let (name, holder, acquired_at, expires_at) = row;
    let utf8 = |bytes: Vec<u8>, col: &str| {
        String::from_utf8(bytes).map_err(|e| {
            PersistenceError::InvalidArgument(format!("column {col} is not UTF-8: {e}"))
        })
    };
    Ok(Lease {
        name: utf8(name, "name")?,
        holder: utf8(holder, "holder")?,
        acquired_at: from_db(acquired_at),
        expires_at: from_db(expires_at),
    })
}

impl MySqlLeaseStore {
    /// Wraps a connection pool.
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }
}

impl LeaseStore for MySqlLeaseStore {
    async fn try_acquire(
        &self,
        name: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<Lease>> {
        let ttl = positive_ttl(ttl)?;
        let now = OffsetDateTime::now_utc();

        let mut tx = begin_tx(&self.pool).await?;
        sqlx::query(SQL_TRY_ACQUIRE)
            .bind(name)
            .bind(holder)
            .bind(to_db(now))
            .bind(to_db(now + ttl))
            .execute(&mut *tx)
            .await
            .map_err(PersistenceError::db("lease tryAcquire"))?;
        // Same transaction, row lock held: the post-state is exactly what our statement
        // left — a concurrent acquirer is still blocked on the row.
        let row: Option<LeaseRow> = sqlx::query_as(SQL_CURRENT)
            .bind(name)
            .fetch_optional(&mut *tx)
            .await
            .map_err(PersistenceError::db("lease tryAcquire read-back"))?;
        tx.commit()
            .await
            .map_err(PersistenceError::db("lease tryAcquire commit"))?;

        Ok(row
            .map(lease_of)
            .transpose()?
            .filter(|lease| lease.holder == holder))
    }

    async fn renew(&self, name: &str, holder: &str, ttl: std::time::Duration) -> Result<bool> {
        let ttl = positive_ttl(ttl)?;
        let updated = sqlx::query(SQL_RENEW)
            .bind(to_db(OffsetDateTime::now_utc() + ttl))
            .bind(name)
            .bind(holder)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("lease renew"))?
            .rows_affected();
        Ok(updated == 1)
    }

    async fn release(&self, name: &str, holder: &str) -> Result<()> {
        sqlx::query(SQL_RELEASE)
            .bind(name)
            .bind(holder)
            .execute(&self.pool)
            .await
            .map_err(PersistenceError::db("lease release"))?;
        Ok(())
    }

    async fn current(&self, name: &str) -> Result<Option<Lease>> {
        let row: Option<LeaseRow> = sqlx::query_as(SQL_CURRENT)
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(PersistenceError::db("lease current"))?;
        row.map(lease_of).transpose()
    }
}
