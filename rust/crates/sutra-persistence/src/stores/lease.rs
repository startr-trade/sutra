//! Lease store (`lease`, V501) — durable leader-election fallback.
//!
//! Semantics: acquire-or-renew resolves the contention race in a single SQL
//! round-trip via `INSERT ... ON CONFLICT (name) DO UPDATE ... WHERE expired-or-same-holder
//! RETURNING *` — when the WHERE clause filters the conflicting row out (another holder owns
//! an unexpired lease), RETURNING yields no row. NOT deployment-scoped: leases are a
//! process-level primitive — no GUC, no RLS.

use sqlx::PgPool;
use time::OffsetDateTime;

use crate::{PersistenceError, Result};

/// One lease row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    /// Lease name (e.g. `timer-leader`, `outbox-leader`).
    pub name: String,
    /// Current holder id.
    pub holder: String,
    /// When the current hold started.
    pub acquired_at: OffsetDateTime,
    /// When the hold lapses unless renewed.
    pub expires_at: OffsetDateTime,
}

/// Store trait for durable leases.
pub trait LeaseStore {
    /// Acquire-or-renew in one round trip. `Some(lease)` when this holder now owns it
    /// (fresh acquire, expired takeover, or renewal-by-acquire); `None` when another holder
    /// owns an unexpired lease.
    async fn try_acquire(
        &self,
        name: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<Lease>>;
    /// Extends expiry while still the holder; `false` when not the holder.
    async fn renew(&self, name: &str, holder: &str, ttl: std::time::Duration) -> Result<bool>;
    /// Releases the lease if held by `holder`; not holding it is a no-op.
    async fn release(&self, name: &str, holder: &str) -> Result<()>;
    /// Reads the current lease row, if any.
    async fn current(&self, name: &str) -> Result<Option<Lease>>;
}

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PgLeaseStore {
    pool: PgPool,
}

const SQL_TRY_ACQUIRE: &str = "INSERT INTO lease (name, holder, acquired_at, expires_at) \
     VALUES ($1, $2, $3, $4) \
     ON CONFLICT (name) DO UPDATE SET \
       holder = EXCLUDED.holder, \
       acquired_at = EXCLUDED.acquired_at, \
       expires_at = EXCLUDED.expires_at \
     WHERE lease.expires_at <= EXCLUDED.acquired_at \
        OR lease.holder = EXCLUDED.holder \
     RETURNING name, holder, acquired_at, expires_at";

const SQL_RENEW: &str = "UPDATE lease SET expires_at = $1 WHERE name = $2 AND holder = $3";

const SQL_RELEASE: &str = "DELETE FROM lease WHERE name = $1 AND holder = $2";

const SQL_CURRENT: &str = "SELECT name, holder, acquired_at, expires_at FROM lease WHERE name = $1";

fn positive_ttl(ttl: std::time::Duration) -> Result<time::Duration> {
    if ttl.is_zero() {
        return Err(PersistenceError::InvalidArgument(
            "ttl must be strictly positive".to_owned(),
        ));
    }
    time::Duration::try_from(ttl)
        .map_err(|_| PersistenceError::InvalidArgument("ttl out of range".to_owned()))
}

impl PgLeaseStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl LeaseStore for PgLeaseStore {
    async fn try_acquire(
        &self,
        name: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<Lease>> {
        let ttl = positive_ttl(ttl)?;
        let now = OffsetDateTime::now_utc();
        let row: Option<(String, String, OffsetDateTime, OffsetDateTime)> =
            sqlx::query_as(SQL_TRY_ACQUIRE)
                .bind(name)
                .bind(holder)
                .bind(now)
                .bind(now + ttl)
                .fetch_optional(&self.pool)
                .await
                .map_err(PersistenceError::db("lease tryAcquire"))?;
        Ok(row.map(|(name, holder, acquired_at, expires_at)| Lease {
            name,
            holder,
            acquired_at,
            expires_at,
        }))
    }

    async fn renew(&self, name: &str, holder: &str, ttl: std::time::Duration) -> Result<bool> {
        let ttl = positive_ttl(ttl)?;
        let updated = sqlx::query(SQL_RENEW)
            .bind(OffsetDateTime::now_utc() + ttl)
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
        let row: Option<(String, String, OffsetDateTime, OffsetDateTime)> =
            sqlx::query_as(SQL_CURRENT)
                .bind(name)
                .fetch_optional(&self.pool)
                .await
                .map_err(PersistenceError::db("lease current"))?;
        Ok(row.map(|(name, holder, acquired_at, expires_at)| Lease {
            name,
            holder,
            acquired_at,
            expires_at,
        }))
    }
}
