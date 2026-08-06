//! Lease store, SQL Server dialect (`lease`) — durable leader-election fallback.
//! NOT deployment-scoped (process-level primitive).
//!
//! Acquire-or-renew resolves the contention race with a key-range-locking read
//! (`WITH (UPDLOCK, HOLDLOCK)`) followed by the INSERT (free/absent), UPDATE (expired or
//! already ours), or nothing (foreign unexpired hold) — all in one transaction. The
//! update lock serialises concurrent acquirers on the key instead of deadlocking them,
//! reproducing the reference dialect's single-winner conditional-upsert semantics: the
//! winner gets the row back, losers get `None`.

use time::{OffsetDateTime, PrimitiveDateTime};

use crate::mssql::{from_db, is_duplicate_key, req, to_db, MssqlPool, MssqlTx};
use crate::stores::{Lease, LeaseStore};
use crate::{PersistenceError, Result};

/// SQL Server implementation of [`LeaseStore`].
#[derive(Clone)]
pub struct MssqlLeaseStore {
    pool: MssqlPool,
}

const SQL_LOCK_ROW: &str = "SELECT holder, expires_at FROM lease WITH (UPDLOCK, HOLDLOCK) \
     WHERE name = @P1";

const SQL_INSERT: &str = "INSERT INTO lease (name, holder, acquired_at, expires_at) \
     VALUES (@P1, @P2, @P3, @P4)";

const SQL_TAKE_OVER: &str = "UPDATE lease \
     SET holder = @P2, acquired_at = @P3, expires_at = @P4 WHERE name = @P1";

const SQL_RENEW: &str = "UPDATE lease SET expires_at = @P1 WHERE name = @P2 AND holder = @P3";

const SQL_RELEASE: &str = "DELETE FROM lease WHERE name = @P1 AND holder = @P2";

const SQL_CURRENT: &str =
    "SELECT name, holder, acquired_at, expires_at FROM lease WHERE name = @P1";

fn positive_ttl(ttl: std::time::Duration) -> Result<time::Duration> {
    if ttl.is_zero() {
        return Err(PersistenceError::InvalidArgument(
            "ttl must be strictly positive".to_owned(),
        ));
    }
    time::Duration::try_from(ttl)
        .map_err(|_| PersistenceError::InvalidArgument("ttl out of range".to_owned()))
}

fn lease_of(row: &tiberius::Row) -> Result<Lease> {
    let acquired_at: PrimitiveDateTime = req(row, "acquired_at")?;
    let expires_at: PrimitiveDateTime = req(row, "expires_at")?;
    Ok(Lease {
        name: req::<&str>(row, "name")?.to_owned(),
        holder: req::<&str>(row, "holder")?.to_owned(),
        acquired_at: from_db(acquired_at),
        expires_at: from_db(expires_at),
    })
}

impl MssqlLeaseStore {
    /// Wraps a connection pool.
    pub fn new(pool: MssqlPool) -> Self {
        Self { pool }
    }
}

impl LeaseStore for MssqlLeaseStore {
    async fn try_acquire(
        &self,
        name: &str,
        holder: &str,
        ttl: std::time::Duration,
    ) -> Result<Option<Lease>> {
        let ttl = positive_ttl(ttl)?;
        let now = OffsetDateTime::now_utc();
        let acquired_at = to_db(now);
        let expires_at = to_db(now + ttl);

        let mut tx = MssqlTx::begin(&self.pool).await?;
        let existing = tx
            .client()
            .query(SQL_LOCK_ROW, &[&name])
            .await
            .map_err(PersistenceError::mssql("lease tryAcquire lock"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("lease tryAcquire lock row"))?;

        let won = match &existing {
            None => {
                // Free: insert. A duplicate-key rejection means another acquirer slipped
                // in ahead of our range lock — that acquirer holds a fresh lease; lost.
                let inserted = tx
                    .client()
                    .execute(SQL_INSERT, &[&name, &holder, &acquired_at, &expires_at])
                    .await;
                match inserted {
                    Ok(_) => true,
                    Err(e) if is_duplicate_key(&e) => false,
                    Err(e) => return Err(PersistenceError::mssql("lease tryAcquire insert")(e)),
                }
            }
            Some(row) => {
                let current_holder: &str = req(row, "holder")?;
                let current_expiry: PrimitiveDateTime = req(row, "expires_at")?;
                if current_expiry <= acquired_at || current_holder == holder {
                    // Expired takeover or renewal-by-acquire.
                    tx.client()
                        .execute(SQL_TAKE_OVER, &[&name, &holder, &acquired_at, &expires_at])
                        .await
                        .map_err(PersistenceError::mssql("lease tryAcquire update"))?;
                    true
                } else {
                    false
                }
            }
        };
        tx.commit().await?;

        Ok(won.then(|| Lease {
            name: name.to_owned(),
            holder: holder.to_owned(),
            acquired_at: from_db(acquired_at),
            expires_at: from_db(expires_at),
        }))
    }

    async fn renew(&self, name: &str, holder: &str, ttl: std::time::Duration) -> Result<bool> {
        let ttl = positive_ttl(ttl)?;
        let expires_at = to_db(OffsetDateTime::now_utc() + ttl);
        let mut conn = self.pool.acquire().await?;
        let updated = conn
            .client()
            .execute(SQL_RENEW, &[&expires_at, &name, &holder])
            .await
            .map_err(PersistenceError::mssql("lease renew"))?
            .total();
        Ok(updated == 1)
    }

    async fn release(&self, name: &str, holder: &str) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.client()
            .execute(SQL_RELEASE, &[&name, &holder])
            .await
            .map_err(PersistenceError::mssql("lease release"))?;
        Ok(())
    }

    async fn current(&self, name: &str) -> Result<Option<Lease>> {
        let mut conn = self.pool.acquire().await?;
        let row = conn
            .client()
            .query(SQL_CURRENT, &[&name])
            .await
            .map_err(PersistenceError::mssql("lease current"))?
            .into_row()
            .await
            .map_err(PersistenceError::mssql("lease current row"))?;
        row.as_ref().map(lease_of).transpose()
    }
}
