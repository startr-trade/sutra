//! Wrapped-DEK store (`data_key`, V1301) — the persistence source for the KEK-wrap envelope
//! KeyProvider.
//!
//! Each row holds one `key_id` → sealed DEK (a [`sutra_crypto::WrappedDataKey`], i.e.
//! ciphertext under the tenant/deployment KEK). At boot the engine materialises the whole map
//! via [`PgDataKeyStore::list_all`] and hands it to `EnvelopeKeyProvider::new`, which unwraps on
//! demand under a KEK resolved through the envref/secret registry. A `WrappedDataKey` is
//! explicitly safe to store — the confidentiality boundary is the KEK, not this table — so this
//! is an ordinary un-scoped infra store (no RLS), keyed by the migration-stable crypto identity
//! like `deployment_archive`/`lease`. pg-only (the engine datasource is always `PgPool`).

use sqlx::{PgPool, Row};

use sutra_crypto::WrappedDataKey;

use crate::{PersistenceError, Result};

/// PostgreSQL wrapped-DEK store.
#[derive(Debug, Clone)]
pub struct PgDataKeyStore {
    pool: PgPool,
}

impl PgDataKeyStore {
    /// Wraps a connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// The full `key_id` → wrapped-DEK set — the envelope provider's boot-load source.
    pub async fn list_all(&self) -> Result<Vec<WrappedDataKey>> {
        let rows = sqlx::query("SELECT key_id, wrapped_dek FROM data_key ORDER BY key_id")
            .fetch_all(&self.pool)
            .await
            .map_err(PersistenceError::db("data_key list_all"))?;
        rows.into_iter()
            .map(|r| {
                let key_id: String = r
                    .try_get("key_id")
                    .map_err(PersistenceError::db("data_key key_id decode"))?;
                let wrapped: Vec<u8> = r
                    .try_get("wrapped_dek")
                    .map_err(PersistenceError::db("data_key wrapped_dek decode"))?;
                Ok(WrappedDataKey::from_parts(key_id, wrapped))
            })
            .collect()
    }

    /// Provision or rotate a wrapped DEK (the future admin/CLI provisioning path). A repeat with
    /// the same `key_id` replaces the wrapped bytes and stamps `rotated_at`.
    pub async fn upsert(&self, wrapped: &WrappedDataKey) -> Result<()> {
        sqlx::query(
            "INSERT INTO data_key (key_id, wrapped_dek) VALUES ($1, $2) \
             ON CONFLICT (key_id) DO UPDATE SET \
               wrapped_dek = EXCLUDED.wrapped_dek, rotated_at = CURRENT_TIMESTAMP",
        )
        .bind(wrapped.key_id())
        .bind(wrapped.as_bytes())
        .execute(&self.pool)
        .await
        .map_err(PersistenceError::db("data_key upsert"))?;
        Ok(())
    }
}
