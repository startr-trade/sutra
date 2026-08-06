//! Deployment scoping on the MySQL/MariaDB dialect — the documented enforced-bind-only
//! posture.
//!
//! The reference dialect enforces isolation at two layers: explicit `deployment_id` binds
//! plus database row-security keyed off a per-transaction setting. MySQL/MariaDB have no
//! row-security policies, so ONLY the first layer exists here: every statement in
//! [`crate::mysql::stores`] carries an explicit `deployment_id` bind, and that bind is the
//! entire isolation story. The posture container tests pin both directions: store-surface
//! reads are deployment-scoped, and a raw unscoped query is NOT filtered by the database.

use sqlx::{MySql, MySqlPool, Transaction};

use crate::{PersistenceError, Result};

/// Opens a store transaction. Unlike the reference dialect there is no per-transaction
/// setting to establish — the function exists so store/step code reads the same on every
/// dialect and the posture stays greppable.
pub async fn begin_tx(pool: &MySqlPool) -> Result<Transaction<'static, MySql>> {
    pool.begin()
        .await
        .map_err(PersistenceError::db("begin transaction"))
}
