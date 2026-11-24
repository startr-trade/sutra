//! Deciding audit attempts the engine could not mark itself.
//!
//! An attempt's records are shipped only once its step is known to have committed (a
//! `COMMITTED` marker in the spool). Two kinds of attempt lack an outcome marker:
//!
//! - at start, the attempts a previous run of this spool left unmarked (it stopped between a
//!   step's spool sync and its marker's);
//! - while running, the attempts whose `COMMIT` returned an error, which may have landed.
//!
//! Both are decided from the step transaction's own outcome: every attempt is `PREPARED` under
//! its transaction id inside the transaction, before the barrier that precedes `COMMIT`, and
//! PostgreSQL reports how that transaction ended (`pg_xact_status`). This only reads the engine
//! database; nothing about audit is written there.

use sqlx::PgPool;
use sutra_channels::{AuditSpool, OpenAttempt};
use tokio::runtime::Handle;
use tracing::{info, warn};

/// Where the bridge sends attempts whose commit outcome is unknown.
pub(crate) type ResolverTx = tokio::sync::mpsc::UnboundedSender<Vec<OpenAttempt>>;

/// How often an attempt whose transaction is still in progress is asked about again.
const RETRY: std::time::Duration = std::time::Duration::from_secs(1);

/// The transaction id of a step that wrote nothing to the database: there is no transaction to
/// ask about, and its audit is kept.
pub(crate) const NO_TRANSACTION: &str = "none";

/// Read the attempts a previous run left unmarked, then start the task that decides them and
/// every attempt the bridge hands over later. Returns the bridge's handle on it.
pub(crate) fn spawn(
    handle: &Handle,
    spool: &AuditSpool,
    pool: Option<PgPool>,
) -> Result<ResolverTx, sutra_channels::audit_spool::SpoolError> {
    let unsettled = sutra_channels::audit_attempts::unsettled_attempts(spool)?;
    let sender = spool.sender();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<OpenAttempt>>();
    if !unsettled.is_empty() {
        info!(
            attempts = unsettled.len(),
            "audit recovery: deciding the attempts the previous run left without a commit marker"
        );
        let _ = tx.send(unsettled);
    }
    handle.spawn(async move {
        while let Some(mut pending) = rx.recv().await {
            loop {
                let mut again = Vec::new();
                for attempt in pending {
                    match decide(pool.as_ref(), &attempt).await {
                        Some(committed) => {
                            if !committed {
                                info!(
                                    attempt = %attempt.attempt.id,
                                    instance = %attempt.instance_id,
                                    "audit attempt aborted: its step did not commit"
                                );
                            }
                            sender.append(attempt.marker(committed));
                        }
                        None => again.push(attempt),
                    }
                }
                if again.is_empty() {
                    break;
                }
                tokio::time::sleep(RETRY).await;
                pending = again;
            }
        }
    });
    Ok(tx)
}

/// Whether `attempt`'s step committed: `None` while that cannot be told yet.
pub(crate) async fn decide(pool: Option<&PgPool>, attempt: &OpenAttempt) -> Option<bool> {
    // Never prepared: the process stopped before the step's barrier, and a commit needs it.
    let Some(xid) = attempt.xid.as_deref() else {
        return Some(false);
    };
    if xid == NO_TRANSACTION {
        return Some(true);
    }
    // No engine database to ask: keep the audit (never dropped on doubt).
    let Some(pool) = pool else {
        return Some(true);
    };
    let status: Result<Option<String>, sqlx::Error> =
        sqlx::query_scalar("SELECT pg_xact_status($1::xid8)::text")
            .bind(xid)
            .fetch_one(pool)
            .await;
    match status {
        Ok(Some(s)) if s == "committed" => Some(true),
        Ok(Some(s)) if s == "aborted" => Some(false),
        // In progress: the server has not ended the dead connection's transaction yet.
        Ok(Some(_)) => None,
        // Older than the server keeps status for: keep the audit.
        Ok(None) => Some(true),
        Err(e) => {
            warn!(error = %e, xid, "audit recovery: transaction status unavailable; retrying");
            None
        }
    }
}
