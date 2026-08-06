//! The engine's two leader-gated `instance_state` housekeeping loops, which share one shape
//! (gate → tick → per-deployment pass) and nothing else:
//!
//! * [`StuckInstanceScanner`] — clears per-instance ownership CLAIMS whose owner has gone silent.
//!   Minutes-scale, non-destructive, the backstop the ownership protocol is designed around.
//! * [`TerminalRetentionSweeper`] — PURGES finished instances past `sutra.instance.retention`.
//!   Days-scale and destructive; it is the second half of P1-2's retain-instead-of-delete.
//!
//! ---
//!
//! The stuck-instance scanner: a leader-gated interval loop that clears per-instance
//! ownership claims whose owner has gone silent — the backstop the ownership protocol is
//! designed around, and the type `V402__instance_claim_columns.sql` has named since the
//! claim columns shipped.
//!
//! **What it does and does not do.** The resume paths (relay correlation, timer fire) claim
//! an instance before rehydrating it and release the claim when the step commits. A replica
//! that dies mid-step never reaches its release, so the row stays owned by an identity that
//! no longer exists — and because an owner id is per-process (`<host>-<pid>-<rand>`), even
//! the SAME pod coming back cannot re-adopt it. The scanner clears exactly those claims:
//! every row whose `last_heartbeat_at` lapsed past `claim_timeout` becomes unowned again.
//!
//! It **never resumes anything**. Clearing the claim is the whole job; the next relay
//! delivery or due timer re-drives the instance through the ordinary path. That keeps the
//! scanner a pure GC role with no execution semantics of its own — nothing to get wrong
//! about frontiers, deployments or ordering — and means a mis-tuned `claim_timeout` costs
//! availability (a resume bounces until the sweep) rather than correctness.
//!
//! **Singleton by lease, like the timer poller.** Every replica could run this safely (the
//! sweep is an idempotent UPDATE), but there is nothing to gain from N replicas issuing the
//! same UPDATE every minute, so it runs under the [`INSTANCE_SWEEPER_ROLE`] DB lease —
//! copying [`crate::timer::spawn_timer_poller`]'s gate-then-tick shape. A follower ticks and
//! returns immediately; leadership flips are picked up on the next tick.

use std::sync::Arc;

use sqlx::PgPool;
use sutra_channels::LeaderGate;
use sutra_persistence::stores::{InstanceStore, PgInstanceStore};
use sutra_persistence::DeploymentId as PersistDeploymentId;
use tracing::{debug, info, warn};

/// The DB-lease role the stuck-instance scanner runs under (the election daemon owns the
/// lease; the gate is injected, exactly as for the timer poller).
pub const INSTANCE_SWEEPER_ROLE: &str = "instance-sweeper";

/// Scanner knobs — `sutra.instance.sweep-interval` / `sutra.instance.claim-timeout`
/// (env `SUTRA_INSTANCE_SWEEP_INTERVAL` / `SUTRA_INSTANCE_CLAIM_TIMEOUT`).
///
/// The two are read together: `claim_timeout` is how long a silent owner keeps an instance
/// (its unavailability window), `interval` is the resolution at which that window is
/// enforced. `claim_timeout` must stay comfortably longer than the longest step — a sweep
/// that fires while the owner is still working would let a second replica claim an instance
/// that is mid-step. The defaults (sweep PT1M, timeout PT5M) put two orders of magnitude
/// between a step (milliseconds to low seconds today) and a reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StuckInstanceScannerConfig {
    /// Sweep cadence (`sutra.instance.sweep-interval`). Default `PT1M`.
    pub interval: std::time::Duration,
    /// How long a claim survives without a heartbeat before the sweep clears it
    /// (`sutra.instance.claim-timeout`). Default `PT5M`.
    pub claim_timeout: std::time::Duration,
}

impl Default for StuckInstanceScannerConfig {
    fn default() -> StuckInstanceScannerConfig {
        StuckInstanceScannerConfig {
            interval: std::time::Duration::from_secs(60),
            claim_timeout: std::time::Duration::from_secs(300),
        }
    }
}

/// The stuck-instance scanner. Holds the pool, the LIVE deployment set (read per tick, so a
/// deployment activation flip is picked up without a restart), the leader gate and the knobs.
pub struct StuckInstanceScanner {
    store: PgInstanceStore,
    deployments: sutra_channels::LiveDeploymentSet,
    gate: Arc<dyn LeaderGate>,
    config: StuckInstanceScannerConfig,
}

impl StuckInstanceScanner {
    /// Wire a scanner. Nothing runs until [`Self::spawn`].
    pub fn new(
        pool: PgPool,
        deployments: sutra_channels::LiveDeploymentSet,
        gate: Arc<dyn LeaderGate>,
        config: StuckInstanceScannerConfig,
    ) -> StuckInstanceScanner {
        StuckInstanceScanner {
            store: PgInstanceStore::new(pool),
            deployments,
            gate,
            config,
        }
    }

    /// One sweep across every live deployment, returning the number of claims cleared.
    /// Ungated (the caller decides) and per-deployment isolated: one deployment's failure is
    /// logged and the sweep continues, so a single bad scope cannot stall the role.
    pub async fn sweep_once(&self) -> u64 {
        let mut swept = 0;
        for deployment in self.deployments.snapshot() {
            let dep = match PersistDeploymentId::new(deployment.value()) {
                Ok(dep) => dep,
                Err(e) => {
                    warn!(deployment = deployment.value(), error = %e,
                          "instance sweeper skips deployment");
                    continue;
                }
            };
            match self
                .store
                .sweep_stuck(&dep, self.config.claim_timeout)
                .await
            {
                Ok(0) => {}
                Ok(cleared) => {
                    // Loud on purpose: every row here is an instance a replica died holding.
                    info!(
                        deployment = deployment.value(),
                        cleared,
                        claim_timeout_s = self.config.claim_timeout.as_secs(),
                        "stuck-instance sweep cleared expired ownership claims (the next relay \
                         or timer re-drives those instances)"
                    );
                    swept += cleared;
                }
                Err(e) => {
                    warn!(deployment = deployment.value(), error = %e,
                          "stuck-instance sweep failed for deployment");
                }
            }
        }
        swept
    }

    /// Spawn the interval loop. Runs until aborted (`RunningEngine::shutdown` / `drain`).
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if !self.gate.is_leading() {
                    continue;
                }
                let swept = self.sweep_once().await;
                debug!(
                    swept,
                    role = INSTANCE_SWEEPER_ROLE,
                    "stuck-instance sweep tick"
                );
            }
        })
    }
}

// ---- terminal retention (P1-2) ----------------------------------------------------------------

/// The DB-lease role the terminal-retention purge runs under. A SEPARATE role from
/// [`INSTANCE_SWEEPER_ROLE`] on purpose: the two sweeps have different cadences (claims lapse in
/// minutes, retention windows in days) and very different blast radii (clearing a claim is
/// reversible, deleting a row is not), so an operator must be able to see — and an election to
/// place — them independently.
pub const RETENTION_SWEEPER_ROLE: &str = "retention-sweeper";

/// Terminal-retention knobs — `sutra.instance.retention` /
/// `sutra.instance.retention-sweep-interval` (env `SUTRA_INSTANCE_RETENTION` /
/// `SUTRA_INSTANCE_RETENTION_SWEEP_INTERVAL`).
///
/// `retention` is the contract an operator reasons about: how long after finishing an instance
/// stays queryable. `interval` is only the resolution at which that contract is enforced, so it is
/// deliberately coarse (hourly by default) — a purge is a bulk DELETE over an indexed predicate and
/// there is nothing to gain from running it every minute.
///
/// `retention = PT0S` is a valid, meaningful setting and NOT a degenerate one: it restores the
/// pre-P1-2 behaviour by making the terminal step delete the row outright
/// ([`crate::bridge::PersistenceBridge::with_retention`]). The sweeper still runs under it, and
/// still does the right thing — it purges the terminal rows written before the operator flipped
/// the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRetentionConfig {
    /// How long a finished (COMPLETED / TERMINATED) instance row is kept
    /// (`sutra.instance.retention`). Default `P7D`; `PT0S` = delete at terminal.
    pub retention: std::time::Duration,
    /// Purge cadence (`sutra.instance.retention-sweep-interval`). Default `PT1H`.
    pub interval: std::time::Duration,
}

impl Default for TerminalRetentionConfig {
    fn default() -> TerminalRetentionConfig {
        TerminalRetentionConfig {
            retention: crate::bridge::DEFAULT_INSTANCE_RETENTION,
            interval: std::time::Duration::from_secs(3600),
        }
    }
}

/// The terminal-retention purge sweeper: the other half of P1-2's retain-instead-of-delete.
///
/// [`crate::bridge::PersistenceBridge::commit_complete`] (and the admin cancel) stop deleting a
/// finished instance's row and stamp `terminal_at` on it instead; without this loop that table
/// would grow forever. Every tick, on the leader, it deletes every terminal row whose
/// `terminal_at` is at or past the retention window, per live deployment.
///
/// **Scope of the delete, stated precisely.** It removes rows from `instance_state` and nothing
/// else. In particular it never touches `audit_event`: the per-token-move journal is the compliance
/// record that an instance RAN, it is opt-in and separately configured, and GDPR erasure treats it
/// as something to REDACT (null the captured payloads, keep the metadata trail) rather than to
/// delete — so folding it into an instance-state retention clock would quietly destroy evidence an
/// operator deliberately asked to keep. An audit-journal retention policy is its own feature with
/// its own key.
///
/// **Singleton by lease, like the stuck-instance scanner.** The DELETE is idempotent, so every
/// replica could run it safely; running it on one keeps the write amplification at 1×. It shares
/// the same gate-then-tick shape as [`StuckInstanceScanner`] and the timer poller, under its own
/// [`RETENTION_SWEEPER_ROLE`].
pub struct TerminalRetentionSweeper {
    store: PgInstanceStore,
    deployments: sutra_channels::LiveDeploymentSet,
    gate: Arc<dyn LeaderGate>,
    config: TerminalRetentionConfig,
}

impl TerminalRetentionSweeper {
    /// Wire a sweeper. Nothing runs until [`Self::spawn`].
    pub fn new(
        pool: PgPool,
        deployments: sutra_channels::LiveDeploymentSet,
        gate: Arc<dyn LeaderGate>,
        config: TerminalRetentionConfig,
    ) -> TerminalRetentionSweeper {
        TerminalRetentionSweeper {
            store: PgInstanceStore::new(pool),
            deployments,
            gate,
            config,
        }
    }

    /// One purge across every live deployment, returning the number of rows purged. Ungated (the
    /// caller decides) and per-deployment isolated — one deployment's failure is logged and the
    /// sweep continues, so a single bad scope cannot stall the role.
    pub async fn purge_once(&self) -> u64 {
        let mut purged = 0;
        for deployment in self.deployments.snapshot() {
            let dep = match PersistDeploymentId::new(deployment.value()) {
                Ok(dep) => dep,
                Err(e) => {
                    warn!(deployment = deployment.value(), error = %e,
                          "retention sweeper skips deployment");
                    continue;
                }
            };
            match self.store.purge_terminal(&dep, self.config.retention).await {
                Ok(0) => {}
                Ok(rows) => {
                    // Loud on purpose: this is the moment finished instances stop being
                    // answerable, and an operator debugging a 404 must be able to find the line
                    // that says why.
                    info!(
                        deployment = deployment.value(),
                        purged = rows,
                        retention_s = self.config.retention.as_secs(),
                        "terminal-retention purge removed finished instances past their \
                         retention window (their audit journal is untouched)"
                    );
                    purged += rows;
                }
                Err(e) => {
                    warn!(deployment = deployment.value(), error = %e,
                          "terminal-retention purge failed for deployment");
                }
            }
        }
        purged
    }

    /// Spawn the interval loop. Runs until aborted (`RunningEngine::shutdown` / `drain`).
    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if !self.gate.is_leading() {
                    continue;
                }
                let purged = self.purge_once().await;
                debug!(
                    purged,
                    role = RETENTION_SWEEPER_ROLE,
                    "terminal-retention purge tick"
                );
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_defaults_are_a_week_kept_and_an_hourly_purge() {
        let c = TerminalRetentionConfig::default();
        assert_eq!(c.retention, std::time::Duration::from_secs(604_800), "P7D");
        assert_eq!(c.interval, std::time::Duration::from_secs(3600), "PT1H");
        assert!(
            c.interval < c.retention,
            "a purge cadence coarser than the retention window would keep rows for an \
             unpredictable multiple of the configured window"
        );
    }

    #[test]
    fn the_two_sweeper_roles_are_distinct_lease_names() {
        // They elect independently; a single lease would couple a minutes-scale GC to a
        // days-scale destructive purge.
        assert_ne!(INSTANCE_SWEEPER_ROLE, RETENTION_SWEEPER_ROLE);
        assert_eq!(RETENTION_SWEEPER_ROLE, "retention-sweeper");
    }

    #[test]
    fn defaults_are_the_documented_sweep_and_claim_windows() {
        let c = StuckInstanceScannerConfig::default();
        assert_eq!(c.interval, std::time::Duration::from_secs(60), "PT1M");
        assert_eq!(c.claim_timeout, std::time::Duration::from_secs(300), "PT5M");
        assert!(
            c.claim_timeout > c.interval,
            "a claim must outlive several sweep ticks — otherwise a live owner's instance \
             could be reclaimed mid-step"
        );
    }
}
