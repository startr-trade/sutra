//! The durable temporal poller: a leader-gated tokio interval loop that, per deployment, claims
//! the two kinds of due temporal row (`FOR UPDATE SKIP LOCKED` — the outbox `next_attempt_at`
//! pattern) and drives each through the engine actor:
//!
//! - DUE `waiting_event` TIMER rows → the RESUME path. An instance is parked at a timer catch /
//!   timer boundary and the clock has caught up with it.
//! - DUE `timer_schedule` rows → the SPAWN path. A deployment declares a timer `<startEvent>`,
//!   and its occurrence has come round: no instance exists yet, so the fire MINTS one.
//!
//! One loop, one leader gate and one batch budget cover both, because they are the same
//! question asked twice ("what is due?") and splitting them would mean two clocks to reason
//! about. The two claims touch different tables and never contend.
//!
//! Firing is at-least-once: the resume step resolves the row atomically with the new
//! quiescent point; a redundant claim (crash between claim and resume, or a racing
//! replica) finds the row RESOLVED or the frontier moved on and no-ops (`Stale`). Leader
//! gating: an injected [`LeaderGate`] under the [`TIMER_LEADER_ROLE`] lease role —
//! [`AlwaysLeading`] by default; `serve` injects the DB-lease election gate.

use std::sync::Arc;

use sqlx::PgPool;
use sutra_bpmn::timer::{TimerCycleAdvance, TimerDefinition};
use sutra_channels::http::EngineHandle;
use sutra_channels::{LeaderGate, ScheduledStartFire, ScheduledStartOutcome, TimerFireOutcome};
use sutra_executor::TimerFire;
use sutra_persistence::stores::{
    DueTimer, DueTimerSchedule, PgTimerScheduleStore, PgWaitStateStore, WaitStateStore,
};
use sutra_persistence::DeploymentId as PersistDeploymentId;
use time::OffsetDateTime;
use tracing::{debug, warn};

/// The DB-lease role the timer poller's leader election runs under (the election daemon
/// owns the lease; the gate is injected here).
pub const TIMER_LEADER_ROLE: &str = "timer-leader";

/// Poller knobs. Defaults: 500 ms tick, 32 rows per claim (a tick keeps claiming while claims
/// come back full — see [`spawn_timer_poller`]), 5 s failure-defer backoff.
#[derive(Debug, Clone)]
pub struct TimerPollerConfig {
    pub tick: std::time::Duration,
    pub batch: i64,
    pub retry_backoff: std::time::Duration,
    /// TEST-ONLY (P1-7 time-skipping test runtime): the per-tick claim `now` reads
    /// [`sutra_executor::TestClock::now`] instead of [`OffsetDateTime::now_utc`] when set.
    /// `None` (the [`Default`] value, and the only value any production boot ever installs —
    /// see `sutra_engine::EngineConfig::now_override`) is byte-identical to before this field
    /// existed.
    pub now_override: Option<sutra_executor::TestClock>,
}

impl Default for TimerPollerConfig {
    fn default() -> TimerPollerConfig {
        TimerPollerConfig {
            tick: std::time::Duration::from_millis(500),
            batch: 32,
            retry_backoff: std::time::Duration::from_secs(5),
            now_override: None,
        }
    }
}

/// Spawn the poller task. `deployments` is the LIVE id set (active + DRAINING — a
/// parked timer pinned to a flipped-away deployment must still fire), read per tick so
/// a deployment activation flip is picked up on the next tick. The task runs until aborted
/// (see `RunningEngine::shutdown`).
///
/// The poller stays ONE per replica, lease-gated. Each tick DRAINS the due timers: it keeps
/// claiming `batch` rows per deployment while claims come back full, for up to a drain budget,
/// so the fire rate follows the due rate instead of being capped at `batch` per tick. Claimed
/// rows are driven with up to max(S, 16) fires in flight, S = the engine's shard count — a
/// sequential loop would leave lanes idle through a timer storm and make every fire wait out a
/// whole lane round trip. Two due timers for one instance serialize on its shard's queue or
/// bounce on the claim (defer + backoff) — both existing behaviors.
pub fn spawn_timer_poller(
    pool: PgPool,
    deployments: sutra_channels::LiveDeploymentSet,
    engine: EngineHandle,
    gate: Arc<dyn LeaderGate>,
    config: TimerPollerConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let schedules = PgTimerScheduleStore::new(pool.clone());
        let store = PgWaitStateStore::new(pool.clone());
        let max_in_flight = fire_concurrency(engine.shard_count());
        let drain_budget = config.tick.max(MIN_DRAIN_BUDGET);
        let mut interval = tokio::time::interval(config.tick);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if !gate.is_leading() {
                continue;
            }
            // Executor-form ids arrive from the loader; persistence-form ids key the claims.
            let deployments: Vec<(sutra_executor::DeploymentId, PersistDeploymentId)> = deployments
                .snapshot()
                .into_iter()
                .filter_map(|d| match PersistDeploymentId::new(d.value()) {
                    Ok(p) => Some((d, p)),
                    Err(e) => {
                        warn!(deployment = d.value(), error = %e, "timer poller skips deployment");
                        None
                    }
                })
                .collect();

            // DUE TIMERS — drained, not sampled. One claim of `batch` rows per deployment per
            // round; a deployment whose claim came back FULL has more due rows behind it, so it
            // gets another round straight away instead of waiting for the next tick. Rounds go
            // round-robin over the deployments still behind, so one deployment's backlog never
            // starves another's, and stop once every claim came back short, the drain budget is
            // spent, or this replica lost the lease. Without this the poller fired at most
            // `batch` timers per deployment per tick (64/s at the defaults) whatever the arrival
            // rate, and under sustained load every parked timer fired later than the last.
            //
            // Each round's fires are all awaited before the next claim. That is what keeps the
            // drain at-most-once per row per round: the claim's row locks end with its
            // transaction, so a re-claim while a fire was still in flight would hand the same row
            // out twice. After the await every claimed row is RESOLVED (fired or stale) or
            // deferred into the future, so the next claim sees only rows it has not fired yet.
            // A row whose resolve/defer itself failed comes back in the next round; the budget
            // bounds that, exactly as the next tick bounded it before.
            let drain_started = std::time::Instant::now();
            let mut behind: Vec<&(sutra_executor::DeploymentId, PersistDeploymentId)> =
                deployments.iter().collect();
            while !behind.is_empty() {
                let mut still_behind = Vec::new();
                for dep in behind {
                    let (exec_dep, persist_dep) = dep;
                    let claimed = fire_due_timers(
                        &pool,
                        &store,
                        &engine,
                        exec_dep,
                        persist_dep,
                        &config,
                        max_in_flight,
                    )
                    .await;
                    if claimed >= config.batch.max(1) as usize {
                        still_behind.push(dep);
                    }
                }
                behind = still_behind;
                if drain_started.elapsed() >= drain_budget || !gate.is_leading() {
                    if !behind.is_empty() {
                        debug!(
                            deployments_behind = behind.len(),
                            "timer drain budget spent with due timers left — next tick resumes"
                        );
                    }
                    break;
                }
            }

            for (exec_dep, persist_dep) in &deployments {
                let now = poll_now(&config);
                // The SECOND claim on the same tick: due timer-START schedules. Deliberately the
                // same loop, the same leader gate and the same batch budget — one clock drives
                // every temporal thing the engine does. A DRAINING deployment is in this id set
                // (its parked instances must still resume) but has no armed schedules, because
                // the activation flip resolved them when it stopped being ACTIVE, so this claim
                // is a cheap no-op for it. One claim per tick is enough here: a schedule row is
                // one per timer start event, not one per instance.
                let due_schedules = match schedules.claim_due(persist_dep, now, config.batch).await
                {
                    Ok(due) => due,
                    Err(e) => {
                        warn!(deployment = exec_dep.value(), error = %e, "due-schedule claim failed");
                        continue;
                    }
                };
                // Schedule fires (spawns, round-robin arrival — never a hop) get the same
                // bounded fan-out; row advancement is per-row and rides each task.
                let mut in_flight = tokio::task::JoinSet::new();
                for schedule in due_schedules {
                    if in_flight.len() >= max_in_flight {
                        let _ = in_flight.join_next().await;
                    }
                    let schedules = PgTimerScheduleStore::new(pool.clone());
                    let engine = engine.clone();
                    let exec_dep = exec_dep.clone();
                    let persist_dep = persist_dep.clone();
                    in_flight.spawn(async move {
                        fire_schedule(&schedules, &engine, &exec_dep, &persist_dep, schedule, now)
                            .await;
                    });
                }
                while in_flight.join_next().await.is_some() {}
            }
        }
    })
}

/// The floor on how long one tick may keep draining due timers before it yields to the next
/// tick (and re-reads the live deployment set). A tick shorter than this still drains for this
/// long; a longer tick drains for its own length.
const MIN_DRAIN_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);

/// The floor on fires in flight at once. The fires of one claim are independent instances, and
/// each one is a request queued onto the owning shard's lane, which already serializes the
/// fires of any one instance — so more than one in flight changes nothing about ordering. What
/// it changes is throughput: with one fire in flight the poller waits out a full queue-and-commit
/// round trip per timer, behind every new-instance request already in the lane, while inbound
/// traffic has as many requests in flight as it has callers.
const MIN_FIRE_CONCURRENCY: usize = 16;

/// Fires in flight at once for an engine with `shards` lanes: at least one per lane, and never
/// fewer than [`MIN_FIRE_CONCURRENCY`].
fn fire_concurrency(shards: u32) -> usize {
    (shards.max(1) as usize).max(MIN_FIRE_CONCURRENCY)
}

/// The claim `now`. TEST-ONLY (P1-7): `config.now_override` is `None` on every production boot.
fn poll_now(config: &TimerPollerConfig) -> OffsetDateTime {
    config
        .now_override
        .as_ref()
        .map(sutra_executor::TestClock::now)
        .unwrap_or_else(OffsetDateTime::now_utc)
}

/// One claim of due timer rows for one deployment, every claimed row fired (at most
/// `max_in_flight` at once) and awaited. Returns how many rows the claim handed out, so the
/// caller can tell a drained deployment (short claim) from one with more due rows behind it.
async fn fire_due_timers(
    pool: &PgPool,
    store: &PgWaitStateStore,
    engine: &EngineHandle,
    exec_dep: &sutra_executor::DeploymentId,
    persist_dep: &PersistDeploymentId,
    config: &TimerPollerConfig,
    max_in_flight: usize,
) -> usize {
    let now = poll_now(config);
    let due = match store.claim_due_timers(persist_dep, now, config.batch).await {
        Ok(due) => due,
        Err(e) => {
            warn!(deployment = exec_dep.value(), error = %e, "due-timer claim failed");
            return 0;
        }
    };
    let claimed = due.len();
    // Bounded fan-out: the fires are spawned tasks (each with its own cheap pool-backed store
    // clone), so a slow fire delays only its window slot, never the claim.
    let mut in_flight = tokio::task::JoinSet::new();
    for timer in due {
        if in_flight.len() >= max_in_flight {
            let _ = in_flight.join_next().await;
        }
        let store = PgWaitStateStore::new(pool.clone());
        let engine = engine.clone();
        let exec_dep = exec_dep.clone();
        let persist_dep = persist_dep.clone();
        let config = config.clone();
        in_flight.spawn(async move {
            fire_one(
                &store,
                &engine,
                &exec_dep,
                &persist_dep,
                timer,
                now,
                &config,
            )
            .await;
        });
    }
    while in_flight.join_next().await.is_some() {}
    claimed
}

/// Fire one claimed due timer-START schedule, then move the row to whatever comes next.
///
/// The row's fate after the fire is entirely a function of its KIND, computed by
/// [`sutra_bpmn::timer::advance_cycle`] so the poller holds no calendar arithmetic of its own:
/// a single-shot `DURATION`/`DATE` schedule RESOLVES (it has done its one job), and a `CYCLE`
/// either advances to its next occurrence or resolves when the `R<n>` budget is spent.
///
/// Failure posture, and it differs from a timer FIRE on purpose: a timer fire resumes an
/// existing instance, so deferring and retrying is free. A schedule MINTS one, so a retry loop
/// on a permanently-broken start would mint an unbounded pile of failed instances. The row is
/// therefore advanced past the failed occurrence exactly as if it had succeeded — the occurrence
/// is LOST, loudly (an error-level log naming the deployment, process and cause), and the next
/// occurrence still happens on time. A single-shot schedule that fails simply resolves.
async fn fire_schedule(
    store: &PgTimerScheduleStore,
    engine: &EngineHandle,
    exec_dep: &sutra_executor::DeploymentId,
    persist_dep: &PersistDeploymentId,
    schedule: DueTimerSchedule,
    now: OffsetDateTime,
) {
    let rfc3339 = &time::format_description::well_known::Rfc3339;
    let fire = ScheduledStartFire {
        deployment: exec_dep.clone(),
        tenant: schedule.tenant.clone(),
        module_key: schedule.module_key.clone(),
        process_id: schedule.process_id.clone(),
        node_id: schedule.node_id.clone(),
        due_at: schedule.next_due_at.format(rfc3339).unwrap_or_default(),
        fired_at: now.format(rfc3339).unwrap_or_default(),
    };
    match engine.start_scheduled(fire).await {
        Ok(ScheduledStartOutcome::Started { instance_id, .. }) => {
            debug!(
                deployment = exec_dep.value(),
                instance_id,
                process = schedule.process_id,
                node_id = schedule.node_id,
                "timer start fired"
            );
        }
        // Unreachable: `EngineHandle::start_scheduled` drives a synchronous chain to its end.
        Ok(ScheduledStartOutcome::InlineCalls(_)) => {}
        Ok(ScheduledStartOutcome::Stale) => {
            // The row outlived its model (process or start node gone, or no longer
            // timer-triggered). Resolve it so it stops claiming a slot every tick.
            warn!(
                deployment = exec_dep.value(),
                process = schedule.process_id,
                node_id = schedule.node_id,
                "timer-start schedule no longer matches its model — resolving the row"
            );
            resolve_schedule(store, exec_dep, persist_dep, &schedule).await;
            return;
        }
        Err(diagnostic) => {
            tracing::error!(
                deployment = exec_dep.value(),
                process = schedule.process_id,
                node_id = schedule.node_id,
                code = diagnostic.code,
                message = diagnostic.message,
                "timer start FAILED to fire — this occurrence is lost; the schedule continues"
            );
        }
    }
    advance_schedule(store, exec_dep, persist_dep, &schedule, now).await;
}

/// Move a fired schedule row on: resolve a spent one, advance a live cycle.
async fn advance_schedule(
    store: &PgTimerScheduleStore,
    exec_dep: &sutra_executor::DeploymentId,
    persist_dep: &PersistDeploymentId,
    schedule: &DueTimerSchedule,
    now: OffsetDateTime,
) {
    let timer = match TimerDefinition::from_persisted(&schedule.kind, &schedule.spec) {
        Ok(t) => t,
        Err(rejection) => {
            // A row this engine cannot read back is inert data — resolve it rather than
            // re-claiming it forever.
            warn!(
                deployment = exec_dep.value(),
                node_id = schedule.node_id,
                reason = %rejection,
                "timer-start schedule row is unreadable — resolving it"
            );
            resolve_schedule(store, exec_dep, persist_dep, schedule).await;
            return;
        }
    };
    let TimerDefinition::Cycle(spec) = &timer else {
        // Single-shot: it has fired, so it is done.
        resolve_schedule(store, exec_dep, persist_dep, schedule).await;
        return;
    };
    let remaining = schedule.remaining_fires.map(|n| n.max(0) as u32);
    match sutra_bpmn::timer::advance_cycle(spec, schedule.next_due_at, remaining, now) {
        Ok(TimerCycleAdvance::Next { due_at, remaining }) => {
            if let Err(e) = store
                .advance(
                    persist_dep,
                    &schedule.process_id,
                    &schedule.node_id,
                    due_at,
                    remaining.map(|n| n as i32),
                )
                .await
            {
                warn!(
                    deployment = exec_dep.value(),
                    node_id = schedule.node_id,
                    error = %e,
                    "timer-start schedule could not be advanced — it will re-fire this occurrence"
                );
            }
        }
        Ok(TimerCycleAdvance::Exhausted) => {
            debug!(
                deployment = exec_dep.value(),
                node_id = schedule.node_id,
                "timer-start cycle exhausted its repeat budget — resolving"
            );
            resolve_schedule(store, exec_dep, persist_dep, schedule).await;
        }
        Err(rejection) => {
            warn!(
                deployment = exec_dep.value(),
                node_id = schedule.node_id,
                reason = %rejection,
                "timer-start cycle could not be advanced — resolving the row"
            );
            resolve_schedule(store, exec_dep, persist_dep, schedule).await;
        }
    }
}

async fn resolve_schedule(
    store: &PgTimerScheduleStore,
    exec_dep: &sutra_executor::DeploymentId,
    persist_dep: &PersistDeploymentId,
    schedule: &DueTimerSchedule,
) {
    if let Err(e) = store
        .resolve(persist_dep, &schedule.process_id, &schedule.node_id)
        .await
    {
        warn!(
            deployment = exec_dep.value(),
            node_id = schedule.node_id,
            error = %e,
            "timer-start schedule could not be resolved"
        );
    }
}

async fn fire_one(
    store: &PgWaitStateStore,
    engine: &EngineHandle,
    exec_dep: &sutra_executor::DeploymentId,
    persist_dep: &PersistDeploymentId,
    timer: DueTimer,
    now: OffsetDateTime,
    config: &TimerPollerConfig,
) {
    let rfc3339 = &time::format_description::well_known::Rfc3339;
    let fire = TimerFire {
        deployment: exec_dep.clone(),
        instance_id: timer.instance_id.to_string(),
        node_id: timer.node_id.clone(),
        due_at: timer.due_at.format(rfc3339).unwrap_or_default(),
        fired_at: now.format(rfc3339).unwrap_or_default(),
    };
    match engine.fire_timer(fire).await {
        Ok(TimerFireOutcome::Resumed {
            instance_id,
            completed,
        }) => {
            debug!(
                deployment = exec_dep.value(),
                instance_id,
                node_id = timer.node_id,
                completed,
                "timer fired and resumed"
            );
            // The resume step resolved the TIMER row atomically — nothing else to do.
        }
        // Unreachable: `EngineHandle::fire_timer` drives a synchronous chain to its end.
        Ok(TimerFireOutcome::InlineCalls(_)) => {}
        Ok(TimerFireOutcome::Stale) => {
            // The row outlived its instance/frontier — resolve it so it stops firing.
            if let Err(e) = store
                .resolve(persist_dep, timer.instance_id, &timer.node_id)
                .await
            {
                warn!(
                    deployment = exec_dep.value(),
                    node_id = timer.node_id,
                    error = %e,
                    "stale timer row could not be resolved"
                );
            }
        }
        Err(diagnostic) if diagnostic.code == sutra_channels::codes::DISPATCH_INSTANCE_FAILED => {
            // The instance is durably FAILED — the fire failed CLOSED rather than re-driving a
            // dead flow. Resolve the row like the Stale branch: deferring it would re-claim and
            // re-fail every backoff tick forever, turning one dead instance into a permanent hot
            // loop. (The failure commit resolves an instance's rows itself; reaching here means
            // this row was claimed in the same window, so it still needs resolving.)
            warn!(
                deployment = exec_dep.value(),
                instance_id = %timer.instance_id,
                node_id = timer.node_id,
                code = diagnostic.code,
                "timer fire refused: the instance is FAILED — resolving the row so it stops firing"
            );
            if let Err(e) = store
                .resolve(persist_dep, timer.instance_id, &timer.node_id)
                .await
            {
                warn!(
                    deployment = exec_dep.value(),
                    node_id = timer.node_id,
                    error = %e,
                    "failed instance's timer row could not be resolved"
                );
            }
        }
        Err(diagnostic) => {
            // An uncaught timeout error already killed the instance (its failure step resolved
            // the row — the defer below is a no-op then); an infra failure
            // leaves the row WAITING, so push it out by the backoff instead of
            // hot-looping every tick.
            warn!(
                deployment = exec_dep.value(),
                instance_id = %timer.instance_id,
                node_id = timer.node_id,
                code = diagnostic.code,
                message = diagnostic.message,
                "timer fire failed"
            );
            let new_due = now + config.retry_backoff;
            if let Err(e) = store
                .defer_timer(persist_dep, timer.instance_id, &timer.node_id, new_due)
                .await
            {
                warn!(node_id = timer.node_id, error = %e, "timer defer failed");
            }
        }
    }
}
