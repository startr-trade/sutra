//! `sutra test simulate` (F5) — a CLI wrapper over the P1-7 time-skipping test seam
//! (`sutra_engine::{TestClock, fast_forward_until}`) for app authors who want durable-timer
//! DX without writing a Rust test binary: boot a real engine against a sealed-archive
//! directory with a virtual clock installed, fast-forward it, report, shut down.
//!
//! # Naming: why `test simulate`, not `simulate`
//!
//! `sutra simulate` already exists and means something else entirely: a `--dry-run`-only
//! routing report over a single BPMN file (`crate::commands::simulate`) — no execution, no
//! engine boot, no datasource. Its own doc comment reserves the words "non-dry `simulate`" /
//! `sutra test` for a DIFFERENT future feature (full fixture execution with channel-call
//! stubs, parked to a later release). This command is neither of those: it never stubs a
//! channel, and it drives ALREADY-DEPLOYED BPMN through a REAL datasource — the "durable
//! timers, fast-forwarded" DX Temporal reviewers praise, not fixture execution. Rather than
//! overload the taken `simulate` verb or squat on the `sutra test` word for an unrelated
//! purpose, this lands as its own subcommand GROUP (`sutra test <action>`, currently just
//! `simulate`), leaving room for the originally-envisioned fixture-execution feature to claim
//! a sibling action (`sutra test run`, say) without a rename fight later.
//!
//! # Is this a hole in the "`TestClock` is operator-unreachable" invariant?
//!
//! No. [`sutra_engine::TestClock`]'s own docs are emphatic: "There is no config key, env var,
//! or CLI flag that installs it… wiring it into a boot is always an explicit Rust-level choice
//! at the call site." That invariant is about [`sutra_engine::EngineConfig::load`] — the ONLY
//! boot path the production `sutra-engine` binary (`sutra-dist`) uses, driven by
//! `sutra.*`/`SUTRA_*` config, which never touches `now_override`. This command is a
//! DIFFERENT binary (`sutra`, this crate) that constructs `EngineConfig` directly in Rust —
//! exactly the sanctioned pattern the docs describe, just packaged as a reusable tool instead
//! of a bespoke Testcontainers IT per application. `EngineConfig::load` itself is unchanged by
//! this file.
//!
//! # Safety posture
//!
//! This is a TEST tool. Fast-forwarding a virtual clock against a database that already holds
//! real, in-flight instances would durably fire their REAL timers early — a datasource mixup
//! (pointing this at something closer to production) must not silently do that. So:
//!
//! - `--datasource` is REQUIRED. A persistence-less simulate has no durable timers to
//!   advance (wait-state inbound already fails closed without one — the same posture
//!   `sutra_engine::serve` itself takes), so this command refuses to even try.
//! - Before booting anything, a cheap `COUNT(*) FROM instance_state` on the target database
//!   must come back zero, or the run refuses with [`exit::USAGE`] — unless `--allow-existing-data`
//!   acknowledges the risk explicitly.
//!
//! # Output convention
//!
//! Human-readable progress goes to STDERR (milestones: dir/datasource validated, migrations
//! applied, safety check passed, engine booted on its dynamic port, fast-forward started/
//! finished). The FINAL summary is one JSON object on STDOUT — nothing else touches stdout —
//! so `sutra test simulate … | jq .` is always safe, matching this CLI's stated convention
//! that stdout stays machine-consumable while logs/progress live on stderr. `--format` is not
//! interpreted by this command (the summary shape is the one accepted contract, not a
//! text/json choice).
//!
//! # Quiescence, precisely
//!
//! `--until-quiescent` fast-forwards until, simultaneously: no `waiting_event` row is an
//! armed `TIMER` (`status='WAITING'`), no `timer_schedule` row is armed (`status='SCHEDULED'`),
//! and no `instance_state` row is live (`terminal_at IS NULL`) — or `--timeout` elapses first.
//! A `FAILED` instance's `terminal_at` stays NULL BY DESIGN (it needs an operator, not a
//! clock), so a fixture that fails under fast-forward correctly reports `timedOut: true`
//! rather than a false "quiescent" — the JSON summary's `instancesFailed` field says why.
//!
//! # `schedulesFired`, precisely
//!
//! Computed from the `timer_schedule.remaining_fires` budget delta (before vs after) — exact
//! for single-shot and bounded (`R<n>/…`) cycles. An UNBOUNDED cycle (`R/…`, `remaining_fires
//! IS NULL`) has no budget to diff; its fires still show up in `instancesStarted` (each fire
//! mints one instance) but are not separately counted here in v1.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration as StdDuration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{PgPool, Row};
use sutra_bpmn::duration::parse_iso8601_duration;
use sutra_engine::{fast_forward_until, serve, DeploymentSourceKind, EngineConfig, TestClock};
use sutra_persistence::migrate::apply_migrations;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::exit;
use crate::output::Io;
use crate::GlobalArgs;

/// `sutra test …` — a namespace for engine-driving test tooling. `simulate` is the only
/// action today; see the module docs for why it is not folded into `sutra simulate`.
#[derive(Debug, clap::Args)]
pub struct TestArgs {
    #[command(subcommand)]
    pub action: TestAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum TestAction {
    /// Boot a real engine (dynamic port) against a sealed-deployments directory with a
    /// virtual clock installed, then fast-forward it so durable timers/schedules fire in
    /// real seconds instead of real wall-clock time.
    Simulate(SimulateArgs),
}

pub fn execute(args: TestArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    match args.action {
        TestAction::Simulate(simulate_args) => execute_simulate(simulate_args, global, io),
    }
}

#[derive(Debug, clap::Args)]
pub struct SimulateArgs {
    /// Directory of sealed `.sutra` deployment archives to serve.
    #[arg(long, value_name = "DIR")]
    pub deployments: PathBuf,

    #[command(flatten)]
    pub datasource: DatasourceArgs,

    /// Fast-forward the virtual clock by this ISO-8601 duration (e.g. `PT24H`), firing
    /// everything due along the way, then stop and report. Exactly one of `--advance` /
    /// `--until-quiescent` is required.
    #[arg(long, value_name = "DURATION", conflicts_with = "until_quiescent")]
    pub advance: Option<String>,

    /// Fast-forward until no armed timer/schedule remains and every instance is terminal —
    /// see the module docs for the precise definition — or `--timeout` elapses. Exactly one
    /// of `--advance` / `--until-quiescent` is required.
    #[arg(long, conflicts_with = "advance")]
    pub until_quiescent: bool,

    /// Real wall-clock budget for the fast-forward loop (ISO-8601 duration). Applies to BOTH
    /// modes — the safety ceiling against a fixture that never settles. Default `PT30S`.
    #[arg(long, value_name = "DURATION")]
    pub timeout: Option<String>,

    /// Virtual start instant (RFC 3339). Default: the real current instant.
    #[arg(long, value_name = "RFC3339")]
    pub start: Option<String>,

    /// Proceed even though the datasource already has `instance_state` rows. Without this
    /// flag the run refuses before booting anything — see the module docs' safety posture.
    #[arg(long)]
    pub allow_existing_data: bool,
}

/// Engine datasource connection settings — the same long-flag/env-fallback idiom
/// `sutra migrate`'s `ConnectionArgs` uses, but naming the ENGINE's own canonical keys
/// (`sutra.datasource.*` / `SUTRA_DATASOURCE_*`, see `sutra_engine::config`) rather than the
/// deploy-Job migration contract's `SUTRA_DB_*`: this command feeds
/// `EngineConfig::datasource_url/username/password` directly, so the env names that "just
/// work" here are the SAME ones a real engine container reads.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct DatasourceArgs {
    /// Engine datasource URL (`postgres://…`). REQUIRED — a persistence-less simulate has no
    /// durable timers to advance and is refused.
    #[arg(long = "datasource", env = "SUTRA_DATASOURCE_URL", value_name = "URL")]
    pub url: Option<String>,

    /// Datasource user (overrides any user embedded in the URL).
    #[arg(
        long = "datasource-username",
        env = "SUTRA_DATASOURCE_USERNAME",
        value_name = "USER"
    )]
    pub username: Option<String>,

    /// Datasource password (overrides any password embedded in the URL).
    #[arg(
        long = "datasource-password",
        env = "SUTRA_DATASOURCE_PASSWORD",
        hide_env_values = true,
        value_name = "PASSWORD"
    )]
    pub password: Option<String>,
}

/// The default real wall-clock budget for the fast-forward loop when `--timeout` is absent.
const DEFAULT_TIMEOUT: StdDuration = StdDuration::from_secs(30);

/// How many consecutive "nothing armed" real ticks `advance_to` tolerates before concluding
/// nothing more will arm and jumping straight to the target — bridges the same
/// just-committed-row race `fast_forward_until` guards against (a schedule armed synchronously
/// during boot is already visible by the first probe; this only matters if something is
/// racing this process from outside it) without paying an idle-step-per-200ms-of-target for a
/// fixture that truly has nothing left to fire.
const IDLE_GRACE_TICKS: u32 = 10;

fn execute_simulate(args: SimulateArgs, _global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    if !args.deployments.is_dir() {
        let _ = writeln!(
            io.err,
            "test simulate: deployments directory not found: {}",
            args.deployments.display()
        );
        return exit::USAGE;
    }

    let advance_duration = match &args.advance {
        Some(raw) => match parse_iso8601_duration(raw) {
            Ok(d) => Some(d),
            Err(e) => {
                let _ = writeln!(io.err, "test simulate: --advance {raw}: {e}");
                return exit::USAGE;
            }
        },
        None => None,
    };
    if advance_duration.is_none() && !args.until_quiescent {
        let _ = writeln!(
            io.err,
            "test simulate: exactly one of --advance <DURATION> or --until-quiescent is required"
        );
        return exit::USAGE;
    }

    let timeout = match args.timeout.as_deref() {
        Some(raw) => match parse_iso8601_duration(raw) {
            Ok(d) => d,
            Err(e) => {
                let _ = writeln!(io.err, "test simulate: --timeout {raw}: {e}");
                return exit::USAGE;
            }
        },
        None => DEFAULT_TIMEOUT,
    };

    let start = match args.start.as_deref() {
        Some(raw) => match OffsetDateTime::parse(raw, &Rfc3339) {
            Ok(t) => t,
            Err(e) => {
                let _ = writeln!(io.err, "test simulate: --start {raw}: not RFC 3339: {e}");
                return exit::USAGE;
            }
        },
        None => OffsetDateTime::now_utc(),
    };

    let Some(url) = args.datasource.url.clone() else {
        let _ = writeln!(
            io.err,
            "test simulate: --datasource (or SUTRA_DATASOURCE_URL) is required — time-skipping \
             fast-forwards DURABLE timers/schedules recorded in a real database; a \
             persistence-less simulate has nothing to advance and is refused"
        );
        return exit::USAGE;
    };

    block_on(async { run(&args, &url, advance_duration, timeout, start, io).await })
}

/// The four terminal outcomes `run` reports through `io.err` + the exit code — kept as a plain
/// enum rather than threading `-> i32` through every helper.
enum Outcome {
    Ok(serde_json::Value),
    Findings(serde_json::Value),
    Usage(String),
}

async fn run(
    args: &SimulateArgs,
    url: &str,
    advance_duration: Option<StdDuration>,
    timeout: StdDuration,
    start: OffsetDateTime,
    io: &mut Io<'_>,
) -> i32 {
    let outcome = run_inner(args, url, advance_duration, timeout, start, io).await;
    match outcome {
        Outcome::Ok(summary) => {
            let _ = writeln!(io.out, "{summary}");
            exit::OK
        }
        Outcome::Findings(summary) => {
            let _ = writeln!(io.out, "{summary}");
            exit::FINDINGS
        }
        Outcome::Usage(message) => {
            let _ = writeln!(io.err, "test simulate: {message}");
            exit::USAGE
        }
    }
}

async fn run_inner(
    args: &SimulateArgs,
    url: &str,
    advance_duration: Option<StdDuration>,
    timeout: StdDuration,
    start: OffsetDateTime,
    io: &mut Io<'_>,
) -> Outcome {
    let mut options = match PgConnectOptions::from_str(url) {
        Ok(o) => o,
        Err(e) => return Outcome::Usage(format!("invalid --datasource URL: {e}")),
    };
    if let Some(user) = &args.datasource.username {
        options = options.username(user);
    }
    if let Some(password) = &args.datasource.password {
        options = options.password(password);
    }

    let pool = match PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
    {
        Ok(p) => p,
        Err(e) => return Outcome::Usage(format!("cannot connect to the datasource: {e}")),
    };
    progress(io, "connected to the datasource");

    let scripts = match crate::embedded::engine_scripts() {
        Ok(s) => s,
        Err(e) => return Outcome::Usage(format!("embedded migration set: {e}")),
    };
    let mut conn = match pool.acquire().await {
        Ok(c) => c,
        Err(e) => return Outcome::Usage(format!("cannot acquire a connection: {e}")),
    };
    // `conn: PoolConnection<Postgres>` derefs to `PgConnection` (deref coercion at the call).
    if let Err(e) = apply_migrations(&mut conn, &scripts).await {
        return Outcome::Usage(format!("schema migration failed: {e}"));
    }
    drop(conn);
    progress(io, "schema up to date");

    let existing = match count_instances(&pool).await {
        Ok(n) => n,
        Err(e) => return Outcome::Usage(format!("safety check query failed: {e}")),
    };
    if existing > 0 && !args.allow_existing_data {
        return Outcome::Usage(format!(
            "the datasource already has {existing} instance_state row(s); refusing to \
             fast-forward its virtual clock (this would durably fire real timers early). Pass \
             --allow-existing-data to acknowledge and proceed"
        ));
    }
    if existing > 0 {
        progress(
            io,
            &format!("--allow-existing-data set: proceeding with {existing} pre-existing row(s)"),
        );
    }

    let clock = TestClock::new(start);
    let engine_config = EngineConfig {
        deployment_source: DeploymentSourceKind::Dir,
        deployments_dir: Some(args.deployments.clone()),
        http_port: 0,
        datasource_url: Some(url.to_owned()),
        datasource_username: args.datasource.username.clone(),
        datasource_password: args.datasource.password.clone(),
        rls_bypass_check_enabled: rls_bypass_check_enabled_from_env(),
        now_override: Some(clock.clone()),
        ..EngineConfig::default()
    };
    let engine = match serve(engine_config).await {
        Ok(e) => e,
        Err(e) => return Outcome::Usage(format!("engine failed to boot: {e}")),
    };
    progress(
        io,
        &format!(
            "engine booted on {} (dynamic port), deployments={}",
            engine.local_addr,
            args.deployments.display()
        ),
    );
    let base_url = format!("http://{}", engine.local_addr);

    let before_statuses = match fetch_instance_statuses(&base_url).await {
        Ok(s) => s,
        Err(e) => {
            engine.drain().await;
            return Outcome::Usage(format!("instance listing failed: {e}"));
        }
    };
    let before_timers = timers_resolved(&pool).await.unwrap_or(0);
    let before_schedules = schedule_rows(&pool).await;

    let virtual_start = clock.now();
    let wall_start = Instant::now();

    let (mode_label, reached) = match advance_duration {
        Some(d) => {
            let target = virtual_start + std_to_time_duration(d);
            progress(
                io,
                &format!("fast-forwarding: advancing the virtual clock to {target}"),
            );
            let reached = advance_to(&pool, &clock, target, timeout).await;
            ("advance", reached)
        }
        None => {
            progress(io, "fast-forwarding: driving to quiescence");
            let quiescent =
                fast_forward_until(&pool, &clock, timeout, || is_quiescent(&pool)).await;
            ("until-quiescent", quiescent)
        }
    };
    let timed_out = !reached;
    let wall_elapsed = wall_start.elapsed();
    let virtual_end = clock.now();

    progress(
        io,
        &format!(
            "fast-forward {} after {:.3}s wall time (virtual span {})",
            if timed_out { "TIMED OUT" } else { "settled" },
            wall_elapsed.as_secs_f64(),
            virtual_end - virtual_start
        ),
    );

    let after_statuses = match fetch_instance_statuses(&base_url).await {
        Ok(s) => s,
        Err(e) => {
            engine.drain().await;
            return Outcome::Usage(format!("instance listing failed: {e}"));
        }
    };
    let after_timers = timers_resolved(&pool).await.unwrap_or(before_timers);
    let after_schedules = schedule_rows(&pool).await;

    engine.drain().await;
    pool.close().await;
    progress(io, "engine shut down");

    let summary = serde_json::json!({
        "mode": mode_label,
        "deployments": args.deployments.display().to_string(),
        "allowExistingData": args.allow_existing_data,
        "preExistingInstances": existing,
        "virtualStart": rfc3339(virtual_start),
        "virtualEnd": rfc3339(virtual_end),
        "virtualSecondsAdvanced": (virtual_end - virtual_start).as_seconds_f64(),
        "wallSeconds": wall_elapsed.as_secs_f64(),
        "timedOut": timed_out,
        "quiescent": mode_label == "until-quiescent" && reached,
        "instancesStarted": after_statuses.total() - before_statuses.total(),
        "instancesCompleted": after_statuses.completed - before_statuses.completed,
        "instancesFailed": after_statuses.failed - before_statuses.failed,
        "instancesTerminated": after_statuses.terminated - before_statuses.terminated,
        "instancesLive": after_statuses.running + after_statuses.suspended,
        "timersFired": after_timers - before_timers,
        "schedulesFired": schedules_fired(&before_schedules, &after_schedules),
    });

    if timed_out {
        Outcome::Findings(summary)
    } else {
        Outcome::Ok(summary)
    }
}

fn progress(io: &mut Io<'_>, message: &str) {
    let _ = writeln!(io.err, "test simulate: {message}");
}

fn rfc3339(at: OffsetDateTime) -> String {
    at.format(&Rfc3339).unwrap_or_default()
}

/// `SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED`, read the same way `EngineConfig::load` reads
/// it (see `sutra_engine::config`) — this command builds `EngineConfig` directly rather than
/// through `load`, so it would otherwise silently ignore an operator's existing posture
/// choice. Unset or unrecognised ⇒ `true` (the engine's own fail-closed default).
fn rls_bypass_check_enabled_from_env() -> bool {
    match std::env::var("SUTRA_PERSISTENCE_RLS_BYPASS_CHECK_ENABLED") {
        Ok(raw) => !matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "false" | "0" | "no" | "off"
        ),
        Err(_) => true,
    }
}

fn std_to_time_duration(d: StdDuration) -> time::Duration {
    time::Duration::seconds_f64(d.as_secs_f64())
}

async fn count_instances(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM instance_state")
        .fetch_one(pool)
        .await
}

async fn timers_resolved(pool: &PgPool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM waiting_event WHERE kind = 'TIMER' AND status = 'RESOLVED'",
    )
    .fetch_one(pool)
    .await
}

/// `(deployment_id, process_id, node_id, remaining_fires)` for every `timer_schedule` row —
/// snapshotted before and after the fast-forward so [`schedules_fired`] can diff the budget.
type ScheduleRow = (String, String, String, Option<i32>);

async fn schedule_rows(pool: &PgPool) -> Vec<ScheduleRow> {
    sqlx::query_as("SELECT deployment_id, process_id, node_id, remaining_fires FROM timer_schedule")
        .fetch_all(pool)
        .await
        .unwrap_or_default()
}

/// See the module docs' "`schedulesFired`, precisely" section.
fn schedules_fired(before: &[ScheduleRow], after: &[ScheduleRow]) -> i64 {
    let mut total = 0i64;
    for (dep, proc_id, node, after_remaining) in after {
        let Some((_, _, _, before_remaining)) = before
            .iter()
            .find(|(d, p, n, _)| d == dep && p == proc_id && n == node)
        else {
            continue;
        };
        if let (Some(b), Some(a)) = (before_remaining, after_remaining) {
            total += i64::from(*b - *a).max(0);
        }
    }
    total
}

/// The earliest still-armed due instant across both temporal tables the timer poller claims.
/// Duplicated (in miniature) from `sutra_engine::test_clock`'s private probe — that helper is
/// deliberately not exported (see its own docs), and its condition-driven jump-to-next-due
/// loop does not fit `--advance`'s "advance to a fixed CEILING, firing whatever is due along
/// the way" semantics (see [`advance_to`]), so this command owns the query it needs directly
/// against the same schema `sutra_engine::fast_forward_until` already relies on.
async fn earliest_due(pool: &PgPool) -> Option<OffsetDateTime> {
    let row = sqlx::query(
        "SELECT MIN(due) AS due FROM ( \
           SELECT timer_due_at AS due FROM waiting_event \
             WHERE kind = 'TIMER' AND status = 'WAITING' \
           UNION ALL \
           SELECT next_due_at AS due FROM timer_schedule WHERE status = 'SCHEDULED' \
         ) armed",
    )
    .fetch_one(pool)
    .await
    .ok()?;
    row.try_get::<Option<OffsetDateTime>, _>("due")
        .ok()
        .flatten()
}

/// `--until-quiescent`'s condition — see the module docs' "Quiescence, precisely" section.
async fn is_quiescent(pool: &PgPool) -> bool {
    let armed_timers: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM waiting_event WHERE kind = 'TIMER' AND status = 'WAITING'",
    )
    .fetch_one(pool)
    .await
    .unwrap_or(1);
    let armed_schedules: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM timer_schedule WHERE status = 'SCHEDULED'")
            .fetch_one(pool)
            .await
            .unwrap_or(1);
    let live_instances: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM instance_state WHERE terminal_at IS NULL")
            .fetch_one(pool)
            .await
            .unwrap_or(1);
    armed_timers == 0 && armed_schedules == 0 && live_instances == 0
}

/// `--advance`'s fast-forward: repeat {jump `clock` to `min(earliest armed due, target)`, give
/// the timer poller's next real tick a moment to claim it, re-check} until `clock.now() >=
/// target` or `timeout` (real wall-clock) elapses. Unlike `fast_forward_until`, a due instant
/// PAST `target` never moves the clock past `target` — "advance by X" must not fire something
/// scheduled for X+1. Returns whether `target` was reached.
async fn advance_to(
    pool: &PgPool,
    clock: &TestClock,
    target: OffsetDateTime,
    timeout: StdDuration,
) -> bool {
    let idle_step = time::Duration::milliseconds(200);
    let real_poll = StdDuration::from_millis(20);
    let deadline = Instant::now() + timeout;
    let mut consecutive_idle = 0u32;
    loop {
        if clock.now() >= target {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        match earliest_due(pool).await {
            Some(due) if due > clock.now() => {
                clock.set(if due < target { due } else { target });
                consecutive_idle = 0;
            }
            Some(_) => {
                // Already due (armed but not yet claimed this tick) — give the poller a moment.
                consecutive_idle = 0;
            }
            None => {
                consecutive_idle += 1;
                if consecutive_idle > IDLE_GRACE_TICKS {
                    // Nothing has armed for several real ticks in a row — the boot-time race
                    // window (a schedule arming just after `serve()` returns) has long since
                    // closed, and nothing is left to fire before `target`; jump straight there
                    // instead of idle-stepping the whole gap.
                    clock.set(target);
                } else {
                    let remaining = target - clock.now();
                    clock.advance(if idle_step < remaining {
                        idle_step
                    } else {
                        remaining
                    });
                }
            }
        }
        tokio::time::sleep(real_poll).await;
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct StatusCounts {
    running: i64,
    suspended: i64,
    completed: i64,
    terminated: i64,
    failed: i64,
    other: i64,
}

impl StatusCounts {
    fn total(&self) -> i64 {
        self.running + self.suspended + self.completed + self.terminated + self.failed + self.other
    }
}

/// `GET /sutra/instances?includeTerminal=true` — the unauthenticated, cluster-internal operate
/// surface (NOT `/admin/*`; no OIDC gate to reach), reused rather than re-decoding snapshot
/// bytes here: the engine's own handler already applies the correct snapshot codec (typed
/// values, encryption) and is the single source of truth for status classification.
async fn fetch_instance_statuses(base_url: &str) -> Result<StatusCounts, String> {
    let url = format!("{base_url}/sutra/instances?includeTerminal=true");
    let response = reqwest::get(&url).await.map_err(|e| e.to_string())?;
    let body: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    let mut counts = StatusCounts::default();
    for instance in body["instances"].as_array().into_iter().flatten() {
        match instance["status"].as_str().unwrap_or("") {
            "RUNNING" => counts.running += 1,
            "SUSPENDED" => counts.suspended += 1,
            "COMPLETED" => counts.completed += 1,
            "TERMINATED" => counts.terminated += 1,
            "FAILED" => counts.failed += 1,
            _ => counts.other += 1,
        }
    }
    Ok(counts)
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::SutraCli;
    use clap::Parser;

    /// Every case below fails validation BEFORE the async body ever touches a socket (bad dir /
    /// bad flags / missing datasource), so it is safe to drive `execute_simulate` directly
    /// without docker.
    fn run(args: SimulateArgs) -> (i32, String, String) {
        run_captured("", |io| execute_simulate(args, &GlobalArgs::default(), io))
    }

    fn base_args() -> SimulateArgs {
        SimulateArgs {
            deployments: PathBuf::from("/does/not/exist"),
            datasource: DatasourceArgs::default(),
            advance: None,
            until_quiescent: false,
            timeout: None,
            start: None,
            allow_existing_data: false,
        }
    }

    #[test]
    fn missing_deployments_dir_is_a_usage_error() {
        let (code, _, err) = run(base_args());
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("deployments directory not found"), "{err}");
    }

    #[test]
    fn missing_mode_is_a_usage_error() {
        let dir = std::env::temp_dir();
        let mut args = base_args();
        args.deployments = dir;
        let (code, _, err) = run(args);
        assert_eq!(code, exit::USAGE);
        assert!(
            err.contains("exactly one of --advance") || err.contains("--until-quiescent"),
            "{err}"
        );
    }

    #[test]
    fn malformed_advance_duration_is_a_usage_error() {
        let mut args = base_args();
        args.deployments = std::env::temp_dir();
        args.advance = Some("tomorrow".to_owned());
        let (code, _, err) = run(args);
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("--advance"), "{err}");
    }

    #[test]
    fn malformed_timeout_is_a_usage_error() {
        let mut args = base_args();
        args.deployments = std::env::temp_dir();
        args.until_quiescent = true;
        args.timeout = Some("nope".to_owned());
        let (code, _, err) = run(args);
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("--timeout"), "{err}");
    }

    #[test]
    fn malformed_start_instant_is_a_usage_error() {
        let mut args = base_args();
        args.deployments = std::env::temp_dir();
        args.until_quiescent = true;
        args.start = Some("not-a-date".to_owned());
        let (code, _, err) = run(args);
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("--start"), "{err}");
    }

    /// Both halves of the `--datasource`/`SUTRA_DATASOURCE_URL` story share one test — env
    /// mutation must not race `cli_parses_test_simulate_advance`-style tests, so absence and
    /// the env fallback are asserted back to back inside one serialised test function (the
    /// same discipline `sutra_cli::tests::migrate_reads_the_deploy_contract_environment_variables`
    /// uses in `lib.rs`).
    #[test]
    fn missing_datasource_is_a_usage_error_and_the_env_fallback_is_read() {
        std::env::remove_var("SUTRA_DATASOURCE_URL");
        let mut args = base_args();
        args.deployments = std::env::temp_dir();
        args.until_quiescent = true;
        let (code, _, err) = run(args);
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("--datasource"), "{err}");
        assert!(err.contains("SUTRA_DATASOURCE_URL"), "{err}");

        std::env::set_var("SUTRA_DATASOURCE_URL", "postgres://envhost/db");
        let cli = SutraCli::try_parse_from([
            "sutra",
            "test",
            "simulate",
            "--deployments",
            "/tmp/deps",
            "--until-quiescent",
        ])
        .expect("parses");
        std::env::remove_var("SUTRA_DATASOURCE_URL");
        let crate::commands::Command::Test(test_args) = cli.command else {
            panic!("expected test");
        };
        let TestAction::Simulate(simulate) = test_args.action;
        assert_eq!(
            simulate.datasource.url.as_deref(),
            Some("postgres://envhost/db")
        );
    }

    #[test]
    fn schedules_fired_diffs_the_remaining_fires_budget() {
        let before: Vec<ScheduleRow> = vec![
            ("d".into(), "p".into(), "n1".into(), Some(3)),
            ("d".into(), "p".into(), "n2".into(), None),
        ];
        let after: Vec<ScheduleRow> = vec![
            ("d".into(), "p".into(), "n1".into(), Some(0)),
            ("d".into(), "p".into(), "n2".into(), None),
        ];
        // n1: bounded R3 budget spent -> 3 fires. n2: unbounded, not separately counted.
        assert_eq!(schedules_fired(&before, &after), 3);
    }

    #[test]
    fn cli_parses_test_simulate_advance() {
        let cli = SutraCli::try_parse_from([
            "sutra",
            "test",
            "simulate",
            "--deployments",
            "/tmp/deps",
            "--datasource",
            "postgres://db/x",
            "--advance",
            "PT24H",
        ])
        .expect("parses");
        let crate::commands::Command::Test(test_args) = cli.command else {
            panic!("expected test");
        };
        let TestAction::Simulate(simulate) = test_args.action;
        assert_eq!(simulate.deployments, PathBuf::from("/tmp/deps"));
        assert_eq!(simulate.datasource.url.as_deref(), Some("postgres://db/x"));
        assert_eq!(simulate.advance.as_deref(), Some("PT24H"));
        assert!(!simulate.until_quiescent);
    }

    #[test]
    fn cli_parses_test_simulate_until_quiescent_with_timeout_and_allow_existing_data() {
        let cli = SutraCli::try_parse_from([
            "sutra",
            "test",
            "simulate",
            "--deployments",
            "/tmp/deps",
            "--datasource",
            "postgres://db/x",
            "--until-quiescent",
            "--timeout",
            "PT1M",
            "--allow-existing-data",
        ])
        .expect("parses");
        let crate::commands::Command::Test(test_args) = cli.command else {
            panic!("expected test");
        };
        let TestAction::Simulate(simulate) = test_args.action;
        assert!(simulate.until_quiescent);
        assert_eq!(simulate.timeout.as_deref(), Some("PT1M"));
        assert!(simulate.allow_existing_data);
    }

    #[test]
    fn cli_rejects_advance_and_until_quiescent_together() {
        let err = SutraCli::try_parse_from([
            "sutra",
            "test",
            "simulate",
            "--deployments",
            "/tmp/deps",
            "--datasource",
            "postgres://db/x",
            "--advance",
            "PT1H",
            "--until-quiescent",
        ])
        .expect_err("clap rejects the mutually exclusive pair");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }
}
