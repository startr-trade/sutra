//! `sutra coverage cycle declare|activate|deactivate|list` — compliance cycles.
//!
//! A cycle is a free-form label an operator declares for one measurement campaign, typically one
//! release of the system under test. While a cycle is active, everything the engine records goes
//! to it; with none active, coverage records as it always has. At most one cycle is active per
//! deployment, and the database enforces that. The engine reads the active cycle on each mark, so
//! these commands talk only to the coverage store and the running engine needs no signal.

use serde_json::json;
use sutra_datastore::{CoverageCycleActivation, CoverageStore};

use super::coverage::{block_on, codes, open_coverage_store};
use crate::exit;
use crate::output::{Io, ReportFormat};

#[derive(Debug, clap::Args)]
pub struct CycleArgs {
    #[command(subcommand)]
    pub action: CycleAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum CycleAction {
    /// Declare a free-form label as a cycle and seed its paths uncovered. Records nothing until
    /// the cycle is activated.
    Declare(CycleLabelArgs),
    /// Route all coverage to a declared cycle. Refused while another cycle is active. Activating
    /// an earlier cycle again continues it.
    Activate(CycleLabelArgs),
    /// Stop routing coverage to a cycle. Its marks stay, and coverage records outside any cycle
    /// again.
    Deactivate(CycleLabelArgs),
    /// List the declared cycles with their coverage, and the no-cycle coverage beside them.
    List(CycleStoreArgs),
}

#[derive(Debug, clap::Args)]
pub struct CycleLabelArgs {
    /// The cycle's label.
    #[arg(value_name = "LABEL")]
    pub label: String,

    #[command(flatten)]
    pub store: CycleStoreArgs,
}

#[derive(Debug, clap::Args)]
pub struct CycleStoreArgs {
    /// The sealed `.sutra` deployment archive the cycle belongs to.
    #[arg(long, value_name = "FILE")]
    pub archive: std::path::PathBuf,

    /// Coverage-store database URL.
    #[arg(long, env = "SUTRA_DB_URL", value_name = "URL")]
    pub database_url: Option<String>,

    /// Coverage-store database user (overrides any user embedded in the URL).
    #[arg(long, env = "SUTRA_DB_USERNAME", value_name = "USER")]
    pub db_user: Option<String>,

    /// Coverage-store database password (overrides any password embedded in the URL).
    #[arg(
        long,
        env = "SUTRA_DB_PASSWORD",
        hide_env_values = true,
        value_name = "PASSWORD"
    )]
    pub db_password: Option<String>,
}

/// What one cycle command came to, ready to render.
struct CycleOutcome {
    command: &'static str,
    deployment_id: String,
    fields: serde_json::Value,
    text: String,
    exit: i32,
}

pub(super) fn execute(args: CycleArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let (command, store_args) = match &args.action {
        CycleAction::Declare(a) => ("coverage cycle declare", &a.store),
        CycleAction::Activate(a) => ("coverage cycle activate", &a.store),
        CycleAction::Deactivate(a) => ("coverage cycle deactivate", &a.store),
        CycleAction::List(a) => ("coverage cycle list", a),
    };
    let loaded = match sutra_loader::read_archive_file(&store_args.archive) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "{command}: cannot read {}: {e}",
                store_args.archive.display()
            );
            return exit::USAGE;
        }
    };
    let deployment_id = loaded.id.value().to_string();
    let declared = loaded.deployment.declared_coverage_urns();

    // Pool + work in ONE runtime: a pool built under one `block_on` and driven under a second
    // wedges on the first runtime's dropped reactor until the acquire timeout.
    let result = block_on(async {
        let store = open_coverage_store(
            store_args.database_url.as_deref(),
            store_args.db_user.as_deref(),
            store_args.db_password.as_deref(),
        )?
        .inner;
        match &args.action {
            CycleAction::Declare(a) => declare(&store, &deployment_id, &a.label, &declared).await,
            CycleAction::Activate(a) => activate(&store, &deployment_id, &a.label).await,
            CycleAction::Deactivate(a) => deactivate(&store, &deployment_id, &a.label).await,
            CycleAction::List(_) => list(&store, &deployment_id).await,
        }
        .map_err(|e| format!("coverage store error: {e}"))
    });
    let outcome = match result {
        Ok(o) => o,
        Err(msg) => {
            let _ = writeln!(io.err, "{command}: {msg}");
            return exit::USAGE;
        }
    };
    render(&outcome, command, format, io);
    outcome.exit
}

fn render(outcome: &CycleOutcome, command: &str, format: ReportFormat, io: &mut Io<'_>) {
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "{} — deployment {}: {}",
                outcome.command, outcome.deployment_id, outcome.text
            );
        }
        ReportFormat::Json => {
            let mut payload = json!({
                "command": command,
                "deploymentId": outcome.deployment_id,
            });
            if let (Some(target), Some(fields)) =
                (payload.as_object_mut(), outcome.fields.as_object())
            {
                target.extend(fields.clone());
            }
            let _ = writeln!(io.out, "{payload}");
        }
    }
}

async fn declare(
    store: &CoverageStore,
    deployment_id: &str,
    label: &str,
    declared: &[String],
) -> Result<CycleOutcome, sutra_datastore::DataStoreError> {
    let newly = store.declare_cycle(deployment_id, label).await?;
    // Every declared path counts from the start, so an untouched path reads as uncovered rather
    // than as absent. Idempotent: a repeat declaration leaves an already-covered flag alone.
    store
        .in_cycle(Some(label))
        .seed_declared(deployment_id, declared)
        .await?;
    let outcome = if newly {
        "declared"
    } else {
        "already-declared"
    };
    Ok(CycleOutcome {
        command: "coverage cycle declare",
        deployment_id: deployment_id.to_string(),
        fields: json!({ "cycle": label, "outcome": outcome, "total": declared.len() }),
        text: format!(
            "cycle '{label}' {outcome}, {} path(s) seeded",
            declared.len()
        ),
        exit: exit::OK,
    })
}

async fn activate(
    store: &CoverageStore,
    deployment_id: &str,
    label: &str,
) -> Result<CycleOutcome, sutra_datastore::DataStoreError> {
    let activation = store.activate_cycle(deployment_id, label).await?;
    let (fields, text, code) = match &activation {
        CoverageCycleActivation::Activated => (
            json!({ "cycle": label, "outcome": "activated", "activeCycle": label }),
            format!("cycle '{label}' is active; coverage now records to it"),
            exit::OK,
        ),
        CoverageCycleActivation::AlreadyActive => (
            json!({ "cycle": label, "outcome": "already-active", "activeCycle": label }),
            format!("cycle '{label}' was already active"),
            exit::OK,
        ),
        CoverageCycleActivation::Refused { active_cycle } => (
            json!({
                "cycle": label,
                "outcome": "refused",
                "activeCycle": active_cycle,
                "code": codes::CYCLE_ALREADY_ACTIVE,
            }),
            format!(
                "{}: cycle '{active_cycle}' is active, and only one can be. Deactivate it \
                 first; '{label}' was not activated",
                codes::CYCLE_ALREADY_ACTIVE
            ),
            exit::FINDINGS,
        ),
        CoverageCycleActivation::Undeclared => (
            json!({
                "cycle": label,
                "outcome": "undeclared",
                "code": codes::CYCLE_UNDECLARED,
            }),
            format!(
                "{}: cycle '{label}' has not been declared on this deployment",
                codes::CYCLE_UNDECLARED
            ),
            exit::FINDINGS,
        ),
    };
    Ok(CycleOutcome {
        command: "coverage cycle activate",
        deployment_id: deployment_id.to_string(),
        fields,
        text,
        exit: code,
    })
}

async fn deactivate(
    store: &CoverageStore,
    deployment_id: &str,
    label: &str,
) -> Result<CycleOutcome, sutra_datastore::DataStoreError> {
    let released = store.deactivate_cycle(deployment_id, label).await?;
    let active = if released {
        None
    } else {
        store.active_cycle(deployment_id).await?
    };
    let (fields, text, code) = match (released, active) {
        (true, _) => (
            json!({ "cycle": label, "outcome": "deactivated" }),
            format!("cycle '{label}' deactivated; coverage records outside any cycle"),
            exit::OK,
        ),
        // Nothing was active, so what was asked for already holds. A cleanup step can repeat this.
        (false, None) => (
            json!({ "cycle": label, "outcome": "not-active" }),
            format!("cycle '{label}' was not active"),
            exit::OK,
        ),
        // A caller who believed its cycle was running has to hear that another one is.
        (false, Some(active)) => (
            json!({
                "cycle": label,
                "outcome": "not-active",
                "activeCycle": active,
                "code": codes::CYCLE_NOT_ACTIVE,
            }),
            format!(
                "{}: cycle '{label}' is not active; '{active}' is, and was left active",
                codes::CYCLE_NOT_ACTIVE
            ),
            exit::FINDINGS,
        ),
    };
    Ok(CycleOutcome {
        command: "coverage cycle deactivate",
        deployment_id: deployment_id.to_string(),
        fields,
        text,
        exit: code,
    })
}

async fn list(
    store: &CoverageStore,
    deployment_id: &str,
) -> Result<CycleOutcome, sutra_datastore::DataStoreError> {
    let active = store.active_cycle(deployment_id).await?;
    let no_cycle = store.count_metrics(deployment_id).await?;
    let mut rows = Vec::new();
    let mut lines = Vec::new();
    for label in store.list_cycles(deployment_id).await? {
        let counts = store
            .in_cycle(Some(&label))
            .count_metrics(deployment_id)
            .await?;
        let is_active = active.as_deref() == Some(label.as_str());
        lines.push(format!(
            "\n  {} {label}: {:.2}%  ({} of {})",
            if is_active { "*" } else { " " },
            counts.coverage_percentage(),
            counts.covered,
            counts.total
        ));
        rows.push(json!({
            "cycle": label,
            "active": is_active,
            "total": counts.total,
            "covered": counts.covered,
            "coveragePercentage": counts.coverage_percentage(),
        }));
    }
    let text = format!(
        "{} cycle(s), no-cycle coverage {:.2}%  ({} of {}){}",
        rows.len(),
        no_cycle.coverage_percentage(),
        no_cycle.covered,
        no_cycle.total,
        lines.concat()
    );
    Ok(CycleOutcome {
        command: "coverage cycle list",
        deployment_id: deployment_id.to_string(),
        fields: json!({
            "activeCycle": active,
            "cycles": rows,
            "noCycle": {
                "total": no_cycle.total,
                "covered": no_cycle.covered,
                "coveragePercentage": no_cycle.coverage_percentage(),
            },
        }),
        text,
        exit: exit::OK,
    })
}
