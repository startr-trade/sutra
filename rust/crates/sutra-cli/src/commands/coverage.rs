//! `sutra coverage init|check` — path-coverage tooling. The model is described in the
//! book's *Coverage: declared routes as the compliance signal* chapter.
//!
//! - `init <process.bpmn>`: enumerates the process's routes over the engine's own model
//!   ([`crate::routes`], read-only `sutra-bpmn` dep), seeds `<q:coverage path flows>`
//!   declarations into the process, and scaffolds the ADMIN pair — `coverage-report.bpmn` /
//!   `coverage-reset.bpmn` over the reserved `coverage:report/reset:<process>` serviceTask
//!   ops — plus their reply templates, the two admin channels and the `coverage` store
//!   declaration (the money-transfer hand-authored set is the golden reference). No coverage
//!   SQL is scaffolded: the engine owns the coverage schema and applies it to that store's
//!   connection on first use (`datastore-schema-projection.md` §7), so the declaration carries
//!   no `migrations:` key — what the author chooses is the DATABASE the marks live in.
//! - `init <coverage-file> <processId…>` (cross-process): emits the **connectable graph** of
//!   the named processes into a `coverage/**` file (URN `urn:sutra:coverage:…`) as a
//!   coverage-file scaffold to draw from — intra-process sequence-flow adjacency
//!   (`A.targetRef == B.sourceRef`) plus inter-process hops
//!   (`<q:send channel=X>` → a channel-X start-event `<q:source>` = spawn / `imec` relay-wait =
//!   relay, each marked park/hydrate vs fire-and-forget). It surfaces the graph, NOT a
//!   pre-enumerated path set (no `--max-paths`), and does NOT seed `<q:coverage>` into the BPMN.
//!   `--single` restricts to intra-process adjacency; the processId list is the correlated set.
//! - `check <process.bpmn>`: drift lint — every declared path must still be an ordered
//!   subsequence of a current route (the runtime covering rule); routes no declared path
//!   covers are reported informationally; a declaring process without a `coverage` store is
//!   an error (`SUTRA.CONFIG.COVERAGE.STORE_MISSING`, the static package-time check).
//!
//! Write semantics throughout: enumeration errors out at 256 routes unless `--max-paths`
//! raises the cap; regeneration never touches user-edited files without `--force`
//! (generated files carry a `generated-by:` header); re-runs complete missing pieces and
//! leave everything else alone.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use sutra_bpmn::qbindings::AckMode;
use sutra_bpmn::{BpmnModelLoader, Node, ProcessDefinition, ProcessModule, SutraError};
use sutra_executor::{CoverageFragment, CoverageMetricStore, CoverageMetrics, StoreError};
use sutra_loader::{CoverageFile, CoverageRoute};

use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat, Severity};
use crate::routes::{
    enumerate_coverage_routes, enumerate_full_routes, is_subsequence, Route, DEFAULT_MAX_PATHS,
};
use crate::scaffold::{self, asset, render, WriteOutcome, WriteReport};
use crate::GlobalArgs;

/// Coverage diagnostic codes surfaced by this command. The first three are the engine's
/// own load-time family (`sutra-bpmn`); STORE_MISSING mirrors the executor's runtime code
/// (this is its static, package-side check); ROUTE_UNDECLARED is CLI-informational.
mod codes {
    pub const INVALID_ROUTE: &str = "SUTRA.CONFIG.COVERAGE.INVALID_ROUTE";
    pub const STORE_MISSING: &str = "SUTRA.CONFIG.COVERAGE.STORE_MISSING";
    pub const ROUTE_UNDECLARED: &str = "SUTRA.CONFIG.COVERAGE.ROUTE_UNDECLARED";
    pub const ROUTE_EXPLOSION: &str = "SUTRA.CONFIG.COVERAGE.ROUTE_EXPLOSION";
}

#[derive(Debug, clap::Args)]
pub struct CoverageArgs {
    #[command(subcommand)]
    pub action: CoverageAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum CoverageAction {
    /// Seed <q:coverage> route declarations and scaffold the report/reset admin set.
    Init(InitArgs),
    /// Lint declared coverage paths against the current flow graph (drift check), OR — with
    /// `--archive` + a coverage-store connection — the cross-process correlation-aware check
    /// mode: read the metric flags, union-find the reconstruction fragments to flip
    /// covered cross-process routes, report `total`/`covered`/`coveragePercentage`, fail
    /// closed below the threshold.
    Check(CheckArgs),
    /// Reset a deployment's coverage store: re-seed every declared path
    /// `covered = false` and clear its reconstruction fragments.
    Reset(ResetArgs),
}

#[derive(Debug, clap::Args)]
pub struct InitArgs {
    /// Legacy (single-process) form: the BPMN file whose process gets the `<q:coverage>`
    /// declarations. Cross-process form: the coverage-file NAME to emit under the
    /// package's `coverage/` folder (URN `urn:sutra:coverage:…`) — followed by processIds.
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// Cross-process form: the processIds the coverage file spans. When present, `<FILE>` is a
    /// coverage-file name and init emits the connectable-graph scaffold instead of seeding the
    /// BPMN. A list is the correlated cross-process set; `--single` keeps it intra-process.
    #[arg(value_name = "PROCESS_ID")]
    pub process_ids: Vec<String>,

    /// Cross-process form: restrict the emitted graph to intra-process sequence-flow adjacency
    /// (no inter-process hops).
    #[arg(long)]
    pub single: bool,

    /// Cross-process form: the deployment-package directory holding `bpmn/` (and where
    /// `coverage/` is written). Defaults to the current directory.
    #[arg(long, value_name = "DIR")]
    pub package: Option<PathBuf>,

    /// Legacy form: target process id (required only when the file declares several).
    #[arg(long, value_name = "ID")]
    pub process: Option<String>,

    /// Legacy form: route-explosion cap — error out beyond this many enumerated routes.
    /// Not used by the cross-process form (the connectable graph dissolves path explosion).
    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_PATHS)]
    pub max_paths: usize,

    /// Regenerate: replace drifted declarations / overwrite user-edited generated files, and
    /// (cross-process form) overwrite an existing coverage file.
    #[arg(long)]
    pub force: bool,
}

#[derive(Debug, clap::Args)]
pub struct CheckArgs {
    /// Drift-lint mode (single-process): the BPMN file to lint. Optional — omit when running
    /// the cross-process correlation-aware check (`--archive`).
    pub bpmn_file: Option<PathBuf>,

    /// Restrict the drift lint to one process id.
    #[arg(long, value_name = "ID")]
    pub process: Option<String>,

    /// Route-explosion cap for the drift-lint validity walk.
    #[arg(long, value_name = "N", default_value_t = DEFAULT_MAX_PATHS)]
    pub max_paths: usize,

    // ---- cross-process correlation-aware mode ----
    /// The sealed `.sutra` deployment archive to check coverage for. Selects the
    /// correlation-aware mode: the archive supplies both the content-addressed `deploymentId`
    /// (matching the store the deploy seeded) and the parsed cross-process coverage routes.
    #[arg(long, value_name = "FILE")]
    pub archive: Option<PathBuf>,

    /// Coverage-store database URL (`postgres://…`).
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

    /// Fail-closed CI gate: minimum coverage percentage to pass (default 100 — every declared
    /// path must be covered).
    #[arg(long, value_name = "PERCENT", default_value_t = 100.0)]
    pub threshold: f64,
}

/// `sutra coverage reset` — re-seed a deployment's coverage store to the fully-uncovered
/// baseline: every declared path back to `covered = false`, reconstruction fragments
/// cleared. The `.sutra` archive supplies the content-addressed `deploymentId`.
#[derive(Debug, clap::Args)]
pub struct ResetArgs {
    /// The sealed `.sutra` deployment archive whose coverage store is reset.
    #[arg(long, value_name = "FILE")]
    pub archive: PathBuf,

    /// Coverage-store database URL (`postgres://…`).
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

pub fn execute(args: CoverageArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "coverage: {msg}");
            return exit::USAGE;
        }
    };
    match args.action {
        CoverageAction::Init(a) => init(a, format, io),
        CoverageAction::Check(a) => check(a, format, io),
        CoverageAction::Reset(a) => reset(a, format, io),
    }
}

// ---------------------------------------------------------------------------------- init

fn init(args: InitArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    // Dispatch on positional arity: a trailing processId list selects the cross-process,
    // connectable-graph form; a bare `<FILE>` is the legacy single-process form.
    if !args.process_ids.is_empty() {
        return init_connectable_graph(args, format, io);
    }
    let (xml, module) = match load(&args.file, "coverage init", io) {
        Ok(v) => v,
        Err(code) => return code,
    };
    let process = match select_process(&module, args.process.as_deref()) {
        Ok(p) => p,
        Err(msg) => {
            let _ = writeln!(io.err, "coverage init: {msg}");
            return exit::USAGE;
        }
    };

    // 1. Route enumeration (the generator half of the design's load-time validator).
    let routes = match enumerate_coverage_routes(process, args.max_paths) {
        Ok(r) => r,
        Err(explosion) => {
            let d = Diagnostic::error(codes::ROUTE_EXPLOSION, explosion.to_string())
                .at(location(&args.file, &process.id));
            let _ = writeln!(io.err, "{}", d.render_text());
            return exit::FINDINGS;
        }
    };
    if routes.is_empty() {
        let _ = writeln!(
            io.err,
            "coverage init: process '{}' has no start->end routes to declare",
            process.id
        );
        return exit::FINDINGS;
    }

    // 2. Desired declarations: keep the user's path id wherever the flows match (renames
    //    survive regeneration), name the rest path-1..n in discovery order.
    let existing: Vec<(String, Route)> = process
        .coverage_paths
        .iter()
        .map(|p| (p.id.clone(), p.flows.clone()))
        .collect();
    let desired = assign_path_ids(&routes, &existing);
    let up_to_date = same_declarations(&existing, &desired);

    if !existing.is_empty() && !up_to_date && !args.force {
        let _ = writeln!(
            io.err,
            "coverage init: process '{}' already declares {} <q:coverage> path(s) that do \
             not match the enumerated routes — hand-tuned declarations are never replaced \
             without --force (run `sutra coverage check` to see the drift)",
            process.id,
            existing.len()
        );
        return exit::FINDINGS;
    }

    let mut report = WriteReport::default();

    // 3. Seed the target process (surgical text edit — the file is the user's).
    if up_to_date {
        report.record(&args.file, WriteOutcome::Unchanged);
    } else {
        let seeded = match seed_coverage(&xml, &process.id, &desired, !existing.is_empty()) {
            Ok(s) => s,
            Err(msg) => {
                let _ = writeln!(io.err, "coverage init: {msg}");
                return exit::USAGE;
            }
        };
        // The edited file must round-trip through the engine loader with exactly the
        // desired declarations before it is allowed onto disk.
        match BpmnModelLoader::new().load(seeded.as_bytes()) {
            Ok(reloaded) => {
                let got: Vec<(String, Route)> = reloaded
                    .process(&process.id)
                    .map(|p| {
                        p.coverage_paths
                            .iter()
                            .map(|c| (c.id.clone(), c.flows.clone()))
                            .collect()
                    })
                    .unwrap_or_default();
                if !same_declarations(&got, &desired) {
                    let _ = writeln!(
                        io.err,
                        "coverage init: internal error — seeded declarations did not \
                         round-trip; file left untouched"
                    );
                    return exit::USAGE;
                }
            }
            Err(e) => {
                let _ = writeln!(
                    io.err,
                    "coverage init: internal error — seeded file no longer loads ({e}); \
                     file left untouched"
                );
                return exit::USAGE;
            }
        }
        if let Err(e) = std::fs::write(&args.file, &seeded) {
            let _ = writeln!(
                io.err,
                "coverage init: cannot write {}: {e}",
                args.file.display()
            );
            return exit::USAGE;
        }
        report.record(&args.file, WriteOutcome::Updated);
    }

    // 4. The admin set (report/reset flows + templates + channels + store + migration).
    //    Regeneration completes missing pieces; user-owned files stay protected.
    let root = package_root(&args.file);
    let bpmn_dir = args.file.parent().unwrap_or(Path::new(".")).to_path_buf();
    let vars: Vec<(&str, &str)> = vec![
        ("PROCESS", process.id.as_str()),
        ("NAMESPACE", module.target_namespace.as_str()),
    ];
    let emit =
        |path: PathBuf, content: String, report: &mut WriteReport| match scaffold::write_generated(
            &path, &content, args.force,
        ) {
            Ok(outcome) => report.record(&path, outcome),
            Err(e) => {
                tracing::error!(path = %path.display(), error = %e, "coverage asset write failed");
                report.record(&path, WriteOutcome::SkippedUserFile);
            }
        };
    emit(
        bpmn_dir.join("coverage-report.bpmn"),
        render(asset("coverage/coverage-report.bpmn"), &vars),
        &mut report,
    );
    emit(
        bpmn_dir.join("coverage-reset.bpmn"),
        render(asset("coverage/coverage-reset.bpmn"), &vars),
        &mut report,
    );
    emit(
        root.join("templates/coverage-report.hbs"),
        asset("coverage/coverage-report.hbs").to_string(),
        &mut report,
    );
    emit(
        root.join("templates/coverage-reset.hbs"),
        asset("coverage/coverage-reset.hbs").to_string(),
        &mut report,
    );
    let mut notes: Vec<String> = Vec::new();
    if let Err(msg) = wire_channels(&root, process, &mut report, &mut notes) {
        let _ = writeln!(io.err, "coverage init: {msg}");
        return exit::USAGE;
    }
    if let Err(msg) = wire_datastore(&root, &mut report, &mut notes) {
        let _ = writeln!(io.err, "coverage init: {msg}");
        return exit::USAGE;
    }

    // 5. Report.
    let kept = desired
        .iter()
        .filter(|(id, flows)| existing.iter().any(|(ei, ef)| ei == id && ef == flows))
        .count();
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "coverage init: {} — process '{}', {} path(s) declared ({} kept, {} new)",
                args.file.display(),
                process.id,
                desired.len(),
                kept,
                desired.len() - kept
            );
            for (id, flows) in &desired {
                let _ = writeln!(io.out, "  path {id}: {}", flows.join(" "));
            }
            let _ = write!(io.out, "{}", report.render_text(Path::new(".")));
            for note in &notes {
                let _ = writeln!(io.out, "  note: {note}");
            }
        }
        ReportFormat::Json => {
            let paths: Vec<serde_json::Value> = desired
                .iter()
                .map(|(id, flows)| {
                    serde_json::json!({
                        "id": id,
                        "flows": flows,
                        "kept": existing.iter().any(|(ei, ef)| ei == id && ef == flows),
                    })
                })
                .collect();
            let payload = serde_json::json!({
                "command": "coverage init",
                "file": args.file.display().to_string(),
                "process": process.id,
                "paths": paths,
                "files": report.to_json(Path::new(".")),
                "notes": notes,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    for skipped in report.skipped_user_files() {
        let _ = writeln!(
            io.err,
            "coverage init: {} has user edits — left untouched (re-run with --force to \
             regenerate)",
            skipped.display()
        );
    }
    exit::OK
}

// -------------------------------------------------- init: cross-process connectable graph

/// One inferred inter-process hop: a `<q:send channel=X>` emitter matched to a channel-X
/// consumer. `kind` distinguishes a spawn (start-event `<q:source>`) from a relay (`imec`
/// wait); `ack` marks park/hydrate (request-reply) vs fire-and-forget; `key` is the correlation
/// alias inferred from the consumer (`None` when not uniquely inferable).
struct HopInfo {
    from: String,
    to: String,
    channel: String,
    kind: HopKind,
    ack: HopAck,
    key: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HopKind {
    Spawn,
    Relay,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HopAck {
    RequestReply,
    FireAndForget,
}

impl HopKind {
    fn label(self) -> &'static str {
        match self {
            HopKind::Spawn => "spawn",
            HopKind::Relay => "relay",
        }
    }
}

impl HopAck {
    fn label(self) -> &'static str {
        match self {
            HopAck::RequestReply => "request-reply",
            HopAck::FireAndForget => "fire-and-forget",
        }
    }
}

/// The cross-process form: `sutra coverage init <coverage-file> <processId…>`. Loads the
/// package's processes, derives the CONNECTABLE GRAPH — intra-process sequence-flow adjacency
/// (`A.targetRef == B.sourceRef`, reusing the loader/`routes` contiguity relation via
/// [`ProcessDefinition::outgoing`]) plus inter-process `<q:send>`→consumer hops — and writes it
/// into `coverage/<file>` as a coverage-file scaffold to draw connected walks from. It emits the
/// graph, NOT a pre-enumerated path set (no `--max-paths`), and touches no BPMN.
fn init_connectable_graph(args: InitArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let package = args.package.clone().unwrap_or_else(|| PathBuf::from("."));
    let bpmn_dir = package.join("bpmn");
    if !bpmn_dir.is_dir() {
        let _ = writeln!(
            io.err,
            "coverage init: no bpmn/ directory under {} (run inside a deployment package or \
             pass --package <DIR>)",
            package.display()
        );
        return exit::USAGE;
    }

    // 1. Load every process the package declares (bpmn/**), then index by processId.
    let modules = match load_package_modules(&bpmn_dir) {
        Ok(m) => m,
        Err(msg) => {
            let _ = writeln!(io.err, "coverage init: {msg}");
            return exit::USAGE;
        }
    };
    let mut by_id: BTreeMap<&str, &ProcessDefinition> = BTreeMap::new();
    for module in &modules {
        for def in module.processes() {
            by_id.insert(def.id.as_str(), def);
        }
    }

    // 2. Resolve the requested processIds in the order given (fail closed on an unknown id).
    let mut requested: Vec<&ProcessDefinition> = Vec::new();
    for pid in &args.process_ids {
        match by_id.get(pid.as_str()) {
            Some(def) => requested.push(def),
            None => {
                let known: Vec<&str> = by_id.keys().copied().collect();
                let _ = writeln!(
                    io.err,
                    "coverage init: process '{pid}' not found under {} (declared: {})",
                    bpmn_dir.display(),
                    if known.is_empty() {
                        "none".to_string()
                    } else {
                        known.join(", ")
                    }
                );
                return exit::USAGE;
            }
        }
    }

    // 3. Resolve the coverage-file name → write path + URN (reusing phase-1's URN scheme).
    let rel = coverage_subpath(&args.file);
    let urn = sutra_loader::coverage::coverage_urn(&rel);
    let out_path = package.join("coverage").join(&rel);
    if out_path.exists() && !args.force {
        let _ = writeln!(
            io.err,
            "coverage init: {} already exists — re-run with --force to overwrite",
            out_path.display()
        );
        return exit::FINDINGS;
    }

    // 4. Derive the connectable graph.
    let adjacency = intra_process_adjacency(&requested);
    let hops = if args.single {
        Vec::new()
    } else {
        inter_process_hops(&requested)
    };

    // 5. Render the scaffold and write it.
    let corr_id = correlation_id(&rel);
    let yaml = render_scaffold(&urn, &corr_id, &requested, &adjacency, &hops, args.single);
    if let Some(parent) = out_path.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            let _ = writeln!(
                io.err,
                "coverage init: cannot create {}: {e}",
                parent.display()
            );
            return exit::USAGE;
        }
    }
    if let Err(e) = std::fs::write(&out_path, &yaml) {
        let _ = writeln!(
            io.err,
            "coverage init: cannot write {}: {e}",
            out_path.display()
        );
        return exit::USAGE;
    }

    // 6. Report.
    match format {
        ReportFormat::Text => {
            let _ = writeln!(io.out, "coverage init: {} — {urn}", out_path.display());
            let _ = writeln!(io.out, "  processes: {}", args.process_ids.join(", "));
            let flows: usize = adjacency.iter().map(|(_, rows)| rows.len()).sum();
            let _ = writeln!(
                io.out,
                "  intra-process adjacency: {flows} flow(s) across {} process(es)",
                adjacency.len()
            );
            if args.single {
                let _ = writeln!(io.out, "  inter-process hops: none (--single)");
            } else {
                let _ = writeln!(io.out, "  inter-process hops: {}", hops.len());
                for h in &hops {
                    let _ = writeln!(
                        io.out,
                        "    {} --{}--> {} [{} {}]{}",
                        h.from,
                        h.channel,
                        h.to,
                        h.kind.label(),
                        h.ack.label(),
                        h.key
                            .as_deref()
                            .map(|k| format!(" key={k}"))
                            .unwrap_or_default(),
                    );
                }
            }
            let _ = writeln!(
                io.out,
                "  scaffold written — draw connected walks from it (sutra lint validates connectivity)"
            );
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "command": "coverage init",
                "form": "cross-process",
                "file": out_path.display().to_string(),
                "urn": urn,
                "processes": args.process_ids,
                "single": args.single,
                "hops": hops.iter().map(|h| serde_json::json!({
                    "from": h.from,
                    "to": h.to,
                    "channel": h.channel,
                    "kind": h.kind.label(),
                    "ack": h.ack.label(),
                    "key": h.key,
                })).collect::<Vec<_>>(),
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    exit::OK
}

/// processId → ordered `(flowId, connectable successor flowIds)` rows — the intra-process
/// sequence-flow adjacency (`A.targetRef == B.sourceRef`).
type Adjacency = Vec<(String, Vec<(String, Vec<String>)>)>;

/// Load every `.bpmn` under `bpmn_dir` (recursively) into its `ProcessModule`, in path order.
fn load_package_modules(bpmn_dir: &Path) -> Result<Vec<ProcessModule>, String> {
    let mut files: Vec<PathBuf> = Vec::new();
    collect_bpmn_files(bpmn_dir, &mut files)?;
    files.sort();
    let mut modules = Vec::with_capacity(files.len());
    for f in files {
        let xml =
            std::fs::read_to_string(&f).map_err(|e| format!("cannot read {}: {e}", f.display()))?;
        let module = BpmnModelLoader::new()
            .load(xml.as_bytes())
            .map_err(|e| format!("{} does not load: {e}", f.display()))?;
        modules.push(module);
    }
    Ok(modules)
}

fn collect_bpmn_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("cannot read {}: {e}", dir.display()))?;
    for entry in entries {
        let path = entry
            .map_err(|e| format!("cannot read {}: {e}", dir.display()))?
            .path();
        if path.is_dir() {
            collect_bpmn_files(&path, out)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("bpmn") {
            out.push(path);
        }
    }
    Ok(())
}

/// Normalise the `<coverage-file>` positional to a `coverage/`-relative subpath with a
/// `.yaml`/`.yml` extension (accepts `orders/e2e`, `orders/e2e.yaml`, `coverage/orders/e2e.yaml`).
fn coverage_subpath(file: &Path) -> String {
    let mut s = file.to_string_lossy().replace('\\', "/");
    if let Some(rest) = s.strip_prefix("coverage/") {
        s = rest.to_string();
    }
    if !(s.ends_with(".yaml") || s.ends_with(".yml")) {
        s.push_str(".yaml");
    }
    s
}

/// The correlation mnemonic: the coverage file's stem (`orders/e2e.yaml` → `e2e`).
fn correlation_id(rel: &str) -> String {
    let stem = rel.rsplit('/').next().unwrap_or(rel);
    let stem = stem
        .strip_suffix(".yaml")
        .or_else(|| stem.strip_suffix(".yml"))
        .unwrap_or(stem);
    if stem.is_empty() {
        "flow-1".to_string()
    } else {
        stem.to_string()
    }
}

/// Per-process sequence-flow adjacency: flow A connects to flow B when `A.targetRef ==
/// B.sourceRef` (`ProcessDefinition::outgoing(A.target_ref)`).
fn intra_process_adjacency(procs: &[&ProcessDefinition]) -> Adjacency {
    procs
        .iter()
        .map(|p| {
            let rows = p
                .flows()
                .iter()
                .map(|f| {
                    let succ = p
                        .outgoing(&f.target_ref)
                        .iter()
                        .map(|s| s.id.clone())
                        .collect();
                    (f.id.clone(), succ)
                })
                .collect();
            (p.id.clone(), rows)
        })
        .collect()
}

/// A channel-X consumer: a start-event `<q:source>` (spawn) or an `imec` relay-wait
/// (`MessageCatchEvent`/`UserTask`, relay), with its inferred ack-mode + correlation key.
struct Consumer<'a> {
    process: &'a str,
    node: &'a str,
    kind: HopKind,
    ack: HopAck,
    key: Option<String>,
}

/// Match every `<q:send channel=X>` emitter against channel-X consumers across the requested
/// processes (inter-process only), reusing the `NodeBindings`/channel wiring. Deterministic:
/// emitters in requested-process then document order, consumers by (sorted) channel then
/// discovery order.
fn inter_process_hops(procs: &[&ProcessDefinition]) -> Vec<HopInfo> {
    // channel → consumers.
    let mut consumers: BTreeMap<String, Vec<Consumer>> = BTreeMap::new();
    for p in procs {
        for node in p.nodes() {
            let kind = match node {
                Node::StartEvent { .. } => HopKind::Spawn,
                Node::MessageCatchEvent { .. } | Node::UserTask { .. } => HopKind::Relay,
                _ => continue,
            };
            let channels = match node {
                Node::StartEvent { channels, .. }
                | Node::MessageCatchEvent { channels, .. }
                | Node::UserTask { channels, .. } => channels,
                _ => continue,
            };
            if channels.is_empty() {
                continue;
            }
            let bindings = p.bindings_for(node.id());
            let ack = match bindings.source().map(|s| s.ack) {
                Some(AckMode::OnComplete) => HopAck::RequestReply,
                _ => HopAck::FireAndForget,
            };
            let key = if bindings.aliases.len() == 1 {
                Some(bindings.aliases[0].name.clone())
            } else {
                None
            };
            for ch in channels {
                consumers.entry(ch.clone()).or_default().push(Consumer {
                    process: p.id.as_str(),
                    node: node.id(),
                    kind,
                    ack,
                    key: key.clone(),
                });
            }
        }
    }

    let mut hops: Vec<HopInfo> = Vec::new();
    for p in procs {
        for node in p.nodes() {
            let Some(channel) = p
                .bindings_for(node.id())
                .send
                .as_ref()
                .and_then(|s| s.channel.clone())
            else {
                continue;
            };
            let Some(matches) = consumers.get(&channel) else {
                continue;
            };
            for c in matches {
                if c.process == p.id.as_str() {
                    continue; // inter-process hops only (intra-process is adjacency)
                }
                hops.push(HopInfo {
                    from: format!("{}:{}", p.id, node.id()),
                    to: format!("{}:{}", c.process, c.node),
                    channel: channel.clone(),
                    kind: c.kind,
                    ack: c.ack,
                    key: c.key.clone(),
                });
            }
        }
    }
    hops
}

/// The correlation's default hop key: the most frequently inferred key (ties → first seen).
fn default_hop_key(hops: &[HopInfo]) -> Option<String> {
    let mut counts: Vec<(String, usize)> = Vec::new();
    for h in hops {
        if let Some(k) = &h.key {
            match counts.iter_mut().find(|(name, _)| name == k) {
                Some(e) => e.1 += 1,
                None => counts.push((k.clone(), 1)),
            }
        }
    }
    let mut best: Option<(String, usize)> = None;
    for (k, n) in counts {
        if best.as_ref().is_none_or(|(_, bn)| n > *bn) {
            best = Some((k, n));
        }
    }
    best.map(|(k, _)| k)
}

/// Render the coverage-file scaffold: a leading comment block (URN + intra-process adjacency +
/// inter-process hops) followed by a `correlations:` block (inferred `links` with per-hop keys)
/// and a `coverages:` starter (one `path`, `segments` = each process's connectable flow ids).
fn render_scaffold(
    urn: &str,
    corr_id: &str,
    procs: &[&ProcessDefinition],
    adjacency: &Adjacency,
    hops: &[HopInfo],
    single: bool,
) -> String {
    let default_key = default_hop_key(hops);
    let mut s = String::new();

    // --- Comment block: the connectable graph, spelled out for the author. ---
    s.push_str(
        "# generated-by: sutra coverage init — connectable-graph scaffold (cross-process \
         coverage).\n",
    );
    s.push_str(&format!("# urn: {urn}\n#\n"));
    s.push_str(
        "# A SCAFFOLD to draw from, NOT a validated path set. Trim each coverage route's \
         `segments`\n# into a CONNECTED walk per process (contiguous flows: A.targetRef == \
         B.sourceRef)\n# tied by the inter-process hops below; `sutra lint` validates \
         connectivity. No path\n# enumeration happens here — the graph dissolves the \
         path-explosion concern.\n#\n",
    );
    s.push_str("# Intra-process sequence-flow adjacency (flow -> connectable successors):\n");
    for (pid, rows) in adjacency {
        s.push_str(&format!("#   process {pid}:\n"));
        if rows.is_empty() {
            s.push_str("#     (no sequence flows)\n");
        }
        for (flow, succ) in rows {
            if succ.is_empty() {
                s.push_str(&format!("#     {flow} -> (terminal)\n"));
            } else {
                s.push_str(&format!("#     {flow} -> {}\n", succ.join(", ")));
            }
        }
    }
    s.push_str("#\n# Inter-process hops (<q:send channel=X> -> consumer):\n");
    if single {
        s.push_str("#   (none — --single restricts to intra-process adjacency)\n");
    } else if hops.is_empty() {
        s.push_str(
            "#   (none inferred — no <q:send channel> matched a channel consumer among the \
             requested processes)\n",
        );
    } else {
        for h in hops {
            s.push_str(&format!(
                "#   {} --{}--> {}   [{} {}]{}\n",
                h.from,
                h.channel,
                h.to,
                h.kind.label(),
                h.ack.label(),
                h.key
                    .as_deref()
                    .map(|k| format!("  key={k}"))
                    .unwrap_or_default(),
            ));
        }
    }
    s.push_str("#\n");

    // --- The body: a correlation the author trims into connected walks. ---
    s.push_str("correlations:\n");
    s.push_str(&format!("  - id: {corr_id}\n"));
    s.push_str(&format!(
        "    key: {}\n",
        default_key.as_deref().unwrap_or("TODO-correlation-key")
    ));
    // links: the inferred hops; a per-hop `key` only when it differs from the correlation default.
    if hops.is_empty() {
        s.push_str("    links: []\n");
    } else {
        s.push_str("    links:\n");
        for h in hops {
            let per_hop = match (&h.key, &default_key) {
                (Some(k), Some(d)) if k == d => String::new(),
                (Some(k), _) => format!(", key: \"{k}\""),
                (None, _) => String::new(),
            };
            s.push_str(&format!(
                "      - {{ from: \"{}\", to: \"{}\"{per_hop} }}   # {} {} channel={}\n",
                h.from,
                h.to,
                h.kind.label(),
                h.ack.label(),
                h.channel,
            ));
        }
    }
    // coverages: one starter route; segments list each process's connectable flow ids to trim.
    s.push_str("    coverages:\n");
    s.push_str(&format!(
        "      - path: path-1                  # mnemonic, unique under {urn}\n"
    ));
    s.push_str("        segments:\n");
    for p in procs {
        let ids: Vec<String> = p.flows().iter().map(|f| f.id.clone()).collect();
        s.push_str(&format!(
            "          {}: [{}]   # connectable flow ids — trim to a connected walk\n",
            p.id,
            ids.join(", ")
        ));
    }
    s
}

/// Keep an existing path id when its flows match a route; name new routes path-1..n.
fn assign_path_ids(routes: &[Route], existing: &[(String, Route)]) -> Vec<(String, Route)> {
    let mut used: std::collections::HashSet<String> = existing
        .iter()
        .filter(|(_, flows)| routes.contains(flows))
        .map(|(id, _)| id.clone())
        .collect();
    let mut n = 0usize;
    routes
        .iter()
        .map(|route| {
            if let Some((id, _)) = existing.iter().find(|(_, flows)| flows == route) {
                (id.clone(), route.clone())
            } else {
                loop {
                    n += 1;
                    let candidate = format!("path-{n}");
                    if used.insert(candidate.clone()) {
                        return (candidate, route.clone());
                    }
                }
            }
        })
        .collect()
}

/// Order-insensitive equality of declaration sets.
fn same_declarations(a: &[(String, Route)], b: &[(String, Route)]) -> bool {
    let set = |v: &[(String, Route)]| {
        v.iter()
            .map(|(id, flows)| (id.clone(), flows.clone()))
            .collect::<std::collections::BTreeSet<_>>()
    };
    set(a) == set(b)
}

// --------------------------------------------------------------------- bpmn text surgery

/// Insert (or with `replace` first remove) the `<q:coverage>` declarations of `process_id`
/// in the raw XML — a surgical text edit that leaves every other byte alone (the file is
/// the user's; only the declarations this command owns are touched).
fn seed_coverage(
    xml: &str,
    process_id: &str,
    paths: &[(String, Route)],
    replace: bool,
) -> Result<String, String> {
    let mut xml = ensure_q_namespace(xml)?;

    // Locate the process block.
    let open_re = regex::Regex::new(&format!(
        r#"<([A-Za-z0-9_]+:)?process\b[^>]*\bid="{}""#,
        regex::escape(process_id)
    ))
    .expect("static regex");
    let m = open_re
        .find(&xml)
        .ok_or_else(|| format!("process '{process_id}' not found in the file"))?;
    let prefix = open_re
        .captures(&xml)
        .and_then(|c| c.get(1).map(|p| p.as_str().to_string()))
        .unwrap_or_default();
    let open_end = xml[m.start()..]
        .find('>')
        .map(|i| m.start() + i + 1)
        .ok_or("malformed process open tag")?;
    let close_tag = format!("</{prefix}process>");
    let close_start = xml[open_end..]
        .find(&close_tag)
        .map(|i| open_end + i)
        .ok_or_else(|| format!("closing {close_tag} not found"))?;

    let mut block = xml[open_end..close_start].to_string();

    if replace {
        let mut removed = false;
        block = block
            .lines()
            .filter(|line| {
                let is_coverage = line.trim_start().starts_with("<q:coverage ");
                removed |= is_coverage;
                !is_coverage
            })
            .collect::<Vec<_>>()
            .join("\n");
        if !removed {
            return Err(format!(
                "existing <q:coverage> declarations of '{process_id}' are not one-per-line \
                 and cannot be replaced textually — remove them by hand and re-run"
            ));
        }
    }

    // Find the process's own <extensionElements> (its first child element, by the BPMN
    // sequence) or create one right after the open tag.
    let ext_tag = format!("{prefix}extensionElements");
    let first_child = first_element_name(&block);
    if first_child.as_deref() == Some(ext_tag.as_str()) {
        let ext_close = format!("</{ext_tag}>");
        let close_idx = block
            .find(&ext_close)
            .ok_or_else(|| format!("closing {ext_close} not found"))?;
        // Indentation: the closing tag's line indent + 2 for the declarations.
        let line_start = block[..close_idx].rfind('\n').map(|i| i + 1).unwrap_or(0);
        let indent = &block[line_start..close_idx];
        let child_indent = format!("{indent}  ");
        let lines = coverage_lines(paths, &child_indent);
        block.insert_str(line_start, &format!("{lines}\n"));
    } else {
        // No extensionElements yet: create the block as the first child.
        let process_indent = line_indent(&xml[..m.start()]);
        let ext_indent = format!("{process_indent}  ");
        let child_indent = format!("{ext_indent}  ");
        let lines = coverage_lines(paths, &child_indent);
        let inserted = format!("\n{ext_indent}<{ext_tag}>\n{lines}\n{ext_indent}</{ext_tag}>\n");
        block.insert_str(0, &inserted);
    }

    xml.replace_range(open_end..close_start, &block);
    Ok(xml)
}

fn coverage_lines(paths: &[(String, Route)], indent: &str) -> String {
    paths
        .iter()
        .map(|(id, flows)| {
            format!(
                r#"{indent}<q:coverage path="{id}" flows="{}"/>"#,
                flows.join(" ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The local part of the first element inside the block (skipping comments), qualified.
fn first_element_name(block: &str) -> Option<String> {
    let mut rest = block;
    loop {
        let idx = rest.find('<')?;
        let after = &rest[idx..];
        if let Some(comment) = after.strip_prefix("<!--") {
            let end = comment.find("-->")?;
            rest = &comment[end + 3..];
            continue;
        }
        let name: String = after[1..]
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == ':' || *c == '_' || *c == '-')
            .collect();
        return Some(name);
    }
}

/// Indentation of the last line of `before` (the text preceding a tag).
fn line_indent(before: &str) -> String {
    let line_start = before.rfind('\n').map(|i| i + 1).unwrap_or(0);
    before[line_start..]
        .chars()
        .take_while(|c| *c == ' ' || *c == '\t')
        .collect()
}

/// Add `xmlns:q` to the document element when missing (the declarations need it).
fn ensure_q_namespace(xml: &str) -> Result<String, String> {
    let re = regex::Regex::new(r"<([A-Za-z0-9_]+:)?definitions\b").expect("static regex");
    let m = re.find(xml).ok_or("no <definitions> document element")?;
    let open_end = xml[m.start()..]
        .find('>')
        .map(|i| m.start() + i)
        .ok_or("malformed definitions open tag")?;
    if xml[m.start()..open_end].contains("xmlns:q=") {
        return Ok(xml.to_string());
    }
    let mut out = xml.to_string();
    out.insert_str(open_end, r#" xmlns:q="urn:sutra:q:1.0""#);
    Ok(out)
}

// ------------------------------------------------------------------ channels + datastore

/// Append the two coverage admin channels to channels.yaml (create it when absent). The
/// codec and auth block are copied from the channel already serving the target process —
/// exactly how the golden money-transfer set is wired.
fn wire_channels(
    root: &Path,
    process: &ProcessDefinition,
    report: &mut WriteReport,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let path = root.join("channels.yaml");
    let existing = match std::fs::read_to_string(&path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };

    let mut codec = String::new();
    let mut auth_block = String::new();
    let mut have_query = false;
    let mut have_reset = false;

    if let Some(text) = &existing {
        let value: serde_yaml_ng::Value = serde_yaml_ng::from_str(text)
            .map_err(|e| format!("{} does not parse: {e}", path.display()))?;
        let channels = value
            .get("channels")
            .and_then(|c| c.as_sequence().cloned())
            .unwrap_or_default();
        let name_of = |c: &serde_yaml_ng::Value| {
            c.get("name")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string()
        };
        have_query = channels.iter().any(|c| name_of(c) == "coverage-query");
        have_reset = channels.iter().any(|c| name_of(c) == "coverage-reset");

        // Reference channel: prefer an http channel already bound to the target process.
        let bound = process_channels(process);
        let reference = channels
            .iter()
            .filter(|c| bound.contains(&name_of(c)))
            .find(|c| c.get("transport").and_then(|t| t.as_str()) == Some("http"))
            .or_else(|| channels.iter().find(|c| bound.contains(&name_of(c))))
            .or_else(|| channels.first());
        if let Some(reference) = reference {
            codec = reference
                .get("codec")
                .and_then(|c| c.as_str())
                .unwrap_or_default()
                .to_string();
            if let Some(auth) = reference.get("auth") {
                auth_block = indent_yaml("auth", auth, 4)?;
            }
        }
    }

    if have_query && have_reset {
        report.record(&path, WriteOutcome::Unchanged);
        return Ok(());
    }
    if have_query != have_reset {
        notes.push(format!(
            "channels.yaml declares only one of coverage-query/coverage-reset — treating \
             the pair as user-managed, nothing appended (declare the missing {})",
            if have_query {
                "coverage-reset"
            } else {
                "coverage-query"
            }
        ));
        report.record(&path, WriteOutcome::SkippedUserFile);
        return Ok(());
    }

    if codec.is_empty() {
        notes.push(
            "no existing channel to copy the codec from — set the coverage channels' codec \
             by hand (and ensure its schema declares CoverageQuery/CoverageReset roots)"
                .to_string(),
        );
    }
    let snippet = render(
        asset("coverage/channels-snippet.yaml"),
        &[("PROCESS", process.id.as_str()), ("CODEC", codec.as_str())],
    );
    // The %%AUTH%% token occupies whole lines: substitute the copied auth block or drop them.
    let snippet = if auth_block.is_empty() {
        snippet
            .lines()
            .filter(|l| !l.contains("%%AUTH%%"))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n"
    } else {
        snippet.replace("%%AUTH%%", auth_block.trim_end())
    };

    let new_content = match existing {
        // A skeleton's flow-style empty list (`channels: []`) must open into block form
        // before entries can append under it.
        Some(text) => format!(
            "{}\n{}",
            open_empty_list(&text, "channels").trim_end(),
            snippet
        ),
        None => format!(
            "# generated-by: sutra coverage init — coverage admin channels for {}.\nchannels:\n{}",
            process.id, snippet
        ),
    };
    // Fail closed: never leave a channels.yaml behind that does not carry the appended pair.
    verify_list_entry(&new_content, "channels", "coverage-query", &path)?;
    verify_list_entry(&new_content, "channels", "coverage-reset", &path)?;
    std::fs::write(&path, new_content)
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    report.record(&path, WriteOutcome::Updated);
    Ok(())
}

/// Append the `coverage` store to datastores.yaml (create it when absent), pointing it at
/// the first sql store's connection — the golden set shares the business store's database.
///
/// The declaration is ALL that is scaffolded: it names the database the marks live in, and the
/// engine supplies the schema (§7). No `migrations:` key, no coverage SQL in the package.
fn wire_datastore(
    root: &Path,
    report: &mut WriteReport,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let path = root.join("datastores.yaml");
    let existing = match std::fs::read_to_string(&path) {
        Ok(text) => Some(text),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(format!("cannot read {}: {e}", path.display())),
    };

    let mut sql_props: Vec<(String, String)> = Vec::new();
    if let Some(text) = &existing {
        let value: serde_yaml_ng::Value = serde_yaml_ng::from_str(text)
            .map_err(|e| format!("{} does not parse: {e}", path.display()))?;
        let stores = value
            .get("datastores")
            .and_then(|s| s.as_sequence().cloned())
            .unwrap_or_default();
        if stores
            .iter()
            .any(|s| s.get("name").and_then(|n| n.as_str()) == Some("coverage"))
        {
            report.record(&path, WriteOutcome::Unchanged);
            return Ok(());
        }
        if let Some(sql) = stores
            .iter()
            .find(|s| s.get("type").and_then(|t| t.as_str()) == Some("sql"))
            .and_then(|s| s.get("sql"))
            .and_then(|j| j.as_mapping())
        {
            for (k, v) in sql {
                let (Some(k), Some(v)) = (k.as_str(), yaml_scalar(v)) else {
                    continue;
                };
                if k != "migrations" {
                    sql_props.push((k.to_string(), v));
                }
            }
        }
    }

    let props = if sql_props.is_empty() {
        notes.push(
            "no existing sql store to share a connection with — the coverage store points \
             at env:COVERAGE_DB_* references; adjust or point it at your store's connection"
                .to_string(),
        );
        vec![
            ("url-ref".to_string(), "env:COVERAGE_DB_URL".to_string()),
            (
                "username-ref".to_string(),
                "env:COVERAGE_DB_USER".to_string(),
            ),
            (
                "password-ref".to_string(),
                "env:COVERAGE_DB_PASSWORD".to_string(),
            ),
        ]
    } else {
        order_sql_props(sql_props)
    };
    let prop_lines = props
        .iter()
        .map(|(k, v)| format!("      {k}: {v}"))
        .collect::<Vec<_>>()
        .join("\n");
    let snippet = render(
        asset("coverage/datastores-snippet.yaml"),
        &[("SQL_PROPS", prop_lines.as_str())],
    );

    let new_content = match existing {
        // A skeleton's flow-style empty list (`datastores: []`) must open into block form
        // before entries can append under it.
        Some(text) => format!(
            "{}\n{}",
            open_empty_list(&text, "datastores").trim_end(),
            snippet
        ),
        None => format!(
            "# generated-by: sutra coverage init — where path-coverage marks are persisted.\ndatastores:\n{snippet}"
        ),
    };
    // Fail closed: never leave a datastores.yaml behind without the appended store.
    verify_list_entry(&new_content, "datastores", "coverage", &path)?;
    std::fs::write(&path, new_content)
        .map_err(|e| format!("cannot write {}: {e}", path.display()))?;
    report.record(&path, WriteOutcome::Updated);
    Ok(())
}

/// Open a flow-style empty list (`key: []`, possibly with a trailing comment) into block
/// form (`key:`) so appended entries belong to it.
fn open_empty_list(text: &str, key: &str) -> String {
    let re = regex::Regex::new(&format!(r"(?m)^{key}:\s*\[\s*\]\s*(#.*)?$")).expect("static regex");
    re.replace(text, format!("{key}:")).to_string()
}

/// Verify the appended YAML actually parses with the named entry under `key` — the append
/// is refused (nothing written) when the surrounding document swallows it.
fn verify_list_entry(text: &str, key: &str, name: &str, path: &Path) -> Result<(), String> {
    let value: serde_yaml_ng::Value = serde_yaml_ng::from_str(text).map_err(|e| {
        format!(
            "appending to {} would produce unparseable YAML ({e}) — declare the {name} \
             entry by hand",
            path.display()
        )
    })?;
    let found = value
        .get(key)
        .and_then(|s| s.as_sequence())
        .map(|seq| {
            seq.iter()
                .any(|e| e.get("name").and_then(|n| n.as_str()) == Some(name))
        })
        .unwrap_or(false);
    if found {
        Ok(())
    } else {
        Err(format!(
            "appending to {} did not surface the '{name}' entry under '{key}:' (unusual \
             document shape) — declare it by hand",
            path.display()
        ))
    }
}

/// Golden-conventional ordering: connection refs first, then literals, then the rest.
fn order_sql_props(mut props: Vec<(String, String)>) -> Vec<(String, String)> {
    const ORDER: [&str; 6] = [
        "url-ref",
        "username-ref",
        "password-ref",
        "url",
        "username",
        "password",
    ];
    props.sort_by_key(|(k, _)| ORDER.iter().position(|o| o == k).unwrap_or(ORDER.len() + 1));
    props
}

fn yaml_scalar(v: &serde_yaml_ng::Value) -> Option<String> {
    match v {
        serde_yaml_ng::Value::String(s) => Some(s.clone()),
        serde_yaml_ng::Value::Number(n) => Some(n.to_string()),
        serde_yaml_ng::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Re-emit a YAML subtree under `key`, indented by `indent` spaces (for the auth block).
fn indent_yaml(key: &str, value: &serde_yaml_ng::Value, indent: usize) -> Result<String, String> {
    let rendered =
        serde_yaml_ng::to_string(value).map_err(|e| format!("cannot re-emit {key}: {e}"))?;
    let pad = " ".repeat(indent);
    let mut out = format!("{pad}{key}:\n");
    for line in rendered.trim_end().lines() {
        out.push_str(&format!("{pad}  {line}\n"));
    }
    Ok(out)
}

/// Channels the process's start events subscribe to (via `<q:source channel>`).
fn process_channels(process: &ProcessDefinition) -> Vec<String> {
    let mut out = Vec::new();
    for node in process.nodes() {
        if let Node::StartEvent { id, channels, .. } = node {
            out.extend(channels.iter().cloned());
            for source in &process.bindings_for(id).sources {
                if !out.contains(&source.channel) {
                    out.push(source.channel.clone());
                }
            }
        }
    }
    out
}

// --------------------------------------------------------------------------------- check

fn check(args: CheckArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    // `--archive` selects the cross-process correlation-aware check (C6 phase 5); a bare BPMN
    // file keeps the single-process drift lint. The two take different inputs (a deployment +
    // store vs one BPMN's flow graph), so they dispatch here rather than merge.
    if args.archive.is_some() {
        return check_correlation(args, format, io);
    }
    let Some(bpmn_file) = args.bpmn_file.clone() else {
        let _ = writeln!(
            io.err,
            "coverage check: pass a BPMN file (single-process drift lint) or --archive \
             <FILE.sutra> (cross-process correlation-aware check)"
        );
        return exit::USAGE;
    };

    let (_, module) = match load(&bpmn_file, "coverage check", io) {
        Ok(v) => v,
        Err(code) => return code,
    };

    let mut diagnostics: Vec<Diagnostic> = Vec::new();
    let targets: Vec<&ProcessDefinition> = match &args.process {
        Some(id) => match module.processes().iter().find(|p| &p.id == id) {
            Some(p) => vec![p],
            None => {
                let _ = writeln!(io.err, "coverage check: process '{id}' not found");
                return exit::USAGE;
            }
        },
        None => module.processes().iter().collect(),
    };

    let mut any_declared = false;
    for process in targets {
        if process.coverage_paths.is_empty() {
            continue;
        }
        any_declared = true;
        let loc = location(&bpmn_file, &process.id);

        let full = match enumerate_full_routes(process, args.max_paths) {
            Ok(r) => r,
            Err(explosion) => {
                diagnostics
                    .push(Diagnostic::error(codes::ROUTE_EXPLOSION, explosion.to_string()).at(loc));
                continue;
            }
        };

        // Drift: every declared path must still cover at least one current route.
        for path in &process.coverage_paths {
            if !full.iter().any(|route| is_subsequence(&path.flows, route)) {
                diagnostics.push(
                    Diagnostic::error(
                        codes::INVALID_ROUTE,
                        format!(
                            "declared path '{}' (flows: {}) matches no route of the current \
                             flow graph",
                            path.id,
                            path.flows.join(" ")
                        ),
                    )
                    .at(loc.clone()),
                );
            }
        }

        // Gaps: routes no declared path covers (informational — coverage is opt-in).
        if let Ok(coverage_routes) = enumerate_coverage_routes(process, args.max_paths) {
            for route in &coverage_routes {
                let covered = process
                    .coverage_paths
                    .iter()
                    .any(|p| is_subsequence(&p.flows, route));
                if !covered {
                    diagnostics.push(Diagnostic {
                        severity: Severity::Info,
                        code: codes::ROUTE_UNDECLARED.to_string(),
                        message: format!(
                            "route not covered by any declared path: {} (declare it via \
                             `sutra coverage init`)",
                            route.join(" ")
                        ),
                        location: Some(loc.clone()),
                    });
                }
            }
        }

        // A declaring process needs the `coverage` store, fail-closed at package time.
        if !package_declares_coverage_store(&package_root(&bpmn_file)) {
            diagnostics.push(
                Diagnostic::error(
                    codes::STORE_MISSING,
                    "process declares <q:coverage> but the package's datastores.yaml \
                     declares no `coverage` store — that store is where coverage marks are \
                     persisted (the engine owns its schema; you supply no SQL). Run \
                     `sutra coverage init`",
                )
                .at(loc),
            );
        }
    }

    if !any_declared {
        diagnostics.push(Diagnostic {
            severity: Severity::Info,
            code: codes::ROUTE_UNDECLARED.to_string(),
            message: "no <q:coverage> declarations found (coverage is opt-in; seed with \
                      `sutra coverage init`)"
                .to_string(),
            location: Some(bpmn_file.display().to_string()),
        });
    }

    let errors = diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .count();
    match format {
        ReportFormat::Text => {
            for d in &diagnostics {
                let _ = writeln!(io.out, "{}", d.render_text());
            }
            let _ = writeln!(
                io.out,
                "coverage check: {} error(s), {} note(s)",
                errors,
                diagnostics.len() - errors
            );
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "command": "coverage check",
                "file": bpmn_file.display().to_string(),
                "diagnostics": diagnostics.iter().map(|d| d.to_json()).collect::<Vec<_>>(),
                "errors": errors,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
    if errors > 0 {
        exit::FINDINGS
    } else {
        exit::OK
    }
}

fn package_declares_coverage_store(root: &Path) -> bool {
    let Ok(text) = std::fs::read_to_string(root.join("datastores.yaml")) else {
        return false;
    };
    let Ok(value) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&text) else {
        return false;
    };
    value
        .get("datastores")
        .and_then(|s| s.as_sequence())
        .map(|stores| {
            stores
                .iter()
                .any(|s| s.get("name").and_then(|n| n.as_str()) == Some("coverage"))
        })
        .unwrap_or(false)
}

// ========================= cross-process correlation-aware check + reset =====================
//
// The cross-process verdict: reconstruct each coverage route's cascade as a
// connected component (union-find) over its reconstruction fragments, flip the covered flag when
// complete, then read `total`/`covered`/`coveragePercentage` straight off the seeded metric flags
// (the SAME query serves the fail-closed CI gate and the runtime SLO signal). The core logic
// is store-agnostic (`&dyn CoverageMetricStore`) so it unit-tests over `InMemoryCoverageStore` and
// runs live over the `PgCoverageStore` adapter unchanged.

/// A minimal union-find (disjoint-set) over `n` fragment nodes — the cascade-reconstruction
/// substrate. Self-contained in the CLI — deliberately kept out of `sutra-executor`;
/// path-halving `find` + union-by-rank.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> UnionFind {
        UnionFind {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]]; // path halving
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, a: usize, b: usize) {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return;
        }
        match self.rank[ra].cmp(&self.rank[rb]) {
            std::cmp::Ordering::Less => self.parent[ra] = rb,
            std::cmp::Ordering::Greater => self.parent[rb] = ra,
            std::cmp::Ordering::Equal => {
                self.parent[rb] = ra;
                self.rank[ra] += 1;
            }
        }
    }
}

/// The fully-qualified coverage-route URN (`<file-urn>:<path>`) — matches the `route_urn` the
/// runtime marking (phase 4) tags each fragment with, and the flag `mark_path_covered` flips.
fn route_urn(file: &CoverageFile, route: &CoverageRoute) -> String {
    format!("{}:{}", file.urn, route.path)
}

/// Decide whether one cross-process route's reconstruction fragments form a COMPLETE cascade
/// Nodes are the route's own fragments (those tagged with `urn`); an edge unions two
/// fragments sharing ANY of the three correlation dimensions —
///
/// - **per-hop `businessKey`** (both present and equal): the two endpoints of one request/reply leg
///   (e.g. p1↔p2 on `txnId`, p2↔p3 on a *different* `p2p3Ref`);
/// - **`traceId`** (both present and equal): a shared forward hop (cross-check / fallback);
/// - **`instanceId`** (equal): an instance's several passes/legs within a process (ties, e.g., i2's
///   `txnId` leg to its `p2p3Ref` leg across hydration).
///
/// The route is covered when SOME single connected component's `segment_process` set covers EVERY
/// segment the route declares. A missing segment (no fragment) or a broken key edge (a leg's
/// fragments landing in different components) leaves no such component → uncovered.
fn route_covered(urn: &str, required: &BTreeSet<String>, fragments: &[CoverageFragment]) -> bool {
    let nodes: Vec<&CoverageFragment> = fragments.iter().filter(|f| f.route_urn == urn).collect();

    // Every declared segment must have produced at least one fragment.
    if required
        .iter()
        .any(|p| !nodes.iter().any(|f| &f.segment_process == p))
    {
        return false;
    }

    // Union the fragments over the three edge types.
    let mut uf = UnionFind::new(nodes.len());
    for i in 0..nodes.len() {
        for j in (i + 1)..nodes.len() {
            let (a, b) = (nodes[i], nodes[j]);
            let key_edge =
                matches!((&a.business_key, &b.business_key), (Some(x), Some(y)) if x == y);
            let trace_edge = matches!((&a.trace_id, &b.trace_id), (Some(x), Some(y)) if x == y);
            let instance_edge = a.instance_id == b.instance_id;
            if key_edge || trace_edge || instance_edge {
                uf.union(i, j);
            }
        }
    }

    // Covered iff one component's segment_process set ⊇ the declared set.
    let mut by_root: HashMap<usize, BTreeSet<String>> = HashMap::new();
    for (i, f) in nodes.iter().enumerate() {
        by_root
            .entry(uf.find(i))
            .or_default()
            .insert(f.segment_process.clone());
    }
    by_root
        .values()
        .any(|procs| required.iter().all(|p| procs.contains(p)))
}

/// The outcome of a correlation-aware check — the fail-closed CI gate + SLO signal.
#[derive(Debug, Clone, PartialEq)]
struct CorrelationReport {
    total: u64,
    covered: u64,
    percentage: f64,
    threshold: f64,
    /// Still-uncovered path URNs (`covered = false`), ascending.
    uncovered: Vec<String>,
    /// Cross-process routes flipped to covered by THIS run's union-find.
    newly_covered: Vec<String>,
    passed: bool,
}

/// The store-agnostic heart of `sutra coverage check` (phase 5). Reads the metric flags,
/// union-finds the reconstruction fragments to flip covered cross-process routes, re-reads the
/// flags and derives the verdict — all over `&dyn CoverageMetricStore`.
async fn run_correlation_check(
    store: &dyn CoverageMetricStore,
    deployment_id: &str,
    coverages: &[CoverageFile],
    threshold: f64,
) -> Result<CorrelationReport, StoreError> {
    // Seed every declared cross-process route-level URN so `total` counts it whether or not deploy
    // seeded it (idempotent — never clobbers an already-covered flag). The intra-process
    // sub-path flags (`…#p`) were seeded at deploy and are flipped live by runtime marking;
    // this check only decides the route-level flags.
    let mut route_urns: Vec<String> = Vec::new();
    for file in coverages {
        for corr in &file.correlations {
            for route in &corr.coverages {
                route_urns.push(route_urn(file, route));
            }
        }
    }
    store.seed_declared(deployment_id, &route_urns).await?;

    // Union-find the fragments; flip each covered route.
    let fragments = store.read_fragments(deployment_id).await?;
    let mut newly_covered = Vec::new();
    for file in coverages {
        for corr in &file.correlations {
            for route in &corr.coverages {
                let urn = route_urn(file, route);
                let required: BTreeSet<String> = route.segments.keys().cloned().collect();
                if route_covered(&urn, &required, &fragments) {
                    store.mark_path_covered(deployment_id, &urn).await?;
                    newly_covered.push(urn);
                }
            }
        }
    }

    // Re-read the flags AFTER flipping — total/covered/% straight off the seeded flags.
    let metrics = store.read_metrics(deployment_id).await?;
    let percentage = metrics.coverage_percentage();
    Ok(CorrelationReport {
        total: metrics.total,
        covered: metrics.covered,
        percentage,
        threshold,
        uncovered: metrics.uncovered,
        newly_covered,
        passed: percentage >= threshold,
    })
}

/// The store-agnostic `coverage reset`: re-seed every declared path `covered = false` and
/// clear the reconstruction fragments; returns the post-reset metrics (all uncovered).
async fn run_correlation_reset(
    store: &dyn CoverageMetricStore,
    deployment_id: &str,
) -> Result<CoverageMetrics, StoreError> {
    store.reset(deployment_id).await?;
    store.read_metrics(deployment_id).await
}

// -------- adapter: the declared coverage store → the executor's CoverageMetricStore SPI ------

/// Adapts `sutra_datastore::CoverageStore` (the engine-owned coverage schema over whatever
/// connection the deployment's `coverage` store declares) onto the executor's
/// `CoverageMetricStore` SPI, so the store-agnostic `run_correlation_*` logic runs live unchanged
/// — on any of the three shipped dialects, since the store dispatches on the connection's scheme.
struct DeclaredMetricStore {
    inner: sutra_datastore::CoverageStore,
}

fn store_err(e: sutra_datastore::DataStoreError) -> StoreError {
    StoreError::new(e.to_string())
}

#[async_trait::async_trait(?Send)]
impl CoverageMetricStore for DeclaredMetricStore {
    async fn seed_declared(&self, dep: &str, path_urns: &[String]) -> Result<(), StoreError> {
        self.inner
            .seed_declared(dep, path_urns)
            .await
            .map_err(store_err)?;
        Ok(())
    }

    async fn mark_path_covered(&self, dep: &str, path_urn: &str) -> Result<bool, StoreError> {
        self.inner
            .mark_path_covered(dep, path_urn)
            .await
            .map_err(store_err)
    }

    async fn covered_among(
        &self,
        dep: &str,
        path_urns: &[String],
    ) -> Result<BTreeSet<String>, StoreError> {
        self.inner
            .covered_among(dep, path_urns)
            .await
            .map_err(store_err)
    }

    async fn clear_paths(&self, dep: &str, path_urns: &[String]) -> Result<u64, StoreError> {
        self.inner
            .clear_paths(dep, path_urns)
            .await
            .map_err(store_err)
    }

    async fn write_fragment(
        &self,
        dep: &str,
        fragment: &CoverageFragment,
    ) -> Result<(), StoreError> {
        let row = sutra_datastore::CoverageFragmentRow {
            route_urn: fragment.route_urn.clone(),
            segment_process: fragment.segment_process.clone(),
            instance_id: fragment.instance_id.clone(),
            business_key: fragment.business_key.clone(),
            trace_id: fragment.trace_id.clone(),
        };
        self.inner
            .write_fragment(dep, &row)
            .await
            .map_err(store_err)
    }

    async fn read_metrics(&self, dep: &str) -> Result<CoverageMetrics, StoreError> {
        let m = self.inner.read_metrics(dep).await.map_err(store_err)?;
        Ok(CoverageMetrics {
            total: m.total,
            covered: m.covered,
            uncovered: m.uncovered,
        })
    }

    async fn reset(&self, dep: &str) -> Result<(), StoreError> {
        self.inner.reset(dep).await.map_err(store_err)
    }

    async fn read_fragments(&self, dep: &str) -> Result<Vec<CoverageFragment>, StoreError> {
        Ok(self
            .inner
            .read_fragments(dep)
            .await
            .map_err(store_err)?
            .into_iter()
            .map(|r| CoverageFragment {
                route_urn: r.route_urn,
                segment_process: r.segment_process,
                instance_id: r.instance_id,
                business_key: r.business_key,
                trace_id: r.trace_id,
            })
            .collect())
    }
}

/// Open the coverage store over the connection the operator names (`--database-url` /
/// `SUTRA_DB_URL`, with user/password overrides — the CLI's `migrate`/`deploy` convention). This
/// is the same store the ENGINE writes: point it at the database the deployment's `coverage`
/// store declares, in any of the three shipped dialects (the URL scheme picks it). Nothing is
/// created here that the engine would not create — the DDL is the engine's and idempotent.
fn open_coverage_store(
    url: Option<&str>,
    user: Option<&str>,
    password: Option<&str>,
) -> Result<DeclaredMetricStore, String> {
    let Some(url) = url else {
        return Err(
            "--database-url (or SUTRA_DB_URL) is required for the coverage store".to_owned(),
        );
    };
    let mut properties = std::collections::BTreeMap::new();
    properties.insert("sql.url".to_string(), url.to_string());
    if let Some(user) = user {
        properties.insert("sql.username".to_string(), user.to_string());
    }
    if let Some(password) = password {
        properties.insert("sql.password".to_string(), password.to_string());
    }
    let def = sutra_datastore::StoreDefinition {
        name: sutra_datastore::COVERAGE_STORE_NAME.to_string(),
        store_type: "sql".to_string(),
        properties,
        structure: None,
    };
    sutra_datastore::CoverageStore::from_definition(&def)
        .map(|inner| DeclaredMetricStore { inner })
        .map_err(|e| format!("cannot open the coverage store: {e}"))
}

/// The cross-process correlation-aware `sutra coverage check` command wrapper: load the sealed
/// deployment (canonical `deploymentId` + parsed routes), connect the coverage store, run the
/// store-agnostic check, render and set the fail-closed exit.
fn check_correlation(args: CheckArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let archive_path = args
        .archive
        .as_ref()
        .expect("check_correlation dispatched on Some(archive)");
    let loaded = match sutra_loader::read_archive_file(archive_path) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "coverage check: cannot read {}: {e}",
                archive_path.display()
            );
            return exit::USAGE;
        }
    };
    let deployment_id = loaded.id.value().to_string();
    let coverages = loaded.deployment.coverages.clone();

    // The pool MUST be created and used within ONE runtime: sqlx binds a connection's IO to the
    // ambient tokio reactor at establish time, so a pool built under one `block_on` runtime and then
    // driven under a second (whose reactor is already dropped) wedges `begin()` until the acquire
    // timeout — the "pool timed out while waiting for an open connection" symptom. Connect + check
    // run in ONE async so the pool never outlives its runtime.
    let report = match block_on(async {
        let store = open_coverage_store(
            args.database_url.as_deref(),
            args.db_user.as_deref(),
            args.db_password.as_deref(),
        )?;
        run_correlation_check(&store, &deployment_id, &coverages, args.threshold)
            .await
            .map_err(|e| format!("coverage store error: {}", e.message()))
    }) {
        Ok(r) => r,
        Err(msg) => {
            let _ = writeln!(io.err, "coverage check: {msg}");
            return exit::USAGE;
        }
    };

    render_correlation_report(&report, &deployment_id, format, io);
    if report.passed {
        exit::OK
    } else {
        exit::FINDINGS
    }
}

fn render_correlation_report(
    report: &CorrelationReport,
    deployment_id: &str,
    format: ReportFormat,
    io: &mut Io<'_>,
) {
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "coverage check (cross-process) — deployment {deployment_id}"
            );
            let _ = writeln!(
                io.out,
                "  total: {}  covered: {}  coveragePercentage: {:.2}%",
                report.total, report.covered, report.percentage
            );
            if !report.newly_covered.is_empty() {
                let _ = writeln!(
                    io.out,
                    "  newly covered this run ({}):",
                    report.newly_covered.len()
                );
                for urn in &report.newly_covered {
                    let _ = writeln!(io.out, "    + {urn}");
                }
            }
            if report.uncovered.is_empty() {
                let _ = writeln!(io.out, "  uncovered: none");
            } else {
                let _ = writeln!(io.out, "  uncovered ({}):", report.uncovered.len());
                for urn in &report.uncovered {
                    let _ = writeln!(io.out, "    - {urn}");
                }
            }
            let verdict = if report.passed { "PASS" } else { "FAIL" };
            let _ = writeln!(
                io.out,
                "  threshold: {:.2}%  =>  {verdict}",
                report.threshold
            );
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "command": "coverage check",
                "form": "cross-process",
                "deploymentId": deployment_id,
                "total": report.total,
                "covered": report.covered,
                "coveragePercentage": report.percentage,
                "threshold": report.threshold,
                "uncovered": report.uncovered,
                "newlyCovered": report.newly_covered,
                "passed": report.passed,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
}

/// `sutra coverage reset` — re-seed the deployment's coverage store to the fully-uncovered
/// baseline and clear its reconstruction fragments.
fn reset(args: ResetArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let loaded = match sutra_loader::read_archive_file(&args.archive) {
        Ok(a) => a,
        Err(e) => {
            let _ = writeln!(
                io.err,
                "coverage reset: cannot read {}: {e}",
                args.archive.display()
            );
            return exit::USAGE;
        }
    };
    let deployment_id = loaded.id.value().to_string();

    // Pool + reset in ONE runtime (see `check_correlation`): a pool created under a first `block_on`
    // and driven under a second wedges on a dropped reactor until the acquire timeout.
    let metrics = match block_on(async {
        let store = open_coverage_store(
            args.database_url.as_deref(),
            args.db_user.as_deref(),
            args.db_password.as_deref(),
        )?;
        run_correlation_reset(&store, &deployment_id)
            .await
            .map_err(|e| format!("coverage store error: {}", e.message()))
    }) {
        Ok(m) => m,
        Err(msg) => {
            let _ = writeln!(io.err, "coverage reset: {msg}");
            return exit::USAGE;
        }
    };

    render_reset_report(&metrics, &deployment_id, format, io);
    exit::OK
}

/// The `coverage reset` output — FROZEN field names (`{command, deploymentId, total, covered}`)
/// and text wording. Extracted from [`reset`] so the shape is provable without a database.
fn render_reset_report(
    metrics: &CoverageMetrics,
    deployment_id: &str,
    format: ReportFormat,
    io: &mut Io<'_>,
) {
    match format {
        ReportFormat::Text => {
            let _ = writeln!(
                io.out,
                "coverage reset — deployment {deployment_id}: {} path(s) re-seeded \
                 covered=false, reconstruction fragments cleared",
                metrics.total
            );
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "command": "coverage reset",
                "deploymentId": deployment_id,
                "total": metrics.total,
                "covered": metrics.covered,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

// -------------------------------------------------------------------------------- shared

/// Read + parse the BPMN file. Coverage-family load errors are findings (the lint's own
/// subject matter); anything else is a usage error.
fn load(path: &Path, command: &str, io: &mut Io<'_>) -> Result<(String, ProcessModule), i32> {
    if !path.is_file() {
        let _ = writeln!(io.err, "{command}: file not found: {}", path.display());
        return Err(exit::USAGE);
    }
    let xml = std::fs::read_to_string(path).map_err(|e| {
        let _ = writeln!(io.err, "{command}: cannot read {}: {e}", path.display());
        exit::USAGE
    })?;
    let module = BpmnModelLoader::new().load(xml.as_bytes()).map_err(|e| {
        let code = classify_load_error(&e);
        let _ = writeln!(io.err, "{command}: {e}");
        code
    })?;
    Ok((xml, module))
}

fn classify_load_error(e: &SutraError) -> i32 {
    if e.code.starts_with("SUTRA.CONFIG.COVERAGE.") {
        exit::FINDINGS
    } else {
        exit::USAGE
    }
}

fn select_process<'m>(
    module: &'m ProcessModule,
    wanted: Option<&str>,
) -> Result<&'m ProcessDefinition, String> {
    match wanted {
        Some(id) => module
            .processes()
            .iter()
            .find(|p| p.id == id)
            .ok_or_else(|| format!("process '{id}' not found (file declares: {})", ids(module))),
        None => {
            let mut iter = module.processes().iter();
            match (iter.next(), iter.next()) {
                (Some(p), None) => Ok(p),
                (Some(_), Some(_)) => Err(format!(
                    "the file declares several processes ({}) — pick one with --process",
                    ids(module)
                )),
                (None, _) => Err("the file declares no process".to_string()),
            }
        }
    }
}

fn ids(module: &ProcessModule) -> String {
    module.process_ids().join(", ")
}

/// The package root: the parent of a conventional `bpmn/` directory, else the file's own
/// directory (channels.yaml / datastores.yaml / templates/ / migrations/ live there).
fn package_root(bpmn_file: &Path) -> PathBuf {
    let parent = bpmn_file
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    if parent.file_name().and_then(|n| n.to_str()) == Some("bpmn") {
        parent
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf()
    } else {
        parent.to_path_buf()
    }
}

fn location(file: &Path, process: &str) -> String {
    format!("{}#{process}", file.display())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::scratch_dir;

    const PKG_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  id="Definitions_pay"
                  targetNamespace="urn:sutra:deployment:pay">
  <bpmn:process id="pay" name="Pay" isExecutable="true">
    <bpmn:extensionElements>
      <q:variables>
        <q:variable name="note" type="string"/>
      </q:variables>
    </bpmn:extensionElements>
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements>
        <q:source channel="pay-in" messageTypeValue="PayRequest"/>
      </bpmn:extensionElements>
      <bpmn:outgoing>F1</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="F1" sourceRef="Start" targetRef="GW"/>
    <bpmn:exclusiveGateway id="GW" default="FOk"/>
    <bpmn:sequenceFlow id="FKo" sourceRef="GW" targetRef="Reject">
      <bpmn:conditionExpression>note = "no"</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:sequenceFlow id="FOk" sourceRef="GW" targetRef="Accept"/>
    <bpmn:manualTask id="Reject"/>
    <bpmn:manualTask id="Accept"/>
    <bpmn:sequenceFlow id="FKoEnd" sourceRef="Reject" targetRef="End"/>
    <bpmn:sequenceFlow id="FOkEnd" sourceRef="Accept" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

    const PKG_CHANNELS: &str = r#"# hand-authored
channels:
  - name: pay-in
    transport: http
    bind: "POST /channels/pay-in"
    codec: "schemas/pay"
    cloudevents-mode: none
    auth:
      scheme: apikey
      apikey:
        value: demo-key
        header: X-Api-Key
"#;

    const PKG_DATASTORES: &str = r#"# hand-authored
datastores:
  - name: ledger
    type: sql
    sql:
      url-ref: env:LEDGER_DB_URL
      username-ref: env:LEDGER_DB_USER
      password-ref: env:LEDGER_DB_PASSWORD
      migrations: migrations/ledger
    dataClass: financial
"#;

    fn make_package(label: &str) -> PathBuf {
        let root = scratch_dir(label);
        std::fs::create_dir_all(root.join("bpmn")).unwrap();
        std::fs::write(root.join("bpmn/pay.bpmn"), PKG_BPMN).unwrap();
        std::fs::write(root.join("channels.yaml"), PKG_CHANNELS).unwrap();
        std::fs::write(root.join("datastores.yaml"), PKG_DATASTORES).unwrap();
        root
    }

    fn run_init(file: &Path, force: bool) -> (i32, String, String) {
        run_captured("", |io| {
            execute(
                CoverageArgs {
                    action: CoverageAction::Init(InitArgs {
                        file: file.to_path_buf(),
                        process_ids: Vec::new(),
                        single: false,
                        package: None,
                        process: None,
                        max_paths: DEFAULT_MAX_PATHS,
                        force,
                    }),
                },
                &GlobalArgs::default(),
                io,
            )
        })
    }

    fn run_check(file: &Path) -> (i32, String, String) {
        run_captured("", |io| {
            execute(
                CoverageArgs {
                    action: CoverageAction::Check(CheckArgs {
                        bpmn_file: Some(file.to_path_buf()),
                        process: None,
                        max_paths: DEFAULT_MAX_PATHS,
                        archive: None,
                        database_url: None,
                        db_user: None,
                        db_password: None,
                        threshold: 100.0,
                    }),
                },
                &GlobalArgs::default(),
                io,
            )
        })
    }

    #[test]
    fn init_seeds_paths_and_scaffolds_the_admin_set() {
        let root = make_package("cov-init");
        let bpmn = root.join("bpmn/pay.bpmn");
        let (code, out, err) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::OK, "out={out} err={err}");

        // Declarations seeded into the process and loadable by the engine loader.
        let seeded = std::fs::read_to_string(&bpmn).unwrap();
        let module = BpmnModelLoader::new().load(seeded.as_bytes()).unwrap();
        let p = module.process("pay").unwrap();
        let declared: Vec<Vec<String>> = p.coverage_paths.iter().map(|c| c.flows.clone()).collect();
        assert_eq!(
            declared,
            vec![
                vec!["FKo".to_string(), "FKoEnd".to_string()],
                vec!["FOk".to_string(), "FOkEnd".to_string()],
            ]
        );
        assert_eq!(p.coverage_paths[0].id, "path-1");

        // Admin pair loads and targets the process via the reserved ops.
        for (file, op) in [
            ("bpmn/coverage-report.bpmn", "coverage:report:pay"),
            ("bpmn/coverage-reset.bpmn", "coverage:reset:pay"),
        ] {
            let text = std::fs::read_to_string(root.join(file)).unwrap();
            assert!(text.contains(op), "{file} missing {op}");
            assert!(text.contains(r#"targetNamespace="urn:sutra:deployment:pay""#));
            BpmnModelLoader::new().load(text.as_bytes()).unwrap();
        }
        assert!(root.join("templates/coverage-report.hbs").is_file());
        assert!(root.join("templates/coverage-reset.hbs").is_file());
        // No coverage SQL is scaffolded: the engine owns the coverage schema and applies it to
        // the declared store on first use (`datastore-schema-projection.md` §7).
        assert!(!root.join("migrations/coverage").exists());

        // Channels: appended pair copies the codec + auth of the serving channel and the
        // whole file still parses through the engine channel loader.
        let channels = std::fs::read_to_string(root.join("channels.yaml")).unwrap();
        assert!(channels.starts_with("# hand-authored"), "append-only");
        let defs = sutra_channels::config::load_channel_definitions(
            channels.as_bytes(),
            "default",
            "pay",
            "1.0.0",
            "channels.yaml",
        )
        .unwrap();
        let names: Vec<&str> = defs
            .iter()
            .map(|d| d.binding.channel_name.as_str())
            .collect();
        assert_eq!(names, vec!["pay-in", "coverage-query", "coverage-reset"]);
        for d in &defs[1..] {
            assert_eq!(d.codec.as_deref(), Some("schemas/pay"));
            assert_eq!(d.auth_scheme.as_deref(), Some("apikey"));
            assert_eq!(
                d.properties.get("apikey.value").map(String::as_str),
                Some("demo-key")
            );
        }

        // Datastore: the coverage store shares the ledger connection; file still parses.
        let stores = sutra_datastore::config::parse_datastores(
            &std::fs::read_to_string(root.join("datastores.yaml")).unwrap(),
        )
        .unwrap();
        let coverage = stores.iter().find(|s| s.name == "coverage").unwrap();
        assert_eq!(coverage.store_type, "sql");
        assert_eq!(
            coverage.properties.get("sql.url-ref").map(String::as_str),
            Some("env:LEDGER_DB_URL")
        );
        // …and declares NO migrations: the coverage schema is the engine's.
        assert_eq!(coverage.properties.get("sql.migrations"), None);

        // The check passes clean afterwards.
        let (code, out, _) = run_check(&bpmn);
        assert_eq!(code, crate::exit::OK, "{out}");
        assert!(out.contains("0 error(s)"), "{out}");
    }

    #[test]
    fn init_is_idempotent_and_preserves_renamed_path_ids() {
        let root = make_package("cov-idem");
        let bpmn = root.join("bpmn/pay.bpmn");
        assert_eq!(run_init(&bpmn, false).0, crate::exit::OK);
        let after_first = std::fs::read_to_string(&bpmn).unwrap();

        // Re-run: byte-identical bpmn, everything else unchanged.
        let (code, out, _) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::OK);
        assert_eq!(std::fs::read_to_string(&bpmn).unwrap(), after_first);
        assert!(out.contains("2 kept, 0 new"), "{out}");

        // A rename is a user edit that regeneration must keep.
        let renamed = after_first.replace(r#"path="path-1""#, r#"path="reject""#);
        std::fs::write(&bpmn, renamed).unwrap();
        let (code, out, _) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::OK, "{out}");
        let text = std::fs::read_to_string(&bpmn).unwrap();
        assert!(
            text.contains(r#"path="reject""#),
            "rename preserved: {text}"
        );
        assert!(!text.contains(r#"path="path-1""#));
    }

    #[test]
    fn init_refuses_drifted_declarations_without_force_and_replaces_with_it() {
        let root = make_package("cov-force");
        let bpmn = root.join("bpmn/pay.bpmn");
        assert_eq!(run_init(&bpmn, false).0, crate::exit::OK);

        // Hand-drift a declaration to flows that are no longer a route.
        let drifted = std::fs::read_to_string(&bpmn)
            .unwrap()
            .replace(r#"flows="FKo FKoEnd""#, r#"flows="FKo""#);
        std::fs::write(&bpmn, drifted).unwrap();

        let (code, _, err) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(err.contains("--force"), "{err}");
        assert!(std::fs::read_to_string(&bpmn)
            .unwrap()
            .contains(r#"flows="FKo""#));

        let (code, _, _) = run_init(&bpmn, true);
        assert_eq!(code, crate::exit::OK);
        let text = std::fs::read_to_string(&bpmn).unwrap();
        assert!(text.contains(r#"flows="FKo FKoEnd""#), "{text}");
    }

    #[test]
    fn init_creates_missing_yaml_files_and_extension_block() {
        // A bare package: bpmn file at the top level, no channels/datastores, and the
        // process has NO extensionElements of its own.
        let root = scratch_dir("cov-bare");
        let bpmn = root.join("flow.bpmn");
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:test:bare">
  <bpmn:process id="flow" isExecutable="true">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
      <bpmn:outgoing>A</bpmn:outgoing>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="A" sourceRef="S" targetRef="T"/>
    <bpmn:manualTask id="T"/>
    <bpmn:sequenceFlow id="B" sourceRef="T" targetRef="E"/>
    <bpmn:endEvent id="E"/>
  </bpmn:process>
</bpmn:definitions>
"#;
        std::fs::write(&bpmn, xml).unwrap();
        let (code, out, err) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::OK, "out={out} err={err}");

        let seeded = std::fs::read_to_string(&bpmn).unwrap();
        let module = BpmnModelLoader::new().load(seeded.as_bytes()).unwrap();
        let p = module.process("flow").unwrap();
        assert_eq!(p.coverage_paths.len(), 1);
        assert_eq!(p.coverage_paths[0].flows, vec!["B".to_string()]);

        // Fresh channels.yaml / datastores.yaml created next to the file, parseable, with
        // the fallback connection refs (nothing to copy from).
        let channels = std::fs::read_to_string(root.join("channels.yaml")).unwrap();
        sutra_channels::config::load_channel_definitions(
            channels.as_bytes(),
            "t",
            "m",
            "1",
            "channels.yaml",
        )
        .unwrap();
        let stores = sutra_datastore::config::parse_datastores(
            &std::fs::read_to_string(root.join("datastores.yaml")).unwrap(),
        )
        .unwrap();
        assert_eq!(stores.len(), 1);
        assert_eq!(
            stores[0].properties.get("sql.url-ref").map(String::as_str),
            Some("env:COVERAGE_DB_URL")
        );
        assert!(
            err.contains("COVERAGE_DB") || out.contains("COVERAGE_DB"),
            "note surfaced"
        );
    }

    #[test]
    fn init_opens_the_skeleton_empty_lists_and_check_passes() {
        // The full authoring flow over a `create deployment` skeleton, whose channels.yaml
        // / datastores.yaml declare flow-style empty lists (`channels: []`): init must open
        // them into block form, not append orphan entries below them.
        let parent = scratch_dir("cov-skeleton");
        let (code, _, err) = run_captured("", |io| {
            crate::commands::create::execute(
                crate::commands::create::CreateArgs {
                    action: crate::commands::create::CreateAction::Deployment(
                        crate::commands::create::DeploymentArgs {
                            name: "reporting".into(),
                            dir: Some(parent.clone()),
                            from: None,
                        },
                    ),
                },
                &GlobalArgs::default(),
                io,
            )
        });
        assert_eq!(code, crate::exit::OK, "{err}");
        let pkg = parent.join("reporting");
        let (code, _, err) = run_captured("", |io| {
            crate::commands::create::execute(
                crate::commands::create::CreateArgs {
                    action: crate::commands::create::CreateAction::Bpmn(
                        crate::commands::create::BpmnArgs {
                            process: "monthly-report".into(),
                            package: pkg.clone(),
                            validation: crate::commands::create::ValidationMode::Soft,
                            channel: None,
                            message_type: None,
                            namespace: None,
                            force: false,
                        },
                    ),
                },
                &GlobalArgs::default(),
                io,
            )
        });
        assert_eq!(code, crate::exit::OK, "{err}");

        let bpmn = pkg.join("bpmn/monthly-report.bpmn");
        let (code, out, err) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::OK, "out={out} err={err}");

        // Both files re-parse with the appended entries attached to the list key.
        let channels = std::fs::read_to_string(pkg.join("channels.yaml")).unwrap();
        assert!(!channels.contains("channels: []"), "empty list opened");
        let defs = sutra_channels::config::load_channel_definitions(
            channels.as_bytes(),
            "default",
            "reporting",
            "1.0.0",
            "channels.yaml",
        )
        .unwrap();
        let names: Vec<&str> = defs
            .iter()
            .map(|d| d.binding.channel_name.as_str())
            .collect();
        assert_eq!(names, vec!["coverage-query", "coverage-reset"]);

        let stores = sutra_datastore::config::parse_datastores(
            &std::fs::read_to_string(pkg.join("datastores.yaml")).unwrap(),
        )
        .unwrap();
        assert!(stores.iter().any(|s| s.name == "coverage"));

        // And the drift lint is clean end to end.
        let (code, out, _) = run_check(&bpmn);
        assert_eq!(code, crate::exit::OK, "{out}");
        assert!(out.contains("0 error(s)"), "{out}");
    }

    #[test]
    fn init_cap_errors_and_max_paths_overrides() {
        // 2^9 = 512 routes: 9 sequential binary gateways exceed the 256 default.
        let mut middle = String::new();
        let n = 9;
        for i in 0..n {
            middle.push_str(&format!(
                r#"    <bpmn:exclusiveGateway id="G{i}" default="ga{i}"/>
    <bpmn:sequenceFlow id="ga{i}" sourceRef="G{i}" targetRef="A{i}"/>
    <bpmn:sequenceFlow id="gb{i}" sourceRef="G{i}" targetRef="B{i}">
      <bpmn:conditionExpression>x</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:manualTask id="A{i}"/>
    <bpmn:manualTask id="B{i}"/>
    <bpmn:sequenceFlow id="fa{i}" sourceRef="A{i}" targetRef="G{next}"/>
    <bpmn:sequenceFlow id="fb{i}" sourceRef="B{i}" targetRef="G{next}"/>
"#,
                i = i,
                next = i + 1
            ));
        }
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:test:explode">
  <bpmn:process id="explode" isExecutable="true">
    <bpmn:startEvent id="S">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="s0" sourceRef="S" targetRef="G0"/>
{middle}    <bpmn:exclusiveGateway id="G{n}" default="last"/>
    <bpmn:sequenceFlow id="last" sourceRef="G{n}" targetRef="E"/>
    <bpmn:endEvent id="E"/>
  </bpmn:process>
</bpmn:definitions>
"#
        );
        let root = scratch_dir("cov-cap");
        let bpmn = root.join("explode.bpmn");
        std::fs::write(&bpmn, &xml).unwrap();

        let (code, _, err) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(err.contains("exceeded the cap of 256"), "{err}");
        assert!(err.contains("--max-paths"), "{err}");
        // The target file was never touched.
        assert_eq!(std::fs::read_to_string(&bpmn).unwrap(), xml);

        // Raising the cap lets it through.
        let (code, out, err) = run_captured("", |io| {
            execute(
                CoverageArgs {
                    action: CoverageAction::Init(InitArgs {
                        file: bpmn.clone(),
                        process_ids: Vec::new(),
                        single: false,
                        package: None,
                        process: None,
                        max_paths: 1024,
                        force: false,
                    }),
                },
                &GlobalArgs::default(),
                io,
            )
        });
        assert_eq!(code, crate::exit::OK, "out={out} err={err}");
        assert!(out.contains("512 path(s) declared"), "{out}");
    }

    #[test]
    fn check_reports_drift_gaps_and_store_missing() {
        let root = make_package("cov-check");
        let bpmn = root.join("bpmn/pay.bpmn");
        assert_eq!(run_init(&bpmn, false).0, crate::exit::OK);

        // Drift that still LOADS (the loader fail-closes unknown/non-contiguous flows, so
        // drift-lint targets what it cannot see): re-point a declared path at a detached
        // fragment's flow — declared, contiguous, but on no start->end route. Also drop
        // the coverage store.
        let text = std::fs::read_to_string(&bpmn).unwrap();
        let detached = r#"    <bpmn:manualTask id="Z1"/>
    <bpmn:manualTask id="Z2"/>
    <bpmn:sequenceFlow id="FZ" sourceRef="Z1" targetRef="Z2"/>
  </bpmn:process>"#;
        let text = text
            .replace(r#"flows="FKo FKoEnd""#, r#"flows="FZ""#)
            .replace("  </bpmn:process>", detached);
        std::fs::write(&bpmn, text).unwrap();
        std::fs::write(root.join("datastores.yaml"), "datastores: []\n").unwrap();

        let (code, out, _) = run_check(&bpmn);
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(out.contains("SUTRA.CONFIG.COVERAGE.INVALID_ROUTE"), "{out}");
        assert!(out.contains("SUTRA.CONFIG.COVERAGE.STORE_MISSING"), "{out}");
        // The reject branch is now uncovered — reported as an informational gap.
        assert!(
            out.contains("SUTRA.CONFIG.COVERAGE.ROUTE_UNDECLARED"),
            "{out}"
        );
        assert!(out.contains("2 error(s)"), "{out}");
    }

    #[test]
    fn check_is_clean_on_undeclared_process_and_json_shape_holds() {
        let root = make_package("cov-check-clean");
        let bpmn = root.join("bpmn/pay.bpmn");
        // No init: coverage undeclared → informational only, exit OK.
        let (code, out, _) = run_check(&bpmn);
        assert_eq!(code, crate::exit::OK);
        assert!(out.contains("coverage is opt-in"), "{out}");

        let (code, out, _) = run_captured("", |io| {
            execute(
                CoverageArgs {
                    action: CoverageAction::Check(CheckArgs {
                        bpmn_file: Some(bpmn.clone()),
                        process: None,
                        max_paths: DEFAULT_MAX_PATHS,
                        archive: None,
                        database_url: None,
                        db_user: None,
                        db_password: None,
                        threshold: 100.0,
                    }),
                },
                &GlobalArgs {
                    format: Some("json".into()),
                    verbose: 0,
                },
                io,
            )
        });
        assert_eq!(code, crate::exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["command"], "coverage check");
        assert_eq!(v["errors"], 0);
    }

    #[test]
    fn missing_file_and_multi_process_selection_are_usage_errors() {
        let (code, _, err) = run_init(Path::new("/does/not/exist.bpmn"), false);
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("file not found"), "{err}");

        let root = scratch_dir("cov-multi");
        let bpmn = root.join("two.bpmn");
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:test:two">
  <bpmn:process id="a" isExecutable="true">
    <bpmn:startEvent id="S1"><bpmn:extensionElements><q:source channel="x"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="S1" targetRef="E1"/>
    <bpmn:endEvent id="E1"/>
  </bpmn:process>
  <bpmn:process id="b" isExecutable="true">
    <bpmn:startEvent id="S2"><bpmn:extensionElements><q:source channel="y"/></bpmn:extensionElements></bpmn:startEvent>
    <bpmn:sequenceFlow id="f2" sourceRef="S2" targetRef="E2"/>
    <bpmn:endEvent id="E2"/>
  </bpmn:process>
</bpmn:definitions>
"#;
        std::fs::write(&bpmn, xml).unwrap();
        let (code, _, err) = run_init(&bpmn, false);
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("--process"), "{err}");
    }

    // ============================ correlation-aware check + reset (C6 phase 5) ================

    mod correlation {
        use super::super::*;
        use std::collections::BTreeMap;
        use sutra_executor::InMemoryCoverageStore;
        use sutra_loader::BusinessCorrelation;

        const DEP: &str = "dep-0000000000000000000000c6";
        const ROUTE: &str = "urn:sutra:coverage:orders:e2e:reply1";

        /// The worked example: one route `reply1` over p1/p2/p3, a `txnId` default key with a
        /// per-leg `p2p3Ref` override on the p2↔p3 legs.
        fn cov_file() -> CoverageFile {
            let segments = BTreeMap::from([
                (
                    "p1".to_string(),
                    vec!["startSeq".to_string(), "endSeq".to_string()],
                ),
                (
                    "p2".to_string(),
                    vec!["startSeq".to_string(), "endSeq".to_string()],
                ),
                (
                    "p3".to_string(),
                    vec!["startSeq".to_string(), "endSeq1".to_string()],
                ),
            ]);
            CoverageFile {
                urn: "urn:sutra:coverage:orders:e2e".to_string(),
                correlations: vec![BusinessCorrelation {
                    id: "transfer".to_string(),
                    key: "txnId".to_string(),
                    // Declared hops/links are validated at lint time (phase 2) and are informational
                    // to the phase-5 union-find, which unions on the fragments' effective keys.
                    links: vec![],
                    coverages: vec![CoverageRoute {
                        path: "reply1".to_string(),
                        segments,
                    }],
                }],
            }
        }

        fn frag(
            process: &str,
            instance: &str,
            key: Option<&str>,
            trace: Option<&str>,
        ) -> CoverageFragment {
            CoverageFragment {
                route_urn: ROUTE.to_string(),
                segment_process: process.to_string(),
                instance_id: instance.to_string(),
                business_key: key.map(str::to_string),
                trace_id: trace.map(str::to_string),
            }
        }

        /// A COMPLETE cascade: p1↔p2 join on `txn-1`, p2↔p3 on the *different* `ref-9`, and
        /// i2's two legs (its two passes) are tied by `instanceId` — one connected component.
        fn complete_cascade() -> Vec<CoverageFragment> {
            vec![
                frag("p1", "i1", Some("txn-1"), Some("tA")),
                frag("p2", "i2", Some("txn-1"), Some("tB")), // p1↔p2 leg (txnId)
                frag("p2", "i2", Some("ref-9"), None),       // p2↔p3 leg (p2p3Ref); i2 ties it
                frag("p3", "i3", Some("ref-9"), Some("tC")), // p2↔p3 reply leg
            ]
        }

        /// The three injected intra-process sub-path flags (`…#p1|#p2|#p3`) phase-3 seeds at deploy
        /// and phase-4 flips live as each segment completes.
        fn subpaths() -> Vec<String> {
            ["#p1", "#p2", "#p3"]
                .iter()
                .map(|s| format!("{ROUTE}{s}"))
                .collect()
        }

        // ---- the union-find predicate directly (nodes + 3 edge types + route-covered rule) ----

        #[test]
        fn union_find_unions_transitively() {
            let mut uf = UnionFind::new(4);
            uf.union(0, 1);
            uf.union(2, 3);
            assert_ne!(uf.find(0), uf.find(2));
            uf.union(1, 2); // bridge the two pairs
            assert_eq!(uf.find(0), uf.find(3));
        }

        #[test]
        fn route_covered_predicate_complete_and_broken() {
            let required: BTreeSet<String> =
                ["p1", "p2", "p3"].iter().map(|s| s.to_string()).collect();

            // Complete cascade: p1—p2(txn-1), p2—p2(i2), p2—p3(ref-9) → one component.
            assert!(route_covered(ROUTE, &required, &complete_cascade()));

            // Broken key edge: p3's key no longer matches the p2↔p3 leg → p3 isolated.
            let broken = vec![
                frag("p1", "i1", Some("txn-1"), Some("tA")),
                frag("p2", "i2", Some("txn-1"), Some("tB")),
                frag("p2", "i2", Some("ref-9"), None),
                frag("p3", "i3", Some("ref-MISMATCH"), Some("tC")),
            ];
            assert!(!route_covered(ROUTE, &required, &broken));

            // Missing segment: no p3 fragment at all.
            let missing: Vec<CoverageFragment> = complete_cascade()
                .into_iter()
                .filter(|f| f.segment_process != "p3")
                .collect();
            assert!(!route_covered(ROUTE, &required, &missing));

            // Fragments for a DIFFERENT route are ignored (route-scoped nodes).
            let other = vec![frag("p1", "i1", Some("txn-1"), None)]
                .into_iter()
                .map(|mut f| {
                    f.route_urn = "urn:sutra:coverage:orders:e2e:reply2".to_string();
                    f
                })
                .chain(complete_cascade())
                .collect::<Vec<_>>();
            assert!(route_covered(ROUTE, &required, &other));
        }

        // ---- the full store-agnostic check over InMemoryCoverageStore (percentage math) ----

        #[tokio::test]
        async fn complete_cascade_flips_the_route_flag_to_covered() {
            let store = InMemoryCoverageStore::new();
            // phase-3 seed + phase-4 marking of the three intra-process sub-paths.
            store.seed_declared(DEP, &subpaths()).await.unwrap();
            for u in subpaths() {
                store.mark_path_covered(DEP, &u).await.unwrap();
            }
            for f in complete_cascade() {
                store.write_fragment(DEP, &f).await.unwrap();
            }

            let report = run_correlation_check(&store, DEP, &[cov_file()], 100.0)
                .await
                .unwrap();

            // The route-level URN was seeded (+1 total) and flipped covered by the union-find.
            assert_eq!(report.total, 4, "3 sub-paths + 1 route-level flag");
            assert_eq!(report.covered, 4);
            assert_eq!(report.percentage, 100.0);
            assert!(report.passed);
            assert_eq!(report.newly_covered, vec![ROUTE.to_string()]);
            assert!(report.uncovered.is_empty());
        }

        #[tokio::test]
        async fn incomplete_broken_key_edge_stays_uncovered() {
            let store = InMemoryCoverageStore::new();
            store.seed_declared(DEP, &subpaths()).await.unwrap();
            for u in subpaths() {
                store.mark_path_covered(DEP, &u).await.unwrap();
            }
            // A broken p2↔p3 key edge: p3's business key no longer matches.
            for f in [
                frag("p1", "i1", Some("txn-1"), Some("tA")),
                frag("p2", "i2", Some("txn-1"), Some("tB")),
                frag("p2", "i2", Some("ref-9"), None),
                frag("p3", "i3", Some("ref-MISMATCH"), Some("tC")),
            ] {
                store.write_fragment(DEP, &f).await.unwrap();
            }

            let report = run_correlation_check(&store, DEP, &[cov_file()], 100.0)
                .await
                .unwrap();

            assert_eq!(report.total, 4);
            assert_eq!(report.covered, 3, "route flag NOT flipped");
            assert_eq!(report.percentage, 75.0);
            assert!(!report.passed, "fails the fail-closed 100% gate");
            assert!(report.newly_covered.is_empty());
            assert_eq!(report.uncovered, vec![ROUTE.to_string()]);
        }

        #[tokio::test]
        async fn incomplete_missing_segment_stays_uncovered() {
            let store = InMemoryCoverageStore::new();
            // p3 never ran: only its two upstream sub-paths were marked, no p3 fragment written.
            store.seed_declared(DEP, &subpaths()).await.unwrap();
            store
                .mark_path_covered(DEP, &format!("{ROUTE}#p1"))
                .await
                .unwrap();
            store
                .mark_path_covered(DEP, &format!("{ROUTE}#p2"))
                .await
                .unwrap();
            for f in complete_cascade()
                .into_iter()
                .filter(|f| f.segment_process != "p3")
            {
                store.write_fragment(DEP, &f).await.unwrap();
            }

            let report = run_correlation_check(&store, DEP, &[cov_file()], 100.0)
                .await
                .unwrap();

            assert_eq!(report.total, 4);
            assert_eq!(report.covered, 2, "route + #p3 both uncovered");
            assert_eq!(report.percentage, 50.0);
            assert!(!report.passed);
            assert!(report.newly_covered.is_empty());
            // Ascending: the route-level URN sorts before its `#p3` sub-path.
            assert_eq!(
                report.uncovered,
                vec![ROUTE.to_string(), format!("{ROUTE}#p3")]
            );
        }

        #[tokio::test]
        async fn a_below_threshold_percentage_fails_a_partial_gate() {
            // The same 75% state passes a lenient 70% gate but fails an 80% one — exercising the
            // threshold knob independent of the all-or-nothing default.
            let store = InMemoryCoverageStore::new();
            store.seed_declared(DEP, &subpaths()).await.unwrap();
            for u in subpaths() {
                store.mark_path_covered(DEP, &u).await.unwrap();
            }
            for f in [
                frag("p1", "i1", Some("txn-1"), None),
                frag("p2", "i2", Some("txn-1"), None),
                frag("p2", "i2", Some("ref-9"), None),
                frag("p3", "i3", Some("ref-MISMATCH"), None),
            ] {
                store.write_fragment(DEP, &f).await.unwrap();
            }
            let lenient = run_correlation_check(&store, DEP, &[cov_file()], 70.0)
                .await
                .unwrap();
            assert_eq!(lenient.percentage, 75.0);
            assert!(lenient.passed, "75% clears a 70% gate");

            let strict = run_correlation_check(&store, DEP, &[cov_file()], 80.0)
                .await
                .unwrap();
            assert!(!strict.passed, "75% fails an 80% gate");
        }

        #[tokio::test]
        async fn reset_reseeds_false_and_clears_fragments_then_recheck_is_uncovered() {
            let store = InMemoryCoverageStore::new();
            store.seed_declared(DEP, &subpaths()).await.unwrap();
            for u in subpaths() {
                store.mark_path_covered(DEP, &u).await.unwrap();
            }
            for f in complete_cascade() {
                store.write_fragment(DEP, &f).await.unwrap();
            }
            // Bring the store to fully-covered (route flag flipped).
            let covered = run_correlation_check(&store, DEP, &[cov_file()], 100.0)
                .await
                .unwrap();
            assert_eq!(covered.covered, 4);

            // Reset re-seeds every flag false and clears the reconstruction fragments.
            let metrics = run_correlation_reset(&store, DEP).await.unwrap();
            assert_eq!((metrics.total, metrics.covered), (4, 0));
            assert_eq!(metrics.coverage_percentage(), 0.0);
            assert!(store.read_fragments(DEP).await.unwrap().is_empty());

            // With the evidence gone, a re-check leaves the route uncovered again.
            let recheck = run_correlation_check(&store, DEP, &[cov_file()], 100.0)
                .await
                .unwrap();
            assert_eq!(recheck.covered, 0);
            assert!(!recheck.passed);
            assert!(recheck.newly_covered.is_empty());
        }

        // ---- FROZEN output shapes (field names, ordering, rounding) -----------------------
        //
        // The coverage implementation moved onto SQL aggregates
        // (`datastore-schema-projection.md` §7); the outputs did NOT move. These assert the
        // serialized forms byte-for-byte, so a future refactor cannot quietly rename a field.

        fn sample_report() -> CorrelationReport {
            CorrelationReport {
                total: 4,
                covered: 3,
                // round(3 × 10000 / 4) / 100
                percentage: 75.0,
                threshold: 100.0,
                uncovered: vec![ROUTE.to_string()],
                newly_covered: vec![format!("{ROUTE}#p2")],
                passed: false,
            }
        }

        #[test]
        fn check_json_shape_is_frozen() {
            let (_, out, _) = crate::output::run_captured("", |io| {
                render_correlation_report(&sample_report(), DEP, ReportFormat::Json, io);
                0
            });
            assert_eq!(
                out.trim_end(),
                r#"{"command":"coverage check","form":"cross-process","deploymentId":"dep-0000000000000000000000c6","total":4,"covered":3,"coveragePercentage":75.0,"threshold":100.0,"uncovered":["urn:sutra:coverage:orders:e2e:reply1"],"newlyCovered":["urn:sutra:coverage:orders:e2e:reply1#p2"],"passed":false}"#
            );
        }

        #[test]
        fn check_text_shape_is_frozen() {
            let (_, out, _) = crate::output::run_captured("", |io| {
                render_correlation_report(&sample_report(), DEP, ReportFormat::Text, io);
                0
            });
            assert_eq!(
                out,
                "coverage check (cross-process) — deployment dep-0000000000000000000000c6\n  \
                 total: 4  covered: 3  coveragePercentage: 75.00%\n  \
                 newly covered this run (1):\n    \
                 + urn:sutra:coverage:orders:e2e:reply1#p2\n  \
                 uncovered (1):\n    \
                 - urn:sutra:coverage:orders:e2e:reply1\n  \
                 threshold: 100.00%  =>  FAIL\n"
            );
        }

        #[test]
        fn reset_json_and_text_shapes_are_frozen() {
            let metrics = CoverageMetrics {
                total: 4,
                covered: 0,
                uncovered: vec![ROUTE.to_string()],
            };
            let (_, json, _) = crate::output::run_captured("", |io| {
                render_reset_report(&metrics, DEP, ReportFormat::Json, io);
                0
            });
            assert_eq!(
                json.trim_end(),
                r#"{"command":"coverage reset","deploymentId":"dep-0000000000000000000000c6","total":4,"covered":0}"#
            );

            let (_, text, _) = crate::output::run_captured("", |io| {
                render_reset_report(&metrics, DEP, ReportFormat::Text, io);
                0
            });
            assert_eq!(
                text,
                "coverage reset — deployment dep-0000000000000000000000c6: 4 path(s) re-seeded \
                 covered=false, reconstruction fragments cleared\n"
            );
        }

        #[test]
        fn percentage_rounding_matches_the_frozen_formula() {
            // The one arithmetic the report shapes carry: round(covered × 10000 / total) / 100,
            // and 0.0 — never NaN — for an empty declaration.
            let pct = |covered: u64, total: u64| {
                CoverageMetrics {
                    total,
                    covered,
                    uncovered: Vec::new(),
                }
                .coverage_percentage()
            };
            assert_eq!(pct(0, 0), 0.0);
            assert_eq!(pct(1, 3), 33.33);
            assert_eq!(pct(2, 3), 66.67);
            assert_eq!(pct(3, 4), 75.0);
            assert_eq!(pct(4, 4), 100.0);
        }
    }
}
