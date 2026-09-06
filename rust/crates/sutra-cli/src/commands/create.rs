//! `sutra create` — scaffolding: a language-free application workspace of STANDALONE
//! deployment packages (no tenant/library tree, no inheritance), plus per-package and
//! per-process generators.
//!
//! - `create app <name>`: workspace = `packages/<name>-main/` sample package (archive-shaped:
//!   bpmn/rules/templates/scripts/schemas + channels.yaml + datastores.yaml + migrations/)
//!   and the deploy assets (compose mounting the engine image + the deployments dir, a
//!   k8s variant, a health-gated smoke script with a sample channel POST) and the README.
//! - `create deployment <name> [--from <dir>]`: a fresh package skeleton, or an explicit
//!   copy of an existing package (variants are copies, never inheritance).
//! - `create bpmn <process> --validation fatal|soft`: a process with the validation-gateway
//!   wiring + its reply templates.
//!
//! Write semantics: `create` is idempotent-safe — existing files are NEVER overwritten
//! (`create bpmn` honours `--force`; generated files carry a `generated-by:` header).

use std::path::{Path, PathBuf};

use crate::exit;
use crate::output::{report_format, Io, ReportFormat};
use crate::scaffold::{self, asset, pascal_case, render, validate_name, WriteOutcome, WriteReport};
use crate::GlobalArgs;

#[derive(Debug, clap::Args)]
pub struct CreateArgs {
    #[command(subcommand)]
    pub action: CreateAction,
}

#[derive(Debug, clap::Subcommand)]
pub enum CreateAction {
    /// Scaffold an application workspace: a sample deployment package + deploy assets.
    App(AppArgs),
    /// Scaffold a standalone deployment package (or copy one with --from).
    Deployment(DeploymentArgs),
    /// Generate a process with the validation-gateway wiring into a package.
    Bpmn(BpmnArgs),
    /// Scaffold the CI pipeline for an existing workspace: package, catalog drift gate, smoke,
    /// then publish the catalog (GitHub Pages, or a PDF artifact on Bitbucket).
    Ci(CiArgs),
}

/// Which CI system to write for. Deliberately explicit — a scaffolded pipeline lands in a repo
/// that usually already has one, with its own runners and conventions, so guessing the provider
/// would be guessing wrong in the file most likely to be deleted on sight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum CiProvider {
    /// `.github/workflows/sutra.yml` — verify, then publish the catalog to GitHub Pages.
    Github,
    /// `bitbucket-pipelines.yml` — verify, then print the catalog to a PDF release artifact.
    Bitbucket,
}

#[derive(Debug, clap::Args)]
pub struct CiArgs {
    /// The CI system to generate for.
    #[arg(long, value_enum)]
    pub provider: CiProvider,

    /// Workspace root — the directory holding `packages/` and `deploy/`.
    #[arg(long, default_value = ".")]
    pub dir: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct AppArgs {
    /// Workspace name (lowercase kebab-case) — becomes the directory name.
    pub name: String,

    /// Parent directory to create the workspace under.
    #[arg(long, value_name = "DIR", default_value = ".")]
    pub dir: PathBuf,
}

#[derive(Debug, clap::Args)]
pub struct DeploymentArgs {
    /// Package name (lowercase kebab-case) — becomes the package directory name.
    pub name: String,

    /// Parent directory (defaults to ./packages when present, else the working directory).
    #[arg(long, value_name = "DIR")]
    pub dir: Option<PathBuf>,

    /// Copy an existing package directory as the starting point (an explicit variant —
    /// packages never inherit).
    #[arg(long, value_name = "PACKAGE_DIR")]
    pub from: Option<PathBuf>,
}

/// Validation-outcome threshold for the generated gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ValidationMode {
    /// Reject only FATAL outcomes; SOFT_ERRORS continue down the accepted path.
    Fatal,
    /// Reject any validation issue: FATAL and SOFT_ERRORS both take the rejected path.
    Soft,
}

impl ValidationMode {
    fn label(self) -> &'static str {
        match self {
            ValidationMode::Fatal => "fatal",
            ValidationMode::Soft => "soft",
        }
    }

    fn reject_condition(self) -> &'static str {
        match self {
            ValidationMode::Fatal => r#"validation.outcome = "FATAL""#,
            ValidationMode::Soft => {
                r#"validation.outcome = "FATAL" or validation.outcome = "SOFT_ERRORS""#
            }
        }
    }

    fn condition_doc(self) -> &'static str {
        match self {
            ValidationMode::Fatal => "validation.outcome = FATAL (soft errors continue)",
            ValidationMode::Soft => "any validation issue (FATAL or SOFT_ERRORS)",
        }
    }
}

#[derive(Debug, clap::Args)]
pub struct BpmnArgs {
    /// Process id (lowercase kebab-case) — also the bpmn file and template base name.
    pub process: String,

    /// Package directory to generate into (bpmn/ and templates/ live under it).
    #[arg(long, value_name = "PACKAGE_DIR", default_value = ".")]
    pub package: PathBuf,

    /// Validation-gateway policy: which intake outcomes take the rejected branch.
    #[arg(long, value_enum, default_value_t = ValidationMode::Fatal)]
    pub validation: ValidationMode,

    /// Inbound channel the start event subscribes to (default: <process>-in).
    #[arg(long, value_name = "NAME")]
    pub channel: Option<String>,

    /// Inbound message type the start event matches (default: <Process>Request).
    #[arg(long, value_name = "TYPE")]
    pub message_type: Option<String>,

    /// targetNamespace of the generated definitions (default: urn:sutra:deployment:<package dir name>).
    #[arg(long, value_name = "URI")]
    pub namespace: Option<String>,

    /// Overwrite an existing user-edited file (never done implicitly).
    #[arg(long)]
    pub force: bool,
}

pub fn execute(args: CreateArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "create: {msg}");
            return exit::USAGE;
        }
    };
    match args.action {
        CreateAction::App(a) => create_app(a, format, io),
        CreateAction::Deployment(a) => create_deployment(a, format, io),
        CreateAction::Bpmn(a) => create_bpmn(a, format, io),
        CreateAction::Ci(a) => create_ci(a, format, io),
    }
}

// ---------------------------------------------------------------------------- create app

fn create_app(args: AppArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    if let Err(msg) = validate_name(&args.name) {
        let _ = writeln!(io.err, "create app: {msg}");
        return exit::USAGE;
    }
    let root = args.dir.join(&args.name);
    let package = format!("{}-main", args.name);
    let namespace = format!("urn:sutra:deployment:{package}");
    let pkg_dir = root.join("packages").join(&package);
    // The engine image the scaffolded stack pulls: THIS distribution's runtime, at THIS
    // binary's version. Both halves are load-bearing.
    //
    // The registry comes from the distribution seam rather than from the asset, because the
    // codec registry is per-binary: a distribution can link codecs this engine image does not
    // carry, and a scaffold that hard-coded one registry would package cleanly and then fail
    // at runtime with SUTRA.INBOUND.CODEC_NOT_FOUND.
    //
    // The version comes from this binary rather than `latest`, because a pre-release tag
    // deliberately never moves `latest` — the published registry has no such manifest, and
    // `docker compose up` failed outright on a fresh workspace. It is also the correct
    // pairing: the archive this CLI seals is meant for the engine of the same release.
    let engine_image = crate::default_runtime_image();
    let vars: Vec<(&str, &str)> = vec![
        ("APP", args.name.as_str()),
        ("PACKAGE", package.as_str()),
        ("ENGINE_IMAGE", engine_image.as_str()),
        // A dev-only value for the channel's apikey reference, spelled once and substituted into
        // the compose env and the smoke script so the two cannot drift apart.
        ("APIKEY", "dev-only-sample-key"),
        // The deployment namespace, which is the XSD's targetNamespace — so the smoke POST sends
        // a document the schema can actually declare. An unqualified <SampleRequest> is a
        // DIFFERENT element as far as the validator is concerned, and is rejected with "no
        // declaration found".
        ("NAMESPACE", namespace.as_str()),
    ];

    let mut report = WriteReport::default();
    let mut failed = false;
    let mut put =
        |path: PathBuf, content: String, report: &mut WriteReport| match scaffold::write_pristine(
            &path, &content,
        ) {
            Ok(outcome) => report.record(&path, outcome),
            Err(e) => {
                failed = true;
                report.record(&path, WriteOutcome::SkippedExisting);
                tracing::error!(path = %path.display(), error = %e, "scaffold write failed");
            }
        };

    // Workspace docs + deploy assets: compose + deployments dir + k8s + smoke.
    put(
        root.join("README.md"),
        render(asset("app/README.md"), &vars),
        &mut report,
    );
    put(
        root.join("deploy/compose.yaml"),
        render(asset("app/deploy/compose.yaml"), &vars),
        &mut report,
    );
    let smoke = root.join("deploy/smoke.sh");
    put(
        smoke.clone(),
        render(asset("app/deploy/smoke.sh"), &vars),
        &mut report,
    );
    mark_executable(&smoke);
    // The db init script the compose stack mounts into the postgres container: it creates the
    // NOSUPERUSER/NOBYPASSRLS role the engine connects as. Executable because the postgres
    // entrypoint runs `/docker-entrypoint-initdb.d/*.sh` directly when it can.
    let db_init = root.join("deploy/engine-db-init.sh");
    put(
        db_init.clone(),
        render(asset("app/deploy/engine-db-init.sh"), &vars),
        &mut report,
    );
    mark_executable(&db_init);
    put(
        root.join("deploy/k8s/engine.yaml"),
        render(asset("app/deploy/k8s/engine.yaml"), &vars),
        &mut report,
    );
    put(
        root.join("deploy/deployments/README.md"),
        render(asset("app/deploy/deployments/README.md"), &vars),
        &mut report,
    );
    // Keeps sealed archives out of git: they are derived from packages/, change on every
    // rebuild, and are unreviewable as a diff.
    put(
        root.join("deploy/.gitignore"),
        render(asset("app/deploy/.gitignore"), &vars),
        &mut report,
    );

    // The sample standalone package (archive-shaped).
    put(
        pkg_dir.join("package.yaml"),
        render(
            asset("package/package.yaml"),
            &[
                ("PACKAGE", package.as_str()),
                ("ENTRY_PROCESSES", "\n  - sample"),
            ],
        ),
        &mut report,
    );
    put(
        pkg_dir.join("channels.yaml"),
        render(asset("app/channels.yaml"), &vars),
        &mut report,
    );
    put(
        pkg_dir.join("datastores.yaml"),
        render(asset("package/datastores.yaml"), &vars),
        &mut report,
    );
    put(
        pkg_dir.join("schemas/sample/codec-manifest.yaml"),
        render(asset("app/schemas/codec-manifest.yaml"), &vars),
        &mut report,
    );
    put(
        pkg_dir.join("schemas/sample/sample.xsd"),
        render(asset("app/schemas/sample.xsd"), &vars),
        &mut report,
    );

    // The sample process: the same validation-gateway generator as `create bpmn`.
    let spec = BpmnSpec {
        process: "sample",
        channel: "sample-in",
        message_type: "SampleRequest",
        namespace: &format!("urn:sutra:deployment:{package}"),
        // SOFT: reject on ANY validation issue, not only FATAL. A malformed document fails
        // FATAL and would be caught either way, but a content-tier ruleset (<q:validators>) can
        // report SOFT_ERRORS, and a sample whose gateway ignored those would be teaching the
        // wrong lesson about what the gateway is for.
        validation: ValidationMode::Soft,
    };
    let (bpmn, accepted, rejected) = render_bpmn(&spec);
    debug_assert!(verify_bpmn(&bpmn).is_ok(), "sample bpmn must parse");
    put(pkg_dir.join("bpmn/sample.bpmn"), bpmn, &mut report);
    put(
        pkg_dir.join("templates/sample-accepted.hbs"),
        accepted,
        &mut report,
    );
    // The transform contract for those two templates. It is what makes the reply side
    // CHECKABLE at deploy time: without it the lint has no declared output to compare against
    // the codec's type set, and a template that drifts from the schema is discovered by
    // whoever receives the reply.
    put(
        pkg_dir.join("templates/template-manifest.yaml"),
        render(asset("app/templates/template-manifest.yaml"), &vars),
        &mut report,
    );
    put(
        pkg_dir.join("templates/sample-rejected.hbs"),
        rejected,
        &mut report,
    );

    // Archive-layout dirs that start empty (rules/scripts/migrations).
    for dir in ["rules", "scripts", "migrations"] {
        let _ = std::fs::create_dir_all(pkg_dir.join(dir));
    }

    if failed {
        let _ = writeln!(
            io.err,
            "create app: could not write under {}",
            root.display()
        );
        return exit::USAGE;
    }
    finish(
        "create app",
        &root,
        &report,
        format,
        io,
        &format!(
            "next: docker compose -f {}/deploy/compose.yaml up -d && {}/deploy/smoke.sh \
             (package + drop the sample archive first — see {}/README.md); \
             engine image: {engine_image}",
            root.display(),
            root.display(),
            root.display()
        ),
    );
    exit::OK
}

/// Scaffold the CI pipeline for an existing workspace.
///
/// Four steps in the order that fails cheapest first — `package` (fail-closed validation),
/// `generate docs --check` (the committed catalog still matches), the smoke run (the flow answers),
/// then publish. Docs publishing alone would have been the prettiest and least useful third of it.
///
/// The pipeline CHECKS documentation and never writes it: the catalog is committed, so
/// regenerating in CI would author bot commits and race its own drift gate.
fn create_ci(args: CiArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    let root = args.dir.clone();
    if !root.join("packages").is_dir() {
        let _ = writeln!(
            io.err,
            "create ci: {} has no packages/ — run this inside an app workspace \
             (`sutra create app <name>` makes one)",
            root.display()
        );
        return exit::USAGE;
    }
    // The workspace name is its directory name — the same thing `create app` used.
    let app = root
        .canonicalize()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .unwrap_or_else(|| "app".to_string());

    // The release the pipeline installs, read from the SAME distribution seam `self-update`
    // reads: a distribution's CLI, its engine image and its release channel are one matched set,
    // and a pipeline that fetched some other distribution's binary would resolve a different
    // codec registry than the archive it just sealed.
    let binary = crate::program_name();
    let tag = format!("v{}", crate::product_version());
    let repo = crate::update_source()
        .and_then(|src| match &src.channel {
            crate::UpdateChannel::GithubReleases { repo } => Some(repo.clone()),
            _ => None,
        })
        .unwrap_or_else(|| "OWNER/REPO".to_string());

    let vars: Vec<(&str, &str)> = vec![
        ("APP", app.as_str()),
        ("CLI_BINARY", binary.as_str()),
        ("CLI_TAG", tag.as_str()),
        ("CLI_REPO", repo.as_str()),
    ];

    let (asset_path, target) = match args.provider {
        CiProvider::Github => (
            "ci/github/workflow.yml",
            root.join(".github/workflows/sutra.yml"),
        ),
        CiProvider::Bitbucket => (
            "ci/bitbucket/pipelines.yml",
            root.join("bitbucket-pipelines.yml"),
        ),
    };

    let mut report = WriteReport::default();
    let mut failed = false;
    match scaffold::write_pristine(&target, &render(asset(asset_path), &vars)) {
        Ok(outcome) => report.record(&target, outcome),
        Err(e) => {
            failed = true;
            report.record(&target, WriteOutcome::SkippedExisting);
            tracing::error!(path = %target.display(), error = %e, "scaffold write failed");
        }
    }
    if failed {
        let _ = writeln!(io.err, "create ci: could not write {}", target.display());
        return exit::USAGE;
    }

    let next = if repo == "OWNER/REPO" {
        format!(
            "next: set CLI_REPO in {} — this build publishes no release channel, so the \
             install step could not be resolved",
            target.display()
        )
    } else {
        match args.provider {
            CiProvider::Github => format!(
                "next: enable Pages (Settings -> Pages -> Source: GitHub Actions), then push. \
                 The pipeline pins {binary} {tag}"
            ),
            CiProvider::Bitbucket => format!(
                "next: enable Pipelines, then tag a release (v*) for the catalog PDF. \
                 The pipeline pins {binary} {tag}"
            ),
        }
    };
    finish("create ci", &root, &report, format, io, &next);
    exit::OK
}

fn mark_executable(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mut perm = meta.permissions();
            perm.set_mode(0o755);
            let _ = std::fs::set_permissions(path, perm);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

// --------------------------------------------------------------------- create deployment

fn create_deployment(args: DeploymentArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    if let Err(msg) = validate_name(&args.name) {
        let _ = writeln!(io.err, "create deployment: {msg}");
        return exit::USAGE;
    }
    let parent = args.dir.clone().unwrap_or_else(|| {
        let packages = PathBuf::from("packages");
        if packages.is_dir() {
            packages
        } else {
            PathBuf::from(".")
        }
    });
    let root = parent.join(&args.name);
    let mut report = WriteReport::default();

    if let Some(from) = &args.from {
        if !from.is_dir() {
            let _ = writeln!(
                io.err,
                "create deployment: --from {} is not a directory",
                from.display()
            );
            return exit::USAGE;
        }
        if let Err(e) = copy_tree(from, &root, &mut report) {
            let _ = writeln!(io.err, "create deployment: copy failed: {e}");
            return exit::USAGE;
        }
        // The copy is a NEW package: re-stamp its name (labels stay for the author to edit).
        restamp_package_name(&root.join("package.yaml"), &args.name, &mut report);
    } else {
        let mut ok = true;
        let mut put = |path: PathBuf, content: String, report: &mut WriteReport| {
            match scaffold::write_pristine(&path, &content) {
                Ok(outcome) => report.record(&path, outcome),
                Err(e) => {
                    ok = false;
                    tracing::error!(path = %path.display(), error = %e, "scaffold write failed");
                }
            }
        };
        put(
            root.join("package.yaml"),
            render(
                asset("package/package.yaml"),
                &[("PACKAGE", args.name.as_str()), ("ENTRY_PROCESSES", " []")],
            ),
            &mut report,
        );
        put(
            root.join("channels.yaml"),
            asset("package/channels.yaml").to_string(),
            &mut report,
        );
        put(
            root.join("datastores.yaml"),
            asset("package/datastores.yaml").to_string(),
            &mut report,
        );
        for dir in [
            "bpmn",
            "rules",
            "templates",
            "scripts",
            "schemas",
            "migrations",
        ] {
            let _ = std::fs::create_dir_all(root.join(dir));
        }
        if !ok {
            let _ = writeln!(
                io.err,
                "create deployment: could not write under {}",
                root.display()
            );
            return exit::USAGE;
        }
    }

    finish(
        "create deployment",
        &root,
        &report,
        format,
        io,
        &format!(
            "next: sutra create bpmn <process> --package {} (then `sutra package` when ready)",
            root.display()
        ),
    );
    exit::OK
}

/// Recursive never-clobber copy: existing target files are skipped, directories merge.
fn copy_tree(src: &Path, dst: &Path, report: &mut WriteReport) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let target = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target, report)?;
        } else if target.exists() {
            report.record(&target, WriteOutcome::SkippedExisting);
        } else {
            std::fs::copy(entry.path(), &target)?;
            report.record(&target, WriteOutcome::Created);
        }
    }
    Ok(())
}

/// Rewrite a copied package.yaml's `module` LABEL to the new package name.
///
/// It used to rewrite a top-level `name:` key, which package.yaml's closed schema never
/// accepted — `sutra package` rejected every scaffold that carried one. The package's name is
/// its directory name; `module` is the label that names it for operators and telemetry, so a
/// copy (`create deployment --from`) restamps that instead.
fn restamp_package_name(package_yaml: &Path, name: &str, report: &mut WriteReport) {
    let Ok(text) = std::fs::read_to_string(package_yaml) else {
        return;
    };
    let mut replaced = false;
    let out: Vec<String> = text
        .lines()
        .map(|line| {
            let trimmed = line.trim_start();
            if !replaced && trimmed.starts_with("module:") {
                replaced = true;
                let indent = &line[..line.len() - trimmed.len()];
                format!("{indent}module: {name}")
            } else {
                line.to_string()
            }
        })
        .collect();
    if replaced && std::fs::write(package_yaml, out.join("\n") + "\n").is_ok() {
        report.record(package_yaml, WriteOutcome::Updated);
    }
}

// --------------------------------------------------------------------------- create bpmn

/// Parameter bundle for the validation-gateway process template (shared with `create app`).
pub(crate) struct BpmnSpec<'a> {
    pub process: &'a str,
    pub channel: &'a str,
    pub message_type: &'a str,
    pub namespace: &'a str,
    pub validation: ValidationMode,
}

/// Render the process + its two reply templates from the embedded assets.
pub(crate) fn render_bpmn(spec: &BpmnSpec<'_>) -> (String, String, String) {
    let process_id = spec.process.replace('-', "_");
    let process_name = humanize(spec.process);
    let vars: Vec<(&str, &str)> = vec![
        ("PROCESS", spec.process),
        ("PROCESS_ID", process_id.as_str()),
        ("PROCESS_NAME", process_name.as_str()),
        ("CHANNEL", spec.channel),
        ("MESSAGE_TYPE", spec.message_type),
        ("NAMESPACE", spec.namespace),
        ("MODE", spec.validation.label()),
        ("REJECT_CONDITION", spec.validation.reject_condition()),
        ("CONDITION_DOC", spec.validation.condition_doc()),
    ];
    (
        render(asset("bpmn/process.bpmn"), &vars),
        render(asset("bpmn/accepted.hbs"), &vars),
        render(asset("bpmn/rejected.hbs"), &vars),
    )
}

/// The generated BPMN must load through the engine's own model loader.
pub(crate) fn verify_bpmn(xml: &str) -> Result<sutra_bpmn::ProcessModule, sutra_bpmn::SutraError> {
    sutra_bpmn::BpmnModelLoader::new().load(xml.as_bytes())
}

/// `payments-intake` → `Payments intake`.
fn humanize(name: &str) -> String {
    let mut s = name.replace('-', " ");
    if let Some(first) = s.get(..1) {
        let upper = first.to_ascii_uppercase();
        s.replace_range(..1, &upper);
    }
    s
}

fn create_bpmn(args: BpmnArgs, format: ReportFormat, io: &mut Io<'_>) -> i32 {
    if let Err(msg) = validate_name(&args.process) {
        let _ = writeln!(io.err, "create bpmn: {msg}");
        return exit::USAGE;
    }
    if !args.package.is_dir() {
        let _ = writeln!(
            io.err,
            "create bpmn: package directory not found: {}",
            args.package.display()
        );
        return exit::USAGE;
    }
    let channel = args
        .channel
        .clone()
        .unwrap_or_else(|| format!("{}-in", args.process));
    let message_type = args
        .message_type
        .clone()
        .unwrap_or_else(|| format!("{}Request", pascal_case(&args.process)));
    let namespace = args.namespace.clone().unwrap_or_else(|| {
        let pkg = args
            .package
            .canonicalize()
            .ok()
            .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
            .unwrap_or_else(|| "package".to_string());
        format!("urn:sutra:deployment:{pkg}")
    });

    let spec = BpmnSpec {
        process: &args.process,
        channel: &channel,
        message_type: &message_type,
        namespace: &namespace,
        validation: args.validation,
    };
    let (bpmn, accepted, rejected) = render_bpmn(&spec);
    if let Err(e) = verify_bpmn(&bpmn) {
        // A template defect, not a user error — surface loudly.
        let _ = writeln!(io.err, "create bpmn: generated model failed to load: {e}");
        return exit::USAGE;
    }

    let mut report = WriteReport::default();
    let targets = [
        (
            args.package.join(format!("bpmn/{}.bpmn", args.process)),
            bpmn,
        ),
        (
            args.package
                .join(format!("templates/{}-accepted.hbs", args.process)),
            accepted,
        ),
        (
            args.package
                .join(format!("templates/{}-rejected.hbs", args.process)),
            rejected,
        ),
    ];
    for (path, content) in targets {
        match scaffold::write_generated(&path, &content, args.force) {
            Ok(outcome) => report.record(&path, outcome),
            Err(e) => {
                let _ = writeln!(io.err, "create bpmn: cannot write {}: {e}", path.display());
                return exit::USAGE;
            }
        }
    }

    // Best-effort authoring nudge: the channel must be declared for the intake to exist.
    let channels_yaml = args.package.join("channels.yaml");
    let channel_declared = std::fs::read_to_string(&channels_yaml)
        .map(|text| text.contains(&format!("name: {channel}")))
        .unwrap_or(false);

    // Second nudge, same spirit: a package that DECLARES transform contracts has just gained
    // two templates that are not in them, and an undeclared template is simply unchecked at
    // deploy time. Only ever mentioned — this file is the author's, and merging YAML that
    // somebody hand-edited is not something a scaffolder should do behind their back.
    let manifest = args.package.join("templates/template-manifest.yaml");
    let manifest_gap = std::fs::read_to_string(&manifest)
        .map(|text| !text.contains(&format!("{}-accepted.hbs", args.process)))
        .unwrap_or(false);

    finish(
        "create bpmn",
        &args.package,
        &report,
        format,
        io,
        &match (channel_declared, manifest_gap) {
            (true, false) => "the channel binding is already declared in channels.yaml".to_string(),
            (true, true) => format!(
                "the channel binding is already declared in channels.yaml; add {}-accepted.hbs \
                 and {}-rejected.hbs to templates/template-manifest.yaml to have their output \
                 types checked at deploy time",
                args.process, args.process
            ),
            (false, false) => {
                "note: declare the inbound channel in channels.yaml (see its commented sample)"
                    .to_string()
            }
            (false, true) => format!(
                "note: declare the inbound channel in channels.yaml (see its commented sample), \
                 and add {}-accepted.hbs / {}-rejected.hbs to templates/template-manifest.yaml",
                args.process, args.process
            ),
        },
    );

    if !report.skipped_user_files().is_empty() {
        let _ = writeln!(
            io.err,
            "create bpmn: {} file(s) exist with user edits — re-run with --force to overwrite",
            report.skipped_user_files().len()
        );
        return exit::FINDINGS;
    }
    exit::OK
}

// -------------------------------------------------------------------------------- shared

fn finish(
    command: &str,
    root: &Path,
    report: &WriteReport,
    format: ReportFormat,
    io: &mut Io<'_>,
    next: &str,
) {
    match format {
        ReportFormat::Text => {
            let _ = writeln!(io.out, "{command}: {}", root.display());
            let _ = write!(io.out, "{}", report.render_text(root));
            let _ = writeln!(io.out, "{next}");
        }
        ReportFormat::Json => {
            let payload = serde_json::json!({
                "command": command,
                "root": root.display().to_string(),
                "files": report.to_json(root),
                "next": next,
            });
            let _ = writeln!(io.out, "{payload}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::scratch_dir;

    fn run(action: CreateAction, format: Option<&str>) -> (i32, String, String) {
        let global = GlobalArgs {
            format: format.map(str::to_owned),
            verbose: 0,
        };
        run_captured("", |io| execute(CreateArgs { action }, &global, io))
    }

    fn app_args(name: &str, dir: &Path) -> CreateAction {
        CreateAction::App(AppArgs {
            name: name.into(),
            dir: dir.to_path_buf(),
        })
    }

    /// THE test this file was missing: a scaffolded workspace must survive the very next command
    /// the tool tells the user to run. Every asset assertion elsewhere in this module passed
    /// while `sutra package` rejected the output outright — a `name:` key package.yaml's closed
    /// schema forbids, a codec bound as `schemas/sample` when the loader registers it as
    /// `sample`, an XSD with no targetNamespace so its schema was never exposed. Nothing short
    /// of running the real validator catches that class.
    #[test]
    fn a_scaffolded_app_passes_the_package_validator() {
        let dir = scratch_dir("create-app-lints");
        let (code, out, _) = run(app_args("payments", &dir), None);
        assert_eq!(code, crate::exit::OK, "{out}");

        let pkg = dir.join("payments/packages/payments-main");
        let report = sutra_loader::package::lint_dir(&pkg);
        let errors: Vec<String> = report
            .diagnostics
            .iter()
            .filter(|d| d.severity == sutra_loader::lint::LintSeverity::Error)
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect();
        assert!(
            errors.is_empty(),
            "the scaffolded package does not lint clean:\n  {}",
            errors.join("\n  ")
        );
    }

    #[test]
    fn create_ci_writes_a_pipeline_per_provider_with_every_token_resolved() {
        let dir = scratch_dir("create-ci");
        let (code, out, _) = run(app_args("payments", &dir), None);
        assert_eq!(code, crate::exit::OK, "{out}");
        let root = dir.join("payments");

        for (provider, rel) in [
            (CiProvider::Github, ".github/workflows/sutra.yml"),
            (CiProvider::Bitbucket, "bitbucket-pipelines.yml"),
        ] {
            let action = CreateAction::Ci(CiArgs {
                provider,
                dir: root.clone(),
            });
            let (code, out, _) = run(action, None);
            assert_eq!(code, crate::exit::OK, "{out}");

            let body = std::fs::read_to_string(root.join(rel)).unwrap();
            // An unresolved %%TOKEN%% is a template defect that would ship a broken pipeline.
            assert!(!body.contains("%%"), "unrendered token in {rel}:\n{body}");
            // The four steps, in the order that fails cheapest first.
            assert!(body.contains("package"), "{rel} must seal the packages");
            assert!(
                body.contains("generate docs --input packages --output catalog --check"),
                "{rel} must gate on catalog drift, not regenerate:\n{body}"
            );
            assert!(body.contains("smoke.sh"), "{rel} must run the smoke");
            assert!(
                body.contains("mdbook build catalog"),
                "{rel} must build the book"
            );
            // The release is pinned to THIS binary's version — a distribution's CLI and its
            // engine are a matched pair, and `latest` never moves for a pre-release tag.
            assert!(
                body.contains(&format!("v{}", crate::product_version())),
                "{rel} must pin the CLI version:\n{body}"
            );
        }
    }

    #[test]
    fn create_ci_refuses_a_directory_that_is_not_a_workspace() {
        let dir = scratch_dir("create-ci-bare");
        let action = CreateAction::Ci(CiArgs {
            provider: CiProvider::Github,
            dir: dir.clone(),
        });
        let (code, _, err) = run(action, None);
        assert_eq!(code, crate::exit::USAGE);
        assert!(err.contains("has no packages/"), "{err}");
        assert!(!dir.join(".github/workflows/sutra.yml").exists());
    }

    #[test]
    fn create_app_scaffolds_the_f3_workspace() {
        let dir = scratch_dir("create-app");
        let (code, out, _) = run(app_args("payments", &dir), None);
        assert_eq!(code, crate::exit::OK, "{out}");

        let root = dir.join("payments");
        for path in [
            "README.md",
            "deploy/compose.yaml",
            "deploy/smoke.sh",
            "deploy/k8s/engine.yaml",
            "deploy/deployments/README.md",
            "deploy/.gitignore",
            "packages/payments-main/package.yaml",
            "packages/payments-main/channels.yaml",
            "packages/payments-main/datastores.yaml",
            "packages/payments-main/bpmn/sample.bpmn",
            "packages/payments-main/templates/sample-accepted.hbs",
            "packages/payments-main/templates/sample-rejected.hbs",
            "packages/payments-main/templates/template-manifest.yaml",
            "packages/payments-main/schemas/sample/codec-manifest.yaml",
            "packages/payments-main/schemas/sample/sample.xsd",
        ] {
            assert!(root.join(path).is_file(), "missing {path}");
        }
        // A package names its secrets by reference (`env:MY_API_KEY`) and the ENGINE resolves
        // them in its own environment — so without an env_file the only way to add a package
        // that needs one is to hand-edit this YAML, which is the step people skip. It is also
        // unforgiving when skipped: an unresolvable channel-auth reference makes the engine
        // refuse to serve rather than open an unauthenticated port. `required: false` keeps
        // `up` working before anyone has written a `.env`.
        let compose = std::fs::read_to_string(root.join("deploy/compose.yaml")).unwrap();
        assert!(
            compose.contains("env_file:") && compose.contains("required: false"),
            "the engine service must read an OPTIONAL deploy/.env:\n{compose}"
        );
        // Compose names a project after the compose file's DIRECTORY when nothing says
        // otherwise — `deploy` for every app this scaffolds. Two apps on one machine would then
        // share a project, and `down -v` in either would stop the other's containers and drop
        // its database volume with them.
        assert!(
            compose.contains("\nname: payments\n"),
            "the stack must be scoped to the app, not to the directory name:\n{compose}"
        );

        // The point of the ignore is the sealed archives: they are derived from packages/,
        // change on every rebuild, and are unreviewable as a diff.
        let ignore = std::fs::read_to_string(root.join("deploy/.gitignore")).unwrap();
        assert!(
            ignore.contains("deployments/*.sutra"),
            "the deploy ignore must cover sealed archives:\n{ignore}"
        );
        // …and the env_file it now reads, which holds credentials by design.
        assert!(
            ignore.contains(".env"),
            "the deploy ignore must cover the env_file:\n{ignore}"
        );
        for dir_name in [
            "packages/payments-main/rules",
            "packages/payments-main/scripts",
            "packages/payments-main/migrations",
        ] {
            assert!(root.join(dir_name).is_dir(), "missing dir {dir_name}");
        }

        // The sample process loads through the engine model loader.
        let bpmn =
            std::fs::read_to_string(root.join("packages/payments-main/bpmn/sample.bpmn")).unwrap();
        let module = verify_bpmn(&bpmn).unwrap();
        assert_eq!(module.process_ids(), vec!["sample"]);

        // No tokens survive rendering anywhere in the tree.
        for entry in walk(&root) {
            let text = std::fs::read_to_string(&entry).unwrap();
            assert!(
                !text.contains("%%"),
                "unrendered token in {}",
                entry.display()
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(root.join("deploy/smoke.sh"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o111, 0o111, "smoke.sh must be executable");
        }
    }

    #[test]
    fn the_scaffolded_stack_pulls_this_distributions_engine() {
        // Both manifests name the SAME image, and it is the one this binary's distribution
        // publishes — a scaffold that pulled a different distribution's engine would resolve a
        // different codec set than the CLI that linted the package.
        let dir = scratch_dir("create-app-image");
        let (code, _, _) = run(app_args("acme", &dir), None);
        assert_eq!(code, crate::exit::OK);
        let expected = crate::default_runtime_image();
        let root = dir.join("acme");
        for manifest in ["deploy/compose.yaml", "deploy/k8s/engine.yaml"] {
            let text = std::fs::read_to_string(root.join(manifest)).unwrap();
            assert!(
                text.contains(&expected),
                "{manifest} does not pull {expected}"
            );
        }
        // Pinned, never floating: a pre-release tag never moves `latest`, so `latest` names a
        // manifest the registry does not have. Checked on the `image:` KEYS only — the prose
        // above them explains exactly this, and matching the whole file would catch the comment.
        let compose = std::fs::read_to_string(root.join("deploy/compose.yaml")).unwrap();
        for line in compose
            .lines()
            .filter(|l| l.trim_start().starts_with("image:"))
        {
            assert!(
                !line.trim_end().ends_with(":latest"),
                "unpinned image: {line}"
            );
        }
    }

    #[test]
    fn create_app_output_is_free_of_retired_vocabulary() {
        // The scaffold is product surface: no vocabulary from the retired reference stack
        // anywhere in what it emits.
        let dir = scratch_dir("create-app-vocabulary");
        let (code, _, _) = run(app_args("ledger", &dir), None);
        assert_eq!(code, crate::exit::OK);
        let forbidden = [
            "quarkus",
            "maven",
            concat!("pom", ".xml"),
            "java",
            "jvm",
            "picocli",
            "qute",
        ];
        for entry in walk(&dir.join("ledger")) {
            let text = std::fs::read_to_string(&entry).unwrap().to_lowercase();
            for word in forbidden {
                assert!(
                    !text.contains(word),
                    "forbidden vocabulary '{word}' in {}",
                    entry.display()
                );
            }
        }
    }

    #[test]
    fn create_app_scaffold_parses_through_the_engine_loaders() {
        let dir = scratch_dir("create-app-parse");
        let (code, _, _) = run(app_args("shop", &dir), None);
        assert_eq!(code, crate::exit::OK);
        let pkg = dir.join("shop/packages/shop-main");

        let channels = std::fs::read_to_string(pkg.join("channels.yaml")).unwrap();
        let defs = sutra_channels::config::load_channel_definitions(
            channels.as_bytes(),
            "default",
            "shop-main",
            "1.0.0",
            "channels.yaml",
        )
        .unwrap();
        assert_eq!(defs.len(), 1);
        assert_eq!(defs[0].binding.channel_name, "sample-in");
        // `urn:sample` — the form the ENGINE registry uses (`urn:` + the folder name under
        // schemas/, colon-joined when nested). The bare name resolved at package time and then
        // 500'd on every message, so both the scaffold and the lint now insist on this one.
        assert_eq!(defs[0].codec.as_deref(), Some("urn:sample"));

        let datastores = std::fs::read_to_string(pkg.join("datastores.yaml")).unwrap();
        let stores = sutra_datastore::config::parse_datastores(&datastores).unwrap();
        assert!(stores.is_empty(), "sample package declares no stores");
    }

    #[test]
    fn create_app_is_idempotent_and_never_clobbers() {
        let dir = scratch_dir("create-app-idem");
        assert_eq!(run(app_args("keep", &dir), None).0, crate::exit::OK);
        let readme = dir.join("keep/README.md");
        std::fs::write(&readme, "MINE").unwrap();
        let (code, out, _) = run(app_args("keep", &dir), None);
        assert_eq!(code, crate::exit::OK);
        assert_eq!(std::fs::read_to_string(&readme).unwrap(), "MINE");
        assert!(out.contains("skipped (exists)"), "{out}");
    }

    #[test]
    fn create_deployment_scaffolds_and_copies() {
        let dir = scratch_dir("create-deployment");
        let parent = dir.join("packages");
        std::fs::create_dir_all(&parent).unwrap();

        let (code, _, _) = run(
            CreateAction::Deployment(DeploymentArgs {
                name: "flows".into(),
                dir: Some(parent.clone()),
                from: None,
            }),
            None,
        );
        assert_eq!(code, crate::exit::OK);
        let root = parent.join("flows");
        assert!(root.join("package.yaml").is_file());
        assert!(root.join("channels.yaml").is_file());
        assert!(root.join("datastores.yaml").is_file());
        for d in [
            "bpmn",
            "rules",
            "templates",
            "scripts",
            "schemas",
            "migrations",
        ] {
            assert!(root.join(d).is_dir(), "missing {d}");
        }
        let text = std::fs::read_to_string(root.join("package.yaml")).unwrap();
        // No `name:` — package.yaml's schema is closed and the package name IS the directory
        // name. The module LABEL carries it, which is what `create deployment --from` restamps.
        assert!(
            !text.contains("\nname:"),
            "package.yaml must declare no name key: {text}"
        );
        // Skeleton channels/datastores parse clean (empty declarations).
        let stores = sutra_datastore::config::parse_datastores(
            &std::fs::read_to_string(root.join("datastores.yaml")).unwrap(),
        )
        .unwrap();
        assert!(stores.is_empty());
        let channels = sutra_channels::config::load_channel_definitions(
            std::fs::read_to_string(root.join("channels.yaml"))
                .unwrap()
                .as_bytes(),
            "default",
            "flows",
            "1.0.0",
            "channels.yaml",
        )
        .unwrap();
        assert!(channels.is_empty());

        // Variant copy: --from duplicates and re-stamps the name; never clobbers.
        std::fs::write(root.join("bpmn/x.bpmn"), "<x/>").unwrap();
        let (code, _, _) = run(
            CreateAction::Deployment(DeploymentArgs {
                name: "flows-eu".into(),
                dir: Some(parent.clone()),
                from: Some(root.clone()),
            }),
            None,
        );
        assert_eq!(code, crate::exit::OK);
        let copy = parent.join("flows-eu");
        assert!(copy.join("bpmn/x.bpmn").is_file());
        let stamped = std::fs::read_to_string(copy.join("package.yaml")).unwrap();
        // The MODULE LABEL is what a copy restamps — package.yaml has no `name` key to rewrite.
        assert!(stamped.contains("module: flows-eu"), "{stamped}");
    }

    #[test]
    fn create_bpmn_generates_the_validation_gateway_for_both_modes() {
        for (mode, expected) in [
            (ValidationMode::Fatal, r#"validation.outcome = "FATAL""#),
            (
                ValidationMode::Soft,
                r#"validation.outcome = "FATAL" or validation.outcome = "SOFT_ERRORS""#,
            ),
        ] {
            let dir = scratch_dir("create-bpmn");
            let (code, _, _) = run(
                CreateAction::Bpmn(BpmnArgs {
                    process: "intake".into(),
                    package: dir.clone(),
                    validation: mode,
                    channel: None,
                    message_type: None,
                    namespace: None,
                    force: false,
                }),
                None,
            );
            assert_eq!(code, crate::exit::OK);
            let text = std::fs::read_to_string(dir.join("bpmn/intake.bpmn")).unwrap();
            assert!(text.contains(expected), "{mode:?}: missing condition");
            assert!(text
                .contains(r#"<q:source channel="intake-in" messageTypeValue="IntakeRequest"/>"#));
            assert!(text.contains(r#"default="Flow_Accepted""#));
            assert!(text.contains(r#"<q:onValidation mode="route"/>"#));
            assert!(dir.join("templates/intake-accepted.hbs").is_file());
            assert!(dir.join("templates/intake-rejected.hbs").is_file());

            // Loads through the engine model loader; the gateway routes exist.
            let module = verify_bpmn(&text).unwrap();
            let p = module.process("intake").unwrap();
            let routes =
                crate::routes::enumerate_coverage_routes(p, crate::routes::DEFAULT_MAX_PATHS)
                    .unwrap();
            assert_eq!(routes.len(), 2, "rejected + accepted");
        }
    }

    #[test]
    fn create_bpmn_respects_user_edits_without_force() {
        let dir = scratch_dir("create-bpmn-force");
        let mk = |force: bool| {
            run(
                CreateAction::Bpmn(BpmnArgs {
                    process: "flow".into(),
                    package: dir.clone(),
                    validation: ValidationMode::Fatal,
                    channel: None,
                    message_type: None,
                    namespace: Some("urn:test:x".into()),
                    force,
                }),
                None,
            )
        };
        assert_eq!(mk(false).0, crate::exit::OK);
        // Re-run over the pristine generated file: unchanged, still OK.
        assert_eq!(mk(false).0, crate::exit::OK);

        // A user edit (marker removed) is preserved without --force.
        let bpmn = dir.join("bpmn/flow.bpmn");
        std::fs::write(&bpmn, "<mine/>").unwrap();
        let (code, _, err) = mk(false);
        assert_eq!(code, crate::exit::FINDINGS);
        assert!(err.contains("--force"), "{err}");
        assert_eq!(std::fs::read_to_string(&bpmn).unwrap(), "<mine/>");

        // --force regenerates.
        assert_eq!(mk(true).0, crate::exit::OK);
        assert!(std::fs::read_to_string(&bpmn)
            .unwrap()
            .contains("Validation outcome?"));
    }

    #[test]
    fn invalid_names_are_usage_errors() {
        let dir = scratch_dir("create-names");
        for action in [
            app_args("Bad Name", &dir),
            CreateAction::Deployment(DeploymentArgs {
                name: "UPPER".into(),
                dir: Some(dir.clone()),
                from: None,
            }),
            CreateAction::Bpmn(BpmnArgs {
                process: "1st".into(),
                package: dir.clone(),
                validation: ValidationMode::Fatal,
                channel: None,
                message_type: None,
                namespace: None,
                force: false,
            }),
        ] {
            let (code, _, err) = run(action, None);
            assert_eq!(code, crate::exit::USAGE, "{err}");
            assert!(err.contains("not a valid name"), "{err}");
        }
    }

    fn walk(root: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    stack.push(entry.path());
                } else {
                    out.push(entry.path());
                }
            }
        }
        out
    }
}
