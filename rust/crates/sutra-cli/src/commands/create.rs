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
    let pkg_dir = root.join("packages").join(&package);
    let vars: Vec<(&str, &str)> = vec![("APP", args.name.as_str()), ("PACKAGE", package.as_str())];

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
        validation: ValidationMode::Fatal,
    };
    let (bpmn, accepted, rejected) = render_bpmn(&spec);
    debug_assert!(verify_bpmn(&bpmn).is_ok(), "sample bpmn must parse");
    put(pkg_dir.join("bpmn/sample.bpmn"), bpmn, &mut report);
    put(
        pkg_dir.join("templates/sample-accepted.hbs"),
        accepted,
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
             (package + drop the sample archive first — see {}/README.md)",
            root.display(),
            root.display(),
            root.display()
        ),
    );
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

/// Rewrite the top-level `name:` of a copied package.yaml to the new package name.
fn restamp_package_name(package_yaml: &Path, name: &str, report: &mut WriteReport) {
    let Ok(text) = std::fs::read_to_string(package_yaml) else {
        return;
    };
    let mut replaced = false;
    let out: Vec<String> = text
        .lines()
        .map(|line| {
            if !replaced && line.starts_with("name:") {
                replaced = true;
                format!("name: {name}")
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

    finish(
        "create bpmn",
        &args.package,
        &report,
        format,
        io,
        if channel_declared {
            "the channel binding is already declared in channels.yaml"
        } else {
            "note: declare the inbound channel in channels.yaml (see its commented sample)"
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
            "packages/payments-main/package.yaml",
            "packages/payments-main/channels.yaml",
            "packages/payments-main/datastores.yaml",
            "packages/payments-main/bpmn/sample.bpmn",
            "packages/payments-main/templates/sample-accepted.hbs",
            "packages/payments-main/templates/sample-rejected.hbs",
            "packages/payments-main/schemas/sample/codec-manifest.yaml",
            "packages/payments-main/schemas/sample/sample.xsd",
        ] {
            assert!(root.join(path).is_file(), "missing {path}");
        }
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
        assert_eq!(defs[0].codec.as_deref(), Some("schemas/sample"));

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
        assert!(text.contains("name: flows"), "{text}");
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
        assert!(stamped.contains("name: flows-eu"), "{stamped}");
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
