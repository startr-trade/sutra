//! `sutra lint` — run the full fail-closed package-time validation suite over one
//! standalone deployment-package directory (R13 authoring unit) without emitting
//! anything. Thin binding over `sutra_loader::lint_dir` — lint and package share ONE
//! validation code path.
//!
//! `--overlay` lints the COMPOSED tree: the layers (one shared folder plus a small
//! per-variant folder) are merged exactly as `sutra package --overlay` merges them, so a
//! variant that is incomplete on its own is linted as what would actually be sealed.

use std::path::PathBuf;

use crate::commands::endpoint_check::{
    ask_engine, deployment_specific_connection_warning, facts_of_archive, judge, load_active_set,
    warning, CheckFindings,
};
use crate::exit;
use crate::output::{report_format, Diagnostic, Io, ReportFormat, Severity};
use crate::GlobalArgs;

#[derive(Debug, Default, clap::Args)]
pub struct LintArgs {
    /// Deployment-package directory to validate (contains package.yaml) — or a deployment
    /// manifest (`.yaml`/`.yml`), whose resolved layer stack is printed before the findings.
    pub package_dir: PathBuf,

    /// Directory to compose UNDER the package dir; repeatable, lowest layer first.
    ///
    /// For the "one shared folder plus per-variant overlays" layout: each --overlay is a
    /// BASE layer applied in the order given, and the package dir is applied LAST, so the
    /// package dir's copy of a path wins over every overlay and a later overlay wins over
    /// an earlier one. The composed tree is what gets validated — the same composition
    /// `sutra package --overlay` seals.
    ///
    /// Every overridden path is reported as a warning naming the layer that won. An overlay
    /// may not carry package.yaml (labels come from the package dir alone), may not contain
    /// symlinks, and cannot DELETE a file a lower layer provides — composition only adds and
    /// replaces. With a manifest, every --overlay stacks ABOVE the manifest's last layer.
    #[arg(long = "overlay", value_name = "DIR")]
    pub overlay: Vec<PathBuf>,

    /// The target's active set, to judge this package against the way a deploy would: a sealed
    /// `.sutra` archive or a directory of them; repeatable. An endpoint another archive holds
    /// exclusively is an error; a data store that would share its rows is a warning. Only the
    /// transports this binary links can be judged: a channel on any other is reported as
    /// unchecked, so prefer --engine-url where a target is reachable.
    ///
    /// An archive of this package's own tenant, module and version is taken as the copy being
    /// replaced when its file name is the `tenant--module--version` slot or
    /// `<package-dir-name>.sutra`. Under any other file name it is reported: a folder would keep it
    /// on the endpoints they share, and refuse this package.
    #[arg(long = "active-set", value_name = "PATH")]
    pub active_set: Vec<PathBuf>,

    /// Judge this package against a running engine's active set
    /// (`POST /admin/deployments/check`). The engine links every transport it can serve and
    /// resolves connection references in its own environment, so this is the stronger answer.
    #[arg(long = "engine-url", value_name = "URL")]
    pub engine_url: Option<String>,

    /// Admin auth key/secret for --engine-url. Reads `SUTRA_ADMIN_API_KEY` when omitted.
    #[arg(long = "api-key", env = "SUTRA_ADMIN_API_KEY", value_name = "KEY")]
    pub api_key: Option<String>,

    /// Header carrying the --api-key.
    #[arg(
        long = "api-key-header",
        value_name = "HEADER",
        default_value = "X-API-Key"
    )]
    pub api_key_header: String,
}

/// Convert a loader diagnostic into the CLI's one printable finding shape.
pub(crate) fn diagnostic_of(d: &sutra_loader::LintDiagnostic) -> Diagnostic {
    Diagnostic {
        severity: match d.severity {
            sutra_loader::LintSeverity::Error => Severity::Error,
            sutra_loader::LintSeverity::Warning => Severity::Warn,
        },
        code: d.code.clone(),
        message: d.message.clone(),
        location: None,
    }
}

pub fn execute(args: LintArgs, global: &GlobalArgs, io: &mut Io<'_>) -> i32 {
    let format = match report_format(global.format.as_deref()) {
        Ok(f) => f,
        Err(msg) => {
            let _ = writeln!(io.err, "lint: {msg}");
            return exit::USAGE;
        }
    };
    let mut manifest = None;
    if crate::manifest::is_manifest_path(&args.package_dir) {
        match crate::manifest::load(&args.package_dir, &args.overlay) {
            Ok(m) => manifest = Some(m),
            Err(msg) => {
                let _ = writeln!(
                    io.err,
                    "lint: manifest {}: {msg}",
                    args.package_dir.display()
                );
                return exit::USAGE;
            }
        }
    } else if !args.package_dir.is_dir() {
        let _ = writeln!(
            io.err,
            "lint: package directory not found: {}",
            args.package_dir.display()
        );
        return exit::USAGE;
    }
    for overlay in &args.overlay {
        if manifest.is_none() && !overlay.is_dir() {
            let _ = writeln!(
                io.err,
                "lint: overlay directory not found: {}",
                overlay.display()
            );
            return exit::USAGE;
        }
    }

    let (report, resolution) = match &manifest {
        Some(m) => {
            let (report, resolution) = sutra_loader::lint_stack(&m.stack);
            (report, Some(resolution))
        }
        None => (
            sutra_loader::lint_dir_with_overlays(&args.package_dir, &args.overlay),
            None,
        ),
    };
    let mut diagnostics: Vec<Diagnostic> = report.diagnostics.iter().map(diagnostic_of).collect();
    let mut errors = report.errors().count();
    let mut warnings = report.warnings().count();
    // The target checks judge the sealed result, so they only run on a package that seals.
    if errors == 0 {
        match target_findings(&args, manifest.as_ref()) {
            Ok(findings) => {
                errors += findings.errors.len();
                warnings += findings.warnings.len();
                diagnostics.extend(findings.errors);
                diagnostics.extend(findings.warnings);
            }
            Err(msg) => {
                let _ = writeln!(io.err, "lint: {msg}");
                return exit::USAGE;
            }
        }
    }

    match format {
        ReportFormat::Text => {
            if let (Some(m), Some(r)) = (&manifest, &resolution) {
                let _ = write!(io.out, "{}", crate::manifest::render_resolution_text(m, r));
            }
            for diagnostic in &diagnostics {
                let _ = writeln!(io.out, "{}", diagnostic.render_text());
            }
            let _ = writeln!(io.out, "{errors} error(s), {warnings} warning(s)");
        }
        ReportFormat::Json => {
            let mut payload = serde_json::json!({
                "packageDir": args.package_dir.display().to_string(),
                "errors": errors,
                "warnings": warnings,
                "diagnostics": diagnostics.iter().map(Diagnostic::to_json).collect::<Vec<_>>(),
            });
            if let (Some(m), Some(r)) = (&manifest, &resolution) {
                payload["stack"] = crate::manifest::resolution_json(m, r);
            }
            let _ = writeln!(io.out, "{payload}");
        }
    }
    if errors > 0 {
        exit::FINDINGS
    } else {
        exit::OK
    }
}

/// Seal the package into a throwaway directory and return the archive's bytes. `None` when it does
/// not seal, which the validation above already accounts for.
fn seal_for_check(
    args: &LintArgs,
    manifest: Option<&crate::manifest::DeploymentManifest>,
) -> Option<Vec<u8>> {
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let out = std::env::temp_dir().join(format!("sutra-lint-{}-{unique}", std::process::id()));
    let sealed = match manifest {
        Some(m) => crate::commands::package::seal_manifest(m, &out),
        None => sutra_loader::assemble_dir_with_overlays(
            &args.package_dir,
            &args.overlay,
            &out,
            &Default::default(),
        ),
    };
    let bytes = sealed
        .ok()
        .and_then(|outcome| outcome.archives.first().map(|a| a.file_path.clone()))
        .and_then(|path| std::fs::read(path).ok());
    let _ = std::fs::remove_dir_all(&out);
    bytes
}

/// The findings about this package and a target: against the engine or the active set named, or,
/// with neither, the warning about what co-hosting a copy of it would share.
fn target_findings(
    args: &LintArgs,
    manifest: Option<&crate::manifest::DeploymentManifest>,
) -> Result<CheckFindings, String> {
    let asked_for_target = args.engine_url.is_some() || !args.active_set.is_empty();
    let Some(bytes) = seal_for_check(args, manifest) else {
        // Passing a target's check by not running it would read as a clean answer.
        return if asked_for_target {
            Err("the package could not be sealed, so it was not checked against the target".into())
        } else {
            Ok(CheckFindings::default())
        };
    };
    let mut findings = CheckFindings::default();
    if let Some(engine_url) = args.engine_url.as_deref() {
        let admin_key = args
            .api_key
            .as_deref()
            .map(|k| (args.api_key_header.as_str(), k));
        findings =
            block_on_check(ask_engine(engine_url, &bytes, None, admin_key)).map_err(|reason| {
                format!("engine at {engine_url} gave no verdict for this package: {reason}")
            })?;
    }
    if !args.active_set.is_empty() {
        let others = load_active_set(&args.active_set)?;
        let candidate = facts_of_archive(&bytes)?;
        // Only this same tenant, module and version can be a copy being replaced: a file named after
        // this package may hold another owner's archive. And only a file this package would land
        // on replaces anything; the same version under another name stays, and holds what it holds.
        let (same_version, others): (Vec<_>, Vec<_>) = others.into_iter().partition(|other| {
            other.owner == candidate.owner && other.version == candidate.version
        });
        let file_names = replaced_file_names(args, &candidate);
        for other in same_version
            .iter()
            .filter(|other| !file_names.contains(&other.slot))
        {
            findings
                .warnings
                .extend(same_version_elsewhere(&candidate, other));
        }
        let local = judge(&candidate, &others);
        findings.errors.extend(local.errors);
        findings.warnings.extend(local.warnings);
    }
    if args.engine_url.is_none() && args.active_set.is_empty() {
        if let Ok(archive) = sutra_loader::read_archive(&bytes) {
            findings
                .warnings
                .extend(deployment_specific_connection_warning(&archive.deployment));
        }
    }
    Ok(findings)
}

/// The file names this package replaces in a folder.
fn replaced_file_names(
    args: &LintArgs,
    candidate: &sutra_engine::deployment_facts::DeploymentFacts,
) -> Vec<String> {
    let extension = sutra_loader::archive::ARCHIVE_EXTENSION;
    let mut names = vec![format!("{}.{extension}", candidate.slot)];
    let dir = dunce::canonicalize(&args.package_dir).unwrap_or_else(|_| args.package_dir.clone());
    if let Some(name) = dir.file_name() {
        names.push(format!("{}.{extension}", name.to_string_lossy()));
    }
    names
}

/// The warning owed when `other` is this version under another file name, on an endpoint in common.
fn same_version_elsewhere(
    candidate: &sutra_engine::deployment_facts::DeploymentFacts,
    other: &sutra_engine::deployment_facts::DeploymentFacts,
) -> Option<Diagnostic> {
    let shared = other.endpoints.iter().find(|endpoint| {
        candidate.endpoints.iter().any(|mine| {
            mine.transport == endpoint.transport && mine.claim.identity == endpoint.claim.identity
        })
    })?;
    Some(warning(
        sutra_channels::codes::DEPLOY_ENDPOINT_SAME_VERSION_ELSEWHERE,
        format!(
            "the active set holds {} as '{}', with other content, on {} endpoint '{}'. Replacing \
             that file replaces it. Under any other file name a folder keeps '{}' and refuses this \
             package",
            candidate.label, other.slot, shared.transport, shared.claim.identity, other.slot
        ),
    ))
}

/// A lint run has no runtime of its own; this one call needs one.
fn block_on_check<F: std::future::Future>(future: F) -> F::Output {
    crate::commands::deploy::block_on(future)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::output::run_captured;
    use crate::test_fixtures::scratch_dir;

    const PLAIN_BPMN: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:solo:1.0.0">
  <bpmn:process id="solo" name="Solo" isExecutable="true">
    <bpmn:startEvent id="Start"/>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#;

    pub(crate) fn valid_package_dir(label: &str) -> std::path::PathBuf {
        let dir = scratch_dir(label);
        std::fs::create_dir_all(dir.join("bpmn")).unwrap();
        std::fs::write(dir.join("bpmn/solo.bpmn"), PLAIN_BPMN).unwrap();
        std::fs::write(
            dir.join("package.yaml"),
            "labels:\n  module: \"solo\"\n  tenant: \"default\"\n  version: \"1.0.0\"\n",
        )
        .unwrap();
        dir
    }

    #[test]
    fn clean_package_reports_zero_findings() {
        let dir = valid_package_dir("lint-clean");
        let args = LintArgs {
            package_dir: dir.clone(),
            overlay: Vec::new(),
            ..Default::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::OK);
        assert_eq!(out, "0 error(s), 0 warning(s)\n");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn findings_render_the_shared_diagnostic_shape_and_exit_one() {
        let dir = valid_package_dir("lint-findings");
        // entryProcesses naming an unknown process is a deploy-blocking finding.
        std::fs::write(dir.join("package.yaml"), "entryProcesses:\n  - \"ghost\"\n").unwrap();
        let args = LintArgs {
            package_dir: dir.clone(),
            overlay: Vec::new(),
            ..Default::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::FINDINGS);
        assert!(
            out.starts_with("[ERROR] SUTRA.DEPLOY.PACKAGE.CONFIG_INVALID — "),
            "{out}"
        );
        assert!(out.contains("ghost"), "{out}");
        assert!(out.ends_with("1 error(s), 0 warning(s)\n"), "{out}");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn json_format_is_machine_consumable() {
        let dir = valid_package_dir("lint-json");
        let global = GlobalArgs {
            format: Some("json".into()),
            verbose: 0,
        };
        let args = LintArgs {
            package_dir: dir.clone(),
            overlay: Vec::new(),
            ..Default::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &global, io));
        assert_eq!(code, exit::OK);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["errors"], 0);
        assert_eq!(v["warnings"], 0);
        assert!(v["diagnostics"].as_array().unwrap().is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    /// A package with one HTTP channel on `path`, plus any data stores.
    fn channel_package(label: &str, module: &str, path: &str, stores: &str) -> std::path::PathBuf {
        let dir = scratch_dir(label);
        std::fs::create_dir_all(dir.join("bpmn")).unwrap();
        std::fs::write(
            dir.join("bpmn/solo.bpmn"),
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL"
                  xmlns:q="urn:sutra:q:1.0"
                  targetNamespace="urn:sutra:module:{module}:1.0.0">
  <bpmn:process id="solo" name="Solo" isExecutable="true">
    <bpmn:startEvent id="Start">
      <bpmn:extensionElements><q:source channel="in"/></bpmn:extensionElements>
    </bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="End"/>
    <bpmn:endEvent id="End"/>
  </bpmn:process>
</bpmn:definitions>
"#
            ),
        )
        .unwrap();
        std::fs::write(
            dir.join("channels.yaml"),
            format!(
                "channels:\n  - name: in\n    transport: http\n    bind: \"POST {path}\"\n    \
                 auth:\n      scheme: apikey\n      apikey:\n        value: env:LINT_KEY\n"
            ),
        )
        .unwrap();
        if !stores.is_empty() {
            std::fs::write(dir.join("datastores.yaml"), stores).unwrap();
        }
        std::fs::write(
            dir.join("package.yaml"),
            format!("labels:\n  module: \"{module}\"\n  tenant: \"acme\"\n  version: \"1.0.0\"\n"),
        )
        .unwrap();
        dir
    }

    /// Seal `package` into a fresh directory standing in for a target's active set.
    fn active_set_of(label: &str, package: &std::path::Path) -> std::path::PathBuf {
        let out = scratch_dir(label);
        sutra_loader::assemble_dir(package, &out, &Default::default()).expect("fixture seals");
        out
    }

    fn lint_against(package: std::path::PathBuf, active: std::path::PathBuf) -> (i32, String) {
        let args = LintArgs {
            package_dir: package,
            active_set: vec![active],
            ..Default::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        (code, out)
    }

    const STORE_ON_SHARED_REF: &str =
        "datastores:\n  - name: accounts\n    type: sql\n    sql:\n      url-ref: env:SHARED_DB_URL\n";

    #[test]
    fn an_endpoint_the_active_set_holds_is_an_error() {
        let holder = channel_package("lint-holder", "holder", "/shared", "");
        let active = active_set_of("lint-holder-set", &holder);
        let package = channel_package("lint-claimant", "claimant", "/shared", "");
        let (code, out) = lint_against(package, active);
        assert_eq!(code, exit::FINDINGS, "{out}");
        assert!(out.contains("SUTRA.DEPLOY.ENDPOINT.COLLISION"), "{out}");
        assert!(out.contains("POST /shared"), "{out}");
        assert!(out.contains("acme--holder--1.0.0"), "{out}");
    }

    #[test]
    fn a_free_endpoint_passes_the_active_set() {
        let holder = channel_package("lint-free-holder", "holder", "/one", "");
        let active = active_set_of("lint-free-set", &holder);
        let package = channel_package("lint-free", "claimant", "/two", "");
        let (code, out) = lint_against(package, active);
        assert_eq!(code, exit::OK, "{out}");
        assert!(!out.contains("SUTRA.DEPLOY."), "{out}");
    }

    /// Same tenant, module and version, new content, under the file name this package seals to.
    /// Without the replacement filter this would warn that the package supersedes itself.
    #[test]
    fn the_copy_being_replaced_is_not_judged_at_all() {
        let previous = channel_package("lint-replace-old", "app", "/app", "");
        let active = active_set_of("lint-replace-set", &previous);
        let package = channel_package("lint-replace-new", "app", "/app-moved", "");
        let same_name = active.join(format!(
            "{}.sutra",
            package.file_stem().unwrap().to_string_lossy()
        ));
        for entry in std::fs::read_dir(&active).unwrap() {
            std::fs::rename(entry.unwrap().path(), &same_name).unwrap();
        }
        let (code, out) = lint_against(package, active);
        assert_eq!(code, exit::OK, "{out}");
        assert!(!out.contains("SUTRA.DEPLOY.ENDPOINT"), "{out}");
    }

    /// The same version under another file name replaces nothing: a folder keeps the old content on
    /// the endpoint and refuses this one. Lint cannot know where the file will land, so it warns.
    #[test]
    fn the_same_version_under_another_file_name_is_flagged() {
        let previous = channel_package("lint-elsewhere-old", "app", "/app", "");
        let active = active_set_of("lint-elsewhere-set", &previous);
        let package = channel_package("lint-elsewhere-new", "app", "/app", "");
        let (code, out) = lint_against(package, active);
        assert_eq!(code, exit::OK, "{out}");
        assert!(
            out.contains("SUTRA.DEPLOY.ENDPOINT.SAME_VERSION_ELSEWHERE"),
            "{out}"
        );
        assert!(out.contains("lint-elsewhere-old"), "{out}");
    }

    /// A package directory `orders` and another owner's archive named `orders.sutra`: the name
    /// must not hide the holder.
    #[test]
    fn a_file_named_after_the_package_holding_another_owner_is_still_a_holder() {
        let holder = channel_package("lint-name-holder", "holder", "/shared", "");
        let active = active_set_of("lint-name-set", &holder);
        let package = channel_package("lint-name-pkg", "claimant", "/shared", "");
        let renamed = active.join(format!(
            "{}.sutra",
            package.file_stem().unwrap().to_string_lossy()
        ));
        for entry in std::fs::read_dir(&active).unwrap() {
            std::fs::rename(entry.unwrap().path(), &renamed).unwrap();
        }
        let (code, out) = lint_against(package, active);
        assert_eq!(code, exit::FINDINGS, "{out}");
        assert!(out.contains("SUTRA.DEPLOY.ENDPOINT.COLLISION"), "{out}");
    }

    #[test]
    fn a_package_that_will_not_seal_is_not_passed_as_checked() {
        let holder = channel_package("lint-seal-holder", "holder", "/one", "");
        let active = active_set_of("lint-seal-set", &holder);
        let args = LintArgs {
            package_dir: scratch_dir("lint-seal-nothing-here"),
            active_set: vec![active],
            ..Default::default()
        };
        assert!(target_findings(&args, None).is_err());
    }

    #[test]
    fn a_shared_store_connection_warns_naming_the_other_deployment_and_the_store() {
        let holder = channel_package("lint-store-holder", "holder", "/one", STORE_ON_SHARED_REF);
        let active = active_set_of("lint-store-set", &holder);
        let package = channel_package("lint-store", "claimant", "/two", STORE_ON_SHARED_REF);
        let (code, out) = lint_against(package, active);
        assert_eq!(code, exit::OK, "a warning never refuses: {out}");
        assert!(
            out.contains("[WARN] SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION"),
            "{out}"
        );
        assert!(out.contains("acme--holder--1.0.0"), "{out}");
        assert!(out.contains("accounts"), "{out}");
    }

    #[test]
    fn with_no_target_a_connection_naming_nobody_warns() {
        let package = channel_package("lint-store-alone", "orders", "/one", STORE_ON_SHARED_REF);
        let args = LintArgs {
            package_dir: package,
            ..Default::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::OK, "{out}");
        assert!(
            out.contains("[WARN] SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION"),
            "{out}"
        );
        assert!(out.contains("accounts"), "{out}");
    }

    #[test]
    fn with_no_target_a_connection_naming_the_module_is_quiet() {
        let stores =
            "datastores:\n  - name: accounts\n    type: sql\n    sql:\n      url-ref: env:ORDERS_DB_URL\n";
        let package = channel_package("lint-store-own", "orders", "/one", stores);
        let args = LintArgs {
            package_dir: package,
            ..Default::default()
        };
        let (code, out, _) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::OK, "{out}");
        assert!(!out.contains("SHARED_CONNECTION"), "{out}");
    }

    #[test]
    fn an_unreadable_active_set_is_a_usage_error() {
        let package = channel_package("lint-bad-set", "orders", "/one", "");
        let (code, _) = lint_against(package, "/nonexistent/active.sutra".into());
        assert_eq!(code, exit::USAGE);
    }

    #[test]
    fn missing_directory_is_a_usage_error() {
        let args = LintArgs {
            package_dir: "/nonexistent/pkg".into(),
            overlay: Vec::new(),
            ..Default::default()
        };
        let (code, _, err) = run_captured("", |io| execute(args, &GlobalArgs::default(), io));
        assert_eq!(code, exit::USAGE);
        assert!(err.contains("not found"), "{err}");
    }
}
