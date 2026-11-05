//! The client side of the deploy-time checks, shared by `sutra lint` and `sutra deploy`: the
//! engine's own [`evaluate_candidate`] over archives this process can read, and a call that asks a
//! running engine the same question.
//!
//! Asking the engine is the stronger answer. It links every transport its deployments can use and
//! resolves connection references in its own environment. This binary judges the transports it
//! links and compares references by their text; it reports a channel it could not judge rather
//! than passing it in silence.

use std::path::{Path, PathBuf};

use sutra_engine::deployment_facts::{
    evaluate_candidate, ConnectionView, DeployWarning, DeploymentFacts,
};
// The HTTP transport declares its claims through inventory, which only sees what is linked.
use sutra_transport_http as _;

use crate::output::{Diagnostic, Severity};

/// What a check found, in the CLI's diagnostic shape.
#[derive(Default)]
pub(crate) struct CheckFindings {
    /// Non-empty refuses the deploy.
    pub(crate) errors: Vec<Diagnostic>,
    pub(crate) warnings: Vec<Diagnostic>,
}

pub(crate) fn warning(code: &str, message: impl Into<String>) -> Diagnostic {
    Diagnostic {
        severity: Severity::Warn,
        code: code.to_string(),
        message: message.into(),
        location: None,
    }
}

fn warning_of(w: &DeployWarning) -> Diagnostic {
    warning(&w.code, w.message.clone())
}

/// The facts of a sealed archive, or why it could not be read. Connections compare by their
/// authored text: the target's environment is not here to resolve them in.
pub(crate) fn facts_of_archive(bytes: &[u8]) -> Result<DeploymentFacts, String> {
    let archive = sutra_loader::read_archive(bytes).map_err(|e| format!("{e}"))?;
    DeploymentFacts::of_loaded(&archive.deployment, ConnectionView::Declared)
}

/// Judge `candidate` against `others` with the check the engine runs, this binary's transports
/// permitting. Whichever deployments share `candidate`'s slot are being replaced, so they are
/// never judged against it.
pub(crate) fn judge(candidate: &DeploymentFacts, others: &[DeploymentFacts]) -> CheckFindings {
    let others: Vec<DeploymentFacts> = others
        .iter()
        .filter(|f| f.slot != candidate.slot)
        .cloned()
        .collect();
    let mut verdict = evaluate_candidate(candidate, &others);
    // What this judges against is a folder of archives or a ConfigMap mounted as one.
    verdict.warn_for_folder_target(candidate);
    CheckFindings {
        errors: verdict
            .collisions
            .iter()
            .map(|c| {
                Diagnostic::error(
                    sutra_channels::codes::DEPLOY_ENDPOINT_COLLISION,
                    c.describe(),
                )
            })
            .collect(),
        warnings: verdict.warnings.iter().map(warning_of).collect(),
    }
}

/// Read the active set a user named: `.sutra` files, or directories of them. An archive's slot
/// is its file name, the `dir` source's slot and the ConfigMap's key.
pub(crate) fn load_active_set(paths: &[PathBuf]) -> Result<Vec<DeploymentFacts>, String> {
    let mut files: Vec<PathBuf> = Vec::new();
    for path in paths {
        if path.is_dir() {
            let entries = std::fs::read_dir(path)
                .map_err(|e| format!("cannot list active set {}: {e}", path.display()))?;
            let mut archives: Vec<PathBuf> = entries
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| is_archive(p))
                .collect();
            archives.sort();
            files.extend(archives);
        } else {
            files.push(path.clone());
        }
    }
    files
        .iter()
        .map(|file| {
            let bytes = std::fs::read(file)
                .map_err(|e| format!("cannot read active set {}: {e}", file.display()))?;
            let slot = file
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            facts_of_archive(&bytes)
                .map(|facts| facts.in_slot(&slot))
                .map_err(|e| format!("active set {}: {e}", file.display()))
        })
        .collect()
}

fn is_archive(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .is_some_and(|x| x.eq_ignore_ascii_case(sutra_loader::archive::ARCHIVE_EXTENSION))
}

/// How long a check may take. It is a fast answer ahead of a deploy, and a black-holed engine must
/// not hang the deploy behind it.
const ENGINE_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Ask a running engine to judge an archive (`POST /admin/deployments/check`). `Err` says why it
/// gave no verdict: an older engine, or one unreachable or refusing this caller. What the caller
/// does about that is its own decision, but it has to be told.
pub(crate) async fn ask_engine(
    engine_url: &str,
    archive: &[u8],
    slot: Option<&str>,
    admin_key: Option<(&str, &str)>,
) -> Result<CheckFindings, String> {
    let url = format!(
        "{}/admin/deployments/check",
        engine_url.trim_end_matches('/')
    );
    let client = reqwest::Client::builder()
        .timeout(ENGINE_CHECK_TIMEOUT)
        .build()
        .map_err(|e| e.to_string())?;
    let mut request = client
        .post(&url)
        .header("Content-Type", "application/octet-stream")
        .body(archive.to_vec());
    if let Some(slot) = slot {
        request = request.query(&[("slot", slot)]);
    }
    if let Some((header, value)) = admin_key {
        request = request.header(header, value);
    }
    let response = request.send().await.map_err(|e| e.to_string())?;
    let status = response.status();
    if status.as_u16() != 200 {
        let hint = match status.as_u16() {
            401 | 403 => ": it refused this caller, so check --api-key",
            404 => ": it has no check endpoint, or is an older engine",
            _ => "",
        };
        return Err(format!("HTTP {status}{hint}"));
    }
    let body: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
    verdict_of_engine_body(&body).ok_or_else(|| "its answer was not a verdict".to_string())
}

/// Read the check's JSON body. `None` when the body is not a verdict at all.
fn verdict_of_engine_body(body: &serde_json::Value) -> Option<CheckFindings> {
    let collisions = body["collisions"].as_array()?;
    let text = |v: &serde_json::Value, key: &str| v[key].as_str().unwrap_or_default().to_string();
    Some(CheckFindings {
        errors: collisions
            .iter()
            .map(|c| Diagnostic::error(&text(c, "code"), text(c, "message")))
            .collect(),
        warnings: body["warnings"]
            .as_array()
            .map(|warnings| {
                warnings
                    .iter()
                    .map(|w| warning(&text(w, "code"), text(w, "message")))
                    .collect()
            })
            .unwrap_or_default(),
    })
}

/// The warning for a package linted with no active set to judge it against: what would happen if
/// a copy of it were co-hosted.
pub(crate) fn deployment_specific_connection_warning(
    candidate: &sutra_loader::LoadedDeployment,
) -> Option<Diagnostic> {
    let yaml = candidate.datastores_yaml.as_deref()?;
    let stores = sutra_datastore::parse_datastores(yaml).ok()?;
    let names = sutra_datastore::stores_without_deployment_specific_connection(
        &stores,
        &candidate.tenant,
        &candidate.module,
    );
    if names.is_empty() {
        return None;
    }
    Some(warning(
        sutra_channels::codes::DEPLOY_DATASTORE_SHARED_CONNECTION,
        format!(
            "the connection of data store(s) {} names neither this package's tenant nor its \
             module, so a copy co-hosted on one engine resolves the same connection and shares \
             those stores' rows. If that is not intended, give each variant its own reference \
             in its datastores.yaml. Pass --active-set or --engine-url to check against what a \
             target actually runs",
            names.join(", ")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_engine_verdict_reads_into_findings() {
        let found = verdict_of_engine_body(&json!({
            "ok": false,
            "collisions": [{ "code": "SUTRA.DEPLOY.ENDPOINT.COLLISION", "message": "taken" }],
            "warnings": [{ "code": "SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION", "message": "shared" }],
        }))
        .unwrap();
        assert_eq!(found.errors[0].code, "SUTRA.DEPLOY.ENDPOINT.COLLISION");
        assert_eq!(found.errors[0].message, "taken");
        assert_eq!(
            found.warnings[0].code,
            "SUTRA.DEPLOY.DATASTORE.SHARED_CONNECTION"
        );
    }

    #[test]
    fn a_body_that_is_no_verdict_gives_no_answer() {
        assert!(verdict_of_engine_body(&json!({ "error": "nope" })).is_none());
    }
}
