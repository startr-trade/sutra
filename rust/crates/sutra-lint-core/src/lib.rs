//! The pure, I/O-free deploy-time BPMN lint core — the single source of truth for
//! `sutra lint`'s per-deployment structural validation, carved so it compiles to WASM.
//! The VS Code LSP loads this (as WASM) and runs the SAME checks in-editor, so in-editor
//! diagnostics == deploy-time diagnostics **by construction** (no parallel TypeScript
//! re-implementation to drift).
//!
//! `validate_deployment` operates entirely on an in-memory [`LoadedDeployment`] (pre-parsed
//! Strings / modules / XSD bytes) — no fs/zip/async/network. This crate depends only on the
//! model + parse layer of the workspace: `sutra-channels` and `sutra-datastore` are pulled with
//! their `transport` / `providers` features OFF (via `sutra-loader`'s `default-features = false`),
//! so nothing here drags in tokio / axum / hyper / sqlx.
//!
//! # The WASM boundary
//!
//! [`lint_to_json`] (native) and the wasm-bindgen `lint` export (wasm) are the JSON boundary the
//! LSP calls. The contract:
//!
//! - **IN** — a UTF-8 JSON object of the loose deployment-package files the editor holds:
//!   ```json
//!   { "files": { "bpmn/order.bpmn": "<xml>", "channels.yaml": "…", "schemas/pain/x.xsd": "…" },
//!     "labels": { "tenant": "acme", "module": "pay", "version": "1.0.0" } }
//!   ```
//!   `files` keys are archive-local paths (`bpmn/`, `rules/`, `templates/`, `scripts/`,
//!   `schemas/`, `migrations/`, `channels.yaml`, `datastores.yaml`); `labels` is optional and
//!   cosmetic (it only colours diagnostic location strings). The files are reconstructed into a
//!   [`LoadedDeployment`] via the SAME [`sutra_loader::deployment_from_entries`] the deploy-time
//!   archive reader uses — no drift.
//! - **OUT** — a JSON **array** of diagnostics, each
//!   `{ "severity": "error"|"warning", "code": "SUTRA.…", "message": "…",
//!      "site"?: { "file"?: "bpmn/order.bpmn",
//!                 "anchor"?: { "kind": "bpmnNode", "process": "order", "node": "task1" } } }`.
//!   `site.anchor.kind` is `bpmnNode` / `bpmnProcess` / `namedEntry` (camelCase, internally
//!   tagged). The LSP maps `site` to an editor range.
//! - **Never fails** — a malformed request or an unbuildable deployment (e.g. a `.bpmn` that will
//!   not parse) is itself returned as a single `error` diagnostic, so the caller always gets a
//!   well-formed array.
#![cfg_attr(not(target_arch = "wasm32"), forbid(unsafe_code))]

use std::collections::BTreeMap;

use serde::Deserialize;

pub use sutra_loader::lint::{
    validate_deployment, validate_deployment_with_manifests, LintDiagnostic, LintReport,
    LintSeverity,
};
pub use sutra_loader::{deployment_from_entries, DeploymentId, LoadedDeployment};

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
use wasm_bindgen::prelude::wasm_bindgen;

/// The `SUTRA.*` code carried by a diagnostic the lint core itself synthesises (a malformed
/// request, or a diagnostics-serialization failure) — distinct from the `sutra lint` registry
/// codes so the LSP can tell an infrastructure problem from a deployment finding.
pub const LINT_REQUEST_INVALID: &str = "SUTRA.LSP.REQUEST_INVALID";

/// Run the full per-deployment lint suite and collect every diagnostic (errors + warnings),
/// advisory-style — the editor renders them all at once, unlike the loader's fail-first CLI path.
pub fn lint(deployment: &LoadedDeployment) -> Vec<LintDiagnostic> {
    let mut out = Vec::new();
    validate_deployment(deployment, &mut out);
    out
}

/// The JSON request the LSP hands the WASM lint entry (see the crate docs for the full contract):
/// the loose deployment-package files (archive-local path → UTF-8 content) plus opaque labels.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LintRequest {
    /// archive-local path → file content: `bpmn/order.bpmn`, `channels.yaml`, `datastores.yaml`,
    /// `schemas/<codec>/x.xsd`, `templates/…`, `scripts/…`, `rules/…`, `migrations/…`.
    #[serde(default)]
    files: BTreeMap<String, String>,
    /// Opaque authoring labels (tenant/module/version) — cosmetic, used only in the diagnostic
    /// location strings; absent labels default to "unlabeled".
    #[serde(default)]
    labels: BTreeMap<String, String>,
}

/// Run the full advisory lint over the in-memory deployment described by `request_json` and
/// return the diagnostics as a JSON array. The native entry point (the wasm-bindgen `lint` export
/// is a thin wrapper over this); see the crate docs for the input/output contract. Never panics
/// and never returns an error — every failure mode is expressed as a diagnostic in the array.
pub fn lint_to_json(request_json: &str) -> String {
    let request: LintRequest = match serde_json::from_str(request_json) {
        Ok(request) => request,
        Err(e) => {
            return one_error(
                LINT_REQUEST_INVALID,
                format!("lint request is not valid JSON: {e}"),
            )
        }
    };
    let entries: BTreeMap<String, Vec<u8>> = request
        .files
        .into_iter()
        .map(|(path, content)| (path, content.into_bytes()))
        .collect();
    let diagnostics = match deployment_from_entries(&entries, request.labels) {
        Ok(deployment) => lint(&deployment),
        // The one hard prerequisite failed (e.g. a `.bpmn` that will not parse) — surface it as a
        // single diagnostic carrying the loader error's own `SUTRA.*` code + message.
        Err(e) => vec![LintDiagnostic::error(e.code, e.message)],
    };
    serde_json::to_string(&diagnostics).unwrap_or_else(|e| {
        one_error(
            LINT_REQUEST_INVALID,
            format!("diagnostics failed to serialize: {e}"),
        )
    })
}

/// A one-element diagnostics array carrying a single synthetic error — the fallback for the
/// request-parse / serialization failure modes, so the boundary always returns a valid array.
fn one_error(code: &str, message: String) -> String {
    serde_json::to_string(&[LintDiagnostic::error(code, message)]).unwrap_or_else(|_| {
        // A hand-rolled last resort that cannot itself fail to serialize.
        format!(
            "[{{\"severity\":\"error\",\"code\":{code:?},\"message\":\"diagnostic serialization failed\"}}]"
        )
    })
}

/// The wasm-bindgen boundary the VS Code LSP calls as `lint(requestJson) -> diagnosticsJson`.
/// Delegates to [`lint_to_json`]; only compiled for the wasm target so the native build is
/// unchanged.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
#[wasm_bindgen(js_name = lint)]
pub fn lint_json(request_json: &str) -> String {
    lint_to_json(request_json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    /// A valid request round-trips: the JSON boundary reconstructs the deployment, runs the lint,
    /// and serialises each diagnostic WITH its structured `site`. A channel binding an unresolvable
    /// codec yields `INBOUND_CODEC_NOT_FOUND` anchored (`namedEntry`) at its `channels.yaml` key.
    #[test]
    fn json_boundary_round_trips_diagnostics_with_site() {
        let request = serde_json::json!({
            "files": {
                "channels.yaml":
                    "channels:\n  - name: in\n    transport: http\n    \
                     bind: \"POST /channels/in\"\n    codec: urn:doesnotexist\n"
            },
            "labels": { "tenant": "acme", "module": "pay", "version": "1.0.0" }
        })
        .to_string();

        let out = lint_to_json(&request);
        let diagnostics: Value = serde_json::from_str(&out).expect("output is valid JSON");
        let array = diagnostics.as_array().expect("output is a JSON array");

        let codec = array
            .iter()
            .find(|d| d["code"] == "SUTRA.INBOUND.CODEC_NOT_FOUND")
            .unwrap_or_else(|| panic!("expected a codec-not-found diagnostic in {out}"));
        assert_eq!(codec["severity"], "error");
        // The structured site survived serialization: a channels.yaml NamedEntry anchor.
        assert_eq!(codec["site"]["file"], "channels.yaml");
        assert_eq!(codec["site"]["anchor"]["kind"], "namedEntry");
        assert_eq!(codec["site"]["anchor"]["name"], "in");

        // The diagnostics also deserialize back into the strong type (full round-trip).
        let typed: Vec<LintDiagnostic> = serde_json::from_str(&out).expect("round-trips to type");
        assert!(typed
            .iter()
            .any(|d| d.code == "SUTRA.INBOUND.CODEC_NOT_FOUND"));
    }

    /// The boundary never fails: a request that is not valid JSON comes back as a single
    /// synthetic error diagnostic, not a panic or an error return.
    #[test]
    fn malformed_request_becomes_a_single_error_diagnostic() {
        let out = lint_to_json("{ not json");
        let typed: Vec<LintDiagnostic> = serde_json::from_str(&out).expect("valid JSON array");
        assert_eq!(typed.len(), 1);
        assert_eq!(typed[0].code, LINT_REQUEST_INVALID);
        assert_eq!(typed[0].severity, LintSeverity::Error);
    }

    /// An empty request (no files) is a clean, empty deployment — a valid, empty diagnostics array.
    #[test]
    fn empty_request_is_an_empty_diagnostics_array() {
        let out = lint_to_json("{}");
        let typed: Vec<LintDiagnostic> = serde_json::from_str(&out).expect("valid JSON array");
        assert!(typed.is_empty(), "unexpected diagnostics: {out}");
    }
}
