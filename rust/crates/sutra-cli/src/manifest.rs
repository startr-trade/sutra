//! Deployment manifests — `sutra lint|package|deploy <manifest.yaml>`.
//!
//! A manifest is pure CLI de-sugaring: it names the package metadata (which becomes a
//! generated `package.yaml`), an ordered stack of layer directories (composed by
//! [`sutra_loader::compose_stack`], with per-layer `.ignore` removals), and optionally the
//! `sutra deploy` target arguments. The engine, the archive format, the loader and the
//! deploymentId are unchanged: sealing a manifest is byte-identical to sealing the same
//! merged tree by hand.
//!
//! ```yaml
//! deployment: customer-a-dev          # optional, informational
//! package:                            # the package.yaml keys; tenant/module/version are
//!   tenant: customer-a                #   shorthand for labels.tenant/module/version
//!   module: order-hub
//!   version: 1.1.0
//!   engine: { minContract: 1 }
//! layers:                             # lowest first; later wins; relative to this file
//!   - path: ../shared/schemas
//!     at: schemas                     # optional mount point under the package root
//!   - ../app/shared                   # a bare string is a path
//! target:                             # optional, `sutra deploy` only (flag names)
//!   context: dev-cluster
//!   namespace: sutra
//! ```

use std::path::{Path, PathBuf};

use serde_yaml_ng::{Mapping, Value};
use sutra_loader::{LayerStack, StackLayer, StackResolution};

/// The manifest's closed top-level schema.
const TOP_LEVEL_KEYS: [&str; 4] = ["deployment", "package", "layers", "target"];
/// `package:` keys: the label shorthands plus every `package.yaml` key.
const PACKAGE_KEYS: [&str; 8] = [
    "tenant",
    "module",
    "version",
    "labels",
    "engine",
    "entryProcesses",
    "audit",
    "properties",
];
/// The `package.yaml` keys passed through verbatim, in the order they are generated.
const PASSTHROUGH_KEYS: [&str; 4] = ["engine", "entryProcesses", "audit", "properties"];
/// The labels a manifest must declare.
const REQUIRED_LABELS: [&str; 2] = ["module", "version"];
/// `target:` keys — the `sutra deploy` flag names they stand in for.
const TARGET_KEYS: [&str; 10] = [
    "context",
    "namespace",
    "configmap",
    "secret-name",
    "engine-url",
    "api",
    "async",
    "wait",
    "wait-timeout",
    "api-key-header",
];

/// `sutra deploy` arguments a manifest's `target:` may supply. A value given on the command
/// line always wins over the manifest's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ManifestTarget {
    pub context: Option<String>,
    pub namespace: Option<String>,
    pub configmap: Option<String>,
    pub secret_name: Option<String>,
    pub engine_url: Option<String>,
    pub api: bool,
    pub async_lro: bool,
    pub wait: bool,
    pub wait_timeout: Option<u64>,
    pub api_key_header: Option<String>,
}

/// A parsed, resolved manifest.
#[derive(Debug, Clone)]
pub struct DeploymentManifest {
    pub path: PathBuf,
    pub deployment: Option<String>,
    pub stack: LayerStack,
    pub target: ManifestTarget,
}

/// A manifest is a regular file ending `.yaml` / `.yml`; anything else is a package dir.
pub fn is_manifest_path(path: &Path) -> bool {
    path.is_file()
        && path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml"))
}

/// Read and resolve the manifest at `path`; `overlays` (the explicit `--overlay` flags) stack
/// ABOVE the manifest's last layer. Every refusal is a one-line message naming the problem.
pub fn load(path: &Path, overlays: &[PathBuf]) -> Result<DeploymentManifest, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("cannot read manifest {}: {e}", path.display()))?;
    parse(path, &text, overlays)
}

fn parse(path: &Path, text: &str, overlays: &[PathBuf]) -> Result<DeploymentManifest, String> {
    let doc: Value = serde_yaml_ng::from_str(text).map_err(|e| format!("does not parse: {e}"))?;
    let root = doc
        .as_mapping()
        .ok_or_else(|| "must be a YAML mapping".to_string())?;
    check_keys(root, &TOP_LEVEL_KEYS, "")?;

    let deployment = match root.get("deployment") {
        None | Some(Value::Null) => None,
        Some(v) => Some(scalar(v).ok_or_else(|| "`deployment` must be a string".to_string())?),
    };
    let package = root
        .get("package")
        .and_then(Value::as_mapping)
        .ok_or_else(|| "`package:` is required (a mapping of the package labels)".to_string())?;
    let package_yaml = generate_package_yaml(package)?;

    let base = path.parent().unwrap_or_else(|| Path::new("."));
    let entries = match root.get("layers") {
        Some(Value::Sequence(seq)) if !seq.is_empty() => seq,
        _ => return Err("`layers:` must list at least one layer directory".to_string()),
    };
    let mut layers = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let number = index + 1;
        let (written, mount) = match entry {
            Value::String(s) => (s.clone(), None),
            Value::Mapping(map) => {
                check_keys(map, &["path", "at"], &format!("layers[{number}]."))?;
                let written = map
                    .get("path")
                    .and_then(scalar)
                    .ok_or_else(|| format!("layers[{number}] needs a `path`"))?;
                let mount = match map.get("at") {
                    None | Some(Value::Null) => None,
                    Some(v) => Some(
                        scalar(v).ok_or_else(|| format!("layers[{number}].at must be a string"))?,
                    ),
                };
                (written, mount)
            }
            _ => {
                return Err(format!(
                    "layers[{number}] must be a path or a mapping with `path` (and optional `at`)"
                ))
            }
        };
        let root_dir = base.join(&written);
        if !root_dir.is_dir() {
            return Err(format!(
                "layer {number} ({written}) not found: {}",
                root_dir.display()
            ));
        }
        let label = match &mount {
            Some(m) => format!("layer {number} {written} (at {m})"),
            None => format!("layer {number} {written}"),
        };
        layers.push(StackLayer {
            root: root_dir,
            mount,
            label,
        });
    }
    for overlay in overlays {
        if !overlay.is_dir() {
            return Err(format!(
                "overlay directory not found: {}",
                overlay.display()
            ));
        }
        layers.push(StackLayer {
            root: overlay.clone(),
            mount: None,
            label: format!("overlay {}", overlay.display()),
        });
    }

    let target = match root.get("target") {
        None | Some(Value::Null) => ManifestTarget::default(),
        Some(Value::Mapping(map)) => parse_target(map)?,
        Some(_) => return Err("`target:` must be a mapping".to_string()),
    };

    let name = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "package".to_string());
    Ok(DeploymentManifest {
        path: path.to_path_buf(),
        deployment,
        stack: LayerStack {
            name,
            package_yaml,
            layers,
        },
        target,
    })
}

fn check_keys(map: &Mapping, allowed: &[&str], prefix: &str) -> Result<(), String> {
    for key in map.keys() {
        let name = key.as_str().unwrap_or("<non-string>");
        if !allowed.contains(&name) {
            return Err(format!(
                "unknown key '{prefix}{name}' (allowed: {})",
                allowed.join(", ")
            ));
        }
    }
    Ok(())
}

/// A scalar as text (a YAML `version: 2` is a number, but labels are strings).
fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The generated `package.yaml`: `labels:` (the shorthands merged over `labels:`), then the
/// pass-through keys in a fixed order. Deterministic, so the same manifest always seals to
/// the same bytes.
fn generate_package_yaml(package: &Mapping) -> Result<String, String> {
    check_keys(package, &PACKAGE_KEYS, "package.")?;
    let mut labels = std::collections::BTreeMap::new();
    if let Some(value) = package.get("labels") {
        let map = value
            .as_mapping()
            .ok_or_else(|| "package.labels must be a mapping".to_string())?;
        for (k, v) in map {
            let key = k
                .as_str()
                .ok_or_else(|| "package.labels keys must be strings".to_string())?;
            let val = scalar(v).ok_or_else(|| format!("label '{key}' must be a string"))?;
            labels.insert(key.to_string(), val);
        }
    }
    for key in ["tenant", "module", "version"] {
        if let Some(value) = package.get(key) {
            let val = scalar(value).ok_or_else(|| format!("package.{key} must be a string"))?;
            labels.insert(key.to_string(), val);
        }
    }
    let missing: Vec<&str> = REQUIRED_LABELS
        .iter()
        .copied()
        .filter(|l| labels.get(*l).is_none_or(|v| v.is_empty()))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "`package:` is missing the label(s) {} (set package.<label> or package.labels.<label>)",
            missing.join(", ")
        ));
    }

    let mut out = Mapping::new();
    let mut label_map = Mapping::new();
    for (k, v) in labels {
        label_map.insert(Value::String(k), Value::String(v));
    }
    out.insert(Value::from("labels"), Value::Mapping(label_map));
    for key in PASSTHROUGH_KEYS {
        if let Some(value) = package.get(key) {
            out.insert(Value::from(key), value.clone());
        }
    }
    serde_yaml_ng::to_string(&Value::Mapping(out))
        .map_err(|e| format!("cannot render package.yaml: {e}"))
}

fn parse_target(map: &Mapping) -> Result<ManifestTarget, String> {
    check_keys(map, &TARGET_KEYS, "target.")?;
    let text = |key: &str| -> Result<Option<String>, String> {
        match map.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => scalar(v)
                .map(Some)
                .ok_or_else(|| format!("target.{key} must be a string")),
        }
    };
    let flag = |key: &str| -> Result<bool, String> {
        match map.get(key) {
            None | Some(Value::Null) => Ok(false),
            Some(Value::Bool(b)) => Ok(*b),
            Some(_) => Err(format!("target.{key} must be true or false")),
        }
    };
    let wait_timeout = match map.get("wait-timeout") {
        None | Some(Value::Null) => None,
        Some(v) => Some(
            v.as_u64()
                .ok_or_else(|| "target.wait-timeout must be a number of seconds".to_string())?,
        ),
    };
    Ok(ManifestTarget {
        context: text("context")?,
        namespace: text("namespace")?,
        configmap: text("configmap")?,
        secret_name: text("secret-name")?,
        engine_url: text("engine-url")?,
        api: flag("api")?,
        async_lro: flag("async")?,
        wait: flag("wait")?,
        wait_timeout,
        api_key_header: text("api-key-header")?,
    })
}

/// The text rendering of a resolved stack: each layer in order, each override (who won,
/// whom it overrode) and each `.ignore` removal.
pub fn render_resolution_text(
    manifest: &DeploymentManifest,
    resolution: &StackResolution,
) -> String {
    let mut out = format!("manifest {}", manifest.path.display());
    if let Some(d) = &manifest.deployment {
        out.push_str(&format!(" (deployment {d})"));
    }
    out.push('\n');
    for label in &resolution.layers {
        out.push_str(&format!("  {label}\n"));
    }
    for o in &resolution.overrides {
        out.push_str(&format!(
            "  override {}: {} over {}\n",
            o.path, o.winner, o.overrode
        ));
    }
    for r in &resolution.removals {
        out.push_str(&format!(
            "  removed {}: by {} .ignore '{}' (was from {})\n",
            r.path, r.layer, r.pattern, r.from
        ));
    }
    out
}

/// The JSON rendering of a resolved stack — the same information as the text form.
pub fn resolution_json(
    manifest: &DeploymentManifest,
    resolution: &StackResolution,
) -> serde_json::Value {
    serde_json::json!({
        "manifest": manifest.path.display().to_string(),
        "deployment": manifest.deployment,
        "layers": resolution.layers,
        "overrides": resolution.overrides.iter().map(|o| serde_json::json!({
            "path": o.path, "winner": o.winner, "overrode": o.overrode,
        })).collect::<Vec<_>>(),
        "removals": resolution.removals.iter().map(|r| serde_json::json!({
            "path": r.path, "layer": r.layer, "pattern": r.pattern, "from": r.from,
        })).collect::<Vec<_>>(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_str(text: &str) -> Result<DeploymentManifest, String> {
        let dir = crate::test_fixtures::scratch_dir("manifest-parse");
        std::fs::create_dir_all(dir.join("a")).unwrap();
        let path = dir.join("m.yaml");
        std::fs::write(&path, text).unwrap();
        let r = load(&path, &[]);
        std::fs::remove_dir_all(dir).ok();
        r
    }

    #[test]
    fn unknown_keys_are_refused_listing_the_allowed_ones() {
        let err =
            parse_str("package: {module: m, version: '1'}\nlayers: [a]\nextra: 1\n").unwrap_err();
        assert!(err.contains("unknown key 'extra'"), "{err}");
        assert!(err.contains("deployment, package, layers, target"), "{err}");
        let err =
            parse_str("package: {module: m, version: '1', colour: x}\nlayers: [a]\n").unwrap_err();
        assert!(err.contains("package.colour"), "{err}");
        let err = parse_str("package: {module: m, version: '1'}\nlayers: [a]\ntarget: {kube: x}\n")
            .unwrap_err();
        assert!(err.contains("target.kube"), "{err}");
    }

    #[test]
    fn missing_package_labels_are_refused() {
        let err = parse_str("layers: [a]\n").unwrap_err();
        assert!(err.contains("`package:` is required"), "{err}");
        let err = parse_str("package: {tenant: t, module: m}\nlayers: [a]\n").unwrap_err();
        assert!(err.contains("missing the label(s) version"), "{err}");
    }

    #[test]
    fn package_yaml_is_generated_deterministically() {
        let m = parse_str(
            "package:\n  version: 1.1.0\n  tenant: customer-a\n  module: order-hub\n  \
             engine: { minContract: 1 }\n  labels: { channel: web }\nlayers: [a]\n",
        )
        .unwrap();
        assert_eq!(
            m.stack.package_yaml,
            "labels:\n  channel: web\n  module: order-hub\n  tenant: customer-a\n  \
             version: 1.1.0\nengine:\n  minContract: 1\n"
        );
        assert_eq!(m.stack.name, "m");
    }

    #[test]
    fn target_maps_the_deploy_flag_names() {
        let m = parse_str(
            "package: {module: m, version: '1'}\nlayers: [a]\ntarget:\n  context: dev\n  \
             namespace: sutra\n  engine-url: http://localhost:1\n  api: true\n  wait-timeout: 30\n",
        )
        .unwrap();
        assert_eq!(m.target.context.as_deref(), Some("dev"));
        assert_eq!(m.target.namespace.as_deref(), Some("sutra"));
        assert_eq!(m.target.engine_url.as_deref(), Some("http://localhost:1"));
        assert!(m.target.api);
        assert_eq!(m.target.wait_timeout, Some(30));
    }
}
