//! **Schema-bundle codecs** — the third `schemaKind` family, beside the generic `xsd`
//! ([`crate::structural`]) and `json-schema` ([`crate::json_schema`]) folders.
//!
//! Some standards are not "a folder of schemas the engine can validate against" but a whole
//! PROFILE: an envelope grammar, a wrapper→schema mapping, versioned editions that a deployment
//! adopts on the standard's own release cadence. Their codec lives in its own crate and knows all
//! of that; what a deployment supplies is CONFIGURATION — which schema file backs which message,
//! for this archive version.
//!
//! A bundle folder is therefore an ordinary `schemas/<name>/` folder whose `codec-manifest.yaml`
//! declares a `schemaKind` that a codec crate has registered here:
//!
//! ```yaml
//! # schemas/<name>/codec-manifest.yaml
//! schemaKind: <kind>
//! ```
//!
//! plus whatever files that kind's own manifest references (edition subfolders and all — a bundle
//! sees its folder's WHOLE file tree, not just the direct `.xsd` children).
//!
//! Registration is the same inventory pull model as [`sutra_codec_spi::BuiltinCodec`]: a codec
//! crate `inventory::submit!`s a [`BundleCodecKind`] next to its impl, so implementing the kind IS
//! registering it and this crate stays free of any knowledge about the standards it serves. The
//! dependency direction is codec crate → this crate, never back.
//!
//! Every problem here is a DEPLOY-time, fail-closed error carrying a stable `SUTRA.CONFIG.*`
//! code: the archive either produces a working codec or it does not deploy.

use std::collections::BTreeMap;
use std::sync::Arc;

use sutra_codec_spi::codec::PayloadCodec;
use sutra_codec_spi::codes;

use crate::schema_codec_loader::{CodecLoadError, CODEC_MANIFEST_FILE};

/// The files of ONE bundle folder, keyed by the folder-relative `'/'`-separated path
/// (`release-2025/OrderCreated_order.created.001.08.xsd`).
pub struct BundleSource<'a> {
    codec_name: &'a str,
    files: BTreeMap<String, &'a [u8]>,
}

impl<'a> BundleSource<'a> {
    pub fn new(codec_name: &'a str, files: BTreeMap<String, &'a [u8]>) -> BundleSource<'a> {
        BundleSource { codec_name, files }
    }

    /// The codec folder's name — the bundle's local id, and the diagnostic subject.
    pub fn codec_name(&self) -> &str {
        self.codec_name
    }

    /// One file by folder-relative path.
    pub fn file(&self, relative_path: &str) -> Option<&'a [u8]> {
        self.files.get(relative_path).copied()
    }

    /// Every file in the folder, sorted by relative path.
    pub fn files(&self) -> impl Iterator<Item = (&str, &'a [u8])> {
        self.files.iter().map(|(k, v)| (k.as_str(), *v))
    }
}

/// Builds a codec instance. `Send + Sync` (and therefore usable from a deploy PLAN prepared
/// off the engine actor); the codec it mints is `Send + Sync` too, so the ONE instance the
/// activation builds is shared by every engine lane (execution scale-out §2 row 10) instead of
/// being minted per lane.
pub type BundleFactory = Arc<dyn Fn() -> Arc<dyn PayloadCodec> + Send + Sync>;

/// A bundle build failure, carrying the stable `SUTRA.CONFIG.*` code the deploy fails with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleError {
    pub code: &'static str,
    pub message: String,
}

impl BundleError {
    /// A malformed manifest / an unknown message name — `SUTRA.CONFIG.CODEC_MANIFEST.INVALID`.
    pub fn manifest(message: impl Into<String>) -> BundleError {
        BundleError {
            code: codes::CONFIG_CODEC_MANIFEST_INVALID,
            message: message.into(),
        }
    }

    /// A missing / unreadable / uncompilable schema file — `SUTRA.CONFIG.SCHEMA.INVALID`.
    pub fn schema(message: impl Into<String>) -> BundleError {
        BundleError {
            code: codes::CONFIG_SCHEMA_INVALID,
            message: message.into(),
        }
    }
}

/// A self-registered bundle codec kind. Submitted by the codec crate that implements it.
pub struct BundleCodecKind {
    /// The `schemaKind` token (a free-form name, e.g. `order-envelope`) — must not collide
    /// with the generic `xsd` /
    /// `json-schema` kinds.
    pub kind: &'static str,
    /// Compile the folder into a codec factory, fail-closed.
    pub build: fn(&BundleSource<'_>) -> Result<BundleFactory, BundleError>,
}

inventory::collect!(BundleCodecKind);

/// The registered kind for a `schemaKind` token, if a linked codec crate serves it.
pub fn bundle_kind(kind: &str) -> Option<&'static BundleCodecKind> {
    inventory::iter::<BundleCodecKind>().find(|k| k.kind == kind)
}

/// Every registered bundle kind token, sorted (inventory link order is unspecified) — the
/// diagnostic that tells an author which kinds this build serves.
pub fn bundle_kinds() -> Vec<&'static str> {
    let mut kinds: Vec<&'static str> = inventory::iter::<BundleCodecKind>()
        .map(|k| k.kind)
        .collect();
    kinds.sort_unstable();
    kinds
}

/// One bundle codec compiled out of an archive, ready to register.
pub struct PlannedBundle {
    /// The bundle folder's path under `schemas/`, `'/'`→`':'` — the artifact URN's local id
    /// (`urn:sutra:codec:<local_id>:<deploymentId>`).
    pub local_id: String,
    /// The `schemaKind` that built it.
    pub kind: &'static str,
    pub make: BundleFactory,
}

// Hand-written: the factory is a closure, so `derive(Debug)` cannot cover it.
impl std::fmt::Debug for PlannedBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlannedBundle")
            .field("local_id", &self.local_id)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Discover and compile every schema BUNDLE in an archive's `schemas/**` (keyed by the
/// `'/'`-separated subpath relative to `schemas/`). Generic-kind folders (`xsd`, `json-schema`)
/// are left to the per-folder schema codecs and reported by [`PlannedBundle::local_id`] only for
/// the bundles, so a caller can skip the folders this consumed.
///
/// Fail-closed: an unreadable manifest, an unknown `schemaKind`, or a bundle whose own manifest
/// does not build is a deploy error.
pub fn plan(
    schema_files: &BTreeMap<String, Vec<u8>>,
) -> Result<Vec<PlannedBundle>, CodecLoadError> {
    let mut planned = Vec::new();
    for (subpath, content) in schema_files {
        let Some(folder) = subpath.strip_suffix(CODEC_MANIFEST_FILE) else {
            continue;
        };
        let folder = folder.strip_suffix('/').unwrap_or(folder);
        let Some(kind) = manifest_kind(content, folder)? else {
            continue; // a generic schema-codec folder — not ours
        };
        if folder.is_empty() {
            return Err(CodecLoadError::new(
                codes::CONFIG_CODEC_LAYOUT_INVALID,
                format!(
                    "a '{}' schema bundle must live in its own schemas/<name>/ folder, not \
                     directly under schemas/",
                    kind.kind
                ),
            ));
        }
        let prefix = format!("{folder}/");
        let files: BTreeMap<String, &[u8]> = schema_files
            .iter()
            .filter_map(|(path, bytes)| {
                path.strip_prefix(&prefix)
                    .map(|rest| (rest.to_string(), bytes.as_slice()))
            })
            .collect();
        let local_id = folder.replace('/', ":");
        let source = BundleSource::new(&local_id, files);
        let make = (kind.build)(&source).map_err(|e| {
            CodecLoadError::new(
                e.code,
                format!("schema bundle '{local_id}' ({}): {}", kind.kind, e.message),
            )
        })?;
        planned.push(PlannedBundle {
            local_id,
            kind: kind.kind,
            make,
        });
    }
    Ok(planned)
}

/// The registered bundle kind a `codec-manifest.yaml` declares: `None` for the generic
/// `xsd`/`json-schema` kinds (handled elsewhere), `Err` for an unreadable manifest or a
/// `schemaKind` nothing serves.
fn manifest_kind(
    manifest: &[u8],
    folder: &str,
) -> Result<Option<&'static BundleCodecKind>, CodecLoadError> {
    let name = if folder.is_empty() { "schemas" } else { folder };
    let text = std::str::from_utf8(manifest).map_err(|e| {
        CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!("codec '{name}': {CODEC_MANIFEST_FILE} is not valid UTF-8: {e}"),
        )
    })?;
    let value: serde_yaml_ng::Value = serde_yaml_ng::from_str(text).map_err(|e| {
        CodecLoadError::new(
            codes::CONFIG_CODEC_MANIFEST_INVALID,
            format!(
                "codec '{name}': malformed {CODEC_MANIFEST_FILE} — {}",
                e.to_string().replace(['\n', '\r'], " ")
            ),
        )
    })?;
    let declared = value
        .get("schemaKind")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_lowercase();
    classify(&declared, name)
}

/// `xsd`/`json-schema` → `None` (generic); a registered bundle kind → `Some`; anything else is a
/// fail-closed manifest error naming what this build DOES serve.
pub(crate) fn classify(
    declared: &str,
    codec_name: &str,
) -> Result<Option<&'static BundleCodecKind>, CodecLoadError> {
    match declared {
        "xsd" | "json-schema" | "jsonschema" | "json_schema" => Ok(None),
        other => match bundle_kind(other) {
            Some(kind) => Ok(Some(kind)),
            None => Err(CodecLoadError::new(
                codes::CONFIG_CODEC_MANIFEST_INVALID,
                format!(
                    "codec '{codec_name}': schemaKind must be 'xsd', 'json-schema' or a bundle \
                     kind this build serves ({:?}), got '{other}'",
                    bundle_kinds()
                ),
            )),
        },
    }
}
