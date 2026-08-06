//! Reload-swappable registries the dispatcher resolves against — ports of
//! `dispatch.CodecRegistry`, `dispatch.ChannelRegistry`, and the handler-resolution slice
//! of `dispatch.ProcessRegistry` (`resolveHandlers` / `findModule`), plus the
//! [`DrainingSink`] that hands executor emissions to the channel layer.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use sutra_bpmn::{ProcessDefinition, ProcessModule};
use sutra_codec_spi::{CodecValue, DecodeResult, PayloadCodec, ShapeClass};
use sutra_executor::{
    builtin_key, logical_urn, resolve_scoped, DeploymentId, Emission, EmissionSink,
};

use crate::codes;
use crate::config::ChannelBinding;
use crate::diag::Diagnostic;

// ---- codecs ---------------------------------------------------------------------------

/// The type-rooted URN prefix engine-provided (global) codecs are referenced under
/// (`urn:sutra:codec:order-envelope`, the LOGICAL form — no scope). `urn:sutra:` is reserved — a user
/// codec path can never produce it (enforced by the `sutra`-reserved-first-level-folder lint).
/// Storage keys carry the trailing scope on top of this prefix — see
/// [`CodecRegistry::with_builtins`] / [`CodecRegistry::resolve`].
pub const CODEC_BUILTIN_URN_PREFIX: &str = "urn:sutra:codec:";

/// Name → codec, seeded from the canonical `sutra_codec_spi::builtin_codecs()` global set
/// (`CodecRegistry::with_builtins`).
#[derive(Default, Clone)]
pub struct CodecRegistry {
    codecs: HashMap<String, Arc<dyn PayloadCodec>>,
}

impl CodecRegistry {
    pub fn new() -> CodecRegistry {
        CodecRegistry::default()
    }

    /// A registry pre-seeded with every engine-provided (global) **codec** from the canonical
    /// [`sutra_codec_spi::builtin_codecs`] set (the zero-config schema-backed message-standard
    /// parsers a distribution force-links; EMPTY in the public engine, which bundles no message
    /// standard — every such codec is an extension crate). The schema-less **formats**
    /// (json/xml/yaml/raw-*/csv) are NOT codecs — they live in the [`FormatRegistry`]. Built-ins are
    /// keyed `urn:sutra:codec:<name>:internal` — the `internal` scope is the TRAILING
    /// segment; the reference form `channels.yaml` uses stays the
    /// bare name or the logical `urn:sutra:codec:<name>` (no scope) — [`CodecRegistry::resolve`]
    /// appends it.
    pub fn with_builtins() -> CodecRegistry {
        let mut registry = CodecRegistry::new();
        for codec in sutra_codec_spi::builtin_codecs() {
            let logical = logical_urn("codec", codec.name());
            registry.codecs.insert(builtin_key(&logical), codec);
        }
        registry
    }

    pub fn register(&mut self, codec: impl PayloadCodec + 'static) -> &mut CodecRegistry {
        self.register_shared(Arc::new(codec))
    }

    pub fn register_shared(&mut self, codec: Arc<dyn PayloadCodec>) -> &mut CodecRegistry {
        self.codecs.insert(codec.name().to_string(), codec);
        self
    }

    /// Register under an EXPLICIT registry key rather than the codec's own `name()` — how an
    /// archive-supplied codec takes the deployment scope (`<logical>:<deploymentId>`), so
    /// two versions of the same module can carry
    /// different builds of one logically-named codec without colliding. Mirrors
    /// `ValidatorRegistry::register_under` / the archive-redactor path.
    pub fn register_under(
        &mut self,
        key: &str,
        codec: Arc<dyn PayloadCodec>,
    ) -> &mut CodecRegistry {
        self.codecs.insert(key.to_string(), codec);
        self
    }

    /// Exact registry-key lookup (no scope resolution) — the seam [`CodecRegistry::resolve`]
    /// composes over. Direct callers must supply the full stored key; a `channels.yaml codec:`
    /// reference should go through [`CodecRegistry::resolve`] instead.
    pub fn find(&self, name: &str) -> Option<Arc<dyn PayloadCodec>> {
        self.codecs.get(name).cloned()
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.codecs.keys().cloned().collect();
        names.sort_unstable();
        names
    }

    /// Resolve a `channels.yaml codec:` reference within `deployment`, in tri-tier
    /// most-specific-first order: this deployment's archive codec first
    /// (`<logical>:<deploymentId>`), then a built-in (`<logical>:internal`), then the reference
    /// verbatim (a bare `<name>`/legacy explicit URN — e.g. a user `urn:<path>` schema codec
    /// registered directly, or a pinned cross-deployment URN). `logical` is the reference itself
    /// if it already starts with `urn:sutra:codec:`, else that prefix + the reference. `None` =
    /// unknown ref (the intake raises `INBOUND.CODEC_NOT_FOUND`, fail closed).
    pub fn resolve(
        &self,
        reference: &str,
        deployment: &DeploymentId,
    ) -> Option<Arc<dyn PayloadCodec>> {
        resolve_scoped("codec", reference, deployment, |k| self.find(k))
    }
}

// ---- formats (content negotiation) ----------------------------------------------------

/// One built-in format as a shape contract: its name, [`ShapeClass`], the content-types its
/// parser accepts, and the parser itself.
#[derive(Clone)]
struct FormatSlot {
    name: String,
    shape_class: ShapeClass,
    content_types: Vec<String>,
    codec: Arc<dyn PayloadCodec>,
}

/// The built-in **formats** (json/xml/yaml/raw-*/csv) as *shape contracts*. When a channel binds a
/// bare format (`codec: json`) or **no codec at all**, the parser is chosen by the inbound
/// `Content-Type` within the contract's admissible class-set — so json/xml/yaml are interchangeable
/// (nested-map) and the reply echoes the inbound content-type. A schema-backed codec (a business
/// standard or a user `urn:` schema codec) is NOT a format and never routes here — that codec takes
/// full control (its own `accepted_content_types` + `decode`).
#[derive(Default, Clone)]
pub struct FormatRegistry {
    slots: Vec<FormatSlot>,
}

impl FormatRegistry {
    /// Seed from the canonical [`sutra_codec_spi::builtin_formats`] set (sorted by name).
    pub fn with_builtins() -> FormatRegistry {
        let slots = sutra_codec_spi::builtin_formats()
            .into_iter()
            .map(|e| FormatSlot {
                name: e.name.to_string(),
                shape_class: e.shape_class,
                content_types: e.codec.accepted_content_types(),
                codec: e.codec,
            })
            .collect();
        FormatRegistry { slots }
    }

    /// If `codec_ref` names a built-in format (bare or `urn:sutra:codec:<name>`), return the shape
    /// contract it declares. `None` for a schema-backed codec / unknown reference.
    pub fn contract(&self, codec_ref: &str) -> Option<ShapeClass> {
        let bare = codec_ref
            .strip_prefix(CODEC_BUILTIN_URN_PREFIX)
            .unwrap_or(codec_ref);
        self.slots
            .iter()
            .find(|s| s.name == bare)
            .map(|s| s.shape_class)
    }

    /// True if a parser of `parser_class` is admissible under a `contract` shape. Nested-map is
    /// interchangeable (json/xml/yaml); opaque is fixed; flat-map admits its own `csv` PLUS a
    /// nested-map parser (a flat json/xml/yaml — the FLATNESS of the decoded tree is enforced after
    /// decode by [`is_flat_map_tree`], so a *nested* body is rejected).
    fn admits(contract: ShapeClass, parser_class: ShapeClass) -> bool {
        match contract {
            ShapeClass::Opaque => parser_class == ShapeClass::Opaque,
            ShapeClass::NestedMap => parser_class == ShapeClass::NestedMap,
            ShapeClass::FlatMap => {
                matches!(parser_class, ShapeClass::FlatMap | ShapeClass::NestedMap)
            }
        }
    }

    /// Select the parser for a declared format contract given the inbound content-type. Opaque is
    /// not negotiated (the declared format is used, gated on its content-types); nested/flat-map
    /// pick the admissible-class format whose accepted content-types match the inbound type (with no
    /// content-type, the declared format is used). `None` = capability mismatch.
    pub fn select(
        &self,
        contract: ShapeClass,
        content_type: Option<&str>,
        declared: &str,
    ) -> Option<Arc<dyn PayloadCodec>> {
        let bare = declared
            .strip_prefix(CODEC_BUILTIN_URN_PREFIX)
            .unwrap_or(declared);
        match contract {
            ShapeClass::Opaque => {
                let slot = self.slots.iter().find(|s| s.name == bare)?;
                (content_type.is_none()
                    || crate::content_type::accepts(&slot.content_types, content_type))
                .then(|| slot.codec.clone())
            }
            ShapeClass::NestedMap | ShapeClass::FlatMap => match content_type {
                None => self
                    .slots
                    .iter()
                    .find(|s| s.name == bare)
                    .map(|s| s.codec.clone()),
                Some(ct) => self
                    .slots
                    .iter()
                    .find(|s| {
                        Self::admits(contract, s.shape_class)
                            && crate::content_type::accepts(&s.content_types, Some(ct))
                    })
                    .map(|s| s.codec.clone()),
            },
        }
    }

    /// The content-types admissible under a contract — for the capability-mismatch diagnostic.
    pub fn admissible_content_types(&self, contract: ShapeClass) -> Vec<String> {
        let mut cts: Vec<String> = self
            .slots
            .iter()
            .filter(|s| Self::admits(contract, s.shape_class))
            .flat_map(|s| s.content_types.clone())
            .collect();
        cts.sort_unstable();
        cts.dedup();
        cts
    }

    /// The open (no-declared-codec) parser for a content-type: a SPECIFIC format match (not the raw
    /// `*/*` catch-all) if one exists, else the opaque raw parser (`text/*` → raw-text, else
    /// raw-bytes). `None` only if formats are (impossibly) unbundled.
    pub fn open_select(&self, content_type: Option<&str>) -> Option<Arc<dyn PayloadCodec>> {
        if let Some(ct) = content_type {
            for s in &self.slots {
                let specific: Vec<String> = s
                    .content_types
                    .iter()
                    .filter(|p| p.as_str() != "*/*")
                    .cloned()
                    .collect();
                if !specific.is_empty() && crate::content_type::accepts(&specific, Some(ct)) {
                    return Some(s.codec.clone());
                }
            }
        }
        let raw_name = match content_type {
            Some(ct) if ct.starts_with("text/") => "raw-text",
            _ => "raw-bytes",
        };
        self.slots
            .iter()
            .find(|s| s.name == raw_name)
            .map(|s| s.codec.clone())
    }

    /// No declared codec (case b): decode purely by content-type (via [`Self::open_select`]),
    /// fail-open to opaque raw passthrough so a no-codec channel NEVER rejects. The raw wire bytes
    /// remain available at `event.body`.
    pub fn open_decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
        match self.open_select(content_type) {
            Some(codec) => codec.decode(body, content_type),
            None => DecodeResult::ok(
                CodecValue::Bytes(body.to_vec()),
                content_type.unwrap_or("application/octet-stream"),
            ),
        }
    }
}

/// Does a decoded tree satisfy the FLAT-map contract? A "row" is an object of scalars (a
/// header-mapped csv row / flat json object) OR an array of scalars (a headerless csv row) OR a
/// scalar. Flat = a row, an array of rows, or an object whose every value is a scalar or an array
/// of rows (the csv `{"value":[…]}` / `{key: [rows]}` shape). Any deeper nesting — an object inside
/// a row, an object inside an array inside a row — is NOT flat. The discriminator is flatness, not
/// syntax: csv (header or headerless) always satisfies it; a flat json/xml/yaml does, a nested one
/// does not.
pub fn is_flat_map_tree(v: &serde_json::Value) -> bool {
    use serde_json::Value;
    fn is_scalar(v: &Value) -> bool {
        !matches!(v, Value::Object(_) | Value::Array(_))
    }
    fn is_row(v: &Value) -> bool {
        match v {
            Value::Object(m) => m.values().all(is_scalar), // header-mapped row / flat object
            Value::Array(a) => a.iter().all(is_scalar),    // headerless csv row (array of cells)
            _ => is_scalar(v),                             // scalar
        }
    }
    match v {
        Value::Array(a) => a.iter().all(is_row),
        Value::Object(m) => m.values().all(|x| match x {
            Value::Array(a) => a.iter().all(is_row),
            other => is_scalar(other),
        }),
        _ => true, // a bare scalar is trivially flat
    }
}

// ---- channels -------------------------------------------------------------------------

/// `(module_key, channel)` → binding — the dispatcher's channel resolution.
#[derive(Default, Clone)]
pub struct ChannelRegistry {
    bindings: HashMap<(String, String), ChannelBinding>,
}

impl ChannelRegistry {
    pub fn new() -> ChannelRegistry {
        ChannelRegistry::default()
    }

    /// Register a channel binding — the registry's collision
    /// contract: re-registering the EXACT same binding for a channel URN is an idempotent
    /// no-op (supports startup re-scan / hot-reload safety), but a DIFFERENT binding for an
    /// already-registered `(module_key, channel)` URN raises `SUTRA.CHANNEL.NAME.COLLISION`
    /// (namespace-identity: two DIFFERENT namespaces binding the same channel NAME do not
    /// collide — that is the two-tenant feature).
    pub fn register(&mut self, binding: ChannelBinding) -> Result<(), Diagnostic> {
        let key = (binding.namespace.module_key(), binding.channel_name.clone());
        if let Some(existing) = self.bindings.get(&key) {
            if existing == &binding {
                return Ok(()); // idempotent re-registration
            }
            return Err(Diagnostic::error(
                codes::CHANNEL_NAME_COLLISION,
                format!(
                    "Channel '{}' is already registered under '{}' with a different binding; a \
                     channel URN must map to exactly one binding.",
                    binding.channel_name,
                    binding.namespace.module_key()
                ),
            )
            .with_attribute("channel", &binding.channel_name));
        }
        self.bindings.insert(key, binding);
        Ok(())
    }

    pub fn find(&self, module_key: &str, channel: &str) -> Option<&ChannelBinding> {
        self.bindings
            .get(&(module_key.to_string(), channel.to_string()))
    }

    /// The number of registered bindings.
    pub fn size(&self) -> usize {
        self.bindings.len()
    }
}

// ---- processes ------------------------------------------------------------------------

/// A process and the start event of it that subscribes to a resolved
/// `(channel, messageType)` — the handler-match record.
#[derive(Clone)]
pub struct HandlerMatch {
    pub process: Arc<ProcessDefinition>,
    pub start_event_id: String,
}

/// A process and the id of its WAIT NODE that subscribes to a resolved relay channel —
/// the relay-target record.
#[derive(Clone)]
pub struct RelayTarget {
    pub process: Arc<ProcessDefinition>,
    pub wait_node_id: String,
}

/// Deployment-scoped module registry with cross-process message-type dispatch — the
/// handler-resolution / module-lookup slice of the process registry. A
/// deployment (module version) may span several BPMN files; registering each loaded
/// [`ProcessModule`] under the same [`DeploymentId`] accumulates its processes.
///
/// The processes are held behind `Arc`, not `Rc` (execution scale-out §2 row 10): the graph is
/// immutable once deployed, so the engine builds this registry ONCE per activation and every
/// actor lane shares that one copy. The `register` deep-copy below therefore happens once per
/// deployment per activation instead of once per deployment per LANE.
#[derive(Default, Clone)]
pub struct ProcessModuleRegistry {
    modules: HashMap<String, Vec<Arc<ProcessDefinition>>>,
}

impl ProcessModuleRegistry {
    pub fn new() -> ProcessModuleRegistry {
        ProcessModuleRegistry::default()
    }

    /// Register every process of a loaded BPMN file under the deployment.
    pub fn register(&mut self, deployment: &DeploymentId, module: &ProcessModule) {
        let entry = self
            .modules
            .entry(deployment.value().to_string())
            .or_default();
        for p in module.processes() {
            entry.push(Arc::new(p.clone()));
        }
    }

    /// True when anything is deployed under this id (the module presence check).
    pub fn has_module(&self, deployment: &DeploymentId) -> bool {
        self.modules.contains_key(deployment.value())
    }

    /// The process registered under `deployment` with the given id (coverage-op /
    /// call-activity module resolution).
    pub fn find_in_module(
        &self,
        deployment: &DeploymentId,
        process_id: &str,
    ) -> Option<Arc<ProcessDefinition>> {
        self.modules
            .get(deployment.value())
            .and_then(|ps| ps.iter().find(|p| p.id == process_id).cloned())
    }

    /// Cross-process RELAY resolution: every process in `deployment` with a WAIT
    /// NODE (`<bpmn:userTask>` / intermediate message catch / channel-call serviceTask)
    /// whose `<q:source>` subscribes to `channel`. A relay message on
    /// such a channel is correlated to a WAITING instance and resumes it — the
    /// re-activation counterpart to [`Self::resolve_handlers`]. Empty when the deployment
    /// is unknown or nothing waits on the channel, so the dispatcher falls through to its
    /// normal no-start-event handling. Sorted (process id, node id).
    pub fn resolve_relay_targets(
        &self,
        deployment: &DeploymentId,
        channel: &str,
    ) -> Vec<RelayTarget> {
        let Some(processes) = self.modules.get(deployment.value()) else {
            return Vec::new();
        };
        if channel.trim().is_empty() {
            return Vec::new();
        }
        let mut out: Vec<RelayTarget> = Vec::new();
        for process in processes {
            for node in process.nodes() {
                let subscribed = match node {
                    sutra_bpmn::model::Node::UserTask { channels, .. }
                    | sutra_bpmn::model::Node::MessageCatchEvent { channels, .. } => {
                        channels.iter().any(|c| c == channel)
                    }
                    // A channel-call task's <q:source channel> names the inbound
                    // channel its correlated RESPONSE arrives on.
                    sutra_bpmn::model::Node::ServiceTask {
                        id, implementation, ..
                    } if implementation.starts_with(sutra_bpmn::model::CHANNEL_CALL_PREFIX) => {
                        process
                            .bindings_for(id)
                            .sources
                            .iter()
                            .any(|s| s.channel == channel)
                    }
                    _ => false,
                };
                if subscribed {
                    out.push(RelayTarget {
                        process: Arc::clone(process),
                        wait_node_id: node.id().to_string(),
                    });
                }
            }
        }
        out.sort_by(|a, b| {
            a.process
                .id
                .cmp(&b.process.id)
                .then_with(|| a.wait_node_id.cmp(&b.wait_node_id))
        });
        out
    }

    /// Every process in the channel's deployment whose `<q:source>` subscribes to
    /// `(channel, messageType)` — exact `messageTypeValue` → regex `messageTypePattern` →
    /// unfiltered catch-all — sorted by process id for determinism.
    pub fn resolve_handlers(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        message_type: Option<&str>,
    ) -> Vec<HandlerMatch> {
        let Some(processes) = self.modules.get(deployment.value()) else {
            return Vec::new();
        };
        let mut out: Vec<HandlerMatch> = Vec::new();
        for process in processes {
            if let Some(start) = process.select_start_event(channel, message_type) {
                out.push(HandlerMatch {
                    process: Arc::clone(process),
                    start_event_id: start.id().to_string(),
                });
            }
        }
        out.sort_by(|a, b| a.process.id.cmp(&b.process.id));
        out
    }
}

// ---- emissions -------------------------------------------------------------------------

/// An [`EmissionSink`] the dispatcher drains after every execution — the bridge between
/// the executor's collect-only emission surface and the channel layer's outbox hook.
/// Register the SAME instance on `TokenExecutor::builder(...).with_emission_sink(...)`
/// and on the [`crate::ChannelEngine`].
#[derive(Debug, Default)]
pub struct DrainingSink {
    emissions: RefCell<Vec<Emission>>,
}

impl DrainingSink {
    pub fn new() -> DrainingSink {
        DrainingSink::default()
    }

    /// Take everything collected since the last drain.
    pub fn drain(&self) -> Vec<Emission> {
        self.emissions.borrow_mut().drain(..).collect()
    }
}

impl EmissionSink for DrainingSink {
    fn emit(&self, emission: Emission) {
        self.emissions.borrow_mut().push(emission);
    }
}

// ---- shared error builders ---------------------------------------------------------------

pub(crate) fn codec_not_found(channel: &str, codec: &str, known: &[String]) -> Diagnostic {
    Diagnostic::error(
        codes::INBOUND_CODEC_NOT_FOUND,
        format!(
            "Channel '{channel}' is bound to codec '{codec}' but no PayloadCodec is \
             registered. Known: {known:?}"
        ),
    )
    .with_attribute("channel", channel)
    .with_attribute("codec", codec)
}

#[cfg(test)]
mod tests {
    use super::*;
    // Force-link the extracted `sutra-formats` crate (a dev-dependency) so its `inventory::submit!`
    // format registrations reach THIS lib-test binary — the neutral channels lib never references a
    // concrete format by symbol, so without this the linker would drop them and `with_builtins()`
    // would resolve none of the framework formats below.
    use sutra_formats as _;

    /// Formats are NOT codecs (4c): the framework formats resolve via the [`FormatRegistry`], and
    /// the [`CodecRegistry`] no longer carries them — it holds only schema-backed codecs. A user
    /// codec URN is neither a format nor (here) a seeded codec.
    #[test]
    fn formats_resolve_via_the_format_registry_not_the_codec_registry() {
        let codecs = CodecRegistry::with_builtins();
        let formats = FormatRegistry::with_builtins();
        for name in ["xml", "json", "yaml", "raw-text", "raw-bytes", "csv"] {
            assert!(
                formats.contract(name).is_some(),
                "format '{name}' must resolve as a format contract"
            );
            let urn = format!("{CODEC_BUILTIN_URN_PREFIX}{name}");
            assert!(
                codecs.find(&urn).is_none(),
                "format '{name}' must NOT resolve as a codec (it is a format, not a codec)"
            );
        }
        // A user codec URN (`urn:<path>`) is neither a format nor a seeded built-in codec.
        assert!(formats.contract("urn:transfer").is_none());
        assert!(codecs.find("urn:transfer").is_none());
    }

    // A stub codec whose `name()` we control directly, so a test can seed a registry entry under
    // an EXACT key without needing a real inventory-linked `sutra-codec-<standard>` crate (none is
    // a dev-dependency of this crate — the full inventory round-trip is covered end-to-end by
    // `sutra-dist/tests/builtin_codec_bundle.rs`, which bundles the real codec crates).
    struct FakeCodec(String);
    impl PayloadCodec for FakeCodec {
        fn name(&self) -> &str {
            &self.0
        }
        fn accepted_content_types(&self) -> Vec<String> {
            vec!["*/*".to_string()]
        }
        fn decode(&self, body: &[u8], content_type: Option<&str>) -> DecodeResult {
            DecodeResult::ok(
                CodecValue::Bytes(body.to_vec()),
                content_type.unwrap_or("application/octet-stream"),
            )
        }
        fn encode(
            &self,
            _payload: &CodecValue,
            _content_type: Option<&str>,
        ) -> Result<Vec<u8>, String> {
            Ok(Vec::new())
        }
    }

    /// `resolve()`'s tri-tier order against a
    /// builtin-shaped key (mirrors `with_builtins()`'s own key construction —
    /// `builtin_key(logical_urn("codec", name))`) — a bare name and the logical URN both resolve
    /// (scope appended), and the full internal URN resolves verbatim (tier 3, `find` bypasses
    /// scope entirely).
    #[test]
    fn resolve_finds_a_builtin_keyed_codec_by_bare_name_logical_urn_and_full_urn() {
        let dep = DeploymentId::unresolved();
        let mut registry = CodecRegistry::new();
        let logical = logical_urn("codec", "fake-codec");
        registry.register_shared(Arc::new(FakeCodec(builtin_key(&logical))));
        // `find` is an exact key lookup — the bare name and the logical URN do NOT hit it.
        assert!(registry.find("fake-codec").is_none());
        assert!(registry
            .find("urn:sutra:codec:fake-codec:internal")
            .is_some());
        // `resolve` appends the scope, so all three reference forms find the same entry.
        assert!(registry.resolve("fake-codec", &dep).is_some());
        assert!(registry
            .resolve("urn:sutra:codec:fake-codec", &dep)
            .is_some());
        assert!(registry
            .resolve("urn:sutra:codec:fake-codec:internal", &dep)
            .is_some());
        assert!(registry.resolve("unknown-codec", &dep).is_none());
    }

    /// A format exposes no fixed shape (`PayloadCodec::shape_of` → `None`) — the discriminator that
    /// makes it a format, not a schema-backed codec. Resolved through the negotiation `select`.
    #[test]
    fn a_format_exposes_no_fixed_shape() {
        let formats = FormatRegistry::with_builtins();
        let json = formats
            .select(ShapeClass::NestedMap, None, "json")
            .expect("json format resolves");
        assert!(json.shape_of(None).is_none());
    }

    // ---- FormatRegistry: content negotiation (stage 4) --------------------------------------

    /// A bare format name resolves to its shape contract (bare or reserved-URN form); a
    /// schema-backed codec / user codec is NOT a format.
    #[test]
    fn format_contract_classifies_formats_not_codecs() {
        let f = FormatRegistry::with_builtins();
        assert_eq!(f.contract("json"), Some(ShapeClass::NestedMap));
        assert_eq!(
            f.contract("urn:sutra:codec:yaml"),
            Some(ShapeClass::NestedMap)
        );
        assert_eq!(f.contract("csv"), Some(ShapeClass::FlatMap));
        assert_eq!(f.contract("raw-text"), Some(ShapeClass::Opaque));
        // a schema-backed codec is not a format; a user URN is not a format either.
        assert_eq!(f.contract("a-schema-codec"), None);
        assert_eq!(f.contract("urn:transfer"), None);
    }

    /// The headline feature: a `json` (nested-map) channel is interchangeable — an `application/yaml`
    /// body is parsed by the yaml format, an `application/xml` body by xml; no content-type falls
    /// back to the declared format; a non-nested content-type (text/csv) is NOT admissible.
    #[test]
    fn nested_map_contract_is_interchangeable_by_content_type() {
        let f = FormatRegistry::with_builtins();
        let pick = |ct: Option<&str>| {
            f.select(ShapeClass::NestedMap, ct, "json")
                .map(|c| c.name().to_string())
        };
        assert_eq!(pick(Some("application/yaml")).as_deref(), Some("yaml"));
        assert_eq!(pick(Some("application/xml")).as_deref(), Some("xml"));
        assert_eq!(pick(Some("application/json")).as_deref(), Some("json"));
        assert_eq!(pick(None).as_deref(), Some("json")); // declared default
        assert_eq!(pick(Some("text/csv")), None); // csv is not nested-map admissible
    }

    /// A flat-map (`csv`) contract uses csv for `text/csv` AND cross-accepts a nested-map format —
    /// the parser is chosen by content-type (a flat json/xml/yaml is admissible; flatness is enforced
    /// on the decoded tree, below).
    #[test]
    fn flat_map_contract_cross_accepts_a_nested_format_by_content_type() {
        let f = FormatRegistry::with_builtins();
        let pick = |ct: Option<&str>| {
            f.select(ShapeClass::FlatMap, ct, "csv")
                .map(|c| c.name().to_string())
        };
        assert_eq!(pick(Some("text/csv")).as_deref(), Some("csv"));
        assert_eq!(pick(Some("application/json")).as_deref(), Some("json"));
        assert_eq!(pick(Some("application/yaml")).as_deref(), Some("yaml"));
        assert_eq!(pick(None).as_deref(), Some("csv")); // declared default
    }

    /// The flatness discriminator: csv's own output (header rows = objects-of-scalars, headerless =
    /// arrays-of-scalars), a flat json object, and the csv `{key:[rows]}` wrapper are flat; a nested
    /// object is not.
    #[test]
    fn flatness_check_accepts_flat_rejects_nested() {
        use serde_json::json;
        assert!(is_flat_map_tree(&json!([{"Id": "INB-7", "Amt": "100"}]))); // header rows
        assert!(is_flat_map_tree(&json!([
            ["INB-7", "100"],
            ["INB-8", "200"]
        ]))); // headerless rows
        assert!(is_flat_map_tree(&json!({"Id": "INB-7", "Amt": "100"}))); // flat object
        assert!(is_flat_map_tree(
            &json!({"value": [{"Id": "1"}, {"Id": "2"}]})
        )); // wrapper
        assert!(is_flat_map_tree(&json!({"a": 1, "b": [1, 2, 3]}))); // scalar + array-of-scalars
                                                                     // Nested → not flat.
        assert!(!is_flat_map_tree(&json!({"a": {"b": 1}})));
        assert!(!is_flat_map_tree(&json!([{"nested": {"x": 1}}])));
    }

    /// Case (b): no declared codec → decode purely by content-type; an unknown content-type
    /// fails open to opaque raw passthrough (never rejects).
    #[test]
    fn open_decode_is_content_type_driven_and_fails_open() {
        let f = FormatRegistry::with_builtins();
        let json = f.open_decode(b"{\"a\":1}", Some("application/json"));
        assert_eq!(json.outcome, sutra_codec_spi::DecodeOutcome::Ok);
        assert!(matches!(json.payload, Some(CodecValue::Json(_))));
        // Unknown content-type → raw passthrough, still Ok (raw bytes reachable at event.body).
        let raw = f.open_decode(b"\x00\x01", Some("application/x-unknown"));
        assert_eq!(raw.outcome, sutra_codec_spi::DecodeOutcome::Ok);
    }
}
