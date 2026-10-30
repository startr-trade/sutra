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
    /// deployment → (process id, node id, channel) → that `<q:source>`'s compiled selector.
    ///
    /// Keyed by channel as well as node because a channel-call task may carry several
    /// `<q:source>` elements, one per response channel, each with its own predicate.
    ///
    /// Compiled ONCE per activation, in [`Self::register`], because both halves of a selector are
    /// expensive to build and were previously built per candidate per message: the anchored regex
    /// was `Regex::new`-ed on the hot inbound path, and a FEEL predicate would re-parse the same
    /// way. A deployment whose selectors do not compile never activates (see
    /// [`Self::selector_faults`]), so dispatch can treat a missing entry as fail-closed rather
    /// than as a reason to parse anything.
    selectors: HashMap<String, HashMap<SelectorKey, CompiledSource>>,
}

/// One start event's compiled selector: its predicate AST, and the fault that stopped the
/// selector from compiling at all.
///
/// The message-type clause is NOT cached here. `ProcessDefinition::select_start_events` still
/// owns tier precedence and still compiles `messageTypePattern` per message, as it always has —
/// moving that here would mean moving exact/anchored-pattern matching out of the BPMN model
/// (which cannot hold a `Regex`: it derives `PartialEq`) and relocating the tests that pin those
/// semantics, which is a larger change than this feature needs. What the activation pass DOES
/// take from the pattern is its validity: a malformed regex becomes a [`SelectorFault`] and the
/// deployment never activates, where before it was silently skipped at dispatch and matched
/// nothing — indistinguishable from "no traffic".
#[derive(Clone)]
struct CompiledSource {
    predicate: Option<sutra_feel::ast::FeelExpr>,
    fault: Option<(String, String)>,
}

/// A selector that could not be compiled at activation: `(process id, node id, code, message)`.
pub type SelectorFault = (String, String, String, String);

/// How a compiled selector is keyed: `(process id, node id, channel)`.
type SelectorKey = (String, String, String);

/// One start event's predicate failing to EVALUATE for a particular message —
/// `(process id, node id, code, message)`. Distinct from a [`SelectorFault`], which is a
/// compile-time failure caught at activation.
pub type PredicateFault = (String, String, String, String);

/// What [`ProcessModuleRegistry::resolve_handlers`] found: the handlers, plus any predicate that
/// errored while deciding. The faults travel with the result because a predicate failure is
/// per-message and must reach the operator, while never aborting the dispatch — another
/// subscriber may legitimately match, and one broken process must not deny a shared channel.
pub struct HandlerResolution {
    pub handlers: Vec<HandlerMatch>,
    pub predicate_faults: Vec<PredicateFault>,
}

/// Evaluates the compiled predicates of ONE process against one message's attribute context.
///
/// Fail-closed, as [`sutra_bpmn::SourcePredicate`] requires: an evaluation error, a non-boolean
/// result or a missing compiled entry all answer `false` and record a fault.
struct CompiledPredicates<'a> {
    selectors: Option<&'a HashMap<SelectorKey, CompiledSource>>,
    process_id: &'a str,
    channel: &'a str,
    context: &'a sutra_feel::FeelContext,
    faults: RefCell<Vec<PredicateFault>>,
}

impl<'a> sutra_bpmn::SourcePredicate for CompiledPredicates<'a> {
    fn holds(&self, node_id: &str) -> bool {
        let key = (
            self.process_id.to_string(),
            node_id.to_string(),
            self.channel.to_string(),
        );
        let Some(compiled) = self.selectors.and_then(|t| t.get(&key)) else {
            // Activation refuses a deployment with selector faults, so this is unreachable in a
            // live engine; answering false keeps it fail-closed if it ever is reached.
            self.faults.borrow_mut().push((
                self.process_id.to_string(),
                node_id.to_string(),
                codes::INBOUND_SELECT_PREDICATE_FAILED.to_string(),
                "no compiled selector for this start event".to_string(),
            ));
            return false;
        };
        // A selector that did not COMPILE must never be treated as "declares no predicate",
        // which would make a start event whose filter is broken match everything — fail-open, the
        // exact inverse of the contract. Activation warns about these loudly and `sutra lint`
        // refuses them at package time; if one is reached anyway, it matches nothing.
        if let Some((code, detail)) = compiled.fault.as_ref() {
            self.faults.borrow_mut().push((
                self.process_id.to_string(),
                node_id.to_string(),
                code.clone(),
                detail.clone(),
            ));
            return false;
        }
        let Some(expr) = compiled.predicate.as_ref() else {
            return true;
        };
        match sutra_feel::evaluator::eval_boolean(expr, self.context) {
            Ok(holds) => holds,
            Err(e) => {
                self.faults.borrow_mut().push((
                    self.process_id.to_string(),
                    node_id.to_string(),
                    codes::INBOUND_SELECT_PREDICATE_FAILED.to_string(),
                    format!("{e}"),
                ));
                false
            }
        }
    }
}

impl ProcessModuleRegistry {
    pub fn new() -> ProcessModuleRegistry {
        ProcessModuleRegistry::default()
    }

    /// Register every process of a loaded BPMN file under the deployment, compiling each start
    /// event's selector as it goes.
    pub fn register(&mut self, deployment: &DeploymentId, module: &ProcessModule) {
        let entry = self
            .modules
            .entry(deployment.value().to_string())
            .or_default();
        let compiled = self
            .selectors
            .entry(deployment.value().to_string())
            .or_default();
        for p in module.processes() {
            // EVERY node carrying a `<q:source>`, not only start events: the same element is
            // legal on a user task, a message catch event, a receive task and a channel-call task, where it
            // names the channel a correlated RESPONSE arrives on. Compiling only start events
            // would leave a predicate on a wait node silently doing nothing — which is the one
            // outcome worse than refusing it.
            for node in p.nodes() {
                let node_id = node.id().to_string();
                for source in &p.bindings_for(&node_id).sources {
                    compiled.insert(
                        (p.id.clone(), node_id.clone(), source.channel.clone()),
                        compile_source(&p.id, &node_id, source),
                    );
                }
            }
            entry.push(Arc::new(p.clone()));
        }
    }

    /// Every selector that failed to compile. Activation refuses a deployment with any, so a
    /// malformed `messageTypePattern` or an unparseable / non-deterministic `filterExpression`
    /// can never reach the inbound path — where the old code silently skipped a bad regex and
    /// routed nothing, indistinguishably from "no traffic".
    pub fn selector_faults(&self) -> Vec<SelectorFault> {
        let mut out: Vec<SelectorFault> = Vec::new();
        for table in self.selectors.values() {
            for ((process_id, node_id, _channel), compiled) in table {
                if let Some((code, message)) = compiled.fault.as_ref() {
                    out.push((
                        process_id.clone(),
                        node_id.clone(),
                        code.clone(),
                        message.clone(),
                    ));
                }
            }
        }
        out.sort();
        out
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

    /// Every `(process, start event id)` in `deployment` whose start event subscribes to
    /// `channel`, regardless of message type or predicate — the processes whose instances a
    /// session on that channel can belong to. Sorted (process id, start event id).
    pub fn starts_on_channel(
        &self,
        deployment: &DeploymentId,
        channel: &str,
    ) -> Vec<(Arc<ProcessDefinition>, String)> {
        let Some(processes) = self.modules.get(deployment.value()) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for process in processes {
            for start in process.start_events() {
                if let sutra_bpmn::model::Node::StartEvent { id, channels, .. } = start {
                    if channels.iter().any(|c| c == channel) {
                        out.push((Arc::clone(process), id.clone()));
                    }
                }
            }
        }
        out.sort_by(|a, b| a.0.id.cmp(&b.0.id).then_with(|| a.1.cmp(&b.1)));
        out
    }

    /// Cross-process RELAY resolution: every process in `deployment` with a WAIT
    /// NODE (`<bpmn:userTask>` / intermediate message catch / `<bpmn:receiveTask>` /
    /// channel-call serviceTask)
    /// whose `<q:source>` subscribes to `channel`. A relay message on
    /// such a channel is correlated to a WAITING instance and resumes it — the
    /// re-activation counterpart to [`Self::resolve_handlers`]. Empty when the deployment
    /// is unknown or nothing waits on the channel, so the dispatcher falls through to its
    /// normal no-start-event handling. Sorted (process id, node id).
    /// `attributes` is the same selector context [`Self::resolve_handlers`] takes: a wait node's
    /// `<q:source filterExpression>` is honoured here too, because the element is legal on a wait
    /// node and a predicate that silently did nothing on half its legal hosts would be worse than
    /// one that was refused outright. A target whose predicate does not hold is skipped, exactly
    /// as a start event would be.
    ///
    /// The wait's declared message type is honoured the same way a start event's is:
    /// `message_type` (what the codec reported) selects, in order of preference, the waits whose
    /// `messageTypeValue` equals it, then those whose `messageTypePattern` matches it, then the
    /// waits that declare no message type. A wait pinned to another type is never a target. The
    /// result is ordered by that tier first, then (process id, node id), so the FIRST target is
    /// the most specific match; every compatible target is returned so the resume can still pick
    /// the wait an instance is actually parked at.
    pub fn resolve_relay_targets(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        message_type: Option<&str>,
        attributes: &sutra_feel::FeelContext,
    ) -> Vec<RelayTarget> {
        let Some(processes) = self.modules.get(deployment.value()) else {
            return Vec::new();
        };
        if channel.trim().is_empty() {
            return Vec::new();
        }
        let table = self.selectors.get(deployment.value());
        let mut out: Vec<(u8, RelayTarget)> = Vec::new();
        for process in processes {
            for node in process.nodes() {
                let subscribed = match node {
                    sutra_bpmn::model::Node::UserTask { channels, .. }
                    | sutra_bpmn::model::Node::MessageCatchEvent { channels, .. }
                    | sutra_bpmn::model::Node::ReceiveTask { channels, .. } => {
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
                    let predicates = CompiledPredicates {
                        selectors: table,
                        process_id: &process.id,
                        channel,
                        context: attributes,
                        faults: RefCell::new(Vec::new()),
                    };
                    // Only consult the predicate when the node's source declares one, matching
                    // the start-event rule: no predicate means no opinion.
                    let declares_predicate = process
                        .bindings_for(node.id())
                        .sources
                        .iter()
                        .any(|s| s.channel == channel && s.filter_expression.is_some());
                    if declares_predicate
                        && !sutra_bpmn::SourcePredicate::holds(&predicates, node.id())
                    {
                        continue;
                    }
                    let Some(tier) = relay_type_tier(process, node.id(), channel, message_type)
                    else {
                        continue;
                    };
                    out.push((
                        tier,
                        RelayTarget {
                            process: Arc::clone(process),
                            wait_node_id: node.id().to_string(),
                        },
                    ));
                }
            }
        }
        out.sort_by(|(ta, a), (tb, b)| {
            ta.cmp(tb)
                .then_with(|| a.process.id.cmp(&b.process.id))
                .then_with(|| a.wait_node_id.cmp(&b.wait_node_id))
        });
        out.into_iter().map(|(_, target)| target).collect()
    }

    /// Every start event in the channel's deployment that subscribes to
    /// `(channel, messageType)` and whose `<q:source filterExpression>` holds for `attributes` —
    /// exact `messageTypeValue` → regex `messageTypePattern` → unfiltered catch-all, sorted by
    /// (process id, start-event id) for determinism.
    ///
    /// `attributes` is the FEEL context the predicate is evaluated against: the codec's reported
    /// message attributes under `attr`, plus `messageType`. Pass an EMPTY context for a message
    /// whose decode was fatal — a codec that could not parse cannot classify, so the caller
    /// should select on message type alone rather than let every predicate answer false.
    ///
    /// ONE match per start event, not per process: a single process declaring two start events
    /// that both match is a configuration predicates make ordinary, and it used to resolve
    /// silently by document order. Surfacing both lets the caller's ambiguity check see it.
    pub fn resolve_handlers(
        &self,
        deployment: &DeploymentId,
        channel: &str,
        message_type: Option<&str>,
        attributes: &sutra_feel::FeelContext,
    ) -> HandlerResolution {
        let Some(processes) = self.modules.get(deployment.value()) else {
            return HandlerResolution {
                handlers: Vec::new(),
                predicate_faults: Vec::new(),
            };
        };
        let table = self.selectors.get(deployment.value());
        let mut handlers: Vec<HandlerMatch> = Vec::new();
        let mut predicate_faults: Vec<PredicateFault> = Vec::new();
        for process in processes {
            let predicates = CompiledPredicates {
                selectors: table,
                process_id: &process.id,
                channel,
                context: attributes,
                faults: RefCell::new(Vec::new()),
            };
            for start in process.select_start_events(channel, message_type, &predicates) {
                handlers.push(HandlerMatch {
                    process: Arc::clone(process),
                    start_event_id: start.id().to_string(),
                });
            }
            predicate_faults.extend(predicates.faults.into_inner());
        }
        handlers.sort_by(|a, b| {
            a.process
                .id
                .cmp(&b.process.id)
                .then_with(|| a.start_event_id.cmp(&b.start_event_id))
        });
        predicate_faults.sort();
        HandlerResolution {
            handlers,
            predicate_faults,
        }
    }
}

/// How specifically a wait's `<q:source>` on `channel` accepts `message_type`: `0` for an exact
/// `messageTypeValue`, `1` for a matching `messageTypePattern`, `2` for a source that declares no
/// message type, and `None` when the wait is pinned to a type the message does not have (or the
/// codec reported none). The same three tiers a start event selects by.
fn relay_type_tier(
    process: &ProcessDefinition,
    node_id: &str,
    channel: &str,
    message_type: Option<&str>,
) -> Option<u8> {
    let bindings = process.bindings_for(node_id);
    let Some(source) = bindings.sources.iter().find(|s| s.channel == channel) else {
        // A wait whose channel comes from the node itself (no `<q:source>` row) declares no type.
        return Some(2);
    };
    match (
        source.message_type_value.as_deref(),
        source.message_type_pattern.as_deref(),
    ) {
        (None, None) => Some(2),
        (Some(pinned), _) => (message_type == Some(pinned)).then_some(0),
        (None, Some(pattern)) => {
            let mt = message_type?;
            regex::Regex::new(&format!("^(?:{pattern})$"))
                .map(|re| re.is_match(mt))
                .unwrap_or(false)
                .then_some(1)
        }
    }
}

/// Compile one `<q:source>`'s selector, turning every failure into a fault that keeps the
/// deployment from activating rather than a surprise at dispatch time.
fn compile_source(
    process_id: &str,
    node_id: &str,
    source: &sutra_bpmn::qbindings::SourceBinding,
) -> CompiledSource {
    let mut fault = None;

    // Compiled here only to VALIDATE it — anchored exactly as the matcher anchors it, so a
    // pattern that compiles here cannot fail to compile there.
    if let Some(p) = source.message_type_pattern.as_deref() {
        if let Err(e) = regex::Regex::new(&format!("^(?:{p})$")) {
            fault = Some((
                codes::SELECTOR_COMPILE_FAILED.to_string(),
                format!(
                    "process {process_id} start event {node_id}: messageTypePattern {p:?} is not \
                     a valid regular expression: {e}"
                ),
            ));
        }
    }

    let mut predicate = None;
    if let Some(text) = source.filter_expression.as_deref() {
        // Parsed against the names that WILL be in scope, not with resolution switched off:
        // `expressions::parse` would fail on any multi-word FEEL builtin (`starts with`,
        // `ends with`, `string length`, …), which is exactly how an author would write a prefix
        // filter. The two roots carry no spaces, so they are listed for intent rather than need.
        let names: std::collections::HashSet<String> = [
            crate::intake::SELECTOR_ATTRIBUTE_ROOT.to_string(),
            "messageType".to_string(),
        ]
        .into_iter()
        .collect();
        match sutra_feel::expressions::parse_with_context_names(text, &names) {
            Ok(expr) => {
                // A routing decision that consults the clock cannot be replayed, so a
                // non-deterministic builtin is refused here rather than producing a deployment
                // whose routing drifts.
                match sutra_feel::expressions::require_pure_expr(&expr, "q:source filterExpression")
                {
                    Ok(()) => predicate = Some(expr),
                    Err(e) => {
                        fault = Some((
                            e.code.to_string(),
                            format!(
                                "process {process_id} start event {node_id}: filterExpression is \
                                 not deterministic: {e}"
                            ),
                        ))
                    }
                }
            }
            Err(e) => {
                fault = Some((
                    e.code.to_string(),
                    format!(
                        "process {process_id} start event {node_id}: filterExpression does not \
                         parse: {e}"
                    ),
                ))
            }
        }
    }

    CompiledSource { predicate, fault }
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
