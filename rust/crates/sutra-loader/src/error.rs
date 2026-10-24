//! Loader diagnostics — the `SUTRA.CONFIG.*` codes the resource-layout scan raises
//! (string-identical to the frozen diagnostics-registry values).

use std::fmt;

/// Diagnostic codes raised by the loader (string-identical to the diagnostics-registry
/// values; the authoring-tree subset carries the historical code strings verbatim).
pub mod codes {
    pub const CONFIG_MODULE_MANIFEST_MISSING: &str = "SUTRA.CONFIG.MODULE_MANIFEST.MISSING";
    pub const CONFIG_MODULE_MANIFEST_INVALID: &str = "SUTRA.CONFIG.MODULE_MANIFEST.INVALID";
    pub const CONFIG_MODULE_LAYOUT_INVALID: &str = "SUTRA.CONFIG.MODULE_LAYOUT.INVALID";
    pub const CONFIG_MODULE_NAMESPACE_MISMATCH: &str = "SUTRA.CONFIG.MODULE_NAMESPACE.MISMATCH";
    pub const CONFIG_INHERIT_PATH_NOT_FOUND: &str = "SUTRA.CONFIG.INHERIT_PATH.NOT_FOUND";

    // ---- package-time validation suite — codes mirrored from the diagnostics registry ----
    pub const CONFIG_DATASTORE_INVALID: &str = "SUTRA.CONFIG.DATASTORE.INVALID";

    // ---- projected data stores — the `structure:` block verified against the package's own
    // migrations. A store that declares no
    // `structure:` raises none of these: it stays the opaque key→JSON store it always was.
    //
    // The three-state posture is the point (the `PATH_UNVERIFIABLE` house style): a definite
    // fault is an ERROR, an unprovable one is a WARNING with honest wording, and a projection
    // that matches is silent. The DDL parser deliberately degrades to the WARNING rather than
    // risk a false ERROR — see [`crate::ddl`].
    /// The declared structure type has a nested, repeated or open child (or no projectable
    /// child at all), so it cannot become a flat row. String-identical to
    /// `sutra_datastore::projection::codes::STRUCTURE_NOT_FLAT`, which the fault carries.
    pub const CONFIG_DATASTORE_STRUCTURE_NOT_FLAT: &str =
        "SUTRA.CONFIG.DATASTORE.STRUCTURE_NOT_FLAT";
    /// A declared field projects to a column the effective table (as the store's own
    /// `migrations/<store>/V*.sql` build it) does not have.
    pub const CONFIG_DATASTORE_COLUMN_MISSING: &str = "SUTRA.CONFIG.DATASTORE.COLUMN_MISSING";
    /// The column's type cannot hold the declared field's value space (`VARCHAR(10)` for a
    /// `maxLength = 35` field, an integer column for a fractional decimal), or its nullability
    /// contradicts the declaration (a `NOT NULL` column with no `DEFAULT` for an optional
    /// field, or one an `ALTER` adds that existing rows cannot satisfy).
    pub const CONFIG_DATASTORE_COLUMN_TYPE_MISMATCH: &str =
        "SUTRA.CONFIG.DATASTORE.COLUMN_TYPE_MISMATCH";
    /// The projected table's key is not a key over the projected columns — it declares none at
    /// all, or a key column the projection never writes / may leave absent.
    pub const CONFIG_DATASTORE_KEY_MISMATCH: &str = "SUTRA.CONFIG.DATASTORE.KEY_MISMATCH";
    /// A folded column name collides, is reserved, or exceeds the identifier cap, with no
    /// explicit `columns:` mapping. String-identical to
    /// `sutra_datastore::projection::codes::COLUMN_NAME_INVALID`, which the fault carries.
    pub const CONFIG_DATASTORE_COLUMN_NAME_INVALID: &str =
        "SUTRA.CONFIG.DATASTORE.COLUMN_NAME_INVALID";
    /// WARNING — the projection could not be verified: the migration SQL is outside the parsed
    /// subset, the table is created elsewhere, the declared schema is not an enumerable XSD, or
    /// a column type / declared type pair is not comparable. "May be valid; it is simply not
    /// provable" — never an ERROR.
    pub const CONFIG_DATASTORE_DDL_UNVERIFIABLE: &str = "SUTRA.CONFIG.DATASTORE.DDL_UNVERIFIABLE";
    /// WARNING — the table carries columns the projection never writes (usually fine: a legacy
    /// or operator column).
    pub const CONFIG_DATASTORE_COLUMN_UNMAPPED: &str = "SUTRA.CONFIG.DATASTORE.COLUMN_UNMAPPED";
    pub const CONFIG_CHANNEL_INERT: &str = "SUTRA.CONFIG.CHANNEL.INERT";
    pub const CONFIG_COVERAGE_STORE_MISSING: &str = "SUTRA.CONFIG.COVERAGE.STORE_MISSING";

    // ---- cross-process (collaboration) coverage — the deployment-level checks over
    // `coverage/**`, a `SUTRA.CONFIG.CORRELATION.*` family fired by
    // `check_coverage_correlations` in `lint.rs`. `COVERAGE_STORE_REQUIRED` is deliberately NOT a
    // distinct code: the cross-process routes desugar-inject into the referenced processes'
    // `coverage_paths` at load, so the existing `COVERAGE.STORE_MISSING` check already fires for
    // them.
    /// A coverage route's `segments` key, or a `links` hop's `<process>:<node>` reference, names
    /// a processId the deployment does not declare.
    pub const CONFIG_CORRELATION_PROCESS_UNKNOWN: &str = "SUTRA.CONFIG.CORRELATION.PROCESS_UNKNOWN";
    /// A coverage route's `segments[p]` lists a flow id that is not a `<bpmn:sequenceFlow>` of
    /// process `p`, or the sub-path is not contiguous within `p` (reuses the intra-process
    /// contiguity relation `sutra_bpmn::model::flows_contiguous`).
    pub const CONFIG_CORRELATION_FLOW_UNKNOWN: &str = "SUTRA.CONFIG.CORRELATION.FLOW_UNKNOWN";
    /// A `links` hop does not resolve to a channel link: its from-node emits nothing
    /// (`<q:send>`/`<q:reply>`), its to-node consumes nothing (a start-event `<q:source>` spawn or
    /// an `imec` relay-wait `MessageCatchEvent`/`UserTask`), a named node is absent, or an explicit
    /// `<q:send channel>` names a channel the to-node does not consume.
    pub const CONFIG_CORRELATION_LINK_UNRESOLVED: &str = "SUTRA.CONFIG.CORRELATION.LINK_UNRESOLVED";
    /// A hop's **effective** key (the hop's own `key`, else the correlation's default `key`) does
    /// not resolve at both endpoints: the consumer has no `<q:alias>` reading it, or a
    /// `header.<field>` key is not set by the emitter's `<q:send>`/`<q:reply>` `<q:header>`.
    /// It is a correlation value, NOT the `idempotencyKey`.
    pub const CONFIG_CORRELATION_KEY_RESOLVABLE: &str = "SUTRA.CONFIG.CORRELATION.KEY_RESOLVABLE";
    /// Two coverage routes under one coverage file URN share the same `path` mnemonic.
    pub const CONFIG_CORRELATION_PATH_ID_DUPLICATE: &str =
        "SUTRA.CONFIG.CORRELATION.PATH_ID_DUPLICATE";
    /// `sutra` is the engine's reserved URN keyword — a first-level artifact subfolder named
    /// `sutra` would produce a `urn:sutra:…` codec reference colliding with the engine
    /// namespace, so it is rejected (a deeper `sutra` segment is fine).
    pub const CONFIG_RESERVED_FIRST_LEVEL_FOLDER: &str = "SUTRA.CONFIG.RESERVED_FIRST_LEVEL_FOLDER";
    /// A user codec (a `schemas/` codec-manifest) may not shadow an engine-provided
    /// (built-in) codec name.
    pub const CONFIG_CODEC_RESERVED_NAME: &str = "SUTRA.CONFIG.CODEC.RESERVED_NAME";
    pub const CONFIG_BPMN_MESSAGE_TYPE_UNKNOWN: &str = "SUTRA.CONFIG.BPMN.MESSAGE_TYPE_UNKNOWN";
    pub const CONFIG_BPMN_RULE_MESSAGE_TYPE_MISMATCH: &str =
        "SUTRA.CONFIG.BPMN.RULE_MESSAGE_TYPE_MISMATCH";
    pub const CONFIG_RULES_MESSAGE_TYPE_UNDECLARED: &str =
        "SUTRA.CONFIG.RULES.MESSAGE_TYPE_UNDECLARED";
    pub const CONFIG_TEMPLATE_INPUT_UNSATISFIED: &str = "SUTRA.CONFIG.TEMPLATE.INPUT_UNSATISFIED";
    pub const CONFIG_TEMPLATE_FIELD_UNKNOWN: &str = "SUTRA.CONFIG.TEMPLATE.FIELD_UNKNOWN";
    pub const CONFIG_TEMPLATE_FIELD_UNVERIFIABLE: &str = "SUTRA.CONFIG.TEMPLATE.FIELD_UNVERIFIABLE";
    pub const CONFIG_TEMPLATE_FIELD_TYPE_UNPINNED: &str =
        "SUTRA.CONFIG.TEMPLATE.FIELD_TYPE_UNPINNED";
    /// A `messageTypePattern` payload path declared in only some of the pattern's
    /// matching types — ambiguous, a WARNING (declared-in-none is `FIELD_UNKNOWN`, an ERROR).
    pub const CONFIG_TEMPLATE_FIELD_PARTIAL: &str = "SUTRA.CONFIG.TEMPLATE.FIELD_PARTIAL";
    /// A template uses a construct the analyzer cannot tie to a concrete field — a dynamic
    /// / computed key (`{{lookup obj key}}` with a non-literal key). "Not statically
    /// validatable", deploy-blocking: ambiguity is surfaced, never silently allowed.
    pub const CONFIG_TEMPLATE_NOT_VALIDATABLE: &str = "SUTRA.CONFIG.TEMPLATE.NOT_VALIDATABLE";
    // ---- navigation ⇒ schema ----
    /// A `payload`-rooted FEEL path (alias / dispatch case / idempotencyKey / simpleValidator /
    /// flow condition) navigates a field absent from a *closed* intake-schema container — a
    /// provable typo, deploy-blocking.
    /// WARNING — a node is BPMN's UNTYPED `<task>`, which executes as a pass-through because an
    /// untyped task has no behaviour to run.
    ///
    /// Advisory rather than an error: `<task>` is a concrete, conformant BPMN element
    /// (`tTask` is not abstract in the BPMN 2.0 XSD), so refusing it would reject a valid model
    /// and break a round trip from a modelling tool. But an author who meant `<serviceTask>` and
    /// typed `<task>` has written a step that silently does nothing, and silence is the one
    /// outcome worth preventing. So it loads, and lint says so.
    pub const CONFIG_BPMN_UNTYPED_TASK: &str = "SUTRA.CONFIG.BPMN.UNTYPED_TASK";
    /// One variable name is declared twice over: once in BPMN's own data vocabulary
    /// (`<property>`, `<dataObject>`) and once as a `<q:variable>`.
    ///
    /// Advisory rather than an error: both spellings are accepted on purpose, so a model can
    /// round-trip through a modelling tool, and precedence is defined -- the `<q:variable>` wins,
    /// because it is the only spelling that can say `transient`, `sensitive`, `subjectKey` or
    /// `source`. What is left is ambiguity about which the author meant, and only the author can
    /// settle it, so lint names the element and says which declaration took effect.
    pub const CONFIG_BPMN_VARIABLE_SPELLING_CONFLICT: &str =
        "SUTRA.CONFIG.BPMN.VARIABLE_SPELLING_CONFLICT";
    pub const CONFIG_BPMN_PATH_UNKNOWN_FIELD: &str = "SUTRA.CONFIG.BPMN.PATH_UNKNOWN_FIELD";
    /// A FEEL path used in a numeric operator resolves to a declared non-numeric field (or a
    /// `<q:variables>`-declared non-number variable) — deploy-blocking.
    pub const CONFIG_BPMN_PATH_TYPE_MISMATCH: &str = "SUTRA.CONFIG.BPMN.PATH_TYPE_MISMATCH";
    /// A FEEL path cannot be verified (opaque codec, open schema region, or a descent through a
    /// non-object) — advisory WARNING (advise-don't-gatekeep).
    pub const CONFIG_BPMN_PATH_UNVERIFIABLE: &str = "SUTRA.CONFIG.BPMN.PATH_UNVERIFIABLE";
    /// A process navigates its payload but its codec declares many message types and the
    /// `<q:source>` pins none — no concrete schema can be selected, so its paths are a WARNING.
    pub const CONFIG_BPMN_MESSAGE_TYPE_UNPINNED: &str = "SUTRA.CONFIG.BPMN.MESSAGE_TYPE_UNPINNED";
    // ---- `<q:source filterExpression>` vocabulary — the intake predicate checked against the
    // attributes the bound codec DECLARES it can report. The posture split is the same one the
    // navigation family uses: fail closed on *provably impossible*, warn on *unverifiable*.
    /// The predicate reads `attr.<key>` for a key the codec's declared attribute vocabulary does
    /// not contain. The key set is CLOSED (the codec declared at least one attribute), so the
    /// comparison can never be true and the start event is dead — deploy-blocking, with a
    /// did-you-mean against the declared keys.
    pub const CONFIG_BPMN_SELECT_ATTRIBUTE_UNKNOWN: &str =
        "SUTRA.CONFIG.BPMN.SELECT_ATTRIBUTE_UNKNOWN";
    /// The predicate compares a declared attribute against a value outside its CLOSED declared
    /// domain — the codec can never emit it, so the comparison can never be true.
    /// Deploy-blocking.
    pub const CONFIG_BPMN_SELECT_VALUE_UNKNOWN: &str = "SUTRA.CONFIG.BPMN.SELECT_VALUE_UNKNOWN";
    /// The predicate compares a declared attribute against a literal of another FEEL type, or
    /// applies a relational operator to an attribute the codec declares non-numeric. A FEEL
    /// comparison across types yields `null`, which is not eligible — deploy-blocking.
    pub const CONFIG_BPMN_SELECT_TYPE_MISMATCH: &str = "SUTRA.CONFIG.BPMN.SELECT_TYPE_MISMATCH";
    /// WARNING — the channel's codec declares NO attributes, so its vocabulary is open and the
    /// predicate cannot be checked against anything. Advise-don't-gatekeep, matching
    /// [`CHANNEL_NO_SCHEMA`]: erroring here would make every schema-less channel unusable, and
    /// an open vocabulary is unverifiable, not provably wrong.
    pub const CONFIG_BPMN_SELECT_UNVERIFIABLE: &str = "SUTRA.CONFIG.BPMN.SELECT_UNVERIFIABLE";
    pub const CONFIG_TEMPLATE_OUTPUT_TYPE_UNKNOWN: &str =
        "SUTRA.CONFIG.TEMPLATE.OUTPUT_TYPE_UNKNOWN";
    pub const CONFIG_TEMPLATE_OUTPUT_UNVERIFIABLE: &str =
        "SUTRA.CONFIG.TEMPLATE.OUTPUT_UNVERIFIABLE";
    /// A `template-manifest.yaml` entry declares an `outputAttributes` key the destination
    /// codec never reports. The engine does not know what the key MEANS — it asks the codec
    /// through [`sutra_codec_spi::PayloadCodec::declared_attributes`] and compares.
    pub const CONFIG_TEMPLATE_OUTPUT_ATTRIBUTE_UNKNOWN: &str =
        "SUTRA.CONFIG.TEMPLATE.OUTPUT_ATTRIBUTE_UNKNOWN";
    /// A declared `outputAttributes` VALUE is outside the closed domain the destination codec
    /// declares for that key — so no render could ever carry it.
    pub const CONFIG_TEMPLATE_OUTPUT_ATTRIBUTE_VALUE_UNKNOWN: &str =
        "SUTRA.CONFIG.TEMPLATE.OUTPUT_ATTRIBUTE_VALUE_UNKNOWN";
    /// The destination codec, asked to classify the TEMPLATE'S OWN TEXT, reports a different
    /// message type or attribute than the manifest declares. This is what catches a template
    /// that renders the wrong envelope for its declared output: the engine learns nothing about
    /// envelopes, it just compares the codec's answer against the author's claim.
    pub const CONFIG_TEMPLATE_OUTPUT_MISMATCH: &str = "SUTRA.CONFIG.TEMPLATE.OUTPUT_MISMATCH";
    /// A `<q:send codec>` / `<q:reply codec>` names a codec this deployment does not carry.
    ///
    /// Fail-closed, and deliberately harsher than the inferred case: stating the codec is how an
    /// author asks for the output to be validated, so a typo there would otherwise turn the one
    /// declaration requesting a contract check into the one output that gets none. The runtime
    /// refuses it too; this is the package-time catch so it never reaches the runtime.
    pub const CONFIG_OUTPUT_CODEC_UNKNOWN: &str = "SUTRA.CONFIG.OUTPUT.CODEC_UNKNOWN";
    /// A `<q:variable transient="true">` is read by a node reachable *after* a wait
    /// state. A transient variable is never persisted, so it is gone on resume; a post-wait read
    /// would yield null. Fail-closed, deploy-blocking (the CLI can see the wait-state graph).
    pub const CONFIG_TRANSIENT_READ_AFTER_WAIT: &str = "SUTRA.CONFIG.TRANSIENT.READ_AFTER_WAIT";
    /// Deploy-time static lint — a `<q:variables>`-declared variable that is READ somewhere
    /// in the process but has NO writer at all: no `@source` intake, no data-task output
    /// (`dataOutputAssociation`/`<assignment>`/store read), and no `<q:output variable>` capture.
    /// It can never be initialised, so every read yields null. Advisory WARNING (not deploy
    /// blocking): the check is suppressed for any process that carries an opaque writer
    /// (`scriptTask` / `businessRuleTask` / a non-template serviceTask that full-merges its output),
    /// so it fires only on the clearest statically-provable never-written case.
    pub const CONFIG_VARIABLE_NEVER_INITIALIZED: &str = "SUTRA.CONFIG.VARIABLE.NEVER_INITIALIZED";
    /// Deploy-time migration notice for the fail-closed validation default
    /// an intake that carries a validation CONTRACT — a
    /// schema-backed codec, or a declared `<q:validators>` chain — but no `<q:onValidation>`
    /// policy. Such an intake used to pass a failing payload through to the flow; it now refuses
    /// it. Advisory WARNING naming the two explicit choices (`route` to keep the old behaviour,
    /// `reject` to state the new one), so the change is discovered at package time rather than in
    /// production.
    /// A `schemas/<codec>/codec-manifest.yaml` the codec loader rejects: an unknown format, a
    /// tabular format declared without its layout block, or — the one that matters most — a
    /// fixed-width layout whose columns disagree with the bound type. Package-time ERROR: the
    /// engine builds its codecs through the same entry point, so a manifest that fails here
    /// fails at deployment, and every row of every upload would fail after that.
    pub const CONFIG_CODEC_MANIFEST_REJECTED: &str = "SUTRA.CONFIG.CODEC_MANIFEST.REJECTED";
    pub const CONFIG_VALIDATION_POSTURE_UNDECLARED: &str =
        "SUTRA.CONFIG.VALIDATION.POSTURE_UNDECLARED";
    pub const CHANNEL_AMBIGUOUS_HANDLER: &str = "SUTRA.CHANNEL.AMBIGUOUS_HANDLER";
    pub const CHANNEL_AMBIGUOUS_PATTERN: &str = "SUTRA.CHANNEL.AMBIGUOUS_PATTERN";
    // ---- intake-selector disjointness — the `MT ∧ P` gate over a non-broadcast channel's
    // subscribers, decided by `crate::selector`. `AMBIGUOUS_HANDLER` stays the code for the
    // message-type-only overlap it has always reported; these three cover the predicate tier.
    /// Two subscribers on one non-broadcast channel are provably both eligible for one message:
    /// the message-type clauses overlap AND the predicates do too. The diagnostic prints the
    /// witnessing attribute assignment. Deploy-blocking — routing would resolve on process-id
    /// order, which is not a routing rule.
    pub const CHANNEL_SELECTOR_OVERLAP: &str = "SUTRA.CHANNEL.SELECTOR_OVERLAP";
    /// Two subscribers on one non-broadcast channel share a message type and at least one of
    /// their predicates left the Recognised Predicate Normal Form, so disjointness can be
    /// neither proven nor disproven. Deploy-blocking by design: the gate is a proof obligation,
    /// not advice, and the message names the construct, its offset, and the four escapes.
    pub const CHANNEL_SELECTOR_NOT_ANALYSABLE: &str = "SUTRA.CHANNEL.SELECTOR_NOT_ANALYSABLE";
    /// A `filterExpression` no message can satisfy (contradictory conjuncts, or an exclusion of
    /// every member of a closed declared domain) — a dead start event, the same posture as the
    /// `messageTypeValue` pin-can-never-match error. Reported whether or not the channel has
    /// other subscribers.
    pub const CHANNEL_SELECTOR_UNSATISFIABLE: &str = "SUTRA.CHANNEL.SELECTOR_UNSATISFIABLE";
    pub const CHANNEL_NO_IDEMPOTENCY_KEY: &str = "SUTRA.CHANNEL.NO_IDEMPOTENCY_KEY";
    pub const CHANNEL_NO_SCHEMA: &str = "SUTRA.CHANNEL.NO_SCHEMA";
    pub const CHANNEL_REPLY_NOT_EMITTABLE: &str = "SUTRA.CHANNEL.REPLY_NOT_EMITTABLE";
    pub const CHANNEL_REPLY_SCHEMALESS: &str = "SUTRA.CHANNEL.REPLY_SCHEMALESS";
    pub const INBOUND_CODEC_NOT_FOUND: &str = "SUTRA.INBOUND.CODEC_NOT_FOUND";
    pub const RESOLVE_TEMPLATE_UNKNOWN: &str = "SUTRA.RESOLVE.TEMPLATE.UNKNOWN";
    pub const CONFIG_CHANNEL_OUTBOUND_UNKNOWN: &str = "SUTRA.CONFIG.CHANNEL.OUTBOUND_UNKNOWN";
    /// A `q:stream="true"` channel-call task whose target is not a synchronous
    /// (`ack-mode: on-complete`) `transport: llm` channel — only a model answer made inline can
    /// stream to the waiting caller.
    pub const CONFIG_STREAM_TARGET_NOT_SYNC_LLM: &str = "SUTRA.CONFIG.STREAM.TARGET_NOT_SYNC_LLM";
    /// `reply-stream: sse` on a channel that is not an `http` channel answering `on-complete`
    /// — no other channel holds a connection a stream could be written to.
    pub const CONFIG_STREAM_REPLY_STREAM_UNSUPPORTED: &str =
        "SUTRA.CONFIG.STREAM.REPLY_STREAM_UNSUPPORTED";
    /// An outbound channel binds `local://<target>` (in-process routing), but `<target>` does
    /// not name a `transport: local` inbound channel of the SAME deployment — the in-process
    /// hop would dispatch to a channel that cannot receive it. Fail-closed at load.
    pub const CONFIG_CHANNEL_LOCAL_TARGET_UNKNOWN: &str =
        "SUTRA.CONFIG.CHANNEL.LOCAL_TARGET_UNKNOWN";

    // ---- the reserved SUTRA.DEPLOY.* family — the sealed-archive load checks ----
    /// The archive container is not a readable `.sutra` ZIP (open failure, non-UTF-8 or
    /// non-forward-slash entry name, nested archive, or a depth/entry-count limit breach).
    pub const DEPLOY_ARCHIVE_FORMAT_INVALID: &str = "SUTRA.DEPLOY.ARCHIVE.FORMAT_INVALID";
    /// `manifest.yaml` is missing, unparseable, or violates the manifest schema.
    pub const DEPLOY_ARCHIVE_MANIFEST_INVALID: &str = "SUTRA.DEPLOY.ARCHIVE.MANIFEST_INVALID";
    /// An artifact's bytes do not hash to the sha256 the manifest declares.
    pub const DEPLOY_ARCHIVE_DIGEST_MISMATCH: &str = "SUTRA.DEPLOY.ARCHIVE.DIGEST_MISMATCH";
    /// An archive entry is not listed in `artifacts[]` (no stowaways).
    pub const DEPLOY_ARCHIVE_STOWAWAY: &str = "SUTRA.DEPLOY.ARCHIVE.STOWAWAY";
    /// The deploymentId recomputed from the manifest bytes differs from the expected id.
    pub const DEPLOY_ARCHIVE_ID_MISMATCH: &str = "SUTRA.DEPLOY.ARCHIVE.ID_MISMATCH";
    /// The archive's content failed the package-time validation suite on load.
    pub const DEPLOY_ARCHIVE_CONTENT_INVALID: &str = "SUTRA.DEPLOY.ARCHIVE.CONTENT_INVALID";
    /// The packaging bounds (max 4096 entries, max path depth 8) were exceeded.
    pub const DEPLOY_PACKAGE_LIMIT_EXCEEDED: &str = "SUTRA.DEPLOY.PACKAGE.LIMIT_EXCEEDED";
    /// WARNING — layered composition: a path an upper layer supplied shadowed the copy a
    /// lower layer supplied. Non-fatal and purely informational, but always reported: an
    /// override that the author did not intend is otherwise invisible, and the composed tree
    /// is what gets sealed.
    pub const DEPLOY_PACKAGE_OVERLAY_OVERRIDE: &str = "SUTRA.DEPLOY.PACKAGE.OVERLAY_OVERRIDE";
    /// The package directory's `package.yaml` is missing, unparseable, or violates the
    /// closed schema (labels / engine.minContract / entryProcesses), or the
    /// directory itself cannot be scanned as a deployment package.
    pub const DEPLOY_PACKAGE_CONFIG_INVALID: &str = "SUTRA.DEPLOY.PACKAGE.CONFIG_INVALID";
    /// A multi-process file is only partially shadowed/inherited — the effective process
    /// set cannot be materialised into archive files without duplicating a process id.
    pub const DEPLOY_BPMN_PARTIAL_SHADOW: &str = "SUTRA.DEPLOY.BPMN.PARTIAL_SHADOW";
    /// An artifact kind the sealed-archive interior layout does not admit (e.g. a rule file
    /// in an unsupported format).
    pub const DEPLOY_ARTIFACT_UNSUPPORTED: &str = "SUTRA.DEPLOY.ARTIFACT.UNSUPPORTED";
    /// A BPMN node references a script/decision artifact the deployment does not provide
    /// (a dead reference, caught by package-time reference resolution).
    pub const DEPLOY_ARTIFACT_REF_UNRESOLVED: &str = "SUTRA.DEPLOY.ARTIFACT.REF_UNRESOLVED";
    /// A `direction: outbound` channel missing its transport or `bind` destination.
    pub const DEPLOY_CHANNEL_OUTBOUND_INVALID: &str = "SUTRA.DEPLOY.CHANNEL.OUTBOUND_INVALID";
    /// A `migrations/<dir>` that names no store declared in `datastores.yaml`.
    pub const DEPLOY_MIGRATIONS_STORE_UNDECLARED: &str = "SUTRA.DEPLOY.MIGRATIONS.STORE_UNDECLARED";
    /// A migration script that is not V-numbered well-formed SQL (name pattern
    /// `V<n>__<desc>.sql`, unique version per store, non-empty content).
    pub const DEPLOY_MIGRATIONS_SCRIPT_INVALID: &str = "SUTRA.DEPLOY.MIGRATIONS.SCRIPT_INVALID";
    /// A store's declared `sql.migrations` does not resolve to the archive-normative
    /// `migrations/<store>/` layout (or the declared folder is absent/empty).
    pub const DEPLOY_MIGRATIONS_LAYOUT_INVALID: &str = "SUTRA.DEPLOY.MIGRATIONS.LAYOUT_INVALID";
    /// `tenant-configuration.yaml` schema violation (id validity, status enum,
    /// retention bounds).
    pub const DEPLOY_TENANT_CONFIG_INVALID: &str = "SUTRA.DEPLOY.TENANT_CONFIG.INVALID";
    /// The same channel name is bound by more than one tenant in the authoring tree.
    pub const DEPLOY_TENANT_CHANNEL_OVERLAP: &str = "SUTRA.DEPLOY.TENANT_CONFIG.CHANNEL_OVERLAP";
    /// A tenant with module bindings but no channel declarations anywhere.
    pub const DEPLOY_TENANT_NO_CHANNELS: &str = "SUTRA.DEPLOY.TENANT_CONFIG.NO_CHANNELS";
    /// A datastores/channels connection carries a LITERAL credential — a literal
    /// username/password, or a URL/URI with an embedded literal password — instead of an
    /// `env:` / `secret:` / `${…}` reference (R14 security posture: credentials live in the
    /// mounted estate Secret, never the deployments ConfigMap, which has no field-level
    /// sensitivity). Detection is vocabulary-agnostic on the property's last dotted segment,
    /// so every connection key family is covered regardless of its prefix.
    pub const DEPLOY_CREDENTIALS_LITERAL: &str = "SUTRA.DEPLOY.CREDENTIALS.LITERAL";
}

/// An error-severity loader diagnostic — engine startup refuses (fail-closed): the
/// diagnostic carries its code and message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoaderError {
    pub code: &'static str,
    pub message: String,
}

impl LoaderError {
    pub fn new(code: &'static str, message: impl Into<String>) -> LoaderError {
        LoaderError {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for LoaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for LoaderError {}
