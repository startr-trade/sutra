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
    // migrations (design `datastore-schema-projection.md` §4.6). A store that declares no
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
    pub const CONFIG_TEMPLATE_OUTPUT_TYPE_UNKNOWN: &str =
        "SUTRA.CONFIG.TEMPLATE.OUTPUT_TYPE_UNKNOWN";
    pub const CONFIG_TEMPLATE_OUTPUT_UNVERIFIABLE: &str =
        "SUTRA.CONFIG.TEMPLATE.OUTPUT_UNVERIFIABLE";
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
    pub const CHANNEL_AMBIGUOUS_HANDLER: &str = "SUTRA.CHANNEL.AMBIGUOUS_HANDLER";
    pub const CHANNEL_AMBIGUOUS_PATTERN: &str = "SUTRA.CHANNEL.AMBIGUOUS_PATTERN";
    pub const CHANNEL_NO_IDEMPOTENCY_KEY: &str = "SUTRA.CHANNEL.NO_IDEMPOTENCY_KEY";
    pub const CHANNEL_NO_SCHEMA: &str = "SUTRA.CHANNEL.NO_SCHEMA";
    pub const CHANNEL_REPLY_NOT_EMITTABLE: &str = "SUTRA.CHANNEL.REPLY_NOT_EMITTABLE";
    pub const CHANNEL_REPLY_SCHEMALESS: &str = "SUTRA.CHANNEL.REPLY_SCHEMALESS";
    pub const INBOUND_CODEC_NOT_FOUND: &str = "SUTRA.INBOUND.CODEC_NOT_FOUND";
    pub const RESOLVE_TEMPLATE_UNKNOWN: &str = "SUTRA.RESOLVE.TEMPLATE.UNKNOWN";
    pub const CONFIG_CHANNEL_OUTBOUND_UNKNOWN: &str = "SUTRA.CONFIG.CHANNEL.OUTBOUND_UNKNOWN";
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
