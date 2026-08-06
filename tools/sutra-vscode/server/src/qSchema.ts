/**
 * Sutra q:* namespace schema — the IDE-side mirror of `xsd/q.xsd`.
 *
 * This module is the single place the LSP consults for:
 *   - which `<q:*>` element names are valid inside `<bpmn:extensionElements>`
 *   - which attributes each element allows + which are required
 *   - the enum domains for attributes whose XSD type is a restriction
 *   - hover-time documentation for each element + attribute
 *   - the FEEL-bearing attribute spots (where determinism + completion fire)
 *
 * Keep this file in lock-step with `xsd/q.xsd` — when the XSD is bumped,
 * mirror the change here and re-run `npm test --root server` to surface any
 * fixture / completion / diagnostic drift.
 */
export interface AttributeSpec {
  name: string;
  required: boolean;
  /** When set, the attribute value must be one of these enum strings. */
  enumValues?: readonly string[];
  /** When true, the attribute value is a FEEL expression. */
  feel?: boolean;
  /** Short markdown description shown on hover. */
  doc: string;
}

export interface ElementSpec {
  /** Local name (without the `q:` prefix). */
  localName: string;
  /** Short markdown description shown on element-name hover. */
  doc: string;
  attributes: readonly AttributeSpec[];
  /** Optional list of child elements (by `q:*` qualified name). */
  children?: readonly string[];
}

/**
 * The 9 valid `<q:*>` elements from `xsd/q.xsd`. Order matches the XSD's
 * top-level declaration sequence so that completion / outline rendering
 * tracks the schema's reading order.
 */
export const Q_ELEMENTS: readonly ElementSpec[] = [
  {
    localName: 'source',
    doc: 'Binds an inbound channel to a Message Start Event. Multiple `<q:source>` on the same start event mean the process accepts inbound on any of those channels.',
    attributes: [
      { name: 'channel', required: true, doc: 'Channel id; resolved to a tenant via tenant-configuration.yaml.' },
      { name: 'ack', required: false, enumValues: ['on-persist', 'on-complete'], doc: 'Broker-ack timing. Defaults to `on-persist`.' },
      { name: 'dedupKey', required: false, doc: 'FEEL expression extracting a duplicate-detection value from headers/payload (e.g. `header.X-Request-Id`, `body.GrpHdr.MsgId`). Renamed from idempotencyKey; a dedup key is not an idempotency assertion (see `<q:process idempotent>`).' },
      { name: 'type', required: false, doc: 'Optional CloudEvents `type`. Setting this enables CE detection on the channel.' },
      { name: 'dataClass', required: false, enumValues: ['none', 'pii', 'pci', 'phi', 'financial'], doc: 'GDPR data classification; drives redactor policy + audit expiry. Defaults to `none`.' },
    ],
  },
  {
    localName: 'input',
    doc: 'Declares the codec that decodes the inbound payload into a typed envelope visible to FEEL as `payload.*`.',
    attributes: [
      { name: 'name', required: false, doc: 'Logical name of the resulting payload object (defaults to `payload`).' },
      { name: 'codec', required: true, doc: 'Codec id; resolved against registered `PayloadCodec` beans.' },
      { name: 'accept', required: false, doc: 'Content-type filter; `*` accepts all.' },
    ],
    children: ['q:validators'],
  },
  {
    localName: 'validators',
    doc: 'Plural validator binding. References a validator chain (DRL or extension) by `source` name.',
    attributes: [
      { name: 'source', required: true, doc: 'Validator name; resolves to a DRL file under `<resources>/rules/` or a `sutra-validator-*` extension.' },
      { name: 'scope', required: false, enumValues: ['common', 'tenant'], doc: 'Explicit `common` / `tenant` scope. Unscoped tries tenant first, then common (if inherited).' },
      { name: 'when', required: false, feel: true, doc: 'Optional FEEL guard; the validator chain runs only when this evaluates true.' },
      { name: 'consolidate', required: false, enumValues: ['true', 'false'], doc: 'Aggregate all issues into a single diagnostic. Defaults to `true`.' },
    ],
  },
  {
    localName: 'reply',
    doc: 'Declares an outbound reply. `mode="native"` (default) preserves symmetric-reply behaviour. CloudEvents wraps and match-inbound also supported.',
    attributes: [
      { name: 'mode', required: false, enumValues: ['native', 'cloudevent-binary', 'cloudevent-structured', 'match-inbound'], doc: 'Reply emission mode. Defaults to `native`.' },
      { name: 'destination', required: false, doc: 'Fallback destination when no inbound `Reply-To` / `X-Callback-Handler` override is present.' },
      { name: 'contentType', required: false, doc: 'Outbound content-type header.' },
      { name: 'required', required: false, enumValues: ['true', 'false'], doc: 'When true, emit `SUTRA.OUTBOUND.NO_DESTINATION` if no destination resolves. Defaults to false.' },
      { name: 'type', required: false, doc: 'CloudEvents `type` (cloudevent-* modes only).' },
      { name: 'source', required: false, doc: 'CloudEvents `source` URI (cloudevent-* modes only).' },
      { name: 'subject', required: false, doc: 'CloudEvents `subject` (cloudevent-* modes only).' },
      { name: 'datacontenttype', required: false, doc: 'CloudEvents `datacontenttype` (cloudevent-* modes only).' },
      { name: 'auth', required: false, enumValues: ['mtls', 'bearer', 'apikey'], doc: 'Outbound auth scheme.' },
      { name: 'authSecretRef', required: false, doc: 'URI to the secret store (`env:`, `k8s:`, `vault:` etc.).' },
      { name: 'authHeader', required: false, doc: 'Header name carrying the auth token (defaults to `Authorization` for bearer, `X-Api-Key` for apikey).' },
    ],
  },
  {
    localName: 'alias',
    doc: 'Friendly key derived from a FEEL expression. Replay-bound; FEEL determinism denylist applies.',
    attributes: [
      { name: 'name', required: true, doc: 'Alias name; admin REST + signal routing use this to look up the instance.' },
      { name: 'expression', required: true, feel: true, doc: 'FEEL expression — typically `payload.someField`.' },
      { name: 'unique', required: false, enumValues: ['true', 'false'], doc: 'Whether the alias value must be unique across live instances.' },
      { name: 'onConflict', required: false, enumValues: ['reject', 'correlate'], doc: 'Behaviour when a unique alias collides: reject inbound, or correlate to the existing instance.' },
      { name: 'multi', required: false, enumValues: ['true', 'false'], doc: 'When true, the FEEL expression must return a list and each element is indexed.' },
    ],
  },
  {
    localName: 'dispatch',
    doc: 'Dynamic call-activity dispatch table. Evaluates each `<q:case when="…">` and invokes the matching `calledElement`.',
    attributes: [
      { name: 'default', required: false, doc: 'Fallback `calledElement` when no `<q:case>` matches.' },
      { name: 'onNoMatch', required: false, enumValues: ['error', 'skip'], doc: 'Behaviour when no case matches and no `default` is set. Defaults to `error`.' },
    ],
    children: ['q:case'],
  },
  {
    localName: 'case',
    doc: 'One row in a `<q:dispatch>` table. The first case whose `when` evaluates true selects the `calledElement`.',
    attributes: [
      { name: 'when', required: true, feel: true, doc: 'FEEL expression — when it evaluates true the `calledElement` is invoked.' },
      { name: 'calledElement', required: true, doc: 'BPMN process id to invoke when this case matches.' },
      { name: 'scope', required: false, enumValues: ['common', 'tenant'], doc: 'Resolution scope for the `calledElement` lookup.' },
    ],
  },
  {
    localName: 'onValidation',
    doc: 'Policy for what to do when payload validation fails.',
    attributes: [
      { name: 'mode', required: true, enumValues: ['route', 'reject', 'error'], doc: '`route` branches on `payload.validation`; `reject` returns a synchronous fault; `error` throws a BPMN error inside the started instance.' },
      { name: 'errorCode', required: false, doc: 'BPMN error code used when `mode="error"`.' },
    ],
  },
  {
    localName: 'audit',
    doc: 'Per-process audit configuration.',
    attributes: [
      { name: 'sink', required: false, doc: 'Audit sink name (defaults to `sql`). Must resolve to a registered audit sink.' },
      { name: 'target', required: false, doc: 'Sink-specific target (e.g. table name for sql, file path for jsonl).' },
      { name: 'capture', required: false, enumValues: ['none', 'metadata', 'payload'], doc: 'How much to capture per event. Defaults to `payload`.' },
      { name: 'version', required: false, doc: 'Existing process-level audit version pin.' },
    ],
  },
] as const;

/** Map for quick lookup by qualified `q:<localName>` name. */
export const Q_ELEMENT_BY_NAME: ReadonlyMap<string, ElementSpec> = new Map(
  Q_ELEMENTS.map((e) => [`q:${e.localName}`, e])
);

/** All valid `q:*` element qualified names. */
export const Q_ELEMENT_NAMES: ReadonlySet<string> = new Set(
  Q_ELEMENTS.map((e) => `q:${e.localName}`)
);

/**
 * Map of attribute → SUTRA diagnostic code for required-attribute violations
 * (mirroring `engine/spi/src/main/resources/diagnostics.yaml`).
 */
export const MISSING_REQUIRED_CODES: Readonly<Record<string, Record<string, string>>> = {
  'q:input': {
    codec: 'SUTRA.PARSE.Q_INPUT_MISSING_CODEC',
  },
  'q:case': {
    when: 'SUTRA.PARSE.Q_CASE_MISSING_WHEN',
    calledElement: 'SUTRA.PARSE.Q_CASE_MISSING_CALLED_ELEMENT',
  },
  'q:alias': {
    name: 'SUTRA.PARSE.Q_ALIAS_MISSING_NAME',
    expression: 'SUTRA.PARSE.Q_ALIAS_MISSING_EXPRESSION',
  },
};

/**
 * Map of (element → attribute) → SUTRA diagnostic code for invalid-enum violations.
 */
export const INVALID_ENUM_CODES: Readonly<Record<string, Record<string, string>>> = {
  'q:reply': { mode: 'SUTRA.PARSE.Q_REPLY_INVALID_MODE' },
  'q:onValidation': { mode: 'SUTRA.PARSE.Q_ON_VALIDATION_INVALID_MODE' },
};
