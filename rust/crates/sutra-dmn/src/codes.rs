//! Stable diagnostic code strings emitted by the DMN validator — mirror of
//! `DmnValidatorCodes` (all codes verbatim).
//!
//! All validation codes use the `SUTRA.VALIDATE.DMN.*` phase prefix (content-validator /
//! semantic issues); startup codes mirror the registration lifecycle pair.

/// A `.dmn` file was registered as a content validator at startup / on reload.
pub const STARTUP_DMN_REGISTERED: &str = "SUTRA.STARTUP.DMN.REGISTERED";

/// A `.dmn` file failed to parse/compile and was left unregistered — fail closed.
///
/// Retained for lineage. Under the sealed-archive deployment model a broken `.dmn` is caught at
/// DEPLOY time — `sutra-loader`'s `check_rule_artifacts` and `sutra-engine` assembly both reject
/// it, raising [`DMN_FILE_PARSE_ERROR`] — so it never reaches startup and this boot-time code is
/// not raised in the Rust runtime.
pub const STARTUP_DMN_LOAD_FAILED: &str = "SUTRA.STARTUP.DMN.LOAD_FAILED";

/// A `.dmn` file could not be parsed (malformed XML, missing required element, etc.).
pub const DMN_FILE_PARSE_ERROR: &str = "SUTRA.VALIDATE.DMN.FILE_PARSE_ERROR";

/// A decision referenced by id was not registered.
pub const DMN_DECISION_NOT_FOUND: &str = "SUTRA.VALIDATE.DMN.DECISION_NOT_FOUND";

/// An input expression resolved to a value incompatible with the input clause's `typeRef`.
pub const DMN_INPUT_TYPE_MISMATCH: &str = "SUTRA.VALIDATE.DMN.INPUT_TYPE_MISMATCH";

/// Hit policy is `UNIQUE` but more than one rule fired for the same input.
pub const DMN_UNIQUE_VIOLATION: &str = "SUTRA.VALIDATE.DMN.UNIQUE_VIOLATION";

/// Default code when an output cell doesn't specify a custom `bpm:code`.
pub const DMN_RULESET_FAILED: &str = "SUTRA.VALIDATE.DMN.RULESET_FAILED";

/// Hit policy is `ANY` but two or more matching rules produced disagreeing outputs.
pub const DMN_ANY_HIT_POLICY_AMBIGUOUS: &str = "SUTRA.VALIDATE.DMN.ANY_HIT_POLICY_AMBIGUOUS";

/// Hit policy is `PRIORITY` but the output clause has no `<outputValues>` priority list —
/// falls back to `UNIQUE` semantics with this WARNING.
pub const DMN_PRIORITY_MISSING_OUTPUT_VALUES: &str =
    "SUTRA.VALIDATE.DMN.PRIORITY_MISSING_OUTPUT_VALUES";

/// Hit policy is `OUTPUT_ORDER` but the output clause has no `<outputValues>` priority list —
/// falls back to `COLLECT` with this WARNING.
pub const DMN_OUTPUT_ORDER_MISSING_OUTPUT_VALUES: &str =
    "SUTRA.VALIDATE.DMN.OUTPUT_ORDER_MISSING_OUTPUT_VALUES";
