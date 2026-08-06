//! Stable `SUTRA.FEEL.*` diagnostic code strings — the FEEL section of the shared
//! diagnostic-code catalog.

pub const FEEL_SYNTAX_UNEXPECTED_TOKEN: &str = "SUTRA.FEEL.SYNTAX.UNEXPECTED_TOKEN";
pub const FEEL_SYNTAX_UNCLOSED_BRACKET: &str = "SUTRA.FEEL.SYNTAX.UNCLOSED_BRACKET";
pub const FEEL_DETERMINISM_UNSAFE_BUILTIN: &str = "SUTRA.FEEL.DETERMINISM.UNSAFE_BUILTIN";
pub const FEEL_COMPILE_UNDEFINED_VARIABLE: &str = "SUTRA.FEEL.COMPILE.UNDEFINED_VARIABLE";
pub const FEEL_COMPILE_TYPE_MISMATCH: &str = "SUTRA.FEEL.COMPILE.TYPE_MISMATCH";
pub const FEEL_EVAL_NULL_DEREFERENCE: &str = "SUTRA.FEEL.EVAL.NULL_DEREFERENCE";
/// Invoking an `external` function definition (FEEL rule 55 / DMN `kind="Java"/"PMML"`) — a
/// deliberate semantic rejection: external-function EXECUTION is an optional DMN feature this
/// engine does not provide. Deliberately NOT a `SYNTAX.*` code — the DMN-TCK harness credits an
/// `errorResult` case only on a non-syntax error (a syntax error means "couldn't even parse it").
pub const FEEL_EVAL_EXTERNAL_UNSUPPORTED: &str = "SUTRA.FEEL.EVAL.EXTERNAL_UNSUPPORTED";
