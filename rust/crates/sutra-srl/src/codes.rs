//! Stable diagnostic code strings emitted by the `.srl` lexer / parser / evaluator.
//!
//! Parse-time issues use the `SUTRA.SRL.PARSE.*` phase prefix; evaluation-time issues use
//! `SUTRA.SRL.EVAL.*`. The codes are load-bearing (fail-closed deploy validation keys), so
//! they are treated as a stable contract.

/// A structural token was missing or unexpected (missing `then`/`end`/`;`, wrong token, etc.).
pub const SRL_SYNTAX_ERROR: &str = "SUTRA.SRL.PARSE.SYNTAX_ERROR";

/// An unterminated string literal ran to end-of-input.
pub const SRL_UNCLOSED_STRING: &str = "SUTRA.SRL.PARSE.UNCLOSED_STRING";

/// An unbalanced `(`/`[` in an action argument list ran to end-of-input.
pub const SRL_UNTERMINATED_PAREN: &str = "SUTRA.SRL.PARSE.UNTERMINATED_PAREN";

/// An action verb that is not part of the closed set (`report` / `set`) was used.
pub const SRL_UNKNOWN_VERB: &str = "SUTRA.SRL.PARSE.UNKNOWN_VERB";

/// The `insert` / `retract` verbs are reserved for a stateful rules engine, which is not built.
pub const SRL_RESERVED_VERB: &str = "SUTRA.SRL.PARSE.RESERVED_VERB";

/// A `report(...)` / `set(...)` had the wrong number of arguments.
pub const SRL_BAD_ARITY: &str = "SUTRA.SRL.PARSE.BAD_ARITY";

/// An embedded FEEL expression failed to parse. Wraps the underlying `SUTRA.FEEL.*` diagnostic.
pub const SRL_FEEL_PARSE_ERROR: &str = "SUTRA.SRL.PARSE.FEEL_ERROR";

/// The ruleset bytes were not valid UTF-8.
pub const SRL_INVALID_UTF8: &str = "SUTRA.SRL.PARSE.INVALID_UTF8";

/// An embedded FEEL expression failed to evaluate at agenda-fire time (fail-closed: a rule
/// whose condition or action errors is a hard error, never a silent skip). Wraps the
/// underlying `SUTRA.FEEL.*` diagnostic.
pub const SRL_FEEL_EVAL_ERROR: &str = "SUTRA.SRL.EVAL.FEEL_ERROR";
