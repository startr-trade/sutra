//! Consolidated integration-test binary for sutra-feel (one link unit; modules preserve the original file names as filter paths).

#[path = "all/feel_determinism.rs"]
mod feel_determinism;
#[path = "all/feel_expressions.rs"]
mod feel_expressions;
#[path = "all/feel_external_functions.rs"]
mod feel_external_functions;
#[path = "all/feel_intervals.rs"]
mod feel_intervals;
#[path = "all/feel_parse_spaced_builtins.rs"]
mod feel_parse_spaced_builtins;
#[path = "all/feel_paths.rs"]
mod feel_paths;
#[path = "all/feel_type_conversions.rs"]
mod feel_type_conversions;
