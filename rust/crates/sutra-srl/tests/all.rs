//! Consolidated integration-test binary for sutra-srl (one link unit; modules preserve the
//! original file names as filter paths).

#[path = "all/engine_tests.rs"]
mod engine_tests;
#[path = "all/parser_tests.rs"]
mod parser_tests;
