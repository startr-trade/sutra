//! Consolidated integration-test binary for sutra-xsd (one link unit; modules preserve the original file names as filter paths).

#[path = "all/support.rs"]
mod support;

#[path = "all/compile_subset.rs"]
mod compile_subset;
#[path = "all/field_decls.rs"]
mod field_decls;
#[path = "all/shape_tables.rs"]
mod shape_tables;
#[path = "all/validate_behavior.rs"]
mod validate_behavior;
