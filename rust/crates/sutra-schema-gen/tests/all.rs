//! Consolidated integration-test binary for sutra-schema-gen (one link unit; modules
//! preserve the original file names as filter paths).

#[path = "all/golden.rs"]
mod golden;
#[path = "all/mini_schema.rs"]
mod mini_schema;
