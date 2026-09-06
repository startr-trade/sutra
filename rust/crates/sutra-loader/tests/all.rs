//! Consolidated integration-test binary for sutra-loader (one link unit; modules preserve the original file names as filter paths).

// Force-link the builtin payload codecs so their `inventory::submit!` registrations are present in
// THIS test binary — the example-package lint pulls the builtin set from
// `sutra_codec_spi::builtin_codecs()`/`builtin_formats()`, so without these a package binding one
// fails the lint as CODEC_NOT_FOUND. The CLI/engine binaries force-link them the same way. The
// public examples bind path-derived XSD codecs plus the formats; message-standard codecs live in
// proprietary extension crates, and their packaging cases are linted in the repository that owns
// them.
use sutra_formats as _;

#[path = "all/archive_negative.rs"]
mod archive_negative;
#[path = "all/lint_navigation_test.rs"]
mod lint_navigation_test;
#[path = "all/lint_never_init_test.rs"]
mod lint_never_init_test;
#[path = "all/lint_output_conformance_test.rs"]
mod lint_output_conformance_test;
#[path = "all/lint_store_structure_test.rs"]
mod lint_store_structure_test;
#[path = "all/lint_template_fields_test.rs"]
mod lint_template_fields_test;
#[path = "all/lint_transient_test.rs"]
mod lint_transient_test;
#[path = "all/lint_variable_nav_test.rs"]
mod lint_variable_nav_test;
#[path = "all/lint_variable_schema_test.rs"]
mod lint_variable_schema_test;
#[path = "all/package_examples.rs"]
mod package_examples;
