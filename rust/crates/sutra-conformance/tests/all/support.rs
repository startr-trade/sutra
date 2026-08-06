//! The conformance harness, re-exported under the name the suites have always used.
//!
//! The harness itself was promoted into `sutra-testkit` (`sutra_testkit::conformance`, behind
//! its `conformance` feature) so a suite crate in ANOTHER workspace can drive the same
//! fixtures — postgres/broker/engine containers, the DRY-variant composer, the host + broker
//! recorders, the tier-3 k8s plumbing — instead of copying six test-internal modules. This
//! module is the thin alias that keeps every `crate::support::<m>` path in the suites intact;
//! nothing suite-specific lives here.

// `callback` (host recorders) and `compose` (the `shared/` + `variants/` DRY composer) are NOT
// re-exported: no suite in THIS repo uses them any more — the multi-variant packages and the
// outbound-leg recorders they served belong to the proprietary extension suites. They remain part
// of `sutra_testkit::conformance` for those out-of-workspace suites; re-exporting them here would
// only be an unused import.
pub use sutra_testkit::conformance::{broker, engine, k8s, util};
