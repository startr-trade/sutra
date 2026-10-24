//! The conformance harness, re-exported under the name the suites have always used.
//!
//! The harness itself was promoted into `sutra-testkit` (`sutra_testkit::conformance`, behind
//! its `conformance` feature) so a suite crate in ANOTHER workspace can drive the same
//! fixtures — postgres/broker/engine containers, the DRY-variant composer, the host + broker
//! recorders, the tier-3 k8s plumbing — instead of copying six test-internal modules. This
//! module is the thin alias that keeps every `crate::support::<m>` path in the suites intact;
//! nothing suite-specific lives here.

// `compose` (the `shared/` + `variants/` composer, the same `sutra_loader::overlay` path as
// `sutra package --overlay`) is used by `tc_ai_examples`, whose examples keep one shared layer
// plus one small layer per provider. `callback` (host recorders) is NOT re-exported: no suite in
// THIS repo uses it; it remains part of `sutra_testkit::conformance` for out-of-workspace suites.
pub use sutra_testkit::conformance::{broker, compose, engine, k8s, util};
