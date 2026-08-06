//! Sutra conformance gate — the primary end-to-end certification of the shipped engine.
//!
//! This library is intentionally empty. Every gate lives under `tests/all/`: the tier-2
//! `tc_*` suites boot the `sutra-rust-engine:dev` image via testcontainers and drive each
//! example end to end (the same assertions the retired example integration tests made,
//! now against the shipped image); the tier-3 `k8s_*` suites hot-deploy onto the shared
//! cluster instance and assert through the one Ingress.
//!
//! Running the gate:
//! - tier-2 (docker): `cargo test -p sutra-conformance -- --ignored tc_`
//! - tier-3 (k8s):    `cargo test -p sutra-conformance -- --ignored --test-threads=1 k8s_`
