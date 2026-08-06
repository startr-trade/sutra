//! Consolidated conformance-test binary.
//!
//! Each suite is a module under `tests/all/`, referenced here via an explicit `#[path]` so
//! cargo compiles ONE test binary. Tier markers: every tier-2 test fn name starts `tc_` and
//! carries `#[ignore = "docker"]`; every tier-3 test fn name starts `k8s_` and carries
//! `#[ignore = "k8s"]`. The Makefile filters key off those prefixes (tier-2 runs `tc_` with
//! `--skip k8s_`; tier-3 runs `k8s_` serially).
//!
//! The harness the suites share lives in `sutra_testkit::conformance`; `all/support.rs`
//! re-exports it under the `crate::support::*` paths the suites use.

// Force-link the builtin payload codecs so their `inventory::submit!` registrations are present in
// THIS test binary. `support::engine::assemble_example` calls `sutra_loader`'s `assemble_dir`,
// which lints each example archive against `sutra_codec_spi::builtin_codecs()`/`builtin_formats()`;
// a package binding a codec this binary did not link fails the lint CODEC_NOT_FOUND. Same pattern
// as sutra-loader/tests/all.rs. The public suites' packages bind path-derived XSD codecs plus the
// formats — nothing message-standard.
use sutra_formats as _;

#[path = "all/support.rs"]
mod support;

#[path = "all/tc_approval_hold.rs"]
mod tc_approval_hold;
#[path = "all/tc_money_transfer.rs"]
mod tc_money_transfer;
#[path = "all/tc_multi_replica.rs"]
mod tc_multi_replica;
#[path = "all/tc_shard_lanes.rs"]
mod tc_shard_lanes;

// Tier-3. Both k8s suites deploy the SAME slot (`default--money-transfer--1.0.0`) — the richest
// public example the shared scenario can host unchanged. That is safe because tier-3 runs serially
// (`--test-threads=1`) in name order and each suite's fixture is built lazily by its own first
// test: the `_zz_teardown` of one suite always precedes the provisioning of the next, and
// `undeploy_api_quiet` tolerates an already-absent slot in either direction.
#[path = "all/k8s_money_transfer.rs"]
mod k8s_money_transfer;
#[path = "all/k8s_observability.rs"]
mod k8s_observability;
