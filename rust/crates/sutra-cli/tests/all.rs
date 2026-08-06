//! Consolidated integration-test binary for sutra-cli (one link unit; modules preserve the original file names as filter paths).

// Force-link the HTTP transport (mirrors `sutra-engine/tests/all.rs` and `sutra-cli/src/main.rs`):
// a test binary links only the `sutra_cli` LIB, and the neutral engine `test_simulate_it.rs`
// boots via `serve()` references transports solely through `transport_factories()` — so
// without this the linker drops the crate and the fixture's HTTP-bound channel 404s.
use sutra_transport_http as _;

#[path = "all/coverage_golden.rs"]
mod coverage_golden;
#[path = "all/deployments_list_it.rs"]
mod deployments_list_it;
#[path = "all/pg_migrate_it.rs"]
mod pg_migrate_it;
#[path = "all/test_simulate_it.rs"]
mod test_simulate_it;
