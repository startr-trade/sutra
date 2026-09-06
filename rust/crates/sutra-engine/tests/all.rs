//! Consolidated integration-test binary for `sutra-engine`.
//!
//! Each former top-level `tests/<name>.rs` is now a module under `tests/all/`, referenced
//! here via an explicit `#[path]` so cargo compiles ONE test binary instead of nine. Module
//! names equal the original file stems, so filter paths (e.g. `smoke::boots_and_reports_*`)
//! and `#[ignore = "docker"]` tiering are unchanged. The `tests/resources/` data tree stays
//! in place — it is loaded via `env!("CARGO_MANIFEST_DIR")` absolute paths.

// Force-link every enabled transport (mirrors main.rs): a test binary links only the engine
// LIB, and the neutral engine references transports solely through `transport_factories()` —
// so without these the linker drops the crates, `transport_factories()` is empty, and channels
// (HTTP endpoints, broker consumers) never bind. Tests that boot the engine via `serve()` need
// the full bundle, HTTP included, or channel POSTs 404. Cargo-feature-gated to match the
// binary (default features link all six).
use sutra_transport_amqp as _;
use sutra_transport_file as _;
use sutra_transport_gcp_pubsub as _;
use sutra_transport_http as _;
use sutra_transport_kafka as _;
use sutra_transport_rabbitmq as _;
use sutra_transport_sqs as _;

#[path = "all/archive_activation_conformance.rs"]
mod archive_activation_conformance;
#[path = "all/call_log_csv_e2e.rs"]
mod call_log_csv_e2e;
#[path = "all/channel_call_retry_it.rs"]
mod channel_call_retry_it;
#[path = "all/concurrency_cluster_it.rs"]
mod concurrency_cluster_it;
#[path = "all/external_task_pull_e2e.rs"]
mod external_task_pull_e2e;
#[path = "all/incident_dead_letter_it.rs"]
mod incident_dead_letter_it;
#[path = "all/instance_migration_it.rs"]
mod instance_migration_it;
#[path = "all/leadership_it.rs"]
mod leadership_it;
#[path = "all/otel_it.rs"]
mod otel_it;
#[path = "all/outbox_e2e.rs"]
mod outbox_e2e;
#[path = "all/outbox_it.rs"]
mod outbox_it;
#[path = "all/pool_exhaustion_soak_it.rs"]
mod pool_exhaustion_soak_it;
#[path = "all/rls_bypass_it.rs"]
mod rls_bypass_it;
#[path = "all/shard_scale_out_it.rs"]
mod shard_scale_out_it;
#[path = "all/shard_support.rs"]
mod shard_support;
#[path = "all/smoke.rs"]
mod smoke;
#[path = "all/terminal_retention_it.rs"]
mod terminal_retention_it;
#[path = "all/time_skipping_it.rs"]
mod time_skipping_it;
#[path = "all/timer_channel_call_conformance.rs"]
mod timer_channel_call_conformance;
#[path = "all/timer_start_conformance.rs"]
mod timer_start_conformance;
#[path = "all/typed_snapshot_it.rs"]
mod typed_snapshot_it;
