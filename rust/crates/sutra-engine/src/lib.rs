//! The Rust engine binary, fully assembled. One boot wires: config (`sutra.*` keys
//! from env/file, canonical `SUTRA_*` env names), the legacy directory-tree resource
//! loader, the engine-internal persistence layer (the engine's own tables + the vendored
//! migration SQL), the module-owned `sql` data stores, the executor registries, the channel
//! intake (HTTP transport + structural codecs + DMN validators), the durable suspend→resume
//! bridge (snapshot v2 + the transactional step primitive), `/sutra/health/live` +
//! `/sutra/health/ready`, and structured JSON logging to stdout.
#![forbid(unsafe_code)]

pub mod admin;
pub mod assembly;
pub mod audit_sinks;
pub mod bridge;
pub mod concurrency;
pub mod config;
pub mod deploy;
pub mod envref;
pub mod external_task;
pub mod migrate;
pub mod otel;
pub mod outbox;
pub mod rls_check;
pub mod server;
pub mod snapshot_values;
pub mod stores;
pub mod sweeper;
pub mod test_clock;
pub mod timer;
pub mod xslt;

/// The DB-lease leader election + intake adapter moved DOWN to `sutra-transport-spi`
/// (domain-neutrality refactor). Re-exported at the historical `sutra_engine::leadership`
/// path so callers (the timer election in `server`, the broker leadership IT) are unchanged.
pub use sutra_transport_spi::leadership;

pub use config::{DeploymentSourceKind, EngineConfig, EngineShardConfig};
pub use otel::{Telemetry, TelemetryConfig};
pub use server::{serve, RunningEngine};
pub use sweeper::{StuckInstanceScanner, StuckInstanceScannerConfig, INSTANCE_SWEEPER_ROLE};
pub use test_clock::{fast_forward_until, TestClock};
pub use timer::{spawn_timer_poller, TimerPollerConfig, TIMER_LEADER_ROLE};
