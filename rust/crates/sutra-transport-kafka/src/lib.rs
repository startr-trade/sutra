//! Kafka vendor transport — the `transport: kafka` broker pair + its consumer-lifecycle
//! manager, EXTRACTED out of the neutral engine (domain-neutrality refactor) into its own
//! crate that self-registers a [`sutra_transport_spi::TransportFactory`] via `inventory`.
//! `sutra-engine` bundles it (force-link in the binary) and drives it through the neutral
//! [`sutra_transport_spi::TransportChannels`] trait, never naming Kafka.
//!
//! - [`kafka`] — the moved-from-`sutra-channels` source/sink pair
//!   ([`kafka::KafkaTriggerSource`], [`kafka::KafkaMessageSink`], config, codes).
//! - [`manager`] (re-exported at the root) — [`KafkaChannels`] + [`spawn_kafka_channels`]
//!   + the sink registrar, plus the `TransportChannels`/`TransportFactory` wiring.
#![forbid(unsafe_code)]

pub mod kafka;
mod manager;

pub use kafka::*;
pub use manager::*;
