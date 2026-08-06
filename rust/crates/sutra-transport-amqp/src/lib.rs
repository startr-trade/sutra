//! AMQP 1.0 vendor transport — the `transport: amqp` broker pair + its consumer-lifecycle
//! manager, EXTRACTED out of the neutral engine (domain-neutrality refactor) into its own
//! crate that self-registers a [`sutra_transport_spi::TransportFactory`] via `inventory`.
//! `sutra-engine` bundles it and drives it through the neutral
//! [`sutra_transport_spi::TransportChannels`] trait, never naming the broker.
#![forbid(unsafe_code)]

pub mod amqp;
mod manager;

pub use amqp::*;
pub use manager::*;
