//! GCP Pub/Sub vendor transport — the `transport: gcp-pubsub` broker pair + its
//! consumer-lifecycle manager, EXTRACTED out of the neutral engine (domain-neutrality
//! refactor) into its own crate that self-registers a
//! [`sutra_transport_spi::TransportFactory`] via `inventory`. `sutra-engine` bundles it and
//! drives it through the neutral [`sutra_transport_spi::TransportChannels`] trait, never
//! naming Pub/Sub.
#![forbid(unsafe_code)]

pub mod gcp_pubsub;
mod manager;

pub use gcp_pubsub::*;
pub use manager::*;
