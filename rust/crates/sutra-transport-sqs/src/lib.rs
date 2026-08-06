//! AWS SQS vendor transport — the `transport: aws-sqs` broker pair + its consumer-lifecycle
//! manager, EXTRACTED out of the neutral engine (domain-neutrality refactor) into its own
//! crate that self-registers a [`sutra_transport_spi::TransportFactory`] via `inventory`.
//! `sutra-engine` bundles it and drives it through the neutral
//! [`sutra_transport_spi::TransportChannels`] trait, never naming SQS.
#![forbid(unsafe_code)]

mod manager;
pub mod sqs;

pub use manager::*;
pub use sqs::*;
