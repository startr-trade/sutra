//! File-spool vendor transport for AIR-GAPPED deployments — the `transport: file` source/sink
//! pair + its consumer-lifecycle manager, behind the neutral
//! [`sutra_transport_spi::TransportChannels`] SPI exactly like every other vendor transport.
//! It self-registers a [`sutra_transport_spi::TransportFactory`] via `inventory`; `sutra-engine`
//! bundles it (force-link in the binary) and drives it GENERICALLY, never naming files.
//!
//! Unlike the broker transports it has NO network dependency: an inbound channel watches a
//! spool DIRECTORY and each file that lands there is one delivery; an outbound `file://`
//! destination writes a file. Nothing dials out — the whole crate touches only `std`/`tokio`
//! `fs`. This is the transport an isolated / air-gapped site drives its integration through.
//!
//! - [`file`] — the source/sink pair ([`file::FileTriggerSource`], [`file::FileMessageSink`],
//!   [`file::FileChannelProperties`], codes).
//! - [`manager`] (re-exported at the root) — [`FileChannels`] + [`spawn_file_channels`] + the
//!   sink registrar, plus the `TransportChannels`/`TransportFactory` wiring.
#![forbid(unsafe_code)]

pub mod file;
mod manager;

pub use file::*;
pub use manager::*;
