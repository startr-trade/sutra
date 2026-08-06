//! SQL Server implementations of the store SPIs in [`crate::stores`].
//!
//! Every implementation satisfies THE SAME trait its reference counterpart does, so the
//! engine wires a dialect by constructing the matching store set. The `*_in` associated
//! functions run on a caller-supplied connection (an open [`crate::mssql::MssqlTx`]'s
//! client): the building blocks for the strict transactional step
//! ([`crate::mssql::step`]) and for tests that hold row locks open.

mod alias;
mod channel;
mod deployment_archive;
mod inbox;
mod instance;
mod lease;
mod outbox;
mod wait_state;

pub use alias::MssqlAliasStore;
pub use channel::MssqlChannelConcurrencyStore;
pub use deployment_archive::MssqlDeploymentArchiveStore;
pub use inbox::MssqlInboxStore;
pub use instance::MssqlInstanceStore;
pub use lease::MssqlLeaseStore;
pub use outbox::MssqlOutboxStore;
pub use wait_state::MssqlWaitStateStore;
