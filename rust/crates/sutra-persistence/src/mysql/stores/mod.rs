//! MySQL/MariaDB implementations of the store SPIs in [`crate::stores`].
//!
//! Every implementation satisfies THE SAME trait its reference counterpart does, so the
//! engine wires a dialect by constructing the matching store set — no behavioral switch
//! points above this layer. The `*_in` associated functions run on a caller-supplied
//! connection: the building blocks for the strict transactional step
//! ([`crate::mysql::step`]) and for tests that hold row locks open.

mod alias;
mod channel;
mod deployment_archive;
mod inbox;
mod instance;
mod lease;
mod outbox;
mod wait_state;

pub use alias::MySqlAliasStore;
pub use channel::MySqlChannelConcurrencyStore;
pub use deployment_archive::MySqlDeploymentArchiveStore;
pub use inbox::MySqlInboxStore;
pub use instance::MySqlInstanceStore;
pub use lease::MySqlLeaseStore;
pub use outbox::MySqlOutboxStore;
pub use wait_state::MySqlWaitStateStore;
