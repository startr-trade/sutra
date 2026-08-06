//! Store traits + PostgreSQL (sqlx) implementations for the persistence subsystems
//! (`deployment_id` everywhere, GUC `sutra.deployment_id`).
//!
//! Every deployment-scoped operation follows the same shape: open a transaction, set the
//! `sutra.deployment_id` GUC ([`crate::scope`]), run explicitly-bound SQL, commit. The
//! `*_in` associated functions run the same SQL on a caller-supplied connection — the
//! building blocks for the strict transactional step ([`crate::step`]) and for tests that
//! need to hold transactions open (row locks, SKIP LOCKED).

mod alias;
mod audit_event;
mod channel;
mod data_key;
mod dead_letter;
mod deployment_archive;
mod external_task;
mod inbox;
mod instance;
mod lease;
mod outbox;
mod subject_index;
mod timer_schedule;
mod wait_state;

pub use alias::{AliasRow, AliasStore, PgAliasStore};
pub use audit_event::{
    AuditEventRecord, AuditEventRow, PgAuditEventStore, AUDIT_HISTORY_PAGE_DEFAULT,
    AUDIT_HISTORY_PAGE_MAX,
};
pub use channel::{ChannelConcurrencyStore, PgChannelConcurrencyStore};
pub use data_key::PgDataKeyStore;
pub use dead_letter::{
    DeadLetterRecord, DeadLetterReplayPayload, DeadLetterRow, PgDeadLetterStore,
    DEAD_LETTER_PAGE_DEFAULT, DEAD_LETTER_PAGE_MAX,
};
pub use deployment_archive::{
    ActiveArchive, ArchiveStatus, ArchiveStatusRow, NewArchive, PgDeploymentArchiveStore,
    ServedArchiveRow,
};
pub use external_task::{ExternalTaskRow, ExternalTaskStore, PgExternalTaskStore};
pub use inbox::{InboxStore, PgInboxStore};
pub use instance::{
    InstanceFilter, InstanceState, InstanceStore, InstanceSummary, OwnedInstanceState,
    PgInstanceStore,
};
// Shared decode/status-filter helper for the dialect `list` impls (mysql/mssql).
pub(crate) use instance::summarise_instances;
pub use lease::{Lease, LeaseStore, PgLeaseStore};
pub use outbox::{OutboxEntry, OutboxStore, PgOutboxStore, ReplyMode};
pub use subject_index::{PgSubjectIndexStore, SubjectIndexStore};
pub use timer_schedule::{
    DueTimerSchedule, PgTimerScheduleStore, TimerScheduleArming, TimerScheduleRow,
    SCHEDULE_STATUS_RESOLVED, SCHEDULE_STATUS_SCHEDULED,
};
pub use wait_state::{
    DueTimer, InstanceWait, PgWaitStateStore, WaitStateStore, WaitingEvent, WaitingFilter,
    KIND_MESSAGE, KIND_TIMER, STATUS_RESOLVED, STATUS_WAITING,
};
