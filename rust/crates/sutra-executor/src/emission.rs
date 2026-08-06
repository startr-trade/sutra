//! Emission collection — the emission kinds (`<q:reply>` synchronous response,
//! `<q:send>` detached outbound). Here emissions are **collected, not delivered**: the
//! executor hands each one to an [`EmissionSink`]; transport / the durable outbox sits above.

use std::cell::RefCell;
use std::collections::BTreeMap;

use sutra_bpmn::qbindings::ReplyMode;

use crate::registry::AuthRef;

/// The emission kind (stream-frame emission is reserved, not yet built).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmissionKind {
    /// `<q:reply>` — single synchronous response on the originating connection.
    Reply,
    /// `<q:send>` — durable outbound emission, detached delivery.
    Send,
}

/// The CloudEvents attribute view built for non-native reply modes — the load-bearing
/// `CloudEvent` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudEventLite {
    pub id: String,
    pub source: String,
    pub spec_version: String,
    pub ce_type: String,
    pub subject: Option<String>,
    pub time: Option<String>,
    pub data_content_type: Option<String>,
}

/// One collected outbound emission (the `OutboundReply`/outbox-row analog).
#[derive(Debug, Clone, PartialEq)]
pub struct Emission {
    pub kind: EmissionKind,
    pub node_id: String,
    pub instance_id: String,
    pub mode: ReplyMode,
    pub destination: String,
    pub content_type: Option<String>,
    pub required: bool,
    /// The emission payload — wrapped [`Sensitive`] so a stray `{:?}` on an `Emission` masks it
    /// (a compile-time backstop; the value is `Deref`-transparent for reads, `into_inner()` at the
    /// wire-encode boundary).
    pub body: crate::Sensitive<Vec<u8>>,
    pub cloud_event: Option<CloudEventLite>,
    pub auth_ref: Option<AuthRef>,
    /// Author-declared `<q:header>` attributes, already FEEL-resolved against
    /// the sending process context. Carried through the outbox onto the wire as transport headers /
    /// broker application-properties (the traceparent / `sutra-outbox-key` seam). Empty for
    /// emissions with no `<q:header>` (channel-call requests, header-less sends/replies).
    pub headers: BTreeMap<String, String>,
}

impl Emission {
    pub fn body_utf8(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// Where emissions land — collected, not delivered (transport sits above).
pub trait EmissionSink {
    fn emit(&self, emission: Emission);
}

/// In-memory [`EmissionSink`] for tests (the `RecordingOutbox` analog).
#[derive(Debug, Default)]
pub struct CollectingSink {
    emissions: RefCell<Vec<Emission>>,
}

impl CollectingSink {
    pub fn new() -> CollectingSink {
        CollectingSink::default()
    }

    pub fn emissions(&self) -> Vec<Emission> {
        self.emissions.borrow().clone()
    }

    pub fn len(&self) -> usize {
        self.emissions.borrow().len()
    }

    pub fn is_empty(&self) -> bool {
        self.emissions.borrow().is_empty()
    }
}

impl EmissionSink for CollectingSink {
    fn emit(&self, emission: Emission) {
        self.emissions.borrow_mut().push(emission);
    }
}
