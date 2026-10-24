//! The inline-call seam — a SYNCHRONOUS outbound call (an outbound channel declaring
//! `ack-mode: on-complete`) whose response is merged into the flow without a durable park.
//!
//! Unlike a [`crate::sink::MessageSink`], which only reports whether a delivery was accepted,
//! an [`InlineCaller`] answers with the counterpart's response. The dispatcher drives the call
//! from the router side (never on the shard actor) and feeds the response back as an in-memory
//! resume, so the instance is persisted only when it durably parks or completes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::diag::Diagnostic;
use crate::sink::{scheme_of, BoxFuture, OutboundMessage};

/// The counterpart's answer to an inline call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineResponse {
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    /// Response headers / properties, available to the response's intake like a relay's headers.
    pub headers: BTreeMap<String, String>,
}

/// A failed inline call. `Retryable` is eligible for the task's `<q:retry>` policy;
/// `Permanent` goes straight to the task's failure path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InlineFailure {
    Retryable(Diagnostic),
    Permanent(Diagnostic),
}

/// One piece of a streamed call's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineChunk<'a> {
    /// The next piece of answer text.
    Text(&'a str),
    /// Everything streamed so far is void (the transport fell back to another counterpart
    /// after part of an answer); the pieces that follow start the answer over.
    Reset,
}

/// Receives a streamed call's answer as it arrives. Chunks are delivery-only: they are
/// forwarded to the waiting caller and never logged or audited — the complete answer the call
/// returns is what reaches the flow's variables.
pub type InlineChunkSink = Arc<dyn Fn(InlineChunk<'_>) + Send + Sync>;

/// One transport's inline (request/response) caller, keyed by the URI scheme(s) it serves.
pub trait InlineCaller: Send + Sync {
    /// The lowercase URI schemes this caller claims.
    fn schemes(&self) -> Vec<String>;

    /// Make one call. Must be infallible in the panic sense; the caller enforces no deadline
    /// of its own beyond its transport's — the dispatcher bounds the call by the task's
    /// `<q:timeout>`.
    fn call<'a>(
        &'a self,
        request: &'a OutboundMessage,
    ) -> BoxFuture<'a, Result<InlineResponse, InlineFailure>>;

    /// [`Self::call`], forwarding the answer to `on_chunk` piece by piece while it is produced
    /// (a model's token stream). The returned response is still the WHOLE answer. Default: a
    /// transport that cannot stream makes the plain call and emits no chunk.
    fn call_streaming<'a>(
        &'a self,
        request: &'a OutboundMessage,
        on_chunk: InlineChunkSink,
    ) -> BoxFuture<'a, Result<InlineResponse, InlineFailure>> {
        let _ = on_chunk;
        self.call(request)
    }
}

/// Scheme → inline caller resolution (mirrors [`crate::sink::SinkRegistry`]).
#[derive(Default, Clone)]
pub struct InlineCallerRegistry {
    callers: HashMap<String, Arc<dyn InlineCaller>>,
}

impl InlineCallerRegistry {
    pub fn new() -> InlineCallerRegistry {
        InlineCallerRegistry::default()
    }

    /// Register `caller` under every scheme it claims (lowercased); last registration wins.
    pub fn register(&mut self, caller: Arc<dyn InlineCaller>) -> &mut InlineCallerRegistry {
        for scheme in caller.schemes() {
            self.callers
                .insert(scheme.to_ascii_lowercase(), Arc::clone(&caller));
        }
        self
    }

    /// The caller for `destination`'s scheme, if any.
    pub fn resolve(&self, destination: &str) -> Option<Arc<dyn InlineCaller>> {
        scheme_of(destination).and_then(|s| self.callers.get(&s.to_ascii_lowercase()).cloned())
    }

    /// Every registered scheme, sorted.
    pub fn schemes(&self) -> Vec<String> {
        let mut schemes: Vec<String> = self.callers.keys().cloned().collect();
        schemes.sort_unstable();
        schemes
    }
}
