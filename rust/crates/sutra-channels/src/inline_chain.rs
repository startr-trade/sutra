//! The in-flight half of a SYNCHRONOUS outbound-call chain — what crosses between the shard
//! actor and the router-side task that makes the calls.
//!
//! A start whose pass parks only at calls on synchronous outbound channels does not commit a
//! park. The dispatcher answers [`crate::DispatchOutcome::InlineCalls`] instead, carrying the
//! in-memory instance and the requests; the router side of [`crate::http::EngineHandle`] makes
//! the calls concurrently (never on the actor, which must not spawn request handling), each
//! bounded by its `<q:timeout>`, and posts the answers back to the lane that started the chain
//! one at a time. Each answer resumes the instance in memory; the chain ends at a durable park,
//! a completion, or a failure — so nothing of it is persisted until then, and a fully
//! synchronous chain writes nothing at all.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::diag::Diagnostic;
use crate::dispatch::{DispatchOutcome, InlineChain};
use crate::inline::{InlineCaller, InlineFailure, InlineResponse};
use crate::sink::OutboundMessage;

/// One call the router side must make.
pub(crate) struct InlineCall {
    /// The channel-call task the call answers.
    pub(crate) node_id: String,
    pub(crate) request: OutboundMessage,
    pub(crate) caller: Arc<dyn InlineCaller>,
    /// The call's whole time budget — its `<q:timeout>`. Every attempt and every backoff
    /// between attempts must fit inside it.
    pub(crate) budget: Duration,
    /// The task's `<q:retry>` policy, applied INLINE: a retryable failure re-issues the call
    /// within the budget instead of parking a durable backoff.
    pub(crate) retry: Option<InlineRetry>,
    /// `q:stream="true"` on a chain whose delivery asked for a streamed reply
    /// (`reply-stream: sse`): the answer is forwarded to the caller as it arrives.
    pub(crate) stream: bool,
}

/// One frame of a streamed (server-sent events) response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamFrame {
    /// The first streamed call started: the response switches to `text/event-stream`. Not an
    /// event on the wire.
    Started,
    /// A piece of a streamed call's answer.
    Chunk(String),
    /// The pieces sent so far are void: the call started its answer over (a fallback).
    Reset,
    /// The flow's final reply, once the chain ended.
    Reply(crate::dispatch::SyncReply),
    /// The chain failed: the diagnostic code only — never a message that could carry content.
    Error(String),
}

/// Where a chain's stream goes, and whether it has started.
pub(crate) struct StreamTap {
    frames: tokio::sync::mpsc::UnboundedSender<StreamFrame>,
    started: std::sync::atomic::AtomicBool,
}

impl StreamTap {
    pub(crate) fn new(frames: tokio::sync::mpsc::UnboundedSender<StreamFrame>) -> StreamTap {
        StreamTap {
            frames,
            started: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub(crate) fn started(&self) -> bool {
        self.started.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn start(&self) {
        if !self.started.swap(true, std::sync::atomic::Ordering::SeqCst) {
            let _ = self.frames.send(StreamFrame::Started);
        }
    }

    pub(crate) fn send(&self, frame: StreamFrame) {
        let _ = self.frames.send(frame);
    }
}

/// A `<q:retry>` policy, resolved to durations once on the actor.
#[derive(Debug, Clone)]
pub(crate) struct InlineRetry {
    pub(crate) max_attempts: u32,
    pub(crate) initial_delay: Duration,
    pub(crate) backoff_coefficient: f64,
    pub(crate) max_delay: Duration,
    pub(crate) non_retryable_codes: Vec<String>,
}

impl InlineRetry {
    /// The wait after the `failed`-th failed attempt: `initialDelay × coefficient^(failed-1)`,
    /// clamped at `maxDelay` — the same curve the durable retry uses.
    fn delay_after(&self, failed: u32) -> Duration {
        let grown = self.initial_delay.as_secs_f64()
            * self
                .backoff_coefficient
                .powi(failed.saturating_sub(1) as i32);
        if grown.is_finite() && grown >= 0.0 {
            Duration::from_secs_f64(grown).min(self.max_delay)
        } else {
            self.max_delay
        }
    }
}

/// What one call came back with, after any inline retries.
#[derive(Debug)]
pub(crate) enum InlineCallResult {
    Answered(InlineResponse),
    /// The budget ran out with an attempt still unanswered.
    TimedOut,
    /// The counterpart failed: permanently, or retryably with the policy (or budget) spent.
    Failed(InlineFailure),
}

/// The chain as the actor hands it to the router side: the in-memory instance plus the
/// calls its last pass issued.
pub(crate) struct InlineContinuation {
    pub(crate) chain: Box<InlineChain>,
    pub(crate) calls: Vec<InlineCall>,
    /// For a chain driving a STORED instance: the claim's key `(deployment, instance)`, which
    /// the router side keeps fresh (a re-claim by the same owner restamps the heartbeat) for
    /// as long as the calls take.
    pub(crate) heartbeat: Option<(crate::DeploymentId, String)>,
}

/// The opaque payload of [`crate::DispatchOutcome::InlineCalls`]. Taken exactly once by the
/// router side; the shared cell keeps [`crate::DispatchOutcome`] `Clone` even though the chain
/// it carries (settle callbacks included) cannot be cloned.
#[derive(Clone)]
pub struct InlineHandoff(Arc<Mutex<Option<InlineContinuation>>>);

impl InlineHandoff {
    pub(crate) fn new(continuation: InlineContinuation) -> InlineHandoff {
        InlineHandoff(Arc::new(Mutex::new(Some(continuation))))
    }

    pub(crate) fn take(&self) -> Option<InlineContinuation> {
        self.0.lock().ok().and_then(|mut slot| slot.take())
    }
}

// Identity, not content: two handoffs are equal only when they are the same chain.
impl PartialEq for InlineHandoff {
    fn eq(&self, other: &InlineHandoff) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for InlineHandoff {}

impl std::fmt::Debug for InlineHandoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InlineHandoff(..)")
    }
}

/// The actor's answer to one in-memory resume.
pub(crate) enum InlineStep {
    /// The chain goes on: the instance is still waiting on calls in flight, plus any `calls`
    /// this pass issued.
    More {
        chain: Box<InlineChain>,
        calls: Vec<InlineCall>,
    },
    /// The chain ended. `settle_consumed`: an `ack-mode: on-complete` delivery's settle
    /// callbacks were registered with the durable park, so the transport must not settle now.
    Done {
        outcome: Result<DispatchOutcome, Diagnostic>,
        settle_consumed: bool,
        /// The instance finished (rather than parked) — what a timer fire reports.
        completed: bool,
    },
}

// Everything here crosses from the actor to a router-side task and back.
const _: () = {
    const fn assert_send<T: Send>() {}
    assert_send::<InlineContinuation>();
    assert_send::<InlineStep>();
    assert_send::<InlineCallResult>();
};

/// Make one call to completion: the attempt, any inline retries with the policy's backoff,
/// all inside the call's budget. A panicking caller is reported as a permanent failure rather
/// than tearing down the chain's task.
pub(crate) async fn run_inline_call(
    call: InlineCall,
    tap: Option<Arc<StreamTap>>,
) -> (String, InlineCallResult) {
    let InlineCall {
        node_id,
        request,
        caller,
        budget,
        retry,
        stream,
    } = call;
    // A streamed call forwards its pieces to the caller's response as they arrive.
    let on_chunk: Option<crate::inline::InlineChunkSink> = match (stream, tap) {
        (true, Some(tap)) => {
            tap.start();
            Some(Arc::new(move |chunk: crate::inline::InlineChunk<'_>| {
                tap.send(match chunk {
                    crate::inline::InlineChunk::Text(text) => StreamFrame::Chunk(text.to_string()),
                    crate::inline::InlineChunk::Reset => StreamFrame::Reset,
                })
            }))
        }
        _ => None,
    };
    let deadline = tokio::time::Instant::now() + budget;
    let mut failed_attempts = 0u32;
    loop {
        let answer = match &on_chunk {
            Some(sink) => caller.call_streaming(&request, Arc::clone(sink)),
            None => caller.call(&request),
        };
        let attempt =
            crate::dispatch::catch_unwind_completion(tokio::time::timeout_at(deadline, answer))
                .await;
        let diagnostic = match attempt {
            Ok(Ok(Ok(response))) => return (node_id, InlineCallResult::Answered(response)),
            Ok(Err(_elapsed)) => return (node_id, InlineCallResult::TimedOut),
            Ok(Ok(Err(permanent @ InlineFailure::Permanent(_)))) => {
                return (node_id, InlineCallResult::Failed(permanent))
            }
            Ok(Ok(Err(InlineFailure::Retryable(diagnostic)))) => diagnostic,
            Err(_panic) => {
                return (
                    node_id.clone(),
                    InlineCallResult::Failed(InlineFailure::Permanent(Diagnostic::error(
                        crate::codes::RUNTIME_UNEXPECTED,
                        format!("the inline caller for channel-call task '{node_id}' panicked"),
                    ))),
                )
            }
        };
        failed_attempts += 1;
        let give_up = |d: Diagnostic| {
            (
                node_id.clone(),
                InlineCallResult::Failed(InlineFailure::Retryable(d)),
            )
        };
        let Some(policy) = &retry else {
            return give_up(diagnostic);
        };
        if failed_attempts >= policy.max_attempts
            || policy
                .non_retryable_codes
                .iter()
                .any(|c| c == &diagnostic.code)
        {
            return give_up(diagnostic);
        }
        let delay = policy.delay_after(failed_attempts);
        // A backoff that cannot end before the deadline would only wait out the budget and
        // then time out; the policy is spent as far as this call is concerned.
        if tokio::time::Instant::now() + delay >= deadline {
            return give_up(diagnostic);
        }
        tokio::time::sleep(delay).await;
    }
}
