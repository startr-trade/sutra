//! The `llm` outbound sink: candidate routing for one call, then reply delivery back into
//! the engine as an inbound message on the channel's reply channel.

use std::sync::Arc;

use serde_json::{json, Value};
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::http::EngineHandle;
use sutra_channels::{BoxFuture, Diagnostic, MessageSink, OutboundMessage, SendOutcome};
use sutra_llm_spi::{
    codes, LlmCapabilities, LlmFailure, LlmJobRef, LlmRequest, LlmResponse, RetryableClass,
};
use tracing::field::Empty;
use tracing::Instrument;

use crate::config::{ChannelMode, ChannelTable, LlmCandidate, LlmChannel};
use crate::job_store::{JobStoreSlot, NewLlmJob};
use crate::request::{parse_destination, parse_request_body, CallRequest};
use crate::TRANSPORT;

/// Header naming the provider that answered.
pub const PROVIDER_HEADER: &str = "x-llm-provider";
/// Header naming the model that answered.
pub const MODEL_HEADER: &str = "x-llm-model";

/// Where an answer is delivered: the engine in production, a capture in tests.
pub trait ReplyPort: Send + Sync {
    fn deliver<'a>(&'a self, message: InboundMessage) -> BoxFuture<'a, Result<(), Diagnostic>>;
}

impl ReplyPort for EngineHandle {
    fn deliver<'a>(&'a self, message: InboundMessage) -> BoxFuture<'a, Result<(), Diagnostic>> {
        Box::pin(async move { self.dispatch(message).await.map(|_| ()) })
    }
}

/// The engine actor is gone (shutdown / restart) — a later redelivery can succeed.
pub(crate) fn is_engine_unavailable(diagnostic: &Diagnostic) -> bool {
    diagnostic.code == sutra_channels::codes::RUNTIME_UNEXPECTED
        && diagnostic.message == "engine actor is not running"
}

/// How a routed call ended without an answer.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteFailure {
    /// No candidate has what the channel and the request need.
    NoEligibleCandidate,
    /// The last candidate tried failed with this.
    Failed {
        provider: String,
        model: String,
        failure: LlmFailure,
    },
}

/// The answering candidate and its response.
pub struct RoutedAnswer<'c> {
    pub candidate: &'c LlmCandidate,
    pub response: LlmResponse,
}

fn build_request(channel: &LlmChannel, call: &CallRequest, model: &str) -> LlmRequest {
    LlmRequest {
        model: model.to_string(),
        system: call.system.clone().or_else(|| channel.system.clone()),
        messages: call.messages.clone(),
        tools: call.tools.clone(),
        json_output: call.json_output,
        max_output_tokens: call.max_output_tokens.or(channel.max_output_tokens),
        temperature: call.temperature.or(channel.temperature),
        deadline: channel.timeout,
    }
}

/// A job a candidate accepted.
pub struct SubmittedJob<'c> {
    pub index: usize,
    pub candidate: &'c LlmCandidate,
    pub job: LlmJobRef,
}

fn needed(channel: &LlmChannel, call: &CallRequest) -> LlmCapabilities {
    LlmCapabilities {
        tools: channel.require.tools || !call.tools.is_empty(),
        json_output: channel.require.json_output || call.json_output,
        ..LlmCapabilities::default()
    }
}

/// Try the channel's candidates in order with `op`. A candidate lacking a capability in
/// `need` is skipped; a retryable failure whose class is in `fallback-on` moves to the next;
/// any other failure stops. Each attempt is bounded by the channel deadline.
async fn try_candidates<'c, T, F>(
    channel: &'c LlmChannel,
    call: &CallRequest,
    need: LlmCapabilities,
    op: F,
) -> Result<(usize, &'c LlmCandidate, T), RouteFailure>
where
    F: for<'a> Fn(&'a LlmCandidate, &'a LlmRequest) -> BoxFuture<'a, Result<T, LlmFailure>>,
{
    let span = tracing::Span::current();
    let mut last = RouteFailure::NoEligibleCandidate;
    for (index, candidate) in channel.candidates.iter().enumerate() {
        if !candidate.satisfies(need) {
            continue;
        }
        span.record("candidate", index);
        span.record("provider", candidate.provider.as_str());
        span.record("model", candidate.model());
        let request = build_request(channel, call, candidate.model());
        let outcome = match tokio::time::timeout(channel.timeout, op(candidate, &request)).await {
            Ok(outcome) => outcome,
            Err(_) => Err(LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                retry_after: None,
                detail: "the provider did not answer before the deadline".into(),
            }),
        };
        match outcome {
            Ok(value) => return Ok((index, candidate, value)),
            Err(failure) => {
                let fall_back = matches!(
                    &failure,
                    LlmFailure::Retryable { class, .. } if channel.fallback_on.contains(class)
                );
                if let LlmFailure::Retryable { class, .. } = &failure {
                    tracing::warn!(
                        channel = %channel.name,
                        provider = %candidate.provider,
                        model = %candidate.model(),
                        class = class.as_str(),
                        fall_back,
                        "llm candidate failed"
                    );
                }
                last = RouteFailure::Failed {
                    provider: candidate.provider.clone(),
                    model: candidate.model().to_string(),
                    failure,
                };
                if !fall_back {
                    break;
                }
            }
        }
    }
    Err(last)
}

/// Complete the call on the first candidate that answers (see [`try_candidates`]).
pub async fn route<'c>(
    channel: &'c LlmChannel,
    call: &CallRequest,
) -> Result<RoutedAnswer<'c>, RouteFailure> {
    let (_, candidate, response) = try_candidates(channel, call, needed(channel, call), |c, r| {
        c.client.complete(r)
    })
    .await?;
    Ok(RoutedAnswer {
        candidate,
        response,
    })
}

/// [`route`], streaming the answer to `on_chunk` as the candidate produces it. A candidate that
/// fails after streaming part of an answer falls back like any other failure; the next
/// candidate's first piece is then preceded by a reset, so the caller discards the partial
/// answer.
pub async fn route_streaming<'c>(
    channel: &'c LlmChannel,
    call: &CallRequest,
    on_chunk: sutra_channels::InlineChunkSink,
) -> Result<RoutedAnswer<'c>, RouteFailure> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use sutra_channels::InlineChunk;
    // Whether the caller holds pieces of an answer a later candidate must void.
    let streamed = std::sync::Arc::new(AtomicBool::new(false));
    let (_, candidate, response) =
        try_candidates(channel, call, needed(channel, call), move |c, r| {
            let sink = std::sync::Arc::clone(&on_chunk);
            let streamed = std::sync::Arc::clone(&streamed);
            Box::pin(async move {
                // A fresh candidate: pieces sent by one that failed are void.
                let reset = AtomicBool::new(streamed.load(Ordering::SeqCst));
                let forward = |text: &str| {
                    if reset.swap(false, Ordering::SeqCst) {
                        sink(InlineChunk::Reset);
                    }
                    streamed.store(true, Ordering::SeqCst);
                    sink(InlineChunk::Text(text));
                };
                c.client.complete_streaming(r, &forward).await
            })
        })
        .await?;
    Ok(RoutedAnswer {
        candidate,
        response,
    })
}

/// Submit the call as a job on the first candidate with async jobs that accepts it, with the
/// same fallback rules as [`route`].
pub async fn route_submit<'c>(
    channel: &'c LlmChannel,
    call: &CallRequest,
) -> Result<SubmittedJob<'c>, RouteFailure> {
    let need = LlmCapabilities {
        async_jobs: true,
        ..needed(channel, call)
    };
    let (index, candidate, job) =
        try_candidates(channel, call, need, |c, r| c.client.submit(r)).await?;
    Ok(SubmittedJob {
        index,
        candidate,
        job,
    })
}

/// Map a routing failure to the delivery outcome: a retryable last failure defers the row,
/// anything else poisons it. The diagnostic names channel/provider/model, never content.
pub fn failure_outcome(channel: &LlmChannel, failure: RouteFailure) -> SendOutcome {
    match failure {
        RouteFailure::NoEligibleCandidate => SendOutcome::PermanentFailure(
            Diagnostic::error(
                codes::CALL_FAILED,
                format!(
                    "llm channel '{}': no candidate has the capabilities this call needs",
                    channel.name
                ),
            )
            .with_attribute("channel", channel.name.clone()),
        ),
        RouteFailure::Failed {
            provider,
            model,
            failure,
        } => {
            let diagnostic = Diagnostic::error(
                codes::CALL_FAILED,
                format!(
                    "llm channel '{}': every candidate failed; last ({provider}/{model}): {}",
                    channel.name,
                    failure.detail()
                ),
            )
            .with_attribute("channel", channel.name.clone())
            .with_attribute("provider", provider)
            .with_attribute("model", model);
            match failure {
                LlmFailure::Retryable { class, .. } => SendOutcome::RetryableFailure(
                    diagnostic.with_attribute("class", class.as_str()),
                ),
                LlmFailure::Permanent { .. } => SendOutcome::PermanentFailure(diagnostic),
            }
        }
    }
}

/// The reply body the process receives on the reply channel.
pub fn reply_body(candidate: &LlmCandidate, response: &LlmResponse, request: &Value) -> Value {
    let tool_calls: Vec<Value> = response
        .tool_calls
        .iter()
        .map(|c| json!({"id": c.id, "name": c.name, "arguments": c.arguments}))
        .collect();
    json!({
        "text": response.text,
        "toolCalls": tool_calls,
        "finish": response.finish.as_str(),
        "usage": {
            "inputTokens": response.usage.input_tokens,
            "outputTokens": response.usage.output_tokens,
        },
        "provider": candidate.provider,
        "model": candidate.model(),
        "providerRequestId": response.provider_request_id,
        "request": request,
    })
}

/// Build the reply [`InboundMessage`] for `message`: addressed to the reply channel of the
/// same deployment, carrying the request headers plus the answering provider/model and the
/// restored `traceparent`, deduplicated on the row's `outbox_key`.
pub fn build_reply(
    message: &OutboundMessage,
    tenant: &str,
    channel: &LlmChannel,
    candidate: &LlmCandidate,
    response: &LlmResponse,
    request: &Value,
) -> InboundMessage {
    let mut headers = message.headers.clone();
    headers.insert(PROVIDER_HEADER.to_string(), candidate.provider.clone());
    headers.insert(MODEL_HEADER.to_string(), candidate.model().to_string());
    if let Some(traceparent) = message
        .traceparent
        .as_deref()
        .map(str::trim)
        .filter(|t| !t.is_empty())
    {
        headers.insert(
            sutra_channels::telemetry::TRACEPARENT_HEADER.to_string(),
            traceparent.to_string(),
        );
    }
    let body = serde_json::to_vec(&reply_body(candidate, response, request)).unwrap_or_default();
    InboundMessage {
        tenant: tenant.to_string(),
        module_key: channel.module_key.clone(),
        channel: channel.reply_channel.clone(),
        headers,
        body: body.into(),
        content_type: Some("application/json".to_string()),
        idempotency_key: message.outbox_key.clone(),
        explicit_event_id: true,
        received_at: now_rfc3339(),
        cloud_event: None,
    }
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

/// The `llm` scheme sink: serves calls from a [`ChannelTable`] and delivers answers
/// through a [`ReplyPort`].
pub struct LlmSink {
    table: Arc<ChannelTable>,
    port: Arc<dyn ReplyPort>,
    jobs: Arc<JobStoreSlot>,
}

impl LlmSink {
    pub fn new(table: Arc<ChannelTable>, port: Arc<dyn ReplyPort>) -> LlmSink {
        LlmSink {
            table,
            port,
            jobs: Arc::new(JobStoreSlot::new()),
        }
    }

    /// Record `mode: job` submissions through `jobs`.
    pub fn with_jobs(mut self, jobs: Arc<JobStoreSlot>) -> LlmSink {
        self.jobs = jobs;
        self
    }

    async fn submit(
        &self,
        message: &OutboundMessage,
        tenant: &str,
        channel: &LlmChannel,
        call: &CallRequest,
    ) -> SendOutcome {
        let unrecorded = |detail: String| {
            SendOutcome::RetryableFailure(
                Diagnostic::error(
                    codes::CALL_FAILED,
                    format!("llm channel '{}': {detail}", channel.name),
                )
                .with_attribute("channel", channel.name.clone()),
            )
        };
        let Some(store) = self.jobs.get() else {
            return unrecorded(
                "job mode needs the engine database, which is not configured".into(),
            );
        };
        let span = tracing::info_span!(
            "sutra.llm.submit",
            channel = %channel.name,
            provider = Empty,
            model = Empty,
            candidate = Empty,
        );
        let submitted = match route_submit(channel, call).instrument(span).await {
            Ok(submitted) => submitted,
            Err(failure) => return failure_outcome(channel, failure),
        };
        let job = NewLlmJob {
            deployment: channel.deployment.clone(),
            provider: submitted.candidate.provider.clone(),
            provider_job_id: submitted.job.id.clone(),
            channel: channel.name.clone(),
            candidate_index: submitted.index,
            tenant: tenant.to_string(),
            module_key: channel.module_key.clone(),
            reply_channel: channel.reply_channel.clone(),
            request_body: message.body.clone(),
            headers: message.headers.clone(),
            outbox_key: message.outbox_key.clone(),
            traceparent: message.traceparent.clone(),
            poll_interval: submitted.candidate.job.poll_interval,
            job_timeout: submitted.candidate.job.job_timeout,
        };
        match store.insert(&job).await {
            Ok(_) => SendOutcome::Delivered,
            // The outbox retries, which submits again: the documented cost of a lost insert.
            Err(error) => unrecorded(format!("the submitted job could not be recorded: {error}")),
        }
    }

    async fn serve(&self, message: &OutboundMessage) -> SendOutcome {
        let Some((tenant, module_key, channel_name)) = parse_destination(&message.destination)
        else {
            return SendOutcome::PermanentFailure(Diagnostic::error(
                codes::REQUEST_INVALID,
                format!(
                    "llm destination '{}' is not an 'llm://<module_key>/<channel>' URI",
                    message.destination
                ),
            ));
        };
        let Some(channel) = self.table.get(&module_key, &channel_name) else {
            // The definition may be mid-activation; a later tick finds it.
            return SendOutcome::RetryableFailure(
                Diagnostic::error(
                    codes::CONFIG_INVALID,
                    format!("no active llm channel '{channel_name}' in '{module_key}'"),
                )
                .with_attribute("channel", channel_name),
            );
        };
        let call = match parse_request_body(&message.body) {
            Ok(call) => call,
            Err(diagnostic) => {
                return SendOutcome::PermanentFailure(
                    diagnostic.with_attribute("channel", channel.name.clone()),
                )
            }
        };

        if channel.mode == ChannelMode::Job {
            return self.submit(message, &tenant, &channel, &call).await;
        }

        let permit =
            match tokio::time::timeout(channel.timeout, channel.permits().clone().acquire_owned())
                .await
            {
                Ok(Ok(permit)) => permit,
                _ => {
                    return SendOutcome::RetryableFailure(
                        Diagnostic::error(
                            codes::CALL_FAILED,
                            format!(
                                "llm channel '{}': concurrency limit {} still reached at the \
                                 deadline",
                                channel.name, channel.concurrency
                            ),
                        )
                        .with_attribute("channel", channel.name.clone()),
                    )
                }
            };

        let span = tracing::info_span!(
            "sutra.llm.call",
            channel = %channel.name,
            provider = Empty,
            model = Empty,
            finish = Empty,
            input_tokens = Empty,
            output_tokens = Empty,
            candidate = Empty,
        );
        let routed = route(&channel, &call).instrument(span.clone()).await;
        drop(permit);
        let answer = match routed {
            Ok(answer) => answer,
            Err(failure) => return failure_outcome(&channel, failure),
        };
        span.record("finish", answer.response.finish.as_str());
        span.record("input_tokens", answer.response.usage.input_tokens);
        span.record("output_tokens", answer.response.usage.output_tokens);

        let reply = build_reply(
            message,
            &tenant,
            &channel,
            answer.candidate,
            &answer.response,
            &call.raw,
        );
        match self.port.deliver(reply).await {
            Ok(()) => SendOutcome::Delivered,
            Err(diagnostic) if is_engine_unavailable(&diagnostic) => {
                SendOutcome::RetryableFailure(diagnostic)
            }
            Err(diagnostic) => SendOutcome::PermanentFailure(diagnostic),
        }
    }
}

impl MessageSink for LlmSink {
    fn schemes(&self) -> Vec<String> {
        vec![TRANSPORT.to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(self.serve(message))
    }
}
