//! The `llm` outbound sink: candidate routing for one call, then reply delivery back into
//! the engine as an inbound message on the channel's reply channel.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use serde_json::{json, Value};
use sutra_channels::dispatch::InboundMessage;
use sutra_channels::http::EngineHandle;
use sutra_channels::{BoxFuture, Diagnostic, MessageSink, OutboundMessage, SendOutcome};
use sutra_llm_spi::audio::{adapt, transcode};
use sutra_llm_spi::{
    codes, AudioFormat, FinishReason, LlmAudio, LlmCapabilities, LlmFailure, LlmJobRef, LlmRequest,
    LlmResponse, LlmSpeechRequest, LlmTranscriptionRequest, RetryableClass,
};
use tracing::field::Empty;
use tracing::Instrument;

use crate::config::{ChannelMode, ChannelTable, LlmCandidate, LlmChannel, SpeechStage, VoiceMode};
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
    /// The call cannot be served as asked (audio the transport cannot convert, media on a job
    /// channel). Never retried.
    Rejected { detail: String },
}

/// The answering candidate and its response.
pub struct RoutedAnswer<'c> {
    pub candidate: &'c LlmCandidate,
    /// With a spoken answer, its audio is already in the format the call asked for.
    pub response: LlmResponse,
    /// For a call involving audio: whether the model heard / spoke the audio itself (`true`) or
    /// the `voice: pipeline` transcribed and synthesised it (`false`). `None` for a text call.
    pub audio_preserved: Option<bool>,
}

fn audio_failure(detail: String) -> LlmFailure {
    LlmFailure::Permanent {
        detail: format!("{}: {detail}", codes::AUDIO_UNSUPPORTED),
    }
}

/// The format to ask a provider for so `wanted` can be delivered: `wanted` itself when listed,
/// else the first listed format this crate can convert from.
fn provider_output_format(wanted: AudioFormat, offered: &[AudioFormat]) -> Option<AudioFormat> {
    if offered.contains(&wanted) {
        return Some(wanted);
    }
    if !wanted.is_decodable() {
        return None;
    }
    offered.iter().copied().find(AudioFormat::is_decodable)
}

/// Convert a provider's spoken answer into the format the call asked for.
fn deliver_audio(audio: LlmAudio, wanted: AudioFormat) -> Result<LlmAudio, LlmFailure> {
    if audio.format == wanted {
        return Ok(audio);
    }
    let data = transcode(&audio.data, audio.format, wanted)
        .map_err(|e| audio_failure(format!("the spoken answer could not be converted: {e}")))?;
    Ok(LlmAudio {
        data,
        format: wanted,
    })
}

/// The provider request for `candidate`: each message's audio converted to a format the
/// provider takes, and a spoken answer asked for in a format the transport can deliver.
fn build_request(
    channel: &LlmChannel,
    call: &CallRequest,
    candidate: &LlmCandidate,
) -> Result<LlmRequest, LlmFailure> {
    let mut messages = call.messages.clone();
    if call.has_audio_input() {
        let accepted = candidate.client.audio_input_formats();
        for message in &mut messages {
            if let Some(audio) = &message.audio {
                let adapted = adapt(audio, &accepted).map_err(|e| {
                    audio_failure(format!("the request's audio cannot be sent: {e}"))
                })?;
                message.audio = Some(adapted);
            }
        }
    }
    let audio_output = match call.audio_output {
        None => None,
        Some(wanted) => Some(
            provider_output_format(wanted, &candidate.client.audio_output_formats()).ok_or_else(
                || {
                    audio_failure(format!(
                        "no format the provider speaks converts to {wanted}"
                    ))
                },
            )?,
        ),
    };
    Ok(LlmRequest {
        model: candidate.model().to_string(),
        system: call.system.clone().or_else(|| channel.system.clone()),
        messages,
        tools: call.tools.clone(),
        json_output: call.json_output,
        max_output_tokens: call.max_output_tokens.or(channel.max_output_tokens),
        temperature: call.temperature.or(channel.temperature),
        deadline: channel.timeout,
        audio_output,
        output_voice: call
            .output_voice
            .clone()
            .or_else(|| channel.output_voice.clone()),
    })
}

/// A job a candidate accepted.
pub struct SubmittedJob<'c> {
    pub index: usize,
    pub candidate: &'c LlmCandidate,
    pub job: LlmJobRef,
}

/// What a candidate must have to serve `call` on `channel`: the channel's `require:` plus what
/// the request uses. Audio and video are never dropped, so a candidate that cannot take them is
/// skipped rather than sent a text-only version of the call.
fn needed(channel: &LlmChannel, call: &CallRequest) -> LlmCapabilities {
    let mut need = channel.require;
    need.tools |= !call.tools.is_empty();
    need.json_output |= call.json_output;
    need.audio_input |= call.has_audio_input();
    need.audio_output |= call.audio_output.is_some();
    need.video_understanding |= call.has_video_input();
    need
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
        let outcome = match build_request(channel, call, candidate) {
            Err(failure) => Err(failure),
            Ok(request) => {
                match tokio::time::timeout(channel.timeout, op(candidate, &request)).await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(LlmFailure::Retryable {
                        class: RetryableClass::Timeout,
                        retry_after: None,
                        detail: "the provider did not answer before the deadline".into(),
                    }),
                }
            }
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

/// Complete the call on the first candidate that answers (see [`try_candidates`]). With
/// `voice: pipeline`, the audio is transcribed first and the answer spoken after (see
/// [`route_pipeline`]).
pub async fn route<'c>(
    channel: &'c LlmChannel,
    call: &CallRequest,
) -> Result<RoutedAnswer<'c>, RouteFailure> {
    if let Some(stage) = pipeline_stage(channel, call) {
        return route_pipeline(channel, stage, call, None).await;
    }
    route_native(channel, call).await
}

async fn route_native<'c>(
    channel: &'c LlmChannel,
    call: &CallRequest,
) -> Result<RoutedAnswer<'c>, RouteFailure> {
    let (_, candidate, response) = try_candidates(channel, call, needed(channel, call), |c, r| {
        c.client.complete(r)
    })
    .await?;
    native_answer(call, candidate, response)
}

/// The speech stage, when this call goes through it (a pipeline channel and audio involved).
fn pipeline_stage<'c>(channel: &'c LlmChannel, call: &CallRequest) -> Option<&'c SpeechStage> {
    match channel.voice {
        VoiceMode::Pipeline if call.involves_audio() => channel.speech.as_ref(),
        _ => None,
    }
}

/// Finish a native answer: the spoken answer converted to the format the call asked for.
fn native_answer<'c>(
    call: &CallRequest,
    candidate: &'c LlmCandidate,
    mut response: LlmResponse,
) -> Result<RoutedAnswer<'c>, RouteFailure> {
    if let Some(wanted) = call.audio_output {
        let audio = response.audio.take().ok_or_else(|| RouteFailure::Failed {
            provider: candidate.provider.clone(),
            model: candidate.model().to_string(),
            failure: LlmFailure::Permanent {
                detail: format!(
                    "{}: a spoken answer was asked for and none came back",
                    codes::RESPONSE_INVALID
                ),
            },
        })?;
        response.audio =
            Some(
                deliver_audio(audio, wanted).map_err(|failure| RouteFailure::Failed {
                    provider: candidate.provider.clone(),
                    model: candidate.model().to_string(),
                    failure,
                })?,
            );
    }
    Ok(RoutedAnswer {
        candidate,
        response,
        audio_preserved: call.involves_audio().then_some(true),
    })
}

fn stage_failure(stage: &SpeechStage, model: &str, failure: LlmFailure) -> RouteFailure {
    RouteFailure::Failed {
        provider: stage.provider.clone(),
        model: model.to_string(),
        failure,
    }
}

async fn bounded<T>(
    deadline: std::time::Duration,
    work: impl std::future::Future<Output = Result<T, LlmFailure>>,
) -> Result<T, LlmFailure> {
    tokio::time::timeout(deadline, work)
        .await
        .unwrap_or_else(|_| {
            Err(LlmFailure::Retryable {
                class: RetryableClass::Timeout,
                retry_after: None,
                detail: "the speech provider did not answer before the deadline".into(),
            })
        })
}

/// `voice: pipeline`: every audio turn is transcribed by the speech stage and becomes text,
/// the text model answers through the ordinary candidate routing, and a spoken answer, when
/// asked for, is synthesised from the answer text. The voice never reaches the model, so the
/// answer is marked `audio_preserved: false`.
pub async fn route_pipeline<'c>(
    channel: &'c LlmChannel,
    stage: &SpeechStage,
    call: &CallRequest,
    on_chunk: Option<sutra_channels::InlineChunkSink>,
) -> Result<RoutedAnswer<'c>, RouteFailure> {
    let mut text_call = call.clone();
    text_call.audio_output = None;
    let accepted = stage.client.transcription_formats();
    for message in &mut text_call.messages {
        let Some(audio) = message.audio.take() else {
            continue;
        };
        let audio = adapt(&audio, &accepted).map_err(|e| RouteFailure::Rejected {
            detail: format!("the request's audio cannot be transcribed: {e}"),
        })?;
        let request = LlmTranscriptionRequest {
            model: stage.transcribe_model.clone(),
            audio,
            language: stage.language.clone(),
            deadline: channel.timeout,
        };
        let transcript = bounded(channel.timeout, stage.client.transcribe(&request))
            .await
            .map_err(|f| stage_failure(stage, &stage.transcribe_model, f))?;
        message.content = if message.content.is_empty() {
            transcript.text
        } else {
            format!("{}\n\n{}", message.content, transcript.text)
        };
    }
    let mut answer = match on_chunk {
        Some(sink) => route_streaming_native(channel, &text_call, sink).await?,
        None => route_native(channel, &text_call).await?,
    };
    if let Some(wanted) = call.audio_output {
        let format =
            provider_output_format(wanted, &stage.client.speech_formats()).ok_or_else(|| {
                RouteFailure::Rejected {
                    detail: format!("no format the speech provider produces converts to {wanted}"),
                }
            })?;
        let request = LlmSpeechRequest {
            model: stage.speech_model.clone(),
            text: answer.response.text.clone(),
            voice: call
                .output_voice
                .clone()
                .or_else(|| stage.voice.clone())
                .or_else(|| channel.output_voice.clone()),
            format,
            deadline: channel.timeout,
        };
        let spoken = bounded(channel.timeout, stage.client.synthesize(&request))
            .await
            .and_then(|audio| deliver_audio(audio, wanted))
            .map_err(|f| stage_failure(stage, &stage.speech_model, f))?;
        answer.response.audio = Some(spoken);
        answer.response.transcript = Some(answer.response.text.clone());
    }
    answer.audio_preserved = Some(false);
    Ok(answer)
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
    if let Some(stage) = pipeline_stage(channel, call) {
        return route_pipeline(channel, stage, call, Some(on_chunk)).await;
    }
    route_streaming_native(channel, call, on_chunk).await
}

async fn route_streaming_native<'c>(
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
    native_answer(call, candidate, response)
}

/// Submit the call as a job on the first candidate with async jobs that accepts it, with the
/// same fallback rules as [`route`].
pub async fn route_submit<'c>(
    channel: &'c LlmChannel,
    call: &CallRequest,
) -> Result<SubmittedJob<'c>, RouteFailure> {
    if call.involves_audio() || call.has_video_input() {
        return Err(RouteFailure::Rejected {
            detail: "a 'mode: job' channel carries no audio or video; use 'mode: call'".into(),
        });
    }
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
        RouteFailure::Rejected { detail } => SendOutcome::PermanentFailure(
            Diagnostic::error(
                codes::AUDIO_UNSUPPORTED,
                format!("llm channel '{}': {detail}", channel.name),
            )
            .with_attribute("channel", channel.name.clone()),
        ),
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

/// The parsed answer of a call that asked for JSON (`json: true` on the request): the answer
/// text as a JSON object or array, so a flow reads its fields directly (FEEL has no JSON
/// parser). `None` when the call did not ask, or the text is not a JSON object or array.
pub fn structured_answer(text: &str, request: &Value) -> Option<Value> {
    if request.get("json").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    let trimmed = text.trim();
    let trimmed = trimmed
        .strip_prefix("```json")
        .or_else(|| trimmed.strip_prefix("```"))
        .and_then(|rest| rest.trim_end().strip_suffix("```"))
        .unwrap_or(trimmed)
        .trim();
    match serde_json::from_str::<Value>(trimmed) {
        Ok(value @ (Value::Object(_) | Value::Array(_))) => Some(value),
        _ => None,
    }
}

/// Why a call that asked for JSON got no parsed `json`: the answer was cut off at
/// `max-output-tokens` (a reasoning model's thinking counts against that limit too), or the
/// answer is not a JSON object or array. `None` when the call did not ask, or it parsed.
pub fn structured_answer_error(
    text: &str,
    finish: &FinishReason,
    request: &Value,
) -> Option<String> {
    if request.get("json").and_then(Value::as_bool) != Some(true)
        || structured_answer(text, request).is_some()
    {
        return None;
    }
    Some(if matches!(finish, FinishReason::Length) {
        "the answer was cut off at max-output-tokens before the JSON was complete (finish: \
         length); raise the channel's max-output-tokens — a reasoning model's thinking counts \
         against it"
            .to_string()
    } else {
        "the answer is not a JSON object or array".to_string()
    })
}

/// The reply body the process receives on the reply channel. A spoken answer adds `audio`
/// (`{data: <base64>, format: <media type>}`) and `transcript`; a call involving audio adds
/// `audioPreserved`; a call that asked for JSON adds `json`, the parsed answer, or `jsonError`
/// saying why there is none.
pub fn reply_body(
    candidate: &LlmCandidate,
    response: &LlmResponse,
    request: &Value,
    audio_preserved: Option<bool>,
) -> Value {
    let tool_calls: Vec<Value> = response
        .tool_calls
        .iter()
        .map(|c| json!({"id": c.id, "name": c.name, "arguments": c.arguments}))
        .collect();
    let mut body = json!({
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
    });
    if let Some(audio) = &response.audio {
        body["audio"] = json!({
            "data": BASE64.encode(&audio.data),
            "format": audio.format.media_type(),
        });
    }
    if let Some(transcript) = &response.transcript {
        body["transcript"] = json!(transcript);
    }
    if let Some(preserved) = audio_preserved {
        body["audioPreserved"] = json!(preserved);
    }
    if let Some(parsed) = structured_answer(&response.text, request) {
        body["json"] = parsed;
    } else if let Some(why) = structured_answer_error(&response.text, &response.finish, request) {
        body["jsonError"] = json!(why);
    }
    body
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
    audio_preserved: Option<bool>,
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
    let body = serde_json::to_vec(&reply_body(candidate, response, request, audio_preserved))
        .unwrap_or_default();
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
        let mut call = match parse_request_body(&message.body) {
            Ok(call) => call,
            Err(diagnostic) => {
                return SendOutcome::PermanentFailure(
                    diagnostic.with_attribute("channel", channel.name.clone()),
                )
            }
        };
        match crate::media::resolve_media(&mut call, &channel).await {
            Ok(()) => {}
            Err(crate::media::MediaFailure::Permanent(d)) => {
                return SendOutcome::PermanentFailure(d)
            }
            Err(crate::media::MediaFailure::Retryable(d)) => {
                return SendOutcome::RetryableFailure(d)
            }
        }

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
            answer.audio_preserved,
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
