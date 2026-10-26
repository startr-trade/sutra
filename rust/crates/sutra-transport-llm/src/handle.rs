//! The channel handles this transport publishes to the provider SPI's channel registry
//! ([`sutra_llm_spi::llm_channels`]), so another transport calls a model THROUGH an `llm`
//! channel of its deployment: the same candidate routing, fallback, `require:` filtering,
//! concurrency limit and `sutra.llm.call` span as a process's channel call, with the media going
//! from that transport to the provider directly.

use std::sync::Arc;

use serde_json::Value;
use sutra_channels::BoxFuture;
use sutra_llm_spi::{
    codes, AudioSession, AudioSessionConfig, LlmCapabilities, LlmChannelAnswer, LlmChannelHandle,
    LlmChannelMap, LlmChannelSession, LlmFailure, LlmRequest, RetryableClass,
};
use tokio::sync::OwnedSemaphorePermit;
use tracing::field::Empty;
use tracing::Instrument;

use crate::config::{ChannelTable, LlmChannel};
use crate::request::CallRequest;
use crate::sink::{route, RouteFailure};

/// One live [`LlmChannel`], callable through [`LlmChannelHandle`].
pub struct RoutedChannelHandle {
    channel: Arc<LlmChannel>,
}

impl RoutedChannelHandle {
    pub fn new(channel: Arc<LlmChannel>) -> RoutedChannelHandle {
        RoutedChannelHandle { channel }
    }

    fn failure(&self, detail: impl std::fmt::Display) -> String {
        format!(
            "{}: llm channel '{}': {detail}",
            codes::CALL_FAILED,
            self.channel.name
        )
    }

    /// One of the channel's permits, waited for up to the channel's timeout.
    async fn permit(&self) -> Result<OwnedSemaphorePermit, LlmFailure> {
        match tokio::time::timeout(
            self.channel.timeout,
            self.channel.permits().clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => Ok(permit),
            _ => Err(LlmFailure::Retryable {
                class: RetryableClass::RateLimited,
                retry_after: None,
                detail: self.failure(format_args!(
                    "concurrency limit {} still reached at the deadline",
                    self.channel.concurrency
                )),
            }),
        }
    }

    fn route_failure(&self, failure: RouteFailure) -> LlmFailure {
        match failure {
            RouteFailure::NoEligibleCandidate => LlmFailure::Permanent {
                detail: self.failure("no candidate has the capabilities this call needs"),
            },
            RouteFailure::Rejected { detail } => LlmFailure::Permanent {
                detail: format!(
                    "{}: llm channel '{}': {detail}",
                    codes::AUDIO_UNSUPPORTED,
                    self.channel.name
                ),
            },
            RouteFailure::Failed {
                provider,
                model,
                failure,
            } => {
                let detail = self.failure(format_args!(
                    "every candidate failed; last ({provider}/{model}): {}",
                    failure.detail()
                ));
                match failure {
                    LlmFailure::Retryable {
                        class, retry_after, ..
                    } => LlmFailure::Retryable {
                        class,
                        retry_after,
                        detail,
                    },
                    LlmFailure::Permanent { .. } => LlmFailure::Permanent { detail },
                }
            }
        }
    }

    async fn serve(&self, request: &LlmRequest) -> Result<LlmChannelAnswer, LlmFailure> {
        let call = CallRequest {
            system: request.system.clone(),
            messages: request.messages.clone(),
            tools: request.tools.clone(),
            json_output: request.json_output,
            max_output_tokens: request.max_output_tokens,
            temperature: request.temperature,
            audio_output: request.audio_output,
            output_voice: request.output_voice.clone(),
            media_refs: Vec::new(),
            raw: Value::Null,
        };
        let permit = self.permit().await?;
        let span = tracing::info_span!(
            "sutra.llm.call",
            channel = %self.channel.name,
            provider = Empty,
            model = Empty,
            finish = Empty,
            input_tokens = Empty,
            output_tokens = Empty,
            candidate = Empty,
        );
        let routed = route(&self.channel, &call).instrument(span.clone()).await;
        drop(permit);
        let answer = routed.map_err(|f| self.route_failure(f))?;
        span.record("finish", answer.response.finish.as_str());
        span.record("input_tokens", answer.response.usage.input_tokens);
        span.record("output_tokens", answer.response.usage.output_tokens);
        Ok(LlmChannelAnswer {
            provider: answer.candidate.provider.clone(),
            model: answer.candidate.model().to_string(),
            response: answer.response,
        })
    }

    async fn open(&self, config: &AudioSessionConfig) -> Result<LlmChannelSession, LlmFailure> {
        let channel = &self.channel;
        let need = LlmCapabilities {
            realtime: true,
            ..channel.require
        };
        let permit = self.permit().await?;
        let span = tracing::info_span!(
            "sutra.llm.call",
            channel = %channel.name,
            session = true,
            provider = Empty,
            model = Empty,
            candidate = Empty,
        );
        self.open_on_candidates(config, need, permit)
            .instrument(span)
            .await
    }

    async fn open_on_candidates(
        &self,
        config: &AudioSessionConfig,
        need: LlmCapabilities,
        permit: OwnedSemaphorePermit,
    ) -> Result<LlmChannelSession, LlmFailure> {
        let channel = &self.channel;
        let span = tracing::Span::current();
        let mut last: Option<(String, String, LlmFailure)> = None;
        for (index, candidate) in channel.candidates.iter().enumerate() {
            if !candidate.satisfies(need) {
                continue;
            }
            span.record("candidate", index);
            span.record("provider", candidate.provider.as_str());
            span.record("model", candidate.model());
            let config = AudioSessionConfig {
                model: candidate.model().to_string(),
                transcription_model: candidate
                    .transcription_model
                    .clone()
                    .or_else(|| config.transcription_model.clone()),
                ..config.clone()
            };
            let bound = config
                .connect_deadline
                .max(std::time::Duration::from_millis(1));
            let outcome =
                match tokio::time::timeout(bound, candidate.client.open_audio_session(&config))
                    .await
                {
                    Ok(outcome) => outcome,
                    Err(_) => Err(LlmFailure::Retryable {
                        class: RetryableClass::Timeout,
                        retry_after: None,
                        detail: "the provider did not open the session before the deadline".into(),
                    }),
                };
            match outcome {
                Ok(session) => {
                    return Ok(LlmChannelSession {
                        provider: candidate.provider.clone(),
                        model: candidate.model().to_string(),
                        session: Box::new(PermittedSession {
                            inner: session,
                            _permit: permit,
                        }),
                    })
                }
                Err(failure) => {
                    let fall_back = matches!(
                        &failure,
                        LlmFailure::Retryable { class, .. } if channel.fallback_on.contains(class)
                    );
                    tracing::warn!(
                        channel = %channel.name,
                        provider = %candidate.provider,
                        model = %candidate.model(),
                        fall_back,
                        "llm candidate could not open a live session: {}",
                        failure.detail()
                    );
                    last = Some((
                        candidate.provider.clone(),
                        candidate.model().to_string(),
                        failure,
                    ));
                    if !fall_back {
                        break;
                    }
                }
            }
        }
        Err(match last {
            None => LlmFailure::Permanent {
                detail: self.failure(
                    "no candidate opens live audio sessions ('realtime') with the capabilities \
                     the channel requires",
                ),
            },
            Some((provider, model, failure)) => self.route_failure(RouteFailure::Failed {
                provider,
                model,
                failure,
            }),
        })
    }
}

impl LlmChannelHandle for RoutedChannelHandle {
    fn channel_name(&self) -> &str {
        &self.channel.name
    }

    fn capabilities(&self) -> LlmCapabilities {
        let mut union = LlmCapabilities::default();
        for candidate in &self.channel.candidates {
            if !candidate.satisfies(self.channel.require) {
                continue;
            }
            let c = candidate.client.capabilities();
            union.tools |= c.tools;
            union.json_output |= c.json_output;
            union.async_jobs |= c.async_jobs;
            union.job_callbacks |= c.job_callbacks;
            union.streaming |= c.streaming;
            union.audio_input |= c.audio_input;
            union.audio_output |= c.audio_output;
            union.audio_understanding |= c.audio_understanding;
            union.video_understanding |= c.video_understanding;
            union.realtime |= c.realtime;
            union.transcription |= c.transcription;
            union.speech_synthesis |= c.speech_synthesis;
        }
        union
    }

    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmChannelAnswer, LlmFailure>> {
        Box::pin(self.serve(request))
    }

    fn open_audio_session<'a>(
        &'a self,
        config: &'a AudioSessionConfig,
    ) -> BoxFuture<'a, Result<LlmChannelSession, LlmFailure>> {
        Box::pin(self.open(config))
    }
}

/// A live session holding one of its channel's concurrency permits for as long as it lives.
struct PermittedSession {
    inner: Box<dyn AudioSession>,
    _permit: OwnedSemaphorePermit,
}

impl AudioSession for PermittedSession {
    fn send_audio<'a>(&'a self, frame: &'a [u8]) -> BoxFuture<'a, Result<(), LlmFailure>> {
        self.inner.send_audio(frame)
    }

    fn commit(&self) -> BoxFuture<'_, Result<(), LlmFailure>> {
        self.inner.commit()
    }

    fn next_event(&self) -> BoxFuture<'_, Option<sutra_llm_spi::AudioSessionEvent>> {
        self.inner.next_event()
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        self.inner.close()
    }
}

/// A handle for every channel of `table`, keyed the way the registry is.
pub fn channel_handles(table: &ChannelTable) -> LlmChannelMap {
    table
        .all()
        .into_iter()
        .filter(|channel| channel.mode == crate::config::ChannelMode::Call)
        .map(|channel| {
            let key = (channel.module_key.clone(), channel.name.clone());
            let handle: Arc<dyn LlmChannelHandle> = Arc::new(RoutedChannelHandle::new(channel));
            (key, handle)
        })
        .collect()
}

/// Add every channel of `table` to `registry` (keeping what it holds already).
pub fn publish_channels(table: &ChannelTable, registry: &sutra_llm_spi::LlmChannelRegistry) {
    for ((module_key, name), handle) in channel_handles(table) {
        registry.insert(&module_key, &name, handle);
    }
}
