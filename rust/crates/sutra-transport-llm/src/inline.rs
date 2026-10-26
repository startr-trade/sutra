//! The `llm` inline caller: the same lookup, request parsing and candidate routing as the
//! async sink, but the answer is returned to the caller instead of delivered as a reply.

use std::collections::BTreeMap;
use std::sync::Arc;

use sutra_channels::{
    BoxFuture, Diagnostic, InlineCaller, InlineChunkSink, InlineFailure, InlineResponse,
    OutboundMessage, SendOutcome,
};
use sutra_llm_spi::codes;

use crate::config::ChannelTable;
use crate::request::{parse_destination, parse_request_body};
use crate::sink::{
    failure_outcome, reply_body, route, route_streaming, MODEL_HEADER, PROVIDER_HEADER,
};
use crate::TRANSPORT;

/// Serves synchronous `llm://` calls from a [`ChannelTable`].
pub struct LlmInlineCaller {
    table: Arc<ChannelTable>,
}

impl LlmInlineCaller {
    pub fn new(table: Arc<ChannelTable>) -> LlmInlineCaller {
        LlmInlineCaller { table }
    }

    /// Serve one call; with `on_chunk`, the provider streams the answer text to it as it is
    /// produced (the response is still the whole answer).
    async fn serve(
        &self,
        message: &OutboundMessage,
        on_chunk: Option<InlineChunkSink>,
    ) -> Result<InlineResponse, InlineFailure> {
        let Some((_tenant, module_key, channel_name)) = parse_destination(&message.destination)
        else {
            return Err(InlineFailure::Permanent(Diagnostic::error(
                codes::REQUEST_INVALID,
                format!(
                    "llm destination '{}' is not an 'llm://<module_key>/<channel>' URI",
                    message.destination
                ),
            )));
        };
        let Some(channel) = self.table.get(&module_key, &channel_name) else {
            // Possibly mid-activation; consistent with the async sink.
            return Err(InlineFailure::Retryable(
                Diagnostic::error(
                    codes::CONFIG_INVALID,
                    format!("no active llm channel '{channel_name}' in '{module_key}'"),
                )
                .with_attribute("channel", channel_name),
            ));
        };
        let mut call = parse_request_body(&message.body).map_err(|d| {
            InlineFailure::Permanent(d.with_attribute("channel", channel.name.clone()))
        })?;
        crate::media::resolve_media(&mut call, &channel)
            .await
            .map_err(|failure| match failure {
                crate::media::MediaFailure::Permanent(d) => InlineFailure::Permanent(d),
                crate::media::MediaFailure::Retryable(d) => InlineFailure::Retryable(d),
            })?;
        let permit =
            match tokio::time::timeout(channel.timeout, channel.permits().clone().acquire_owned())
                .await
            {
                Ok(Ok(permit)) => permit,
                _ => {
                    return Err(InlineFailure::Retryable(
                        Diagnostic::error(
                            codes::CALL_FAILED,
                            format!(
                                "llm channel '{}': concurrency limit {} still reached at the \
                             deadline",
                                channel.name, channel.concurrency
                            ),
                        )
                        .with_attribute("channel", channel.name.clone()),
                    ))
                }
            };
        let routed = match on_chunk {
            Some(sink) => route_streaming(&channel, &call, sink).await,
            None => route(&channel, &call).await,
        };
        drop(permit);
        match routed {
            Ok(answer) => {
                let body = serde_json::to_vec(&reply_body(
                    answer.candidate,
                    &answer.response,
                    &call.raw,
                    answer.audio_preserved,
                ))
                .unwrap_or_default();
                let headers = BTreeMap::from([
                    (
                        PROVIDER_HEADER.to_string(),
                        answer.candidate.provider.clone(),
                    ),
                    (
                        MODEL_HEADER.to_string(),
                        answer.candidate.model().to_string(),
                    ),
                ]);
                Ok(InlineResponse {
                    body,
                    content_type: Some("application/json".to_string()),
                    headers,
                })
            }
            Err(failure) => match failure_outcome(&channel, failure) {
                SendOutcome::RetryableFailure(d) => Err(InlineFailure::Retryable(d)),
                SendOutcome::PermanentFailure(d) => Err(InlineFailure::Permanent(d)),
                SendOutcome::Delivered => unreachable!("failure_outcome never delivers"),
            },
        }
    }
}

impl InlineCaller for LlmInlineCaller {
    fn schemes(&self) -> Vec<String> {
        vec![TRANSPORT.to_string()]
    }

    fn call<'a>(
        &'a self,
        request: &'a OutboundMessage,
    ) -> BoxFuture<'a, Result<InlineResponse, InlineFailure>> {
        Box::pin(self.serve(request, None))
    }

    fn call_streaming<'a>(
        &'a self,
        request: &'a OutboundMessage,
        on_chunk: InlineChunkSink,
    ) -> BoxFuture<'a, Result<InlineResponse, InlineFailure>> {
        Box::pin(self.serve(request, Some(on_chunk)))
    }
}
