//! Resolved `llm` channels, callable by another transport.
//!
//! A transport that needs a model for its own media plane — a voice transport transcribing a
//! call live, or analysing a meeting segment by segment — must not configure a provider of its
//! own: that would bypass everything a channel gives a model call (the ordered candidates and
//! their fallback, `require:` capability filtering, key references, the concurrency limit, the
//! call span) and make a second place where models are configured. It names an `llm` channel
//! of its deployment instead, and calls it through an [`LlmChannelHandle`] looked up in the
//! process-wide [`llm_channels`] registry.
//!
//! The `llm` transport fills the registry from its channel table whenever its channel set
//! changes (at spawn and on every activation flip); the caller depends on this crate only, never
//! on the `llm` transport. The media goes from the caller's transport to the provider directly —
//! it never passes through a process.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use crate::LlmResponse;
use crate::{AudioSession, AudioSessionConfig, BoxFuture, LlmCapabilities, LlmFailure, LlmRequest};

/// The answer of a call made through a channel, and which candidate gave it.
#[derive(Debug, Clone, PartialEq)]
pub struct LlmChannelAnswer {
    pub provider: String,
    pub model: String,
    pub response: LlmResponse,
}

/// A live audio session opened through a channel, and which candidate opened it.
pub struct LlmChannelSession {
    pub provider: String,
    pub model: String,
    pub session: Box<dyn AudioSession>,
}

/// One resolved `llm` channel.
pub trait LlmChannelHandle: Send + Sync {
    /// The channel's name.
    fn channel_name(&self) -> &str;

    /// What at least one of the channel's candidates can do (the union over the candidates
    /// that satisfy its `require:`). A request using a capability only some candidates have is
    /// served by those only.
    fn capabilities(&self) -> LlmCapabilities;

    /// Complete `request` the way a channel call is completed: under the channel's concurrency
    /// limit, on the first of its candidates that has what the request uses and what the channel
    /// requires, falling back on the channel's `fallback-on` classes. The request's `model` and
    /// `deadline` are ignored: each candidate's own model, and the channel's timeout, apply.
    fn complete<'a>(
        &'a self,
        request: &'a LlmRequest,
    ) -> BoxFuture<'a, Result<LlmChannelAnswer, LlmFailure>>;

    /// Open a live audio session on the first candidate that supports live sessions
    /// (`realtime`) and satisfies the channel's `require:`, falling back on the channel's
    /// `fallback-on` classes. `config.model` is ignored (the candidate's model applies), and a
    /// candidate's own transcription model, when it declares one, replaces
    /// `config.transcription_model`. The session holds one of the channel's concurrency permits
    /// until it is dropped.
    fn open_audio_session<'a>(
        &'a self,
        config: &'a AudioSessionConfig,
    ) -> BoxFuture<'a, Result<LlmChannelSession, LlmFailure>>;
}

/// Channels keyed by `(module_key, channel_name)`.
pub type LlmChannelMap = HashMap<(String, String), Arc<dyn LlmChannelHandle>>;

/// A set of resolved channels, swapped whole when the channel set changes.
#[derive(Default)]
pub struct LlmChannelRegistry {
    channels: RwLock<LlmChannelMap>,
}

impl LlmChannelRegistry {
    pub fn new() -> LlmChannelRegistry {
        LlmChannelRegistry::default()
    }

    /// Replace the whole set.
    pub fn swap(&self, channels: LlmChannelMap) {
        *self.channels.write().unwrap_or_else(|e| e.into_inner()) = channels;
    }

    /// Add (or replace) one channel.
    pub fn insert(&self, module_key: &str, channel: &str, handle: Arc<dyn LlmChannelHandle>) {
        self.channels
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert((module_key.to_string(), channel.to_string()), handle);
    }

    /// The channel `channel` of the deployment `module_key`, when it is live.
    pub fn get(&self, module_key: &str, channel: &str) -> Option<Arc<dyn LlmChannelHandle>> {
        self.channels
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(module_key.to_string(), channel.to_string()))
            .cloned()
    }

    pub fn len(&self) -> usize {
        self.channels
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

static REGISTRY: LazyLock<LlmChannelRegistry> = LazyLock::new(LlmChannelRegistry::new);

/// The process-wide registry the `llm` transport publishes its live channels to.
pub fn llm_channels() -> &'static LlmChannelRegistry {
    &REGISTRY
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Named(&'static str);

    impl LlmChannelHandle for Named {
        fn channel_name(&self) -> &str {
            self.0
        }
        fn capabilities(&self) -> LlmCapabilities {
            LlmCapabilities::default()
        }
        fn complete<'a>(
            &'a self,
            _request: &'a LlmRequest,
        ) -> BoxFuture<'a, Result<LlmChannelAnswer, LlmFailure>> {
            Box::pin(async {
                Err(LlmFailure::Permanent {
                    detail: "unused".into(),
                })
            })
        }
        fn open_audio_session<'a>(
            &'a self,
            _config: &'a AudioSessionConfig,
        ) -> BoxFuture<'a, Result<LlmChannelSession, LlmFailure>> {
            Box::pin(async {
                Err(LlmFailure::Permanent {
                    detail: "unused".into(),
                })
            })
        }
    }

    #[test]
    fn registry_resolves_by_module_and_channel_and_swaps_whole() {
        let registry = LlmChannelRegistry::new();
        registry.insert(
            "t/m/1.0.0",
            "video-analysis",
            Arc::new(Named("video-analysis")),
        );
        assert_eq!(
            registry
                .get("t/m/1.0.0", "video-analysis")
                .map(|h| h.channel_name().to_string()),
            Some("video-analysis".to_string())
        );
        assert!(registry.get("t/m/2.0.0", "video-analysis").is_none());
        assert!(registry.get("t/m/1.0.0", "other").is_none());
        registry.swap(LlmChannelMap::new());
        assert!(registry.is_empty());
    }
}
