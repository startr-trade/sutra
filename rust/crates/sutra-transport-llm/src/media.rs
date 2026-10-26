//! Media references: a request's audio, image or video-frame part sent as
//! `{"ref": "<object URI>", "sha256": "<hex>", "format": …}` is read here from the channel's
//! object store before the call is routed, so a process can hand a recording to a model without
//! carrying its bytes through its variables.
//!
//! Each reference must lie inside the store the channel names (`llm.media-store`, declared in the
//! engine configuration as `sutra.object-stores.<name>`): another store, another bucket or a key
//! outside the store's prefix is refused. The stored bytes must match the reference's SHA-256
//! and fit `llm.media-max-bytes`; a sealed object is opened with the store's key-encryption key.
//! Diagnostics name the reference's URI, never the content.

use sutra_channels::Diagnostic;
use sutra_llm_spi::{codes, LlmVideo};
use sutra_objectstore::{registry, MediaRefError, ObjectRef};

use crate::config::LlmChannel;
use crate::request::{CallRequest, MediaSlot};

/// Why the references of a call could not be read.
#[derive(Debug, Clone)]
pub enum MediaFailure {
    /// Not worth retrying: the reference is refused, or the channel cannot read references.
    Permanent(Diagnostic),
    /// The store could not be reached.
    Retryable(Diagnostic),
}

// A refused reference is the caller's to fix (the object is missing, outside the store, too
// large or not the bytes its digest names): an HTTP caller is answered 422 with this code.
inventory::submit! { sutra_channels::http::CallerFaultCode { code: codes::MEDIA_REFUSED } }

fn refused(channel: &LlmChannel, message: String) -> Diagnostic {
    Diagnostic::error(codes::MEDIA_REFUSED, message).with_attribute("channel", &channel.name)
}

/// Read every reference of `call` into its slot. A call without references is left as is.
pub async fn resolve_media(
    call: &mut CallRequest,
    channel: &LlmChannel,
) -> Result<(), MediaFailure> {
    if call.media_refs.is_empty() {
        return Ok(());
    }
    let Some(store_name) = channel.media_store.as_deref() else {
        return Err(MediaFailure::Permanent(refused(
            channel,
            format!(
                "llm channel '{}' declares no 'llm.media-store', so it reads no media references",
                channel.name
            ),
        )));
    };
    let Some(store) = registry::named(store_name) else {
        return Err(MediaFailure::Permanent(
            Diagnostic::error(
                codes::CONFIG_INVALID,
                format!(
                    "llm channel '{}': object store '{store_name}' is not declared in the engine \
                     configuration (sutra.object-stores.{store_name}.*)",
                    channel.name
                ),
            )
            .with_attribute("channel", &channel.name),
        ));
    };
    for reference in std::mem::take(&mut call.media_refs) {
        let object = ObjectRef::parse(&reference.uri).map_err(|reason| {
            MediaFailure::Permanent(refused(channel, format!("a media reference: {reason}")))
        })?;
        if store.relative_key(&object).is_none() {
            return Err(MediaFailure::Permanent(refused(
                channel,
                format!(
                    "'{}' is outside object store '{store_name}', the only store llm channel \
                     '{}' may read",
                    reference.uri, channel.name
                ),
            )));
        }
        let content = store
            .read(&object, &reference.sha256, channel.media_max_bytes)
            .await
            .map_err(|error| match error {
                MediaRefError::Unavailable(m) => MediaFailure::Retryable(
                    Diagnostic::error(codes::CALL_FAILED, m)
                        .with_attribute("channel", &channel.name),
                ),
                MediaRefError::Refused(m) => MediaFailure::Permanent(refused(channel, m)),
            })?;
        let Some(message) = call.messages.get_mut(reference.message) else {
            continue;
        };
        match (reference.slot, &mut message.audio, &mut message.video) {
            (MediaSlot::Audio, Some(audio), _) => audio.data = content,
            (MediaSlot::VideoClip, _, Some(LlmVideo::Clip { data, .. })) => *data = content,
            (MediaSlot::VideoFrame(i), _, Some(LlmVideo::Frames(frames))) => {
                if let Some(frame) = frames.get_mut(i) {
                    frame.data = content;
                }
            }
            _ => {}
        }
    }
    Ok(())
}
