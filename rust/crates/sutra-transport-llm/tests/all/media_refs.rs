//! Media references: request parts that name an object in an engine-configured store instead of
//! carrying base64 — resolved (and opened when sealed) before routing, checked against their
//! SHA-256 and the channel's size limit, and refused outside the channel's own store.

use std::sync::Arc;

use serde_json::{json, Value};
use sutra_channels::{MessageSink, SendOutcome};
use sutra_llm_spi::{codes, LlmVideo};
use sutra_objectstore::{
    registry, sealed, MemoryObjectStore, NamedStore, ObjectStore, StoreLocation, StoredObject,
};
use sutra_transport_llm::{parse_channel, parse_request_body, resolve_media, MediaFailure};

use crate::support::{
    channel_yaml, definitions, last_request, outbound, resolver, sink, CapturePort,
};

const KEK: &str = "a media-reference test key secret";

/// Register a sealed in-memory store `name` over bucket `bucket` (unique per test: the registry
/// is process-wide) and return its raw store.
fn store(name: &str, bucket: &str) -> MemoryObjectStore {
    let location = StoreLocation::parse(&format!("memory://{bucket}/media")).unwrap();
    let raw = MemoryObjectStore::new(location.clone());
    registry::register(NamedStore::new(
        name,
        Arc::new(raw.clone()),
        location,
        Some(KEK),
        "media",
    ));
    raw
}

/// Seal `content` under `key` the way a recording transport does, and store it.
async fn put_sealed(raw: &MemoryObjectStore, key: &str, content: &[u8]) -> StoredObject {
    let kek = sutra_crypto::Kek::from_secret(KEK.as_bytes());
    let body = sealed::seal(&kek, "media", &raw.full_key(key), "audio/wav", content).unwrap();
    raw.put(key, body, sealed::SEALED_CONTENT_TYPE)
        .await
        .unwrap()
}

fn audio_ref(stored: &StoredObject) -> Value {
    json!({"messages": [{"role": "user", "content": "what did they say?",
        "audio": {"ref": stored.uri, "sha256": stored.sha256, "format": "audio/wav"}}]})
}

fn refused(outcome: SendOutcome, code: &str) -> String {
    match outcome {
        SendOutcome::PermanentFailure(d) => {
            assert_eq!(d.code, code, "{d:?}");
            d.message
        }
        other => panic!("expected a permanent failure, got {other:?}"),
    }
}

#[tokio::test]
async fn a_sealed_audio_reference_reaches_the_provider_as_plain_audio() {
    let raw = store("media-refs-ok", "media-refs-ok");
    let wav = crate::support::wav_24k(40);
    let stored = put_sealed(&raw, "calls/c1.wav.sealed", &wav).await;
    let port = Arc::new(CapturePort::default());
    let sink = sink(
        &channel_yaml(&["audio-ref-1"], "media-store: media-refs-ok"),
        port.clone(),
    );
    assert_eq!(
        sink.send(&outbound(audio_ref(&stored))).await,
        SendOutcome::Delivered
    );
    let sent = last_request("audio-ref-1").unwrap();
    assert_eq!(sent.messages[0].audio.as_ref().unwrap().data, wav);
    // The reply echoes the reference, never the content.
    let delivered = port.delivered.lock().unwrap();
    let reply: Value = serde_json::from_slice(&delivered[0].body).unwrap();
    assert_eq!(
        reply["request"]["messages"][0]["audio"]["ref"],
        json!(stored.uri)
    );
    assert!(reply["request"]["messages"][0]["audio"]
        .get("data")
        .is_none());
}

#[tokio::test]
async fn image_and_video_frame_references_fill_their_frames() {
    let raw = store("media-refs-frames", "media-refs-frames");
    let one = put_sealed(&raw, "frames/1.jpg.sealed", b"jpeg-one").await;
    let two = put_sealed(&raw, "frames/2.jpg.sealed", b"jpeg-two").await;
    let defs = definitions(&channel_yaml(
        &["video-ref-1"],
        "media-store: media-refs-frames",
    ));
    let channel = parse_channel(&defs[0], resolver).unwrap().unwrap();
    let body = json!({"messages": [
        {"role": "user", "image": {"ref": one.uri, "sha256": one.sha256, "format": "image/jpeg"}},
        {"role": "user", "video": {"frames": [
            {"ref": one.uri, "sha256": one.sha256, "format": "image/jpeg", "atMs": 0},
            {"ref": two.uri, "sha256": two.sha256, "format": "image/jpeg", "atMs": 500}]}}]});
    let mut call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert_eq!(call.media_refs.len(), 3);
    resolve_media(&mut call, &channel).await.unwrap();
    assert!(call.media_refs.is_empty());
    let frames = |i: usize| match call.messages[i].video.as_ref().unwrap() {
        LlmVideo::Frames(frames) => frames.iter().map(|f| f.data.clone()).collect::<Vec<_>>(),
        other => panic!("{other:?}"),
    };
    assert_eq!(frames(0), vec![b"jpeg-one".to_vec()]);
    assert_eq!(frames(1), vec![b"jpeg-one".to_vec(), b"jpeg-two".to_vec()]);
}

#[tokio::test]
async fn a_hash_mismatch_is_refused() {
    let raw = store("media-refs-hash", "media-refs-hash");
    let mut stored = put_sealed(&raw, "c.wav.sealed", &crate::support::wav_24k(20)).await;
    stored.sha256 = "0".repeat(64);
    let sink = sink(
        &channel_yaml(&["audio-ref-2"], "media-store: media-refs-hash"),
        Arc::new(CapturePort::default()),
    );
    let message = refused(
        sink.send(&outbound(audio_ref(&stored))).await,
        codes::MEDIA_REFUSED,
    );
    assert!(message.contains("SHA-256"), "{message}");
    assert!(last_request("audio-ref-2").is_none(), "no provider call");
}

#[tokio::test]
async fn an_oversized_object_is_refused() {
    let raw = store("media-refs-size", "media-refs-size");
    let stored = put_sealed(&raw, "c.wav.sealed", &crate::support::wav_24k(20)).await;
    let sink = sink(
        &channel_yaml(
            &["audio-ref-3"],
            "media-store: media-refs-size\nmedia-max-bytes: 64",
        ),
        Arc::new(CapturePort::default()),
    );
    let message = refused(
        sink.send(&outbound(audio_ref(&stored))).await,
        codes::MEDIA_REFUSED,
    );
    assert!(message.contains("over the limit of 64"), "{message}");
}

#[tokio::test]
async fn unknown_stores_and_references_outside_the_channel_store_are_refused() {
    let raw = store("media-refs-own", "media-refs-own");
    let stored = put_sealed(&raw, "c.wav.sealed", &crate::support::wav_24k(20)).await;
    let other = store("media-refs-other", "media-refs-other");
    let foreign = put_sealed(&other, "c.wav.sealed", &crate::support::wav_24k(20)).await;
    let send = |yaml: String, body: Value| async move {
        sink(&yaml, Arc::new(CapturePort::default()))
            .send(&outbound(body))
            .await
    };
    // A store the engine configuration does not declare.
    let message = refused(
        send(
            channel_yaml(&["audio-ref-4"], "media-store: media-refs-undeclared"),
            audio_ref(&stored),
        )
        .await,
        codes::CONFIG_INVALID,
    );
    assert!(message.contains("not declared"), "{message}");
    // Another configured store's object, through a channel bound to this one.
    let message = refused(
        send(
            channel_yaml(&["audio-ref-4"], "media-store: media-refs-own"),
            audio_ref(&foreign),
        )
        .await,
        codes::MEDIA_REFUSED,
    );
    assert!(message.contains("outside object store"), "{message}");
    // The same bucket, outside the store's prefix.
    let mut outside = stored.clone();
    outside.uri = "memory://media-refs-own/elsewhere/c.wav.sealed".into();
    refused(
        send(
            channel_yaml(&["audio-ref-4"], "media-store: media-refs-own"),
            audio_ref(&outside),
        )
        .await,
        codes::MEDIA_REFUSED,
    );
    // A channel without a media store reads no references at all.
    refused(
        send(channel_yaml(&["audio-ref-4"], ""), audio_ref(&stored)).await,
        codes::MEDIA_REFUSED,
    );
    assert!(last_request("audio-ref-4").is_none(), "no provider call");
}

#[test]
fn malformed_references_are_request_invalid() {
    let uri = "memory://media-refs-own/media/c.wav.sealed";
    let sha = "a".repeat(64);
    for body in [
        json!({"messages": [{"role": "user", "audio": {"ref": uri, "format": "audio/wav"}}]}),
        json!({"messages": [{"role": "user",
            "audio": {"ref": uri, "sha256": "abc", "format": "audio/wav"}}]}),
        json!({"messages": [{"role": "user",
            "audio": {"ref": uri, "data": "eA==", "sha256": sha, "format": "audio/wav"}}]}),
        json!({"messages": [{"role": "user",
            "image": {"ref": uri, "sha256": sha, "format": "audio/wav"}}]}),
        json!({"messages": [{"role": "user",
            "image": {"ref": uri, "sha256": sha, "format": "image/png"},
            "video": {"ref": uri, "sha256": sha, "format": "video/mp4"}}]}),
        json!({"messages": [{"role": "assistant",
            "image": {"ref": uri, "sha256": sha, "format": "image/png"}}]}),
    ] {
        let error = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap_err();
        assert_eq!(error.code, codes::REQUEST_INVALID, "{body}");
    }
}

#[tokio::test]
async fn an_unreachable_store_is_retryable() {
    // A store whose reads fail as unavailable.
    struct Down;
    impl ObjectStore for Down {
        fn put<'a>(
            &'a self,
            _: &'a str,
            _: Vec<u8>,
            _: &'a str,
        ) -> sutra_objectstore::BoxFuture<
            'a,
            Result<StoredObject, sutra_objectstore::ObjectStoreError>,
        > {
            unreachable!()
        }
        fn get<'a>(
            &'a self,
            _: &'a str,
        ) -> sutra_objectstore::BoxFuture<
            'a,
            Result<Option<Vec<u8>>, sutra_objectstore::ObjectStoreError>,
        > {
            Box::pin(async {
                Err(sutra_objectstore::ObjectStoreError::Unavailable(
                    "down".into(),
                ))
            })
        }
        fn ensure_bucket(
            &self,
        ) -> sutra_objectstore::BoxFuture<'_, Result<(), sutra_objectstore::ObjectStoreError>>
        {
            Box::pin(async { Ok(()) })
        }
        fn uri_of(&self, key: &str) -> String {
            format!("memory://media-refs-down/{key}")
        }
        fn full_key(&self, key: &str) -> String {
            key.to_string()
        }
    }
    registry::register(NamedStore::new(
        "media-refs-down",
        Arc::new(Down),
        StoreLocation::parse("memory://media-refs-down").unwrap(),
        None,
        "media",
    ));
    let defs = definitions(&channel_yaml(
        &["audio-ref-5"],
        "media-store: media-refs-down",
    ));
    let channel = parse_channel(&defs[0], resolver).unwrap().unwrap();
    let body = json!({"messages": [{"role": "user", "audio": {
        "ref": "memory://media-refs-down/c.wav", "sha256": "a".repeat(64), "format": "audio/wav"}}]});
    let mut call = parse_request_body(&serde_json::to_vec(&body).unwrap()).unwrap();
    assert!(matches!(
        resolve_media(&mut call, &channel).await,
        Err(MediaFailure::Retryable(_))
    ));
}

/// A refused reference is registered as the caller's fault, so an HTTP caller is answered
/// `422` with `SUTRA.LLM.MEDIA.REFUSED` rather than a bare `500`.
#[test]
fn a_media_refusal_is_a_caller_fault() {
    assert!(sutra_channels::http::is_caller_fault(codes::MEDIA_REFUSED));
    assert!(!sutra_channels::http::is_caller_fault(codes::CALL_FAILED));
}
