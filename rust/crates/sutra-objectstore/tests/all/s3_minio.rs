//! Tier 2: the S3 client against a real S3-compatible store (MinIO). Put, get and delete over
//! signed requests, the full key character set (unsafe keys refused before any request), a
//! media reference read back with its digest and size checked, a sealed object opened with the
//! store's key, delete being idempotent, and the two failure classes: refused credentials (not worth retrying) and an unreachable store
//! (worth retrying). Then the streamed-write surface: a multipart upload with parts of at least
//! 5 MiB completed and read back, an abort, the uploads in progress listed, ranged gets, and an
//! object listing that spans several pages.

use sutra_crypto::Kek;
use sutra_objectstore::sigv4::Credentials;
use sutra_objectstore::{
    sealed, sha256_hex, MediaRefError, NamedStore, ObjectRef, ObjectStore, ObjectStoreError,
    S3Config, S3ObjectStore, StoreLocation, StoreSettings,
};
use sutra_testkit::minio::{MinioContainer, MINIO_ACCESS_KEY, MINIO_REGION, MINIO_SECRET_KEY};

const SECRET: &str = MINIO_SECRET_KEY;
const KEK: &str = "a test key-encryption secret";
const KEY_ID: &str = "test-kek-1";

fn settings(endpoint: &str, secret: &str, kek: Option<&str>) -> StoreSettings {
    StoreSettings {
        name: "media".into(),
        location: StoreLocation::parse("s3://media-test/calls").unwrap(),
        endpoint: Some(endpoint.to_string()),
        region: MINIO_REGION.into(),
        access_key: Some(MINIO_ACCESS_KEY.into()),
        secret_key: Some(secret.into()),
        kek_secret: kek.map(str::to_string),
        key_id: KEY_ID.into(),
    }
}

/// A running MinIO and its endpoint, with the test bucket created.
async fn minio() -> (MinioContainer, String) {
    let container = MinioContainer::start();
    let endpoint = container.endpoint().to_string();
    NamedStore::open(&settings(&endpoint, SECRET, None))
        .unwrap()
        .store()
        .ensure_bucket()
        .await
        .expect("create the test bucket");
    (container, endpoint)
}

fn reference(uri: &str) -> ObjectRef {
    ObjectRef::parse(uri).unwrap()
}

#[tokio::test]
#[ignore = "docker"]
async fn objects_round_trip_through_a_real_s3_store() {
    let (_minio, endpoint) = minio().await;
    let named = NamedStore::open(&settings(&endpoint, SECRET, Some(KEK))).unwrap();
    let store = named.store();

    // Put then get, with the digest and size the store reports.
    let body = b"RIFF-not-really-a-wav".to_vec();
    let stored = store
        .put("call-1/turn-1.wav", body.clone(), "audio/wav")
        .await
        .unwrap();
    assert_eq!(stored.key, "calls/call-1/turn-1.wav");
    assert_eq!(stored.uri, "s3://media-test/calls/call-1/turn-1.wav");
    assert_eq!(stored.sha256, sha256_hex(&body));
    assert_eq!(stored.bytes, body.len() as u64);
    assert_eq!(
        store.get("call-1/turn-1.wav").await.unwrap(),
        Some(body.clone())
    );

    // Every character a key may carry is signed and addressed the way the store expects.
    let deep = "Call_2/a.b-c/D9/frame_0001.v2.bin";
    store
        .put(deep, b"deep".to_vec(), "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(store.get(deep).await.unwrap(), Some(b"deep".to_vec()));

    // A key outside that set never reaches the store.
    for unsafe_key in [
        "call 2/x.bin",
        "call/../x.bin",
        "call/äudio.bin",
        "a+b=c.bin",
    ] {
        assert!(
            matches!(
                store
                    .put(unsafe_key, b"x".to_vec(), "application/octet-stream")
                    .await,
                Err(ObjectStoreError::Config(_))
            ),
            "{unsafe_key}"
        );
    }

    // A missing object is None, not an error.
    assert_eq!(store.get("call-1/absent.wav").await.unwrap(), None);

    // A media reference is read back only when its digest and size check out.
    let uri = reference(&stored.uri);
    assert_eq!(
        named
            .read(&uri, &stored.sha256.to_uppercase(), 1024)
            .await
            .unwrap(),
        body
    );
    assert!(matches!(
        named.read(&uri, &sha256_hex(b"other"), 1024).await,
        Err(MediaRefError::Refused(_))
    ));
    assert!(matches!(
        named.read(&uri, &stored.sha256, 4).await,
        Err(MediaRefError::Refused(_))
    ));
    assert!(matches!(
        named
            .read(
                &reference("s3://other-bucket/calls/x.wav"),
                &stored.sha256,
                1024
            )
            .await,
        Err(MediaRefError::Refused(_))
    ));

    // Delete removes it; deleting again succeeds; reading it afterwards is refused.
    named.delete(&uri).await.unwrap();
    assert_eq!(store.get("call-1/turn-1.wav").await.unwrap(), None);
    named.delete(&uri).await.unwrap();
    assert!(matches!(
        named.read(&uri, &stored.sha256, 1024).await,
        Err(MediaRefError::Refused(_))
    ));
}

#[tokio::test]
#[ignore = "docker"]
async fn a_sealed_object_opens_only_with_the_store_key() {
    let (_minio, endpoint) = minio().await;
    let named = NamedStore::open(&settings(&endpoint, SECRET, Some(KEK))).unwrap();
    let store = named.store();

    let plaintext = b"the caller's audio".to_vec();
    let relative = "call-3/recording.wav.sealed";
    let full_key = store.full_key(relative);
    let kek = Kek::from_secret(KEK.as_bytes());
    let sealed_bytes = sealed::seal(&kek, KEY_ID, &full_key, "audio/wav", &plaintext).unwrap();
    assert_ne!(sealed_bytes, plaintext);
    let stored = store
        .put(relative, sealed_bytes, sealed::SEALED_CONTENT_TYPE)
        .await
        .unwrap();
    // The store holds ciphertext only.
    let at_rest = store.get(relative).await.unwrap().unwrap();
    assert!(at_rest.starts_with(sealed::MAGIC));
    assert!(!at_rest.windows(plaintext.len()).any(|w| w == plaintext));

    let uri = reference(&stored.uri);
    assert_eq!(
        named.read(&uri, &stored.sha256, 1 << 20).await.unwrap(),
        plaintext
    );

    // The same store without a key refuses it; a different key cannot open it.
    let keyless = NamedStore::open(&settings(&endpoint, SECRET, None)).unwrap();
    assert!(matches!(
        keyless.read(&uri, &stored.sha256, 1 << 20).await,
        Err(MediaRefError::Refused(_))
    ));
    let wrong = NamedStore::open(&settings(&endpoint, SECRET, Some("another key"))).unwrap();
    assert!(matches!(
        wrong.read(&uri, &stored.sha256, 1 << 20).await,
        Err(MediaRefError::Refused(_))
    ));

    named.delete(&uri).await.unwrap();
    assert_eq!(store.get(relative).await.unwrap(), None);
}

#[tokio::test]
#[ignore = "docker"]
async fn refused_credentials_are_rejected_and_an_unreachable_store_is_unavailable() {
    let (_minio, endpoint) = minio().await;

    // A wrong secret: the store answers 403, which is not worth retrying.
    let refused = NamedStore::open(&settings(&endpoint, "not-the-secret", None)).unwrap();
    assert!(matches!(
        refused
            .store()
            .put("x.bin", b"x".to_vec(), "application/octet-stream")
            .await,
        Err(ObjectStoreError::Rejected(_))
    ));
    assert!(matches!(
        refused.store().get("x.bin").await,
        Err(ObjectStoreError::Rejected(_))
    ));
    assert!(matches!(
        refused.store().delete("x.bin").await,
        Err(ObjectStoreError::Rejected(_))
    ));

    // Nothing listening: worth retrying.
    let unreachable = NamedStore::open(&settings("http://127.0.0.1:9", SECRET, None)).unwrap();
    assert!(matches!(
        unreachable.store().get("x.bin").await,
        Err(ObjectStoreError::Unavailable(_))
    ));
    assert!(matches!(
        unreachable
            .read(
                &reference("s3://media-test/calls/x.bin"),
                &sha256_hex(b"x"),
                16
            )
            .await,
        Err(MediaRefError::Unavailable(_))
    ));
}

/// An S3 store on the test bucket that asks for `page` entries per listing request.
fn s3_store(endpoint: &str, page: u32) -> S3ObjectStore {
    let mut config = S3Config::new(
        StoreLocation::parse("s3://media-test/streams").unwrap(),
        MINIO_REGION,
        Credentials {
            access_key: MINIO_ACCESS_KEY.into(),
            secret_key: SECRET.into(),
            session_token: None,
        },
    );
    config.endpoint = Some(endpoint.to_string());
    config.list_page_size = page;
    S3ObjectStore::new(config).unwrap()
}

/// `len` bytes of a pattern that differs per `seed`, so a part out of place shows.
fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
        .collect()
}

#[tokio::test]
#[ignore = "docker"]
async fn multipart_uploads_round_trip_list_and_abort_on_a_real_s3_store() {
    let (_minio, endpoint) = minio().await;
    let store = s3_store(&endpoint, 1000);
    const MIB: usize = 1024 * 1024;

    // Two parts: the first at the 5 MiB minimum S3 holds every part but the last to, the
    // second smaller. Uploaded out of order; completed in order.
    let key = "call-7/recording.wav";
    let id = store.create_upload(key, "audio/wav").await.unwrap();
    let first = pattern(5 * MIB, 1);
    let second = pattern(MIB + 17, 2);
    let second_etag = store
        .upload_part(key, &id, 2, second.clone())
        .await
        .unwrap();
    let first_etag = store.upload_part(key, &id, 1, first.clone()).await.unwrap();
    assert!(!first_etag.is_empty() && first_etag != second_etag);

    // In progress: found by its exact key, with its parts.
    let pending = store.list_uploads(key).await.unwrap();
    assert!(
        pending.iter().any(|p| p.upload_id == id && p.key == key),
        "the upload is listed for its exact key: {pending:?}"
    );
    // (MinIO answers this listing for an exact key only: by the prefix `call-7/` it lists
    // nothing, which is why the trait points a recovering caller at marker objects.)
    let parts = store.list_parts(key, &id).await.unwrap();
    assert_eq!(
        parts.iter().map(|p| (p.number, p.size)).collect::<Vec<_>>(),
        vec![(1, first.len() as u64), (2, second.len() as u64)]
    );
    assert_eq!(parts[0].etag, first_etag);

    store.complete_upload(key, &id, &parts).await.unwrap();
    let mut whole = first.clone();
    whole.extend_from_slice(&second);
    let read = store.get(key).await.unwrap().unwrap();
    assert_eq!(read.len(), whole.len());
    assert!(read == whole, "the object is the parts in order");
    assert!(store.list_uploads(key).await.unwrap().is_empty());

    // A range across the part boundary.
    let boundary = 5 * MIB as u64;
    assert_eq!(
        store
            .get_range(key, boundary - 3, 6)
            .await
            .unwrap()
            .unwrap(),
        whole[5 * MIB - 3..5 * MIB + 3].to_vec()
    );

    // An abort discards the upload; a second abort is harmless; the object never appears.
    let aborted = "call-8/recording.wav";
    let id = store.create_upload(aborted, "audio/wav").await.unwrap();
    store
        .upload_part(aborted, &id, 1, pattern(1024, 3))
        .await
        .unwrap();
    store.abort_upload(aborted, &id).await.unwrap();
    store.abort_upload(aborted, &id).await.unwrap();
    assert!(store
        .list_uploads(aborted)
        .await
        .unwrap()
        .iter()
        .all(|p| p.upload_id != id));
    assert_eq!(store.get(aborted).await.unwrap(), None);
    assert!(matches!(
        store.complete_upload(aborted, &id, &parts).await,
        Err(ObjectStoreError::Rejected(_))
    ));
    store.delete(key).await.unwrap();
}

#[tokio::test]
#[ignore = "docker"]
async fn ranged_gets_and_listings_across_pages_on_a_real_s3_store() {
    let (_minio, endpoint) = minio().await;
    // Two entries per listing request, so five objects take three pages.
    let store = s3_store(&endpoint, 2);

    let names = [
        "seg/0001.bin",
        "seg/0002.bin",
        "seg/0003.bin",
        "seg/0004.bin",
        "seg/0005.bin",
    ];
    for (i, name) in names.iter().enumerate() {
        store
            .put(name, pattern(100 + i, i as u8), "application/octet-stream")
            .await
            .unwrap();
    }
    store
        .put("other/x.bin", b"x".to_vec(), "application/octet-stream")
        .await
        .unwrap();
    let listed = store.list_objects("seg/").await.unwrap();
    assert_eq!(
        listed.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        names.to_vec()
    );
    assert_eq!(
        listed.iter().map(|o| o.bytes).collect::<Vec<_>>(),
        vec![100, 101, 102, 103, 104]
    );
    assert_eq!(store.list_objects("").await.unwrap().len(), 6);
    assert!(store.list_objects("none/").await.unwrap().is_empty());

    // Ranges: inside, running past the end, starting past the end, and a missing object.
    let body = pattern(100, 0);
    assert_eq!(
        store.get_range(names[0], 10, 5).await.unwrap(),
        Some(body[10..15].to_vec())
    );
    assert_eq!(
        store.get_range(names[0], 95, 50).await.unwrap(),
        Some(body[95..].to_vec())
    );
    assert_eq!(
        store.get_range(names[0], 100, 5).await.unwrap(),
        Some(Vec::new())
    );
    assert_eq!(store.get_range("seg/absent.bin", 0, 5).await.unwrap(), None);

    // Uploads in progress, listed a page at a time for an exact key.
    let key = "seg/streaming.bin";
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(
            store
                .create_upload(key, "application/octet-stream")
                .await
                .unwrap(),
        );
    }
    let mut found: Vec<String> = store
        .list_uploads(key)
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.upload_id)
        .collect();
    found.sort();
    ids.sort();
    assert_eq!(found, ids, "every upload of the key, across pages");
    for id in &ids {
        store.abort_upload(key, id).await.unwrap();
    }
}
