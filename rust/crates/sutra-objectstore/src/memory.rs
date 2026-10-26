//! An in-process object store, for tests and for a deployment that wants recordings kept in
//! memory only (`memory://<bucket>`): nothing survives the process. It implements the whole
//! [`ObjectStore`] surface, multipart uploads included, so a caller's streamed-write and
//! recovery paths can be exercised without a server. Clones share their objects.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::{
    checked_full_key, checked_list_prefix, relative_key, sha256_hex, BoxFuture, ListedObject,
    ObjectStore, ObjectStoreError, PendingUpload, StoreLocation, StoredObject, UploadedPart,
};

/// One upload in progress: the full key it completes into, its content type, its parts.
struct MemoryUpload {
    full_key: String,
    content_type: String,
    parts: BTreeMap<u32, Vec<u8>>,
}

#[derive(Default)]
struct MemoryState {
    /// Full key → (body, content type).
    objects: BTreeMap<String, (Vec<u8>, String)>,
    /// Upload id → the upload.
    uploads: BTreeMap<String, MemoryUpload>,
    next_upload: u64,
}

/// Objects by full key, with their content type, and the multipart uploads in progress.
#[derive(Clone)]
pub struct MemoryObjectStore {
    location: StoreLocation,
    state: Arc<Mutex<MemoryState>>,
}

/// The ETag the memory store answers for a part: quoted, like an S3 store's.
fn part_etag(body: &[u8]) -> String {
    format!("\"{}\"", &sha256_hex(body)[..32])
}

impl MemoryObjectStore {
    pub fn new(location: StoreLocation) -> MemoryObjectStore {
        MemoryObjectStore {
            location,
            state: Arc::new(Mutex::new(MemoryState::default())),
        }
    }

    fn state(&self) -> MutexGuard<'_, MemoryState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Every stored object's full key, sorted.
    pub fn keys(&self) -> Vec<String> {
        self.state().objects.keys().cloned().collect()
    }

    /// The content type an object was stored with.
    pub fn content_type(&self, full_key: &str) -> Option<String> {
        self.state()
            .objects
            .get(full_key)
            .map(|(_, kind)| kind.clone())
    }

    /// How many multipart uploads are in progress.
    pub fn pending_upload_count(&self) -> usize {
        self.state().uploads.len()
    }
}

impl ObjectStore for MemoryObjectStore {
    fn put<'a>(
        &'a self,
        key: &'a str,
        body: Vec<u8>,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<StoredObject, ObjectStoreError>> {
        Box::pin(async move {
            let full = checked_full_key(&self.location, key)?;
            let stored = StoredObject {
                uri: self.uri_of(key),
                key: full.clone(),
                sha256: sha256_hex(&body),
                bytes: body.len() as u64,
            };
            self.state()
                .objects
                .insert(full, (body, content_type.to_string()));
            Ok(stored)
        })
    }

    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, ObjectStoreError>> {
        Box::pin(async move {
            Ok(self
                .state()
                .objects
                .get(&self.full_key(key))
                .map(|(body, _)| body.clone()))
        })
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        Box::pin(async move {
            self.state().objects.remove(&self.full_key(key));
            Ok(())
        })
    }

    fn ensure_bucket(&self) -> BoxFuture<'_, Result<(), ObjectStoreError>> {
        Box::pin(async { Ok(()) })
    }

    fn uri_of(&self, key: &str) -> String {
        self.location.uri_of(key)
    }

    fn full_key(&self, key: &str) -> String {
        self.location.full_key(key)
    }

    fn get_range<'a>(
        &'a self,
        key: &'a str,
        start: u64,
        len: u64,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, ObjectStoreError>> {
        Box::pin(async move {
            Ok(self
                .state()
                .objects
                .get(&self.full_key(key))
                .map(|(body, _)| {
                    let from = usize::try_from(start).unwrap_or(usize::MAX).min(body.len());
                    let to = usize::try_from(start.saturating_add(len))
                        .unwrap_or(usize::MAX)
                        .min(body.len());
                    body[from..to].to_vec()
                }))
        })
    }

    fn list_objects<'a>(
        &'a self,
        prefix: &'a str,
    ) -> BoxFuture<'a, Result<Vec<ListedObject>, ObjectStoreError>> {
        Box::pin(async move {
            checked_list_prefix(prefix)?;
            let base = self.location.full_key("");
            Ok(self
                .state()
                .objects
                .iter()
                .filter_map(|(full, (body, _))| {
                    relative_key(&base, full).map(|key| ListedObject {
                        key,
                        bytes: body.len() as u64,
                    })
                })
                .filter(|listed| listed.key.starts_with(prefix))
                .collect())
        })
    }

    fn create_upload<'a>(
        &'a self,
        key: &'a str,
        content_type: &'a str,
    ) -> BoxFuture<'a, Result<String, ObjectStoreError>> {
        Box::pin(async move {
            let full = checked_full_key(&self.location, key)?;
            let mut state = self.state();
            state.next_upload += 1;
            let id = format!("memory-upload-{}", state.next_upload);
            state.uploads.insert(
                id.clone(),
                MemoryUpload {
                    full_key: full,
                    content_type: content_type.to_string(),
                    parts: BTreeMap::new(),
                },
            );
            Ok(id)
        })
    }

    fn upload_part<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
        number: u32,
        body: Vec<u8>,
    ) -> BoxFuture<'a, Result<String, ObjectStoreError>> {
        Box::pin(async move {
            if !(1..=10_000).contains(&number) {
                return Err(ObjectStoreError::Rejected(format!(
                    "upload part: part number {number} is outside 1..=10000"
                )));
            }
            let full = self.full_key(key);
            let mut state = self.state();
            let Some(upload) = state.uploads.get_mut(upload_id) else {
                return Err(ObjectStoreError::Rejected(format!(
                    "upload part: no upload {upload_id}"
                )));
            };
            if upload.full_key != full {
                return Err(ObjectStoreError::Rejected(
                    "upload part: the upload is for another key".to_string(),
                ));
            }
            let etag = part_etag(&body);
            upload.parts.insert(number, body);
            Ok(etag)
        })
    }

    fn complete_upload<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
        parts: &'a [UploadedPart],
    ) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        Box::pin(async move {
            let full = self.full_key(key);
            let mut state = self.state();
            let Some(upload) = state.uploads.get(upload_id) else {
                return Err(ObjectStoreError::Rejected(format!(
                    "complete upload: no upload {upload_id}"
                )));
            };
            if upload.full_key != full {
                return Err(ObjectStoreError::Rejected(
                    "complete upload: the upload is for another key".to_string(),
                ));
            }
            if parts.is_empty() {
                return Err(ObjectStoreError::Rejected(
                    "complete upload: no parts".to_string(),
                ));
            }
            let mut body = Vec::new();
            let mut previous = 0;
            for part in parts {
                if part.number <= previous {
                    return Err(ObjectStoreError::Rejected(
                        "complete upload: the parts are not in ascending order".to_string(),
                    ));
                }
                previous = part.number;
                match upload.parts.get(&part.number) {
                    Some(bytes) if part_etag(bytes) == part.etag => body.extend_from_slice(bytes),
                    Some(_) => {
                        return Err(ObjectStoreError::Rejected(format!(
                            "complete upload: part {} does not match its ETag",
                            part.number
                        )))
                    }
                    None => {
                        return Err(ObjectStoreError::Rejected(format!(
                            "complete upload: part {} was not uploaded",
                            part.number
                        )))
                    }
                }
            }
            let content_type = upload.content_type.clone();
            state.uploads.remove(upload_id);
            state.objects.insert(full, (body, content_type));
            Ok(())
        })
    }

    fn abort_upload<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
    ) -> BoxFuture<'a, Result<(), ObjectStoreError>> {
        Box::pin(async move {
            let full = self.full_key(key);
            let mut state = self.state();
            if state
                .uploads
                .get(upload_id)
                .is_some_and(|upload| upload.full_key == full)
            {
                state.uploads.remove(upload_id);
            }
            Ok(())
        })
    }

    fn list_uploads<'a>(
        &'a self,
        key_or_prefix: &'a str,
    ) -> BoxFuture<'a, Result<Vec<PendingUpload>, ObjectStoreError>> {
        Box::pin(async move {
            checked_list_prefix(key_or_prefix)?;
            let base = self.location.full_key("");
            let mut found: Vec<PendingUpload> = self
                .state()
                .uploads
                .iter()
                .filter_map(|(id, upload)| {
                    relative_key(&base, &upload.full_key).map(|key| PendingUpload {
                        key,
                        upload_id: id.clone(),
                    })
                })
                .filter(|pending| pending.key.starts_with(key_or_prefix))
                .collect();
            found.sort_by(|a, b| (&a.key, &a.upload_id).cmp(&(&b.key, &b.upload_id)));
            Ok(found)
        })
    }

    fn list_parts<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
    ) -> BoxFuture<'a, Result<Vec<UploadedPart>, ObjectStoreError>> {
        Box::pin(async move {
            let full = self.full_key(key);
            let state = self.state();
            match state.uploads.get(upload_id) {
                Some(upload) if upload.full_key == full => Ok(upload
                    .parts
                    .iter()
                    .map(|(number, body)| UploadedPart {
                        number: *number,
                        etag: part_etag(body),
                        size: body.len() as u64,
                    })
                    .collect()),
                _ => Err(ObjectStoreError::Rejected(format!(
                    "list parts: no upload {upload_id} of that key"
                ))),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MemoryObjectStore {
        MemoryObjectStore::new(StoreLocation::parse("memory://rec/calls").unwrap())
    }

    #[tokio::test]
    async fn memory_multipart_round_trips_and_lists() {
        let store = store();
        let id = store.create_upload("a/one.bin", "audio/wav").await.unwrap();
        let first = store
            .upload_part("a/one.bin", &id, 1, b"hello ".to_vec())
            .await
            .unwrap();
        let second = store
            .upload_part("a/one.bin", &id, 2, b"world".to_vec())
            .await
            .unwrap();
        assert_eq!(
            store.list_uploads("a/").await.unwrap(),
            vec![PendingUpload {
                key: "a/one.bin".into(),
                upload_id: id.clone()
            }]
        );
        let parts = store.list_parts("a/one.bin", &id).await.unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1].etag, second);
        let in_order = [
            UploadedPart {
                number: 1,
                etag: first,
                size: 6,
            },
            parts[1].clone(),
        ];
        store
            .complete_upload("a/one.bin", &id, &in_order)
            .await
            .unwrap();
        assert_eq!(
            store.get("a/one.bin").await.unwrap(),
            Some(b"hello world".to_vec())
        );
        assert_eq!(
            store.content_type("calls/a/one.bin").as_deref(),
            Some("audio/wav")
        );
        assert_eq!(store.pending_upload_count(), 0);
        assert_eq!(
            store.get_range("a/one.bin", 6, 100).await.unwrap(),
            Some(b"world".to_vec())
        );
        assert_eq!(
            store.get_range("a/one.bin", 50, 2).await.unwrap(),
            Some(Vec::new())
        );
        assert_eq!(store.get_range("missing", 0, 2).await.unwrap(), None);
        assert_eq!(
            store.list_objects("a/").await.unwrap(),
            vec![ListedObject {
                key: "a/one.bin".into(),
                bytes: 11
            }]
        );
    }

    #[tokio::test]
    async fn memory_abort_discards_and_a_wrong_etag_is_refused() {
        let store = store();
        let id = store.create_upload("b.bin", "x/y").await.unwrap();
        store
            .upload_part("b.bin", &id, 1, b"x".to_vec())
            .await
            .unwrap();
        let wrong = [UploadedPart {
            number: 1,
            etag: "\"nope\"".into(),
            size: 1,
        }];
        assert!(matches!(
            store.complete_upload("b.bin", &id, &wrong).await,
            Err(ObjectStoreError::Rejected(_))
        ));
        store.abort_upload("b.bin", &id).await.unwrap();
        store.abort_upload("b.bin", &id).await.unwrap();
        assert!(store.list_uploads("").await.unwrap().is_empty());
        assert!(store.get("b.bin").await.unwrap().is_none());
    }
}
