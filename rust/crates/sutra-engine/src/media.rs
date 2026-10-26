//! Named object stores, and the recorded media erasure and retention delete.
//!
//! The stores are engine configuration (`sutra.object-stores.<name>.*`), installed once at boot
//! into the process-wide [`sutra_objectstore::registry`]: an `llm` channel reads media references
//! from the one its `llm.media-store` names, and the two deletion paths below find the store a
//! recording lives in from the reference's URI.
//!
//! A `<q:audit capture="media">` process records, on its audit events, references
//! (`{"media": [{kind: "media", uri, sha256, …}]}`) to the media a transport stored for it.
//! When the instance's data has to go — a data subject's erasure, or the end of terminal
//! retention — the referenced objects are deleted and each event's payload is amended to say so:
//! `{"media": [{kind, uri, sha256, bytes, erased: true}], "mediaErasure": {at, reason,
//! objects}}`. A reference whose object cannot be deleted (no declared store holds it, or the
//! store refused) is never dropped silently: erasure fails and can be re-run, and retention keeps
//! the instance and retries on its next sweep.

use serde_json::{json, Value};
use sutra_objectstore::{registry, NamedStore, StoreLocation, StoreSettings};

use crate::config::ObjectStoreConfig;

/// The payload key that marks a media event whose media has been erased.
pub const MEDIA_ERASURE_KEY: &str = "mediaErasure";
/// `mediaErasure.reason` for a data subject's erasure.
pub const ERASED_FOR_SUBJECT: &str = "subject-erasure";
/// `mediaErasure.reason` for the end of terminal retention.
pub const ERASED_FOR_RETENTION: &str = "retention";

/// Resolve each declared store's secret references and install the stores (boot). Any store
/// that cannot be built refuses startup.
pub fn install_object_stores(configs: &[ObjectStoreConfig]) -> Result<(), String> {
    let resolve = |name: &str, field: &str, reference: &Option<String>| {
        reference
            .as_deref()
            .map(|r| {
                crate::envref::resolve_value(r)
                    .map_err(|e| format!("sutra.object-stores.{name}.{field}: {e}"))
            })
            .transpose()
    };
    let mut stores = Vec::new();
    for config in configs {
        let location = StoreLocation::parse(&config.uri)
            .map_err(|e| format!("sutra.object-stores.{}.uri: {e}", config.name))?;
        let settings = StoreSettings {
            name: config.name.clone(),
            location,
            endpoint: config.endpoint.clone(),
            region: config.region.clone(),
            access_key: resolve(&config.name, "access-key", &config.access_key_ref)?,
            secret_key: resolve(&config.name, "secret-key", &config.secret_key_ref)?,
            kek_secret: resolve(&config.name, "kek", &config.kek_ref)?,
            key_id: config.key_id.clone(),
        };
        stores.push(
            NamedStore::open(&settings)
                .map_err(|e| format!("sutra.object-stores.{}: {e}", config.name))?,
        );
    }
    registry::install(stores);
    Ok(())
}

/// Delete every object the media payload `payload` references (references already marked
/// erased are skipped) and return the amended payload. `Err` lists, per reference, why its
/// object could not be deleted; nothing is amended then.
pub async fn erase_media(payload: &str, reason: &str) -> Result<String, Vec<String>> {
    let value: Value = serde_json::from_str(payload).unwrap_or(Value::Null);
    let references: Vec<Value> = value
        .get("media")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut failures = Vec::new();
    let mut amended = Vec::new();
    for reference in &references {
        let Some(uri) = reference.get("uri").and_then(Value::as_str) else {
            continue;
        };
        if reference.get("erased") != Some(&Value::Bool(true)) {
            match registry::for_uri(uri) {
                None => failures.push(format!(
                    "{uri}: no declared object store (sutra.object-stores.<name>) holds it"
                )),
                Some((store, object)) => {
                    if let Err(e) = store.delete(&object).await {
                        failures.push(format!("{uri}: {e}"));
                    }
                }
            }
        }
        let mut kept = json!({"kind": "media", "uri": uri, "erased": true});
        for field in ["sha256", "bytes"] {
            if let Some(v) = reference.get(field) {
                kept[field] = v.clone();
            }
        }
        amended.push(kept);
    }
    if !failures.is_empty() {
        return Err(failures);
    }
    let at = time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();
    Ok(json!({
        "media": amended,
        MEDIA_ERASURE_KEY: {"at": at, "reason": reason, "objects": amended.len()},
    })
    .to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sutra_objectstore::{MemoryObjectStore, ObjectStore};

    use super::*;

    fn store(name: &str, uri: &str) -> MemoryObjectStore {
        let location = StoreLocation::parse(uri).unwrap();
        let raw = MemoryObjectStore::new(location.clone());
        registry::register(NamedStore::new(
            name,
            Arc::new(raw.clone()),
            location,
            None,
            "recordings",
        ));
        raw
    }

    #[tokio::test]
    async fn erasing_deletes_the_objects_and_marks_the_payload() {
        let raw = store("media-erase-ok", "memory://media-erase-ok/calls");
        let a = raw
            .put("c1.wav", b"one".to_vec(), "audio/wav")
            .await
            .unwrap();
        let b = raw
            .put("c2.wav", b"two".to_vec(), "audio/wav")
            .await
            .unwrap();
        let payload = json!({"media": [
            {"kind": "media", "uri": a.uri, "sha256": a.sha256, "bytes": 3, "callId": "c1"},
            {"kind": "media", "uri": b.uri, "sha256": b.sha256},
        ]})
        .to_string();
        let amended = erase_media(&payload, ERASED_FOR_RETENTION).await.unwrap();
        assert!(raw.keys().is_empty(), "both objects deleted");
        assert!(amended.starts_with("{\"media\":"), "{amended}");
        let amended: Value = serde_json::from_str(&amended).unwrap();
        assert_eq!(
            amended["media"][0],
            json!({"kind": "media", "uri": a.uri, "sha256": a.sha256, "bytes": 3, "erased": true})
        );
        assert_eq!(amended[MEDIA_ERASURE_KEY]["reason"], ERASED_FOR_RETENTION);
        assert_eq!(amended[MEDIA_ERASURE_KEY]["objects"], 2);
        // Erasing again is harmless: erased references are skipped.
        assert!(erase_media(&amended.to_string(), ERASED_FOR_RETENTION)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn a_reference_no_declared_store_holds_is_reported_not_dropped() {
        let raw = store("media-erase-partial", "memory://media-erase-partial");
        let a = raw
            .put("c1.wav", b"one".to_vec(), "audio/wav")
            .await
            .unwrap();
        let payload = json!({"media": [
            {"kind": "media", "uri": a.uri, "sha256": a.sha256},
            {"kind": "media", "uri": "s3://media-erase-undeclared/c.wav", "sha256": "00"},
        ]})
        .to_string();
        let failures = erase_media(&payload, ERASED_FOR_SUBJECT).await.unwrap_err();
        assert_eq!(failures.len(), 1);
        assert!(
            failures[0].contains("media-erase-undeclared"),
            "{failures:?}"
        );
    }
}
