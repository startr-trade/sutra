//! The file outbound sink — [`FileMessageSink`] implements [`MessageSink`] for the `file://`
//! destination scheme: it writes [`OutboundMessage::body`] to a filesystem path.
//!
//! Destination shape: `file://<path>`. The `file://` prefix is stripped and the remainder is
//! taken as a filesystem path:
//! - a path that ends in `/` (or names an existing directory) is a SPOOL DIRECTORY — the file
//!   is written as `<dir>/<outbox_key>` (the outbox key is the file name, so the consumer side's
//!   idempotency token rides the name, symmetric with the inbound projection);
//! - otherwise the path IS the full target file.
//!
//! Parent directories are created as needed. A malformed destination (no `file://` scheme, empty
//! path) is a PERMANENT failure — a retry can never fix it; an IO error (disk full, permission)
//! is RETRYABLE — a later dispatcher tick may succeed. There is no network and no connection to
//! pool: the sink is stateless.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use sutra_channels::diag::Diagnostic;
use sutra_channels::sink::{BoxFuture, MessageSink, OutboundMessage, SendOutcome, SinkRegistry};

use super::codes;

/// The outbound file transport — stateless (no connection, no config).
#[derive(Debug, Default, Clone)]
pub struct FileMessageSink;

impl FileMessageSink {
    pub fn new() -> FileMessageSink {
        FileMessageSink
    }
}

impl MessageSink for FileMessageSink {
    fn schemes(&self) -> Vec<String> {
        vec!["file".to_string()]
    }

    fn send<'a>(&'a self, message: &'a OutboundMessage) -> BoxFuture<'a, SendOutcome> {
        Box::pin(async move {
            // Malformed destination — a retry can never fix it (poison posture).
            let path = match target_path(&message.destination, &message.outbox_key) {
                Ok(p) => p,
                Err(diagnostic) => return SendOutcome::PermanentFailure(diagnostic),
            };
            // Create the parent tree; an IO failure here is transient (retryable).
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                        return SendOutcome::RetryableFailure(Diagnostic::error(
                            codes::OUTBOUND_WRITE_FAILED,
                            format!("file sink could not create '{}': {e}", parent.display()),
                        ));
                    }
                }
            }
            match tokio::fs::write(&path, &message.body).await {
                Ok(()) => SendOutcome::Delivered,
                Err(e) => SendOutcome::RetryableFailure(Diagnostic::error(
                    codes::OUTBOUND_WRITE_FAILED,
                    format!("file sink write to '{}' failed: {e}", path.display()),
                )),
            }
        })
    }
}

/// Resolve a `file://<path>` destination + the row's outbox key into the on-disk target.
/// A path ending in `/` (or naming an existing directory) writes `<dir>/<outbox_key>`; otherwise
/// the path IS the file. Failures are PERMANENT (a malformed URI never becomes valid on retry).
fn target_path(destination: &str, outbox_key: &str) -> Result<PathBuf, Diagnostic> {
    let malformed = |detail: &str| {
        Diagnostic::error(
            codes::OUTBOUND_SEND_FAILED,
            format!("file destination '{destination}' {detail}"),
        )
    };
    let Some(rest) = destination.strip_prefix("file://") else {
        return Err(malformed("must start with the 'file://' scheme"));
    };
    if rest.is_empty() {
        return Err(malformed("has no path — expected file://<path>"));
    }
    let as_directory = rest.ends_with('/') || Path::new(rest).is_dir();
    if as_directory {
        if outbox_key.trim().is_empty() {
            return Err(malformed(
                "names a directory but the outbox key is empty — no file name to write",
            ));
        }
        Ok(Path::new(rest).join(outbox_key))
    } else {
        Ok(PathBuf::from(rest))
    }
}

/// Register the file sink into an outbox [`SinkRegistry`] under its claimed scheme (`file`).
pub fn register_file_sink(registry: &mut SinkRegistry) {
    registry.register(Arc::new(FileMessageSink::new()));
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    fn unique_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "sutra-file-sink-{tag}-{}-{nanos}",
            std::process::id()
        ));
        dir
    }

    fn message(destination: &str) -> OutboundMessage {
        OutboundMessage {
            destination: destination.to_string(),
            headers: BTreeMap::new(),
            body: b"hello-air-gapped".to_vec(),
            content_type: Some("application/octet-stream".to_string()),
            outbox_key: "out-key-1".to_string(),
            traceparent: None,
        }
    }

    #[tokio::test]
    async fn send_writes_the_body_to_a_directory_destination() {
        let dir = unique_dir("write");
        // A `file://<dir>/` destination writes `<dir>/<outbox_key>`.
        let destination = format!("file://{}/", dir.display());
        let sink = FileMessageSink::new();
        assert_eq!(
            sink.send(&message(&destination)).await,
            SendOutcome::Delivered
        );

        let written = std::fs::read(dir.join("out-key-1")).expect("the file must exist");
        assert_eq!(
            written, b"hello-air-gapped",
            "the body was written verbatim"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn send_writes_the_body_to_an_explicit_file_path() {
        let dir = unique_dir("explicit");
        let file = dir.join("reply.bin");
        let destination = format!("file://{}", file.display());
        let sink = FileMessageSink::new();
        assert_eq!(
            sink.send(&message(&destination)).await,
            SendOutcome::Delivered
        );
        assert_eq!(
            std::fs::read(&file).expect("file exists"),
            b"hello-air-gapped"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn malformed_destination_is_a_permanent_failure() {
        let sink = FileMessageSink::new();
        for bad in ["http://host/x", "not-a-uri", "file://", "kafka://topic"] {
            match sink.send(&message(bad)).await {
                SendOutcome::PermanentFailure(d) => {
                    assert_eq!(d.code, codes::OUTBOUND_SEND_FAILED, "for '{bad}'")
                }
                other => panic!("expected PermanentFailure for '{bad}', got {other:?}"),
            }
        }
    }

    #[test]
    fn sink_claims_only_the_file_scheme() {
        assert_eq!(FileMessageSink::new().schemes(), vec!["file"]);
    }
}
