//! File transport — the spool pair behind the transport seams:
//! [`source::FileTriggerSource`] implements [`sutra_channels::source::TriggerSource`] (an inbound
//! consumer that polls a spool DIRECTORY, leader-gated for `singleton: true` channels) and
//! [`sink::FileMessageSink`] implements [`sutra_channels::sink::MessageSink`] for the `file://`
//! destination scheme.
//!
//! There is no broker and no wire: the "delivery" is a file appearing in the spool directory,
//! the "publish" is a file written to a `file://` path. Because a file is either fully present
//! or not, the inbound path is at-least-once by construction — a claimed file rides through a
//! `.processing/` staging subdir, then lands in `.done/` (ack) or `.failed/` (drop), or is put
//! back for a later poll (requeue). Idempotency rides the FILE NAME (explicit event id), so
//! inbox dedup absorbs any redelivery the same way it does for a broker.

pub mod sink;
pub mod source;

pub use sink::{register_file_sink, FileMessageSink};
pub use source::{FileSourceConfig, FileTriggerSource};

use std::path::PathBuf;
use std::time::Duration;

use sutra_channels::config::ChannelDefinition;
use sutra_channels::diag::Diagnostic;

/// Stable diagnostic-code strings — the exact file diagnostic codes this module raises.
pub mod codes {
    /// An inbound file channel was authored without a spool directory
    /// (`spool.dir` / `directory`), or with a non-positive poll interval — fail-closed at wiring.
    pub const INBOUND_CONFIG_INVALID: &str = "SUTRA.INBOUND.FILE.CONFIG_INVALID";
    /// A spooled file could not be read / staged / moved during a poll (WARN + skip; the file
    /// is left where it is and a later poll retries — the poll loop never dies on one bad file).
    pub const INBOUND_READ_FAILED: &str = "SUTRA.INBOUND.FILE.READ_FAILED";

    /// An outbound `file://` destination is malformed — a retry can never fix it (permanent).
    pub const OUTBOUND_SEND_FAILED: &str = "SUTRA.OUTBOUND.FILE.SEND_FAILED";
    /// Writing the outbound file failed at the filesystem (disk full, permission) — retryable.
    pub const OUTBOUND_WRITE_FAILED: &str = "SUTRA.OUTBOUND.FILE.WRITE_FAILED";
}

/// The channel `transport:` value this module serves.
pub const TRANSPORT: &str = "file";

/// The spool-root subdirectory a claimed file is staged into for the duration of one delivery,
/// so a concurrent poll (or an overlapping tick) never double-delivers it.
pub const SUBDIR_PROCESSING: &str = ".processing";
/// Where an acked file lands.
pub const SUBDIR_DONE: &str = ".done";
/// Where a dropped (NackDrop) file lands.
pub const SUBDIR_FAILED: &str = ".failed";

/// Effective ack modes of a file channel — parsed leniently, mirroring the broker
/// transports: `on-complete` (ASCII case-insensitive) opts in, anything else is `on-persist`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    /// Move the staged file to its terminal subdir as soon as the intake made the delivery
    /// durable (default) — the decision the `deliver` call answers with.
    OnPersist,
    /// Hold the staged file in `.processing/` until the instance's terminal event: COMPLETED
    /// moves it to `.done/`, FAILED (and registry timeout/overflow) to `.failed/`.
    OnComplete,
}

impl AckMode {
    fn parse(raw: Option<&str>) -> AckMode {
        match raw {
            Some(v) if v.trim().eq_ignore_ascii_case("on-complete") => AckMode::OnComplete,
            _ => AckMode::OnPersist,
        }
    }
}

/// Typed view over a file channel's transport-specific properties. Derives `Eq` so the manager
/// can fingerprint a running consumer against a flipped-in definition exactly like the brokers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileChannelProperties {
    /// The spool directory watched inbound (channel property `spool.dir`, alias `directory`).
    /// REQUIRED — a file channel with no spool dir fails closed at wiring
    /// ([`codes::INBOUND_CONFIG_INVALID`]).
    pub spool_dir: PathBuf,
    /// How often the source lists the spool directory (`poll.interval.ms`, default 1000ms).
    pub poll_interval: Duration,
    /// Per-channel singleton declaration (`singleton`, default **true**): a spool must be drained
    /// by exactly one replica, so unlike the engine-wide `ChannelDefinition::singleton` (which
    /// defaults false) a file channel is singleton unless the author opts out with
    /// `singleton: false`.
    pub singleton: bool,
    /// Engine-level ack semantics (`ack-mode`, default `on-persist`) — `on-complete` defers the
    /// terminal file move to the instance's terminal event.
    pub ack_mode: AckMode,
}

impl FileChannelProperties {
    pub const DEFAULT_POLL_INTERVAL_MS: u64 = 1000;

    /// Read the typed properties off a channel definition. Fails closed
    /// ([`codes::INBOUND_CONFIG_INVALID`]) when no spool directory is declared, or when
    /// `poll.interval.ms` is present but not a positive integer.
    pub fn from_definition(def: &ChannelDefinition) -> Result<FileChannelProperties, Diagnostic> {
        let props = &def.properties;
        let channel = &def.binding.channel_name;

        let spool_dir =
            non_blank(props.get("spool.dir")).or_else(|| non_blank(props.get("directory")));
        let Some(spool_dir) = spool_dir else {
            return Err(Diagnostic::error(
                codes::INBOUND_CONFIG_INVALID,
                format!(
                    "file channel '{channel}' requires a spool directory \
                     (property 'spool.dir' or 'directory')"
                ),
            ));
        };

        let poll_interval = match non_blank(props.get("poll.interval.ms")) {
            None => Duration::from_millis(Self::DEFAULT_POLL_INTERVAL_MS),
            Some(raw) => match raw.parse::<u64>() {
                Ok(ms) if ms > 0 => Duration::from_millis(ms),
                _ => {
                    return Err(Diagnostic::error(
                        codes::INBOUND_CONFIG_INVALID,
                        format!(
                            "file channel '{channel}' property 'poll.interval.ms' must be a \
                             positive integer, got '{raw}'"
                        ),
                    ))
                }
            },
        };

        // Default TRUE (a spool is drained by one replica); only an explicit non-`true` opts out.
        let singleton = match non_blank(props.get("singleton")) {
            None => true,
            Some(raw) => raw.eq_ignore_ascii_case("true"),
        };

        Ok(FileChannelProperties {
            spool_dir: PathBuf::from(spool_dir),
            poll_interval,
            singleton,
            ack_mode: AckMode::parse(props.get("ack-mode").map(String::as_str)),
        })
    }

    /// True when a spool directory is declared.
    pub fn has_spool_dir(&self) -> bool {
        !self.spool_dir.as_os_str().is_empty()
    }
}

fn non_blank(value: Option<&String>) -> Option<String> {
    value
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sutra_channels::config::{ChannelBinding, Namespace};
    use sutra_channels::DeploymentId;

    fn definition(props: &[(&str, &str)]) -> ChannelDefinition {
        let namespace = Namespace::new("acme", "orders", "v1");
        let binding = ChannelBinding::new("spool-in", namespace, DeploymentId::unresolved(), "");
        ChannelDefinition {
            binding,
            transport: Some("file".to_string()),
            bind_spec: None,
            codec: None,
            cloud_events_mode: None,
            auth_scheme: None,
            idempotency_key_header: None,
            payload_cap_bytes: None,
            properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn spool_dir_and_defaults() {
        let def = definition(&[("spool.dir", "/var/spool/in")]);
        let props = FileChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.spool_dir, PathBuf::from("/var/spool/in"));
        assert_eq!(props.poll_interval, Duration::from_millis(1000));
        assert!(props.singleton, "file channels default to singleton");
        assert_eq!(
            props.ack_mode,
            AckMode::OnPersist,
            "brokers/spools default on-persist"
        );
        assert!(props.has_spool_dir());
    }

    #[test]
    fn ack_mode_on_complete_opts_in_case_insensitively() {
        // a declared `ack-mode: on-complete` defers the terminal move; anything else
        // (including a typo) stays on-persist — the lenient broker-transport parse.
        for raw in ["on-complete", "On-Complete", " ON-COMPLETE "] {
            let def = definition(&[("spool.dir", "/x"), ("ack-mode", raw)]);
            let props = FileChannelProperties::from_definition(&def).expect("props");
            assert_eq!(props.ack_mode, AckMode::OnComplete, "for '{raw}'");
        }
        for raw in ["on-persist", "oncomplete", ""] {
            let def = definition(&[("spool.dir", "/x"), ("ack-mode", raw)]);
            let props = FileChannelProperties::from_definition(&def).expect("props");
            assert_eq!(props.ack_mode, AckMode::OnPersist, "for '{raw}'");
        }
    }

    #[test]
    fn directory_alias_and_overrides() {
        let def = definition(&[
            ("directory", "/data/incoming"),
            ("poll.interval.ms", "250"),
            ("singleton", "false"),
        ]);
        let props = FileChannelProperties::from_definition(&def).expect("props");
        assert_eq!(props.spool_dir, PathBuf::from("/data/incoming"));
        assert_eq!(props.poll_interval, Duration::from_millis(250));
        assert!(!props.singleton, "singleton: false opts out");
    }

    #[test]
    fn missing_spool_dir_fails_closed() {
        let def = definition(&[("poll.interval.ms", "100")]);
        let err = FileChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }

    #[test]
    fn invalid_poll_interval_fails_closed() {
        let def = definition(&[("spool.dir", "/x"), ("poll.interval.ms", "not-a-number")]);
        let err = FileChannelProperties::from_definition(&def).unwrap_err();
        assert_eq!(err.code, codes::INBOUND_CONFIG_INVALID);
    }
}
