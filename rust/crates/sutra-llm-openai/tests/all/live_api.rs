//! The OpenAI Realtime API through the real adapter: the WebSocket handshake (TLS included), the
//! session configuration, the sample WAV up, audio and transcripts back, a clean close; and,
//! through a logging hop, that no URL the adapter builds carries the key — it travels in the
//! `Authorization` header and nowhere else.
//!
//! ENVIRONMENT
//! - `OPENAI_API_KEY` (required): never printed; a failure reports OpenAI's answer, which never
//!   echoes it whole.
//! - `OPENAI_REALTIME_MODEL` (optional): the Realtime model id, default [`DEFAULT_REALTIME_MODEL`]
//!   (the model the llm-channels guide's live-session example names). When OpenAI retires or
//!   renames it, the handshake is refused with a `model_not_found`-style reason; set this to a
//!   current Realtime model from your account's model list.
//! - `OPENAI_TRANSCRIBE_MODEL` (optional): the model that transcribes the caller's speech inside
//!   the session, default [`DEFAULT_TRANSCRIBE_MODEL`] (the example's `transcribe-model`).
//! - `OPENAI_REALTIME_WAV` (optional): a 16 kHz mono 16-bit PCM WAV to send instead of the
//!   example's `spoken-question.wav` ("What is the capital of France?").
//!
//! The sample is sent as headerless 16 kHz L16 frames: the adapter takes L16 at any rate (or G.711
//! at 8 kHz, passed through) and resamples to the API's 24 kHz itself, so the WAV container is the
//! only thing cut away here. The answer is asked for as 16 kHz L16, so the resampling runs both
//! ways.
//!
//! Each test prints one `LIVE:` line (what was heard and said, how much audio came back), which the
//! live-check script shows as the reason of its PASS / FAIL line.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sutra_llm_spi::{
    find_provider, AudioFormat, AudioSession, AudioSessionConfig, AudioSessionEvent, LlmProvider,
    LlmProviderConfig, TurnDetection,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls;

/// The Realtime model the llm-channels guide's live-session example uses.
const DEFAULT_REALTIME_MODEL: &str = "gpt-realtime";
/// The example's `transcribe-model` (examples/ai-advanced/variants/live/channels.yaml).
const DEFAULT_TRANSCRIBE_MODEL: &str = "gpt-transcribe";
/// Where the OpenAI API lives.
const OPENAI_HOST: &str = "api.openai.com";
/// The rate the sample is recorded at, and the rate the answer is asked for.
const RATE: u32 = 16000;
/// 100 ms of 16-bit mono PCM at [`RATE`].
const CHUNK: usize = (RATE as usize / 10) * 2;

/// The key, or `None` with a notice when it is unset. The tests are `#[ignore = "live"]`, and the
/// tier-2 sweep (`make test-docker`) runs every ignored test, so a run without a key skips them
/// instead of failing. `examples/ai-advanced/live-check.sh` runs them only when the key is set,
/// so a skip never reaches its report.
fn api_key() -> Option<String> {
    match std::env::var("OPENAI_API_KEY") {
        Ok(key) if !key.trim().is_empty() => Some(key),
        _ => {
            eprintln!("SKIP: set OPENAI_API_KEY to run the live OpenAI tests");
            None
        }
    }
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|m| !m.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn realtime_model() -> String {
    env_or("OPENAI_REALTIME_MODEL", DEFAULT_REALTIME_MODEL)
}

fn transcribe_model() -> String {
    env_or("OPENAI_TRANSCRIBE_MODEL", DEFAULT_TRANSCRIBE_MODEL)
}

/// The provider exactly as a channel candidate builds it: the registered `openai` factory.
fn provider(base_url: Option<String>, key: &str) -> Arc<dyn LlmProvider> {
    // Naming a public item keeps the crate (and its factory registration) linked in.
    let _ = sutra_llm_openai::realtime::REALTIME_PCM_RATE;
    let factory = find_provider("openai").expect("the openai provider is registered");
    (factory.build)(&LlmProviderConfig {
        model: realtime_model(),
        base_url,
        api_key: Some(key.to_string()),
    })
    .unwrap_or_else(|d| panic!("build the openai provider: {d:?}"))
}

fn session_config(output: Option<AudioFormat>) -> AudioSessionConfig {
    AudioSessionConfig {
        model: realtime_model(),
        instructions: Some("Answer the spoken question in one short sentence.".into()),
        input: AudioFormat::L16 {
            rate: RATE,
            channels: 1,
        },
        output,
        voice: None,
        transcription_model: Some(transcribe_model()),
        // Each round trip sets it: server voice-activity detection, or manual turns.
        turn_detection: TurnDetection::Manual,
        transcription_only: false,
        language: None,
        connect_deadline: Duration::from_secs(30),
    }
}

/// The PCM samples of a 16 kHz, mono, 16-bit WAV: the adapter takes headerless L16 frames, not a
/// WAV container, so the `data` chunk is cut out here.
fn sample_pcm() -> Vec<u8> {
    let path = std::env::var("OPENAI_REALTIME_WAV")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../examples/ai-advanced/sample/spoken-question.wav")
        });
    let wav = std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    assert!(
        wav.len() > 12 && &wav[0..4] == b"RIFF" && &wav[8..12] == b"WAVE",
        "{} is not a WAV file",
        path.display()
    );
    let mut at = 12;
    let mut format_ok = false;
    while at + 8 <= wav.len() {
        let id = &wav[at..at + 4];
        let size = u32::from_le_bytes(wav[at + 4..at + 8].try_into().unwrap()) as usize;
        let body = &wav[at + 8..(at + 8 + size).min(wav.len())];
        if id == b"fmt " {
            let tag = u16::from_le_bytes([body[0], body[1]]);
            let channels = u16::from_le_bytes([body[2], body[3]]);
            let rate = u32::from_le_bytes(body[4..8].try_into().unwrap());
            let bits = u16::from_le_bytes([body[14], body[15]]);
            assert!(
                tag == 1 && channels == 1 && rate == RATE && bits == 16,
                "the sample must be 16 kHz mono 16-bit PCM (it is {rate} Hz, {channels} ch, {bits} bit, tag {tag})"
            );
            format_ok = true;
        } else if id == b"data" {
            assert!(format_ok, "the WAV has no fmt chunk before its data");
            return body.to_vec();
        }
        at += 8 + size + (size & 1);
    }
    panic!("{} has no data chunk", path.display());
}

/// What one session turn produced.
#[derive(Default, Debug)]
struct Turn {
    heard: String,
    said: String,
    audio_bytes: usize,
    errors: Vec<String>,
    ended: bool,
}

impl Turn {
    fn take(&mut self, event: AudioSessionEvent) {
        match event {
            // OpenAI sends the completed transcript (and, for some models, deltas first): the
            // final text replaces whatever the deltas built.
            AudioSessionEvent::InputTranscript { text, is_final, .. } => {
                if is_final {
                    self.heard = text;
                } else {
                    self.heard.push_str(&text);
                }
            }
            AudioSessionEvent::OutputTranscript { text, is_final, .. }
            | AudioSessionEvent::OutputText { text, is_final, .. } => {
                if is_final {
                    self.said = text;
                } else {
                    self.said.push_str(&text);
                }
            }
            AudioSessionEvent::OutputAudio { data, .. } => self.audio_bytes += data.len(),
            AudioSessionEvent::Error { detail } => self.errors.push(detail),
            AudioSessionEvent::TurnEnded => self.ended = true,
            _ => {}
        }
    }
}

async fn collect_turn(session: &dyn AudioSession, wait: Duration) -> Turn {
    let mut turn = Turn::default();
    let deadline = tokio::time::Instant::now() + wait;
    while !turn.ended {
        match tokio::time::timeout_at(deadline, session.next_event()).await {
            Ok(None | Some(AudioSessionEvent::Closed)) | Err(_) => return turn,
            Ok(Some(event)) => turn.take(event),
        }
    }
    // The caller's transcript is produced alongside the answer and may land after it.
    let grace = tokio::time::Instant::now() + Duration::from_secs(5);
    while turn.heard.trim().is_empty() {
        match tokio::time::timeout_at(grace, session.next_event()).await {
            Ok(None | Some(AudioSessionEvent::Closed)) | Err(_) => break,
            Ok(Some(event)) => turn.take(event),
        }
    }
    turn
}

/// Drain the session after `close`: it must end with `Closed`, promptly.
async fn closes_cleanly(session: &dyn AudioSession) -> bool {
    session.close().await;
    let drained = tokio::time::timeout(Duration::from_secs(10), async {
        let mut last = None;
        while let Some(event) = session.next_event().await {
            last = Some(event);
        }
        last
    })
    .await;
    matches!(drained, Ok(Some(AudioSessionEvent::Closed)))
}

fn one_line(text: &str) -> String {
    let text = text.replace("\\n", " ");
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn shorten(text: &str) -> String {
    let text = text.trim();
    if text.chars().count() > 80 {
        format!("{}…", text.chars().take(80).collect::<String>())
    } else {
        text.to_string()
    }
}

/// A refusal's reason, with the variable to set when it is about a model id.
fn refusal(failure: &str) -> String {
    let lower = failure.to_lowercase();
    let hint = if lower.contains("model") && (lower.contains("not") || lower.contains("exist")) {
        format!(
            " — the model was not found: set OPENAI_REALTIME_MODEL (now {}) or \
             OPENAI_TRANSCRIBE_MODEL (now {}) to one your account offers",
            realtime_model(),
            transcribe_model()
        )
    } else {
        String::new()
    };
    format!("{}{hint}", one_line(failure))
}

/// Open a session through the real adapter and the real endpoint (TLS included), send the
/// sample question at real time followed by 1.5 s of silence, and receive the spoken answer
/// and its transcripts; then close. With manual turns a commit closes the question and asks for
/// the answer; with server detection OpenAI's voice-activity detection does both. The answer
/// must be to the whole question (it names Paris), not to a fragment of it.
async fn round_trip(detection: TurnDetection, label: &str) {
    let Some(key) = api_key() else { return };
    let pcm = sample_pcm();
    let output = AudioFormat::L16 {
        rate: RATE,
        channels: 1,
    };
    let provider = provider(None, &key);
    let mut config = session_config(Some(output));
    config.turn_detection = detection;
    let session = match provider.open_audio_session(&config).await {
        Ok(session) => session,
        Err(failure) => {
            let reason = refusal(&format!("{failure:?}"));
            assert!(!reason.contains(key.as_str()), "a failure carries the key");
            println!("LIVE: session refused: {reason}");
            panic!("the realtime session did not open");
        }
    };
    assert_eq!(session.next_event().await, Some(AudioSessionEvent::Ready));
    for chunk in pcm.chunks(CHUNK) {
        session.send_audio(chunk).await.expect("send audio");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let silence = vec![0u8; CHUNK];
    for _ in 0..15 {
        session.send_audio(&silence).await.expect("send silence");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if detection == TurnDetection::Manual {
        session.commit().await.expect("commit the question");
    }
    let turn = collect_turn(session.as_ref(), Duration::from_secs(60)).await;
    let closed = closes_cleanly(session.as_ref()).await;
    let audio_ms = turn.audio_bytes * 1000 / (RATE as usize * 2);
    let on_topic = format!("{} {}", turn.heard, turn.said)
        .to_lowercase()
        .contains("paris");
    let errors = if turn.errors.is_empty() {
        String::new()
    } else {
        format!(", errors: {}", refusal(&format!("{:?}", turn.errors)))
    };
    println!(
        "LIVE: model {} ({label}) heard \"{}\", said \"{}\", {audio_ms} ms of audio back, turn ended: {}, \
         answered the question: {on_topic}, closed cleanly: {closed}{errors}",
        realtime_model(),
        shorten(&turn.heard),
        shorten(&turn.said),
        turn.ended,
    );
    assert!(
        turn.audio_bytes > 0 || !turn.said.trim().is_empty(),
        "no audio and no answer transcript came back: {turn:?}"
    );
    assert!(turn.ended, "the answer never finished (no response.done)");
    assert!(
        turn.errors.is_empty(),
        "the session reported errors: {:?}",
        turn.errors
    );
    assert!(
        on_topic,
        "the answer does not answer the sample question (\"What is the capital of France?\")"
    );
    assert!(closed, "the session did not close cleanly");
}

/// Manual turns: the whole question is one turn.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live"]
async fn live_session_answers_the_sample_wav() {
    round_trip(TurnDetection::Manual, "manual turns").await;
}

/// OpenAI's own voice-activity detection ends the turn and asks for the answer.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live"]
async fn live_session_answers_with_server_turn_detection() {
    round_trip(TurnDetection::Server, "server turn detection").await;
}

/// What the logging hop saw: each connection's request head, as the adapter sent it.
#[derive(Default)]
struct HopLog {
    heads: Vec<String>,
}

/// Read one request head (up to and including the blank line) from `client`; the bytes after it
/// that arrived in the same reads are returned too.
async fn read_head(client: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        let n = client.read(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        raw.extend_from_slice(&buf[..n]);
        if let Some(end) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&raw[..end + 4]).to_string();
            return Some((head, raw[end + 4..].to_vec()));
        }
        if raw.len() > 64 * 1024 {
            return None;
        }
    }
}

/// A plain-HTTP hop in front of the real endpoint: each connection's request head is logged, its
/// `Host` rewritten, and the connection forwarded over TLS to OpenAI, both ways, unchanged. The
/// adapter is pointed at the hop (`base-url`), so every URL it builds and every header it sends is
/// visible here.
async fn logging_hop() -> (String, Arc<Mutex<HopLog>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let log = Arc::new(Mutex::new(HopLog::default()));
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    let tls = Arc::new(
        rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth(),
    );
    let seen = log.clone();
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            let seen = seen.clone();
            let tls = tls.clone();
            tokio::spawn(async move {
                let Some((head, rest)) = read_head(&mut client).await else {
                    return;
                };
                seen.lock().unwrap().heads.push(head.clone());
                let rewritten: String = head
                    .split_inclusive("\r\n")
                    .map(|line| {
                        if line.to_ascii_lowercase().starts_with("host:") {
                            format!("host: {OPENAI_HOST}\r\n")
                        } else {
                            line.to_string()
                        }
                    })
                    .collect();
                let Ok(tcp) = TcpStream::connect((OPENAI_HOST, 443)).await else {
                    return;
                };
                let name = rustls::pki_types::ServerName::try_from(OPENAI_HOST).unwrap();
                let Ok(mut upstream) = tokio_rustls::TlsConnector::from(tls)
                    .connect(name, tcp)
                    .await
                else {
                    return;
                };
                if upstream.write_all(rewritten.as_bytes()).await.is_err()
                    || upstream.write_all(&rest).await.is_err()
                {
                    return;
                }
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    (base, log)
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.trim()
            .eq_ignore_ascii_case(name)
            .then_some(value.trim())
    })
}

/// Through the logging hop: the WebSocket handshake carries the key in `Authorization: Bearer …`
/// and nowhere else; no request line carries the key or any query credential; a refusal's
/// reason never carries it either. The session must then open (`session.updated`) and close
/// cleanly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live"]
async fn live_urls_carry_no_key() {
    let Some(key) = api_key() else { return };
    let (base, log) = logging_hop().await;
    let provider = provider(Some(base), &key);
    let opened = provider.open_audio_session(&session_config(None)).await;
    let closed = match &opened {
        Ok(session) => closes_cleanly(session.as_ref()).await,
        Err(_) => false,
    };
    let heads = log.lock().unwrap().heads.clone();
    let lines: Vec<&str> = heads.iter().filter_map(|h| h.lines().next()).collect();
    for line in &lines {
        assert!(
            !line.contains(key.as_str()),
            "a request URL carries the key"
        );
        let lower = line.to_ascii_lowercase();
        for parameter in ["key=", "api_key=", "token=", "authorization="] {
            assert!(
                !lower.contains(parameter),
                "a request URL has a credential parameter ({parameter})"
            );
        }
    }
    let socket_head = heads.iter().find(|h| {
        h.lines()
            .next()
            .is_some_and(|l| l.starts_with("GET ") && l.contains("/v1/realtime?model="))
    });
    let reason = match &opened {
        Ok(_) => format!("opened and closed cleanly: {closed}"),
        Err(failure) => format!("refused: {}", refusal(&format!("{failure:?}"))),
    };
    assert!(
        !reason.contains(key.as_str()),
        "the failure the adapter reports carries the key"
    );
    println!(
        "LIVE: {} request(s) seen [{}], the key only in the Authorization header, session {reason}",
        lines.len(),
        lines
            .iter()
            .map(|l| l.split(' ').take(2).collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join("; "),
    );
    let head = socket_head.expect("the adapter never opened the WebSocket");
    // Plain `assert!`s: an `assert_eq!` would print the header, and with it the key.
    assert!(
        header(head, "authorization") == Some(format!("Bearer {key}").as_str()),
        "the key does not travel as Authorization: Bearer <key>"
    );
    assert!(
        head.matches(key.as_str()).count() == 1,
        "the key appears in the handshake outside the Authorization header"
    );
    let session = opened.unwrap_or_else(|_| panic!("the realtime session did not open"));
    drop(session);
    assert!(closed, "the session did not close cleanly");
}
