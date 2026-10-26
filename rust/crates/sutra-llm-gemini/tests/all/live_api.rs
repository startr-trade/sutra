//! Gemini Live through the real adapter: the ephemeral token, the constrained WebSocket, the
//! sample WAV up, audio and transcripts back, a clean close; and, through a logging hop, that no
//! URL the adapter builds carries the key or the token.
//!
//! ENVIRONMENT
//! - `GEMINI_API_KEY` (required): never printed; a failure reports Google's answer, which never
//!   echoes it.
//! - `GEMINI_LIVE_MODEL` (optional): the Live model id, default [`DEFAULT_LIVE_MODEL`] (the model the
//!   llm-channels guide's live-session example names). When Google retires or renames it, the
//!   session is refused with a "model not found"-style reason; set this to a current Live model
//!   from Google's model list (for example a `gemini-*-native-audio-*` id).
//! - `GEMINI_LIVE_WAV` (optional): a 16-bit PCM WAV to send instead of the example's
//!   `spoken-question.wav` ("What is the capital of France?").
//!
//! Each test prints one `LIVE:` line (what was heard and said, how much audio came back), which
//! the live-check script shows as the reason of its PASS / FAIL line.

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

/// The Live model the llm-channels guide's live-session example uses.
const DEFAULT_LIVE_MODEL: &str = "gemini-3.8-live";
/// The model the server-turn-detection round trip uses by default. The sample question is
/// synthetic speech, and gemini-3.8-live's voice-activity detection does not take most of it for
/// speech (it hears "What" and answers that), while it handles a real voice; this model handles
/// both. Set GEMINI_LIVE_MODEL (and GEMINI_LIVE_WAV to a recording of your own) to check another.
const DEFAULT_SERVER_DETECTION_MODEL: &str = "gemini-2.5-flash-native-audio-latest";
/// Where the Gemini Developer API lives.
const GOOGLE_HOST: &str = "generativelanguage.googleapis.com";
/// The rate the sample is recorded at, and the Live API's input rate.
const RATE: u32 = 16000;
/// 100 ms of 16-bit mono PCM at [`RATE`].
const CHUNK: usize = (RATE as usize / 10) * 2;
/// How long a session must stay quiet after a turn before the question counts as answered.
const QUIET_AFTER_TURN: Duration = Duration::from_secs(6);

/// The key, or `None` with a notice when it is unset. The tests are `#[ignore = "live"]`, and
/// the tier-2 sweep (`make test-docker`) runs every ignored test, so a run without a key skips
/// them instead of failing. `examples/ai-advanced/live-check.sh` runs them only when the key is
/// set, so a skip never reaches its report.
fn api_key() -> Option<String> {
    match std::env::var("GEMINI_API_KEY") {
        Ok(key) if !key.trim().is_empty() => Some(key),
        _ => {
            eprintln!("SKIP: set GEMINI_API_KEY to run the live Gemini tests");
            None
        }
    }
}

fn live_model() -> String {
    model_or(DEFAULT_LIVE_MODEL)
}

/// `GEMINI_LIVE_MODEL` when set, else `default`.
fn model_or(default: &str) -> String {
    std::env::var("GEMINI_LIVE_MODEL")
        .ok()
        .filter(|m| !m.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// The provider exactly as a channel candidate builds it: the registered `gemini` factory.
fn provider(base_url: Option<String>, key: &str, model: &str) -> Arc<dyn LlmProvider> {
    // Naming a public item keeps the crate (and its factory registration) linked in.
    let _ = sutra_llm_gemini::LIVE_INPUT_RATE;
    let factory = find_provider("gemini").expect("the gemini provider is registered");
    (factory.build)(&LlmProviderConfig {
        model: model.to_string(),
        base_url,
        api_key: Some(key.to_string()),
    })
    .unwrap_or_else(|d| panic!("build the gemini provider: {d:?}"))
}

fn session_config(output: Option<AudioFormat>, model: &str) -> AudioSessionConfig {
    AudioSessionConfig {
        model: model.to_string(),
        instructions: Some("Answer the spoken question in one short sentence.".into()),
        input: AudioFormat::L16 {
            rate: RATE,
            channels: 1,
        },
        output,
        voice: None,
        // Any value switches the Live API's input transcription on.
        transcription_model: Some("default".into()),
        turn_detection: TurnDetection::Server,
        transcription_only: false,
        language: None,
        connect_deadline: Duration::from_secs(30),
    }
}

/// The PCM samples of a 16 kHz, mono, 16-bit WAV: the Live adapter takes headerless L16 frames,
/// not a WAV container, so the `data` chunk is cut out here.
fn sample_pcm() -> Vec<u8> {
    let path = std::env::var("GEMINI_LIVE_WAV")
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
    /// How many model turns the question produced: more than one means the server's
    /// voice-activity detection ended the caller's turn early and the model answered a fragment.
    turns: usize,
    /// What happened when, in ms from the first audio frame sent: the session's events (audio
    /// chunks collapsed to the first of a run) and the sender's milestones.
    trace: Vec<String>,
}

/// Everything the session says until `wait` runs out, or until it has been quiet for
/// [`QUIET_AFTER_TURN`] after a turn ended once all the audio is sent: one question can produce
/// several turns when the server's voice-activity detection splits it, and the answer to the
/// whole question is in the last one. Runs while the audio is still being sent, so the trace
/// shows when the model started answering.
async fn collect_turn(
    session: &dyn AudioSession,
    wait: Duration,
    started: tokio::time::Instant,
    sent: &std::sync::atomic::AtomicBool,
) -> Turn {
    use std::sync::atomic::Ordering;
    let mut turn = Turn::default();
    let deadline = started + wait;
    let at = || started.elapsed().as_millis();
    let mut in_audio = false;
    loop {
        let until = if turn.ended && sent.load(Ordering::SeqCst) {
            deadline.min(tokio::time::Instant::now() + QUIET_AFTER_TURN)
        } else {
            deadline.min(tokio::time::Instant::now() + Duration::from_millis(500))
        };
        let event = match tokio::time::timeout_at(until, session.next_event()).await {
            Ok(event) => event,
            // Still sending, or a turn has not ended yet: keep listening until the deadline.
            Err(_)
                if tokio::time::Instant::now() < deadline
                    && !(turn.ended && sent.load(Ordering::SeqCst)) =>
            {
                continue
            }
            Err(_) => break,
        };
        if !matches!(event, Some(AudioSessionEvent::OutputAudio { .. })) {
            in_audio = false;
        }
        match event {
            None | Some(AudioSessionEvent::Closed) => break,
            Some(AudioSessionEvent::InputTranscript {
                text,
                is_final: false,
                ..
            }) => {
                turn.trace.push(format!("+{} heard {text:?}", at()));
                turn.heard.push_str(&text)
            }
            Some(
                AudioSessionEvent::OutputTranscript {
                    text,
                    is_final: false,
                    ..
                }
                | AudioSessionEvent::OutputText {
                    text,
                    is_final: false,
                    ..
                },
            ) => {
                turn.trace.push(format!("+{} said {text:?}", at()));
                turn.said.push_str(&text)
            }
            Some(AudioSessionEvent::OutputAudio { data, .. }) => {
                if !in_audio {
                    turn.trace.push(format!("+{} audio", at()));
                    in_audio = true;
                }
                turn.audio_bytes += data.len()
            }
            Some(AudioSessionEvent::Error { detail }) => {
                turn.trace.push(format!("+{} error", at()));
                turn.errors.push(detail)
            }
            Some(AudioSessionEvent::TurnEnded) => {
                turn.trace.push(format!("+{} turn-ended", at()));
                turn.ended = true;
                turn.turns += 1;
                turn.said.push(' ');
            }
            Some(AudioSessionEvent::SpeechStarted { .. }) => {
                turn.trace.push(format!("+{} speech-started", at()))
            }
            Some(AudioSessionEvent::SpeechStopped { .. }) => {
                turn.trace.push(format!("+{} speech-stopped", at()))
            }
            Some(_) => {}
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

/// A provider failure on one line: its whitespace collapsed (Google's error bodies are
/// pretty-printed JSON).
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

/// Open a session through the real adapter and the real endpoint (TLS included), send the
/// sample question with `detection` deciding where the caller's turn ends, receive the spoken
/// answer and its transcripts, close, and require an answer to the question itself.
async fn round_trip(detection: TurnDetection, label: &str, model: &str) {
    let Some(key) = api_key() else { return };
    let pcm = sample_pcm();
    let output = AudioFormat::L16 {
        rate: RATE,
        channels: 1,
    };
    let provider = provider(None, &key, model);
    let mut config = session_config(Some(output), model);
    config.turn_detection = detection;
    let session = match provider.open_audio_session(&config).await {
        Ok(session) => session,
        Err(failure) => {
            println!(
                "LIVE: session refused: {}",
                one_line(&format!("{failure:?}"))
            );
            panic!("the live session did not open: {failure:?}");
        }
    };
    assert_eq!(session.next_event().await, Some(AudioSessionEvent::Ready));
    // The question in 100 ms frames, one every GEMINI_LIVE_PACE_MS (default 100: real time),
    // then 1.5 s of silence so a server's voice-activity detection sees the end of the turn,
    // then the end of the turn (manual) or of the audio stream. Events are read meanwhile.
    let pace = Duration::from_millis(
        std::env::var("GEMINI_LIVE_PACE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100),
    );
    let sent = std::sync::atomic::AtomicBool::new(false);
    let started = tokio::time::Instant::now();
    let send = async {
        let mut marks = Vec::new();
        for chunk in pcm.chunks(CHUNK) {
            session.send_audio(chunk).await.expect("send audio");
            tokio::time::sleep(pace).await;
        }
        marks.push(format!("+{} question-sent", started.elapsed().as_millis()));
        let silence = vec![0u8; CHUNK];
        for _ in 0..15 {
            session.send_audio(&silence).await.expect("send silence");
            tokio::time::sleep(pace).await;
        }
        session.commit().await.expect("end the turn");
        marks.push(format!("+{} committed", started.elapsed().as_millis()));
        sent.store(true, std::sync::atomic::Ordering::SeqCst);
        marks
    };
    let (marks, mut turn) = tokio::join!(
        send,
        collect_turn(session.as_ref(), Duration::from_secs(60), started, &sent)
    );
    turn.trace.extend(marks);
    turn.trace.sort_by_key(|line| {
        line[1..]
            .split(' ')
            .next()
            .and_then(|ms| ms.parse::<u128>().ok())
            .unwrap_or(0)
    });
    let closed = closes_cleanly(session.as_ref()).await;
    let audio_ms = turn.audio_bytes * 1000 / (RATE as usize * 2);
    let on_topic = {
        let all = format!("{} {}", turn.heard, turn.said).to_lowercase();
        all.contains("paris")
    };
    println!(
        "LIVE: model {} ({label}) heard \"{}\", said \"{}\", {audio_ms} ms of audio back, turns: {}, \
         answered the question: {on_topic}, closed cleanly: {closed}{}",
        model,
        shorten(&turn.heard),
        shorten(&turn.said),
        turn.turns,
        if turn.errors.is_empty() {
            String::new()
        } else {
            format!(", errors: {:?}", turn.errors)
        }
    );
    println!(
        "LIVE: trace ({label}, {} ms frames every {} ms): {}",
        CHUNK * 1000 / (RATE as usize * 2),
        pace.as_millis(),
        turn.trace.join(", ")
    );
    assert!(
        turn.errors.is_empty(),
        "the session reported errors: {:?}",
        turn.errors
    );
    assert!(turn.audio_bytes > 0, "no audio came back: {turn:?}");
    assert!(
        on_topic,
        "the answer does not answer the sample question (\"What is the capital of France?\"): {turn:?}"
    );
    assert!(closed, "the session did not close cleanly");
}

/// The server's voice-activity detection decides where the caller's turn ends: the answer must
/// still be to the whole question, not to a fragment of it.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live"]
async fn live_session_answers_the_sample_wav() {
    round_trip(
        TurnDetection::Server,
        "server turn detection",
        &model_or(DEFAULT_SERVER_DETECTION_MODEL),
    )
    .await;
}

/// The caller marks the turn (activityStart … activityEnd): the whole question is one turn.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live"]
async fn live_session_answers_with_manual_turns() {
    round_trip(TurnDetection::Manual, "manual turns", &live_model()).await;
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
/// `Host` rewritten, and the connection forwarded over TLS to Google, both ways, unchanged. The
/// adapter is pointed at the hop (`base-url`), so every URL it builds — the token service's and
/// the WebSocket's — and every header it sends is visible here.
async fn logging_hop() -> (String, Arc<Mutex<HopLog>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
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
                            format!("host: {GOOGLE_HOST}\r\n")
                        } else {
                            line.to_string()
                        }
                    })
                    .collect();
                let Ok(tcp) = TcpStream::connect((GOOGLE_HOST, 443)).await else {
                    return;
                };
                let name = rustls::pki_types::ServerName::try_from(GOOGLE_HOST).unwrap();
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

/// Through the logging hop: the token request carries the key in `x-goog-api-key` and nowhere
/// else; the WebSocket handshake carries the minted token in `Authorization: Token …` and
/// neither credential in its URL; no request line has a query credential at all. The session
/// must then open (`setupComplete`) and close cleanly.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live"]
async fn live_urls_carry_neither_key_nor_token() {
    let Some(key) = api_key() else { return };
    let (base, log) = logging_hop().await;
    let provider = provider(Some(base), &key, &live_model());
    let opened = provider
        .open_audio_session(&session_config(None, &live_model()))
        .await;
    let closed = match &opened {
        Ok(session) => closes_cleanly(session.as_ref()).await,
        Err(_) => false,
    };
    let heads = log.lock().unwrap().heads.clone();
    let lines: Vec<&str> = heads.iter().filter_map(|h| h.lines().next()).collect();

    let token_head = heads.iter().find(|h| {
        h.lines()
            .next()
            .is_some_and(|l| l.contains("/auth_tokens "))
    });
    let token_head = token_head.expect("the adapter asked the token service for a token");
    assert_eq!(
        header(token_head, "x-goog-api-key"),
        Some(key.as_str()),
        "the key travels in the x-goog-api-key header of the token request"
    );
    for line in &lines {
        assert!(
            !line.contains(key.as_str()),
            "a request URL carries the key"
        );
        assert!(!line.contains("key="), "a request URL has a key parameter");
        assert!(
            !line.contains("access_token="),
            "a request URL has a token parameter"
        );
    }
    let socket_head = heads.iter().find(|h| {
        h.lines()
            .next()
            .is_some_and(|l| l.starts_with("GET ") && l.contains("BidiGenerateContent"))
    });
    let token = socket_head.and_then(|h| header(h, "authorization"));
    if let (Some(head), Some(token)) = (socket_head, token) {
        let token = token
            .strip_prefix("Token ")
            .expect("Authorization: Token <token>");
        assert!(!token.is_empty(), "an empty ephemeral token");
        assert!(
            head.lines()
                .next()
                .unwrap()
                .contains("BidiGenerateContentConstrained"),
            "a token opens the constrained endpoint"
        );
        for line in &lines {
            assert!(
                !line.contains(token),
                "a request URL carries the ephemeral token"
            );
        }
        assert!(
            header(head, "x-goog-api-key").is_none() && !head.contains(key.as_str()),
            "the WebSocket handshake carries the long-lived key"
        );
    }
    println!(
        "LIVE: {} request(s) seen [{}], no key or token in any URL, session {}",
        lines.len(),
        lines
            .iter()
            .map(|l| l.split(' ').take(2).collect::<Vec<_>>().join(" "))
            .collect::<Vec<_>>()
            .join("; "),
        match &opened {
            Ok(_) => format!("opened and closed cleanly: {closed}"),
            Err(failure) => format!("refused: {}", one_line(&format!("{failure:?}"))),
        }
    );
    let session =
        opened.unwrap_or_else(|failure| panic!("the live session did not open: {failure:?}"));
    drop(session);
    assert!(
        socket_head.is_some(),
        "the adapter never opened the WebSocket"
    );
    assert!(
        token.is_some(),
        "the WebSocket handshake carried no Authorization header"
    );
    assert!(closed, "the session did not close cleanly");
}
