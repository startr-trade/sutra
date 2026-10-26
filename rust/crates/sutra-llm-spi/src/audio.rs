//! Audio and video content for the provider SPI: the audio-format vocabulary (media types), the
//! media parts a message carries, and the pure-Rust conversions the transport and the adapters
//! use to move audio between what a caller has and what a provider takes — WAV wrap/unwrap,
//! G.711 µ-law / A-law, channel mixing and linear-interpolated resampling.
//!
//! Only linear PCM and G.711 are decoded here. Compressed formats (MP3, Opus, AAC, FLAC, ...)
//! are carried through unchanged: a conversion that would need a compressed codec fails with
//! [`AudioError::Unsupported`] rather than guessing.
//!
//! BYTE ORDER. `audio/L16` here is signed 16-bit **little-endian** PCM — what media gateways and
//! model providers actually exchange (RFC 3551's network-order L16 is not what any of them
//! send). Mono unless the media type says `channels=<n>`.

use std::fmt;

/// An audio format, named by its media type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AudioFormat {
    /// A RIFF/WAVE container (`audio/wav`). Its header states rate, channels and encoding.
    Wav,
    /// Headerless signed 16-bit little-endian PCM (`audio/L16;rate=<hz>[;channels=<n>]`, also
    /// spelled `audio/pcm;rate=<hz>`).
    L16 { rate: u32, channels: u16 },
    /// G.711 µ-law, 8 kHz mono (`audio/PCMU`).
    Pcmu,
    /// G.711 A-law, 8 kHz mono (`audio/PCMA`).
    Pcma,
    /// `audio/mpeg`.
    Mp3,
    /// `audio/flac`.
    Flac,
    /// `audio/opus` (Opus in Ogg).
    Opus,
    /// `audio/ogg` (Vorbis or Opus in Ogg).
    Ogg,
    /// `audio/aac`.
    Aac,
    /// `audio/webm`.
    Webm,
    /// `audio/mp4` (M4A).
    M4a,
}

/// The sample rate G.711 runs at.
pub const G711_RATE: u32 = 8000;

impl AudioFormat {
    /// Parse a media type, case-insensitively, with its parameters. `audio/L16` (or
    /// `audio/pcm`) needs `rate=`; G.711 accepts only `rate=8000`.
    pub fn parse(media_type: &str) -> Result<AudioFormat, String> {
        let mut parts = media_type.split(';');
        let essence = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        let mut rate: Option<u32> = None;
        let mut channels: Option<u16> = None;
        for param in parts {
            let (key, value) = param
                .split_once('=')
                .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().trim_matches('"')))
                .ok_or_else(|| format!("'{media_type}': parameter '{param}' has no value"))?;
            match key.as_str() {
                "rate" => {
                    rate = Some(value.parse().ok().filter(|r| *r > 0).ok_or_else(|| {
                        format!("'{media_type}': rate must be a positive integer")
                    })?)
                }
                "channels" => {
                    channels = Some(value.parse().ok().filter(|c| *c > 0).ok_or_else(|| {
                        format!("'{media_type}': channels must be a positive integer")
                    })?)
                }
                _ => {}
            }
        }
        let fixed = |format: AudioFormat| match (rate, channels) {
            (None | Some(G711_RATE), None | Some(1)) => Ok(format),
            _ => Err(format!("'{media_type}': G.711 is 8000 Hz mono")),
        };
        let plain = |format: AudioFormat| Ok(format);
        match essence.as_str() {
            "audio/wav" | "audio/wave" | "audio/x-wav" | "audio/vnd.wave" => {
                plain(AudioFormat::Wav)
            }
            "audio/l16" | "audio/pcm" => Ok(AudioFormat::L16 {
                rate: rate.ok_or_else(|| format!("'{media_type}' needs a rate=<hz> parameter"))?,
                channels: channels.unwrap_or(1),
            }),
            "audio/pcmu" | "audio/basic" => fixed(AudioFormat::Pcmu),
            "audio/pcma" => fixed(AudioFormat::Pcma),
            "audio/mpeg" | "audio/mp3" => plain(AudioFormat::Mp3),
            "audio/flac" | "audio/x-flac" => plain(AudioFormat::Flac),
            "audio/opus" => plain(AudioFormat::Opus),
            "audio/ogg" => plain(AudioFormat::Ogg),
            "audio/aac" => plain(AudioFormat::Aac),
            "audio/webm" => plain(AudioFormat::Webm),
            "audio/mp4" | "audio/m4a" | "audio/x-m4a" => plain(AudioFormat::M4a),
            _ => Err(format!("'{media_type}' is not a supported audio format")),
        }
    }

    /// The canonical media type.
    pub fn media_type(&self) -> String {
        match self {
            AudioFormat::Wav => "audio/wav".into(),
            AudioFormat::L16 { rate, channels: 1 } => format!("audio/L16;rate={rate}"),
            AudioFormat::L16 { rate, channels } => {
                format!("audio/L16;rate={rate};channels={channels}")
            }
            AudioFormat::Pcmu => "audio/PCMU".into(),
            AudioFormat::Pcma => "audio/PCMA".into(),
            AudioFormat::Mp3 => "audio/mpeg".into(),
            AudioFormat::Flac => "audio/flac".into(),
            AudioFormat::Opus => "audio/opus".into(),
            AudioFormat::Ogg => "audio/ogg".into(),
            AudioFormat::Aac => "audio/aac".into(),
            AudioFormat::Webm => "audio/webm".into(),
            AudioFormat::M4a => "audio/mp4".into(),
        }
    }

    /// Whether [`decode`] can read this format (linear PCM, G.711, or a WAV of either).
    pub fn is_decodable(&self) -> bool {
        matches!(
            self,
            AudioFormat::Wav | AudioFormat::L16 { .. } | AudioFormat::Pcmu | AudioFormat::Pcma
        )
    }
}

impl fmt::Display for AudioFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.media_type())
    }
}

/// An audio clip in a stated format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmAudio {
    pub data: Vec<u8>,
    pub format: AudioFormat,
}

/// One still frame of a video, an image media type (`image/jpeg`, `image/png`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LlmVideoFrame {
    pub data: Vec<u8>,
    pub media_type: String,
    /// Offset of the frame from the start of the clip, when known.
    pub at_ms: Option<u64>,
}

/// Video content: either a whole clip in a container format, or sampled frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LlmVideo {
    Clip { data: Vec<u8>, media_type: String },
    Frames(Vec<LlmVideoFrame>),
}

/// Why an audio conversion failed. Never carries audio content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AudioError {
    /// The conversion needs a codec this crate does not implement (a compressed format).
    Unsupported(String),
    /// The bytes do not match the stated format.
    Malformed(String),
}

impl fmt::Display for AudioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AudioError::Unsupported(d) | AudioError::Malformed(d) => f.write_str(d),
        }
    }
}

impl std::error::Error for AudioError {}

/// Decoded linear audio: interleaved signed 16-bit samples.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pcm {
    pub rate: u32,
    pub channels: u16,
    pub samples: Vec<i16>,
}

impl Pcm {
    /// Duration in milliseconds.
    pub fn duration_ms(&self) -> u64 {
        if self.rate == 0 || self.channels == 0 {
            return 0;
        }
        let frames = self.samples.len() as u64 / u64::from(self.channels);
        frames * 1000 / u64::from(self.rate)
    }
}

// ---- G.711 -----------------------------------------------------------------------------------

const ULAW_BIAS: i32 = 0x84;
const ULAW_CLIP: i32 = 32635;

/// Encode one 16-bit sample as G.711 µ-law.
pub fn ulaw_encode(sample: i16) -> u8 {
    let mut value = i32::from(sample);
    let sign = if value < 0 {
        value = -value;
        0x80
    } else {
        0
    };
    value = value.min(ULAW_CLIP) + ULAW_BIAS;
    let mut exponent = 7;
    let mut mask = 0x4000;
    while exponent > 0 && value & mask == 0 {
        exponent -= 1;
        mask >>= 1;
    }
    let mantissa = (value >> (exponent + 3)) & 0x0F;
    !((sign | (exponent << 4) | mantissa) as u8)
}

/// Decode one G.711 µ-law byte.
pub fn ulaw_decode(byte: u8) -> i16 {
    let byte = !byte;
    let sign = byte & 0x80;
    let exponent = i32::from((byte >> 4) & 0x07);
    let mantissa = i32::from(byte & 0x0F);
    let magnitude = (((mantissa << 3) + ULAW_BIAS) << exponent) - ULAW_BIAS;
    if sign != 0 {
        -magnitude as i16
    } else {
        magnitude as i16
    }
}

/// Encode one 16-bit sample as G.711 A-law.
pub fn alaw_encode(sample: i16) -> u8 {
    // A-law works on 13-bit magnitudes.
    let mut value = i32::from(sample) >> 3;
    let sign = if value >= 0 {
        0x80
    } else {
        value = -value - 1;
        0
    };
    value = value.min(0x0FFF);
    let encoded = if value < 32 {
        value >> 1
    } else {
        let mut exponent = 1;
        while exponent < 7 && value >= (32 << exponent) {
            exponent += 1;
        }
        (exponent << 4) | ((value >> exponent) & 0x0F)
    };
    ((sign | encoded) as u8) ^ 0x55
}

/// Decode one G.711 A-law byte.
pub fn alaw_decode(byte: u8) -> i16 {
    let byte = byte ^ 0x55;
    let exponent = i32::from((byte >> 4) & 0x07);
    let mantissa = i32::from(byte & 0x0F);
    let magnitude = if exponent == 0 {
        (mantissa << 4) + 8
    } else {
        ((mantissa << 4) + 0x108) << (exponent - 1)
    };
    if byte & 0x80 != 0 {
        magnitude as i16
    } else {
        -magnitude as i16
    }
}

// ---- WAV ------------------------------------------------------------------------------------

const WAVE_PCM: u16 = 1;
const WAVE_ALAW: u16 = 6;
const WAVE_ULAW: u16 = 7;
const WAVE_EXTENSIBLE: u16 = 0xFFFE;

/// Wrap 16-bit PCM in a canonical 44-byte-header RIFF/WAVE container.
pub fn wav_wrap(pcm: &Pcm) -> Vec<u8> {
    let data_len = (pcm.samples.len() * 2) as u32;
    let block_align = pcm.channels * 2;
    let byte_rate = pcm.rate * u32::from(block_align);
    let mut out = Vec::with_capacity(44 + data_len as usize);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_len).to_le_bytes());
    out.extend_from_slice(b"WAVE");
    out.extend_from_slice(b"fmt ");
    out.extend_from_slice(&16u32.to_le_bytes());
    out.extend_from_slice(&WAVE_PCM.to_le_bytes());
    out.extend_from_slice(&pcm.channels.to_le_bytes());
    out.extend_from_slice(&pcm.rate.to_le_bytes());
    out.extend_from_slice(&byte_rate.to_le_bytes());
    out.extend_from_slice(&block_align.to_le_bytes());
    out.extend_from_slice(&16u16.to_le_bytes());
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    for sample in &pcm.samples {
        out.extend_from_slice(&sample.to_le_bytes());
    }
    out
}

fn malformed(detail: impl Into<String>) -> AudioError {
    AudioError::Malformed(detail.into())
}

fn le_u16(bytes: &[u8], at: usize) -> Option<u16> {
    bytes
        .get(at..at + 2)
        .map(|b| u16::from_le_bytes([b[0], b[1]]))
}

fn le_u32(bytes: &[u8], at: usize) -> Option<u32> {
    bytes
        .get(at..at + 4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

/// Read a RIFF/WAVE container holding 16-bit PCM, µ-law or A-law (plain or
/// `WAVE_FORMAT_EXTENSIBLE`) into linear PCM. A `data` chunk whose declared size runs past the
/// end (a streamed WAV) is read to the end.
pub fn wav_unwrap(bytes: &[u8]) -> Result<Pcm, AudioError> {
    if bytes.len() < 12 || &bytes[0..4] != b"RIFF" || &bytes[8..12] != b"WAVE" {
        return Err(malformed("not a RIFF/WAVE container"));
    }
    let mut at = 12;
    let mut fmt: Option<(u16, u16, u32, u16)> = None;
    while at + 8 <= bytes.len() {
        let id = &bytes[at..at + 4];
        let size = le_u32(bytes, at + 4).unwrap_or(0) as usize;
        let body = at + 8;
        if id == b"fmt " {
            let tag = le_u16(bytes, body).ok_or_else(|| malformed("truncated fmt chunk"))?;
            let channels =
                le_u16(bytes, body + 2).ok_or_else(|| malformed("truncated fmt chunk"))?;
            let rate = le_u32(bytes, body + 4).ok_or_else(|| malformed("truncated fmt chunk"))?;
            let bits = le_u16(bytes, body + 14).ok_or_else(|| malformed("truncated fmt chunk"))?;
            let tag = if tag == WAVE_EXTENSIBLE {
                // The sub-format GUID starts with the plain format tag.
                le_u16(bytes, body + 24).ok_or_else(|| malformed("truncated extensible fmt"))?
            } else {
                tag
            };
            fmt = Some((tag, channels, rate, bits));
        } else if id == b"data" {
            let (tag, channels, rate, bits) =
                fmt.ok_or_else(|| malformed("the data chunk precedes the fmt chunk"))?;
            if channels == 0 || rate == 0 {
                return Err(malformed("zero channels or sample rate"));
            }
            let end = body.saturating_add(size).min(bytes.len());
            let data = &bytes[body..end];
            let samples = match (tag, bits) {
                (WAVE_PCM, 16) => data
                    .chunks_exact(2)
                    .map(|b| i16::from_le_bytes([b[0], b[1]]))
                    .collect(),
                (WAVE_ULAW, 8) => data.iter().map(|b| ulaw_decode(*b)).collect(),
                (WAVE_ALAW, 8) => data.iter().map(|b| alaw_decode(*b)).collect(),
                (tag, bits) => {
                    return Err(AudioError::Unsupported(format!(
                    "WAV encoding tag {tag} at {bits} bits (16-bit PCM, µ-law and A-law are read)"
                )))
                }
            };
            return Ok(Pcm {
                rate,
                channels,
                samples,
            });
        }
        // Chunks are word-aligned.
        at = body.saturating_add(size).saturating_add(size & 1);
    }
    Err(malformed("no data chunk"))
}

// ---- channel mixing and resampling ----------------------------------------------------------

/// Average interleaved channels down to mono.
pub fn to_mono(pcm: &Pcm) -> Pcm {
    if pcm.channels <= 1 {
        return pcm.clone();
    }
    let n = usize::from(pcm.channels);
    let samples = pcm
        .samples
        .chunks(n)
        .map(|frame| {
            let sum: i32 = frame.iter().map(|s| i32::from(*s)).sum();
            (sum / frame.len() as i32) as i16
        })
        .collect();
    Pcm {
        rate: pcm.rate,
        channels: 1,
        samples,
    }
}

/// Resample mono samples from `from` Hz to `to` Hz by linear interpolation. Good enough for
/// speech between telephony and model rates; not a band-limited resampler.
pub fn resample_linear(samples: &[i16], from: u32, to: u32) -> Vec<i16> {
    if from == to || samples.is_empty() || from == 0 || to == 0 {
        return samples.to_vec();
    }
    let out_len = ((samples.len() as u64 * u64::from(to)) / u64::from(from)).max(1) as usize;
    let step = f64::from(from) / f64::from(to);
    let last = samples.len() - 1;
    (0..out_len)
        .map(|i| {
            let position = i as f64 * step;
            let index = position.floor() as usize;
            if index >= last {
                return samples[last];
            }
            let frac = position - index as f64;
            let a = f64::from(samples[index]);
            let b = f64::from(samples[index + 1]);
            (a + (b - a) * frac).round() as i16
        })
        .collect()
}

/// Convert to `rate` Hz and `channels` channels (mono ↔ n by averaging / duplicating).
pub fn convert(pcm: &Pcm, rate: u32, channels: u16) -> Pcm {
    let mono = to_mono(pcm);
    let resampled = resample_linear(&mono.samples, mono.rate, rate);
    let channels = channels.max(1);
    let samples = if channels == 1 {
        resampled
    } else {
        resampled
            .iter()
            .flat_map(|s| std::iter::repeat_n(*s, usize::from(channels)))
            .collect()
    };
    Pcm {
        rate,
        channels,
        samples,
    }
}

// ---- decode / encode / transcode -------------------------------------------------------------

/// Decode `data` in `format` to linear PCM.
pub fn decode(data: &[u8], format: AudioFormat) -> Result<Pcm, AudioError> {
    match format {
        AudioFormat::Wav => wav_unwrap(data),
        AudioFormat::L16 { rate, channels } => {
            if !data.len().is_multiple_of(2) {
                return Err(malformed("L16 audio has an odd byte count"));
            }
            Ok(Pcm {
                rate,
                channels,
                samples: data
                    .chunks_exact(2)
                    .map(|b| i16::from_le_bytes([b[0], b[1]]))
                    .collect(),
            })
        }
        AudioFormat::Pcmu => Ok(Pcm {
            rate: G711_RATE,
            channels: 1,
            samples: data.iter().map(|b| ulaw_decode(*b)).collect(),
        }),
        AudioFormat::Pcma => Ok(Pcm {
            rate: G711_RATE,
            channels: 1,
            samples: data.iter().map(|b| alaw_decode(*b)).collect(),
        }),
        other => Err(AudioError::Unsupported(format!(
            "{other} cannot be decoded here (a compressed format)"
        ))),
    }
}

/// Encode linear PCM as `format`, converting rate and channels as the format needs. A WAV keeps
/// the source rate and channels.
pub fn encode(pcm: &Pcm, format: AudioFormat) -> Result<Vec<u8>, AudioError> {
    match format {
        AudioFormat::Wav => Ok(wav_wrap(pcm)),
        AudioFormat::L16 { rate, channels } => Ok(convert(pcm, rate, channels)
            .samples
            .iter()
            .flat_map(|s| s.to_le_bytes())
            .collect()),
        AudioFormat::Pcmu => Ok(convert(pcm, G711_RATE, 1)
            .samples
            .iter()
            .map(|s| ulaw_encode(*s))
            .collect()),
        AudioFormat::Pcma => Ok(convert(pcm, G711_RATE, 1)
            .samples
            .iter()
            .map(|s| alaw_encode(*s))
            .collect()),
        other => Err(AudioError::Unsupported(format!(
            "{other} cannot be encoded here (a compressed format)"
        ))),
    }
}

/// Convert `data` from one format to another. The same format is passed through unchanged;
/// anything else must be decodable and encodable here.
pub fn transcode(data: &[u8], from: AudioFormat, to: AudioFormat) -> Result<Vec<u8>, AudioError> {
    if from == to {
        return Ok(data.to_vec());
    }
    encode(&decode(data, from)?, to)
}

/// Convert `audio` into the first of `accepted` it can reach: unchanged when its format is
/// listed, else transcoded to the first decodable-target format listed.
pub fn adapt(audio: &LlmAudio, accepted: &[AudioFormat]) -> Result<LlmAudio, AudioError> {
    if accepted.contains(&audio.format) || accepted.is_empty() {
        return Ok(audio.clone());
    }
    let target = accepted
        .iter()
        .copied()
        .find(AudioFormat::is_decodable)
        .ok_or_else(|| {
            AudioError::Unsupported(format!(
                "{} is not accepted and no listed format can be produced here",
                audio.format
            ))
        })?;
    Ok(LlmAudio {
        data: transcode(&audio.data, audio.format, target)?,
        format: target,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(rate: u32, ms: u32) -> Pcm {
        let n = (rate * ms / 1000) as usize;
        let samples = (0..n)
            .map(|i| ((i as f64 * 0.05).sin() * 8000.0) as i16)
            .collect();
        Pcm {
            rate,
            channels: 1,
            samples,
        }
    }

    #[test]
    fn media_types_parse_and_round_trip() {
        for spelling in [
            "audio/wav",
            "audio/L16;rate=8000",
            "audio/L16;rate=16000;channels=2",
            "audio/PCMU",
            "audio/PCMA",
            "audio/mpeg",
            "audio/flac",
            "audio/opus",
            "audio/ogg",
            "audio/aac",
            "audio/webm",
            "audio/mp4",
        ] {
            let format = AudioFormat::parse(spelling).unwrap();
            assert_eq!(format.media_type(), spelling);
            assert_eq!(AudioFormat::parse(&format.media_type()).unwrap(), format);
        }
        assert_eq!(
            AudioFormat::parse("Audio/PCM; rate=24000").unwrap(),
            AudioFormat::L16 {
                rate: 24000,
                channels: 1
            }
        );
        assert_eq!(
            AudioFormat::parse("audio/pcmu;rate=8000").unwrap(),
            AudioFormat::Pcmu
        );
        assert_eq!(AudioFormat::parse("audio/x-wav").unwrap(), AudioFormat::Wav);
    }

    #[test]
    fn bad_media_types_are_rejected() {
        assert!(AudioFormat::parse("audio/L16").is_err(), "L16 needs a rate");
        assert!(AudioFormat::parse("audio/L16;rate=0").is_err());
        assert!(AudioFormat::parse("audio/PCMU;rate=16000").is_err());
        assert!(AudioFormat::parse("video/mp4").is_err());
        assert!(AudioFormat::parse("audio/L16;rate").is_err());
    }

    #[test]
    fn ulaw_reference_values() {
        assert_eq!(ulaw_encode(0), 0xFF);
        assert_eq!(ulaw_decode(0xFF), 0);
        assert_eq!(ulaw_encode(i16::MAX), 0x80);
        assert_eq!(ulaw_encode(i16::MIN), 0x00);
        assert_eq!(ulaw_decode(0x80), 32124);
        assert_eq!(ulaw_decode(0x00), -32124);
    }

    #[test]
    fn alaw_reference_values() {
        assert_eq!(alaw_encode(0), 0xD5);
        assert_eq!(alaw_decode(0xD5), 8);
        assert_eq!(alaw_encode(i16::MAX), 0xAA);
        assert_eq!(alaw_decode(0xAA), 32256);
        assert_eq!(alaw_decode(0x2A), -32256);
    }

    #[test]
    fn g711_round_trip_stays_within_quantisation_error() {
        for sample in (-32000i32..32000).step_by(97) {
            let s = sample as i16;
            let tolerance = (i32::from(s).abs() / 16).max(16);
            let u = i32::from(ulaw_decode(ulaw_encode(s)));
            let a = i32::from(alaw_decode(alaw_encode(s)));
            assert!((u - sample).abs() <= tolerance, "µ-law {sample} -> {u}");
            assert!((a - sample).abs() <= tolerance, "A-law {sample} -> {a}");
            // Re-encoding a decoded value is stable.
            assert_eq!(ulaw_encode(u as i16), ulaw_encode(s));
            assert_eq!(alaw_encode(a as i16), alaw_encode(s));
        }
    }

    #[test]
    fn wav_wrap_unwrap_round_trips() {
        let pcm = tone(16000, 50);
        let wav = wav_wrap(&pcm);
        assert_eq!(wav.len(), 44 + pcm.samples.len() * 2);
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(wav_unwrap(&wav).unwrap(), pcm);
    }

    #[test]
    fn wav_with_extra_chunks_and_g711_is_read() {
        let mut wav = Vec::new();
        let data = [0xFFu8, 0x80, 0x00];
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&0u32.to_le_bytes());
        wav.extend_from_slice(b"WAVE");
        wav.extend_from_slice(b"LIST");
        wav.extend_from_slice(&3u32.to_le_bytes());
        wav.extend_from_slice(b"abc\0"); // odd chunk + pad byte
        wav.extend_from_slice(b"fmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&WAVE_ULAW.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&8000u32.to_le_bytes());
        wav.extend_from_slice(&8000u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&8u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&u32::MAX.to_le_bytes()); // streamed: size unknown
        wav.extend_from_slice(&data);
        let pcm = wav_unwrap(&wav).unwrap();
        assert_eq!(pcm.rate, 8000);
        assert_eq!(pcm.samples, vec![0, 32124, -32124]);
    }

    #[test]
    fn malformed_wav_is_rejected() {
        assert!(matches!(wav_unwrap(b"RIFX"), Err(AudioError::Malformed(_))));
        let mut wav = wav_wrap(&tone(8000, 10));
        wav[20] = 3; // IEEE float
        assert!(matches!(wav_unwrap(&wav), Err(AudioError::Unsupported(_))));
    }

    #[test]
    fn resampling_scales_length_and_keeps_a_constant_signal() {
        let flat = vec![1000i16; 800];
        let up = resample_linear(&flat, 8000, 24000);
        assert_eq!(up.len(), 2400);
        assert!(up.iter().all(|s| *s == 1000));
        let down = resample_linear(&flat, 24000, 8000);
        assert_eq!(down.len(), 266);
        let ramp: Vec<i16> = (0..10).map(|i| i * 100).collect();
        assert_eq!(&resample_linear(&ramp, 1, 2)[..4], &[0, 50, 100, 150]);
        assert!(resample_linear(&[], 8000, 16000).is_empty());
    }

    #[test]
    fn stereo_mixes_down_and_mono_spreads_up() {
        let stereo = Pcm {
            rate: 8000,
            channels: 2,
            samples: vec![100, 300, -100, -300],
        };
        assert_eq!(to_mono(&stereo).samples, vec![200, -200]);
        let spread = convert(&to_mono(&stereo), 8000, 2);
        assert_eq!(spread.samples, vec![200, 200, -200, -200]);
    }

    #[test]
    fn transcode_between_call_and_model_formats() {
        let wav = wav_wrap(&tone(24000, 100));
        let l16 = AudioFormat::L16 {
            rate: 8000,
            channels: 1,
        };
        let narrow = transcode(&wav, AudioFormat::Wav, l16).unwrap();
        assert_eq!(narrow.len(), 800 * 2, "100 ms at 8 kHz");
        let ulaw = transcode(&narrow, l16, AudioFormat::Pcmu).unwrap();
        assert_eq!(ulaw.len(), 800);
        let back = transcode(&ulaw, AudioFormat::Pcmu, AudioFormat::Wav).unwrap();
        let pcm = wav_unwrap(&back).unwrap();
        assert_eq!((pcm.rate, pcm.channels, pcm.duration_ms()), (8000, 1, 100));
        assert_eq!(
            transcode(b"mp3", AudioFormat::Mp3, AudioFormat::Mp3).unwrap(),
            b"mp3"
        );
        assert!(matches!(
            transcode(b"mp3", AudioFormat::Mp3, AudioFormat::Wav),
            Err(AudioError::Unsupported(_))
        ));
        assert!(matches!(
            decode(&[1, 2, 3], l16),
            Err(AudioError::Malformed(_))
        ));
    }

    #[test]
    fn adapt_picks_an_accepted_format() {
        let clip = LlmAudio {
            data: vec![0xFF; 160],
            format: AudioFormat::Pcmu,
        };
        let adapted = adapt(&clip, &[AudioFormat::Wav, AudioFormat::Mp3]).unwrap();
        assert_eq!(adapted.format, AudioFormat::Wav);
        assert_eq!(wav_unwrap(&adapted.data).unwrap().samples.len(), 160);
        assert_eq!(adapt(&clip, &[AudioFormat::Pcmu]).unwrap(), clip);
        assert!(adapt(&clip, &[AudioFormat::Mp3]).is_err());
    }
}
