#!/usr/bin/env python3
"""A stand-in for the model providers, shared by the ai-summarize and ai-advanced examples.
Python 3 stdlib only. Every answer is fixed, so a run is deterministic and needs no key.

OpenAI wire (also every Chat Completions service: Gemini, Mistral, ...)
  POST /v1/chat/completions      a fixed answer; `stream: true` as server-sent events; `tools`
                                 -> a call of the first tool; `response_format: json_object` ->
                                 a JSON object as the text
                                 `modalities` with "audio" -> a spoken answer: a fixed WAV
                                 (24 kHz mono) plus its transcript; an `input_audio` part must
                                 be valid base64 (and a RIFF/WAVE file when its format is wav)
                                 A model id containing `voice` (sutra-voice: examples/voice-assistant) speaks
                                 a longer, audible answer (800 ms of 440 Hz): a greeting when the
                                 request carries no audio, "Transferring you to an agent now."
                                 when the caller's latest turn lasts 2.5 s or more, else a fixed
                                 answer. A model id containing `analytics` with
                                 `response_format: json_object` answers the post-call analysis
                                 (customer emotion per turn) as a JSON object
  POST /v1/audio/transcriptions  multipart form (model + file) -> a fixed transcript
  POST /v1/audio/speech          {model, input, voice, response_format} -> the fixed WAV
                                 (`wav`) or its samples (`pcm`, 24 kHz 16-bit LE)
  GET  /v1/realtime              a WebSocket (sutra-voice: examples/meeting-assistant): a Realtime session. A
                                 transcription session (`session.update` with `type:
                                 transcription`) is answered `session.updated`; the audio it is
                                 sent (`input_audio_buffer.append`, PCM 24 kHz or G.711) is cut
                                 at 500 ms silences by an energy detector, and each stretch of
                                 speech is answered `speech_started` / `speech_stopped` (with its
                                 audio offsets) and a completed transcription: the next line of
                                 MEETING_LINES. `input_audio_buffer.commit` ends the stretch in
                                 progress
  POST /v1/responses             a background job (`background: true`): answers `queued`
  GET  /v1/responses/<id>        `in_progress` on the first poll, `completed` after it (a JSON job
                                 whose model id contains `meeting` answers MEETING_REPORT). A job
                                 whose model id ends in `-webhook` is instead completed by a
                                 signed webhook (see WEBHOOKS) and stays `in_progress` until then,
                                 when STUB_WEBHOOK_SECRET is set

Gemini native wire (sutra-voice: examples/video-meeting-assistant)
  POST /v1beta/models/<model>:generateContent
                                 a fixed answer; with `responseMimeType: application/json` a
                                 segment analysis (VIDEO_ANALYSIS) that echoes what it was sent:
                                 `framesSeen` (image parts) and `audioSeen` (an audio part), so a
                                 test can check the frames reached the model; the modalities it
                                 claims follow what was sent. A model id containing `vision`
                                 (examples/ai-advanced vision.bpmn) answers JSON mode with a
                                 fixed scene description plus `framesSeen` and `clipsSeen`
  GET  /gemini/requests          every generateContent request so far, oldest first, as
                                 {model, parts: [{mimeType, bytes, sha256} for each inlineData
                                 part, the bytes decoded]}: a test can check the very media it
                                 sent reached the model in the native wire shape

Anthropic wire
  POST /v1/messages              a fixed answer; `stream: true` as Messages API events; `tools`
                                 -> a `tool_use` block; a system prompt asking for a JSON object
                                 -> a JSON object as the text
  POST /v1/messages/batches      a Message Batches job: answers `in_progress`
  GET  /v1/messages/batches/<id> `in_progress` on the first poll, `ended` after it
  GET  /v1/messages/batches/<id>/results   the JSONL results, one line per request

Failure and pass-through
  /flaky/<path>                  503 while the outage switch is on (the default), else <path>
                                 as above: a candidate whose base-url carries /flaky is "down"
  /relay/anthropic/<path>        503 while the outage switch is on, else forwarded unchanged to
                                 https://api.anthropic.com/<path> (live-check.sh uses this to
                                 take a REAL Anthropic candidate down and up again)
  POST /control/outage           {"on": true|false} flips the switch; GET reads it

Bookkeeping
  POST /sink                     records and logs the body (the async flows' hand-off target)
  GET  /sink                     every body recorded so far, as a JSON array
  GET  /stats                    requests served, by route (Chat Completions also by model, as
                                 `chat:<model>`)
  GET  /health                   200

WEBHOOKS. With STUB_WEBHOOK_SECRET set, a `-webhook` job is completed about a second after it is
submitted and announced with an OpenAI-style `response.completed` event, signed the Standard
Webhooks way (HMAC-SHA256 over `id.timestamp.body`, `webhook-signature: v1,<base64>`). It is
POSTed to STUB_WEBHOOK_URL, or by default to http://<the submitter's address>:8080/llm/openai-hooks
— the engine that submitted the job. Non-200 answers are retried a few times.

The stub never reads a prompt for meaning, and never logs prompts, answers or headers.
"""
import base64
import hashlib
import hmac
import io
import json
import os
import sys
import threading
import time
import wave
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SUMMARY = "STUB SUMMARY: the customer reports a problem and asks for it to be looked at."
JSON_ANSWER = '{"category": "outage", "urgency": "high"}'
TOOL_ARGUMENT = "Paris"
USAGE_IN, USAGE_OUT = 42, 17
SPOKEN_TRANSCRIPT = "STUB SPOKEN ANSWER: your appointment is confirmed for Tuesday."
STT_TRANSCRIPT = "STUB TRANSCRIPT: I would like to confirm my appointment."
AUDIO_RATE, AUDIO_MS = 24000, 200
VOICE_GREETING = ("Hello, you have reached the assistant. This call is recorded and analysed to "
                  "help us serve you. How can I help?")
VOICE_ANSWER = "STUB VOICE ANSWER: your appointment is confirmed for Tuesday at ten."
VOICE_TRANSFER = "Transferring you to an agent now."
VOICE_LONG_TURN_MS = 2500
ANALYTICS = {
    "analyzedParties": ["customer"],
    "customer": {
        "overall": "neutral",
        "trajectory": [{"turn": 1, "emotion": "concerned", "sentiment": -0.2},
                       {"turn": 2, "emotion": "calm", "sentiment": 0.3}],
        "escalation": False,
    },
    "resolved": True,
    "topic": "appointment",
    "summary": "The customer asked about an appointment and was answered.",
}


# The meeting-assistant's conference, line by line (sutra-voice: examples/meeting-assistant/deploy/sipp
# plays four stretches of speech, two voices taking turns).
MEETING_LINES = [
    "Good morning, everyone. Let's go through the launch plan for the new booking page.",
    "Thanks. The design is ready; the open question is the release date.",
    "Then we agree to ship on the fifteenth. Action item: Priya books the accessibility review.",
    "Agreed. I will send the summary to the client by Friday.",
]
MEETING_REPORT = {
    "summary": "The team reviewed the launch plan for the new booking page and set the release date.",
    "decisions": ["Ship the new booking page on the fifteenth."],
    "actionItems": [
        {"owner": "Priya", "item": "Book the accessibility review", "due": None},
        {"owner": "Sam Rivera", "item": "Send the summary to the client", "due": "Friday"},
    ],
    "speakers": [
        {"name": "Alex Chen", "role": "internal", "talkTimeMs": 4300, "attribution": "inferred",
         "sentiment": "positive", "emotion": "focused"},
        {"name": "Sam Rivera", "role": "external", "talkTimeMs": 3500, "attribution": "inferred",
         "sentiment": "positive", "emotion": "engaged"},
    ],
    "attribution": "Speakers inferred from the transcript; the call carried no speaker labels.",
}
REALTIME_SILENCE_MS = 500
REALTIME_MIN_SPEECH_MS = 300
REALTIME_LOUD_RMS = 1000


def fixed_samples():
    """200 ms of a 400 Hz square wave at 24 kHz, 16-bit little-endian mono."""
    period = AUDIO_RATE // 400
    return b"".join((4000 if (i % period) < period // 2 else -4000).to_bytes(2, "little", signed=True)
                    for i in range(AUDIO_RATE * AUDIO_MS // 1000))


def fixed_wav():
    out = io.BytesIO()
    with wave.open(out, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(AUDIO_RATE)
        w.writeframes(fixed_samples())
    return out.getvalue()


FIXED_SAMPLES = fixed_samples()
FIXED_WAV = fixed_wav()


def voice_wav():
    """800 ms of a 440 Hz sine at 24 kHz: loud enough to tell apart from comfort noise."""
    import math
    out = io.BytesIO()
    with wave.open(out, "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(AUDIO_RATE)
        w.writeframes(b"".join(int(9000 * math.sin(2 * math.pi * 440 * i / AUDIO_RATE)).to_bytes(
            2, "little", signed=True) for i in range(AUDIO_RATE * 800 // 1000)))
    return out.getvalue()


VOICE_WAV = voice_wav()


def wav_ms(data):
    """The duration of a WAV file in milliseconds, 0 when it is not one."""
    try:
        with wave.open(io.BytesIO(data), "rb") as w:
            return w.getnframes() * 1000 // max(1, w.getframerate())
    except (wave.Error, EOFError):
        return 0


def last_turn_ms(request):
    """The duration of the latest user turn's audio, None when the request carries none."""
    found = None
    for message in request.get("messages") or []:
        content = message.get("content")
        for part in content if isinstance(content, list) else []:
            if part.get("type") == "input_audio":
                audio = part.get("input_audio") or {}
                found = wav_ms(base64.b64decode(audio.get("data", "") or ""))
    return found


def voice_transcript(request):
    turn = last_turn_ms(request)
    if turn is None:
        return VOICE_GREETING
    return VOICE_TRANSFER if turn >= VOICE_LONG_TURN_MS else VOICE_ANSWER
RELAY_TARGETS = {"anthropic": "https://api.anthropic.com"}

STATE = {
    "outage": os.environ.get("STUB_OUTAGE", "1") not in ("0", "false", "off", ""),
    "sink": [],
    "stats": {},
    "gemini": [],   # generateContent requests: {model, parts: [{mimeType, bytes, sha256}]}
    "jobs": {},     # OpenAI background jobs: id -> {model, polls, completed, json}
    "batches": {},  # Anthropic batches: id -> {custom_id, model, polls, json}
    "seq": 0,
}
LOCK = threading.Lock()


def log(message):
    print(f"model-stub: {message}", flush=True)


def next_id(prefix):
    with LOCK:
        STATE["seq"] += 1
        return f"{prefix}{STATE['seq']}"


def count(route):
    with LOCK:
        STATE["stats"][route] = STATE["stats"].get(route, 0) + 1


def tool_arguments(schema):
    """Fill every property of the tool's JSON Schema with a fixed value."""
    properties = (schema or {}).get("properties") or {}
    return {name: TOOL_ARGUMENT for name in properties}


# ---- OpenAI wire -----------------------------------------------------------------------------


def input_audio_problem(request):
    """None when every `input_audio` part is well-formed, else what is wrong with one."""
    for message in request.get("messages") or []:
        content = message.get("content")
        for part in content if isinstance(content, list) else []:
            if part.get("type") != "input_audio":
                continue
            audio = part.get("input_audio") or {}
            count("chat-audio-in")
            try:
                data = base64.b64decode(audio.get("data", ""), validate=True)
            except ValueError:
                return "input_audio.data is not base64"
            if audio.get("format") not in ("wav", "mp3"):
                return "input_audio.format must be wav or mp3"
            if audio.get("format") == "wav" and not (data[:4] == b"RIFF" and data[8:12] == b"WAVE"):
                return "input_audio.data is not a WAV file"
    return None


def chat_answer(request):
    model = request.get("model", "test-model")
    message = {"role": "assistant", "content": SUMMARY}
    finish = "stop"
    tools = request.get("tools") or []
    if "voice" in model and "audio" in (request.get("modalities") or []):
        count("chat-voice")
        message = {"role": "assistant", "content": None, "audio": {
            "id": "audio_stub_voice", "data": base64.b64encode(VOICE_WAV).decode(),
            "expires_at": int(time.time()) + 3600, "transcript": voice_transcript(request)}}
    elif "analytics" in model and (request.get("response_format") or {}).get("type") == "json_object":
        count("chat-analytics")
        message["content"] = json.dumps(ANALYTICS)
    elif "audio" in (request.get("modalities") or []):
        count("chat-audio-out")
        message = {"role": "assistant", "content": None, "audio": {
            "id": "audio_stub_1", "data": base64.b64encode(FIXED_WAV).decode(),
            "expires_at": int(time.time()) + 3600, "transcript": SPOKEN_TRANSCRIPT}}
    elif tools:
        function = tools[0].get("function", {})
        message = {"role": "assistant", "content": None, "tool_calls": [{
            "id": "call_stub_1", "type": "function",
            "function": {"name": function.get("name", ""),
                         "arguments": json.dumps(tool_arguments(function.get("parameters")))},
        }]}
        finish = "tool_calls"
    elif (request.get("response_format") or {}).get("type") == "json_object":
        message["content"] = JSON_ANSWER
    return {
        "id": "chatcmpl-stub-1", "object": "chat.completion", "model": model,
        "choices": [{"index": 0, "message": message, "finish_reason": finish}],
        "usage": {"prompt_tokens": USAGE_IN, "completion_tokens": USAGE_OUT,
                  "total_tokens": USAGE_IN + USAGE_OUT},
    }


def chat_stream_events(request):
    model = request.get("model", "test-model")
    base = {"id": "chatcmpl-stub-1", "object": "chat.completion.chunk", "model": model}
    for i, word in enumerate(SUMMARY.split(" ")):
        piece = word if i == 0 else " " + word
        yield None, {**base, "choices": [{"index": 0, "delta": {"content": piece},
                                          "finish_reason": None}]}
    yield None, {**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]}
    yield None, {**base, "choices": [], "usage": {"prompt_tokens": USAGE_IN,
                                                   "completion_tokens": USAGE_OUT,
                                                   "total_tokens": USAGE_IN + USAGE_OUT}}
    yield None, "[DONE]"


def response_object(job_id, job, status):
    body = {"id": job_id, "object": "response", "status": status, "model": job["model"]}
    if status == "completed":
        if job["json"] and "meeting" in job["model"]:
            text = json.dumps(MEETING_REPORT)
        else:
            text = JSON_ANSWER if job["json"] else SUMMARY
        body["output"] = [{"type": "message", "role": "assistant", "status": "completed",
                           "content": [{"type": "output_text", "text": text}]}]
        body["usage"] = {"input_tokens": USAGE_IN, "output_tokens": USAGE_OUT}
    return body


def send_webhook(job_id, target, secret):
    """Complete a `-webhook` job and announce it, signed with Standard Webhooks."""
    time.sleep(1.0)
    with LOCK:
        STATE["jobs"][job_id]["completed"] = True
    raw = secret[len("whsec_"):] if secret.startswith("whsec_") else secret
    try:
        key = base64.b64decode(raw, validate=True)
    except ValueError:
        key = raw.encode()
    body = json.dumps({"object": "event", "id": "evt_" + job_id, "type": "response.completed",
                       "created_at": int(time.time()), "data": {"id": job_id}}).encode()
    for attempt in range(1, 6):
        message_id, stamp = f"msg_{job_id}_{attempt}", str(int(time.time()))
        signed = message_id.encode() + b"." + stamp.encode() + b"." + body
        signature = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
        request = urllib.request.Request(target, data=body, method="POST", headers={
            "content-type": "application/json", "webhook-id": message_id,
            "webhook-timestamp": stamp, "webhook-signature": f"v1,{signature}"})
        try:
            with urllib.request.urlopen(request, timeout=10) as answer:
                log(f"webhook for {job_id} -> HTTP {answer.status}")
                if answer.status == 200:
                    return
        except urllib.error.HTTPError as error:
            log(f"webhook for {job_id} -> HTTP {error.code}")
        except OSError as error:
            log(f"webhook for {job_id} failed: {error}")
        time.sleep(attempt)


# ---- OpenAI Realtime (a minimal stdlib WebSocket) --------------------------------------------

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_read(stream):
    """One frame from a client: (opcode, payload), or (None, b"") at the end."""
    head = stream.read(2)
    if len(head) < 2:
        return None, b""
    opcode, length = head[0] & 0x0F, head[1] & 0x7F
    if length == 126:
        length = int.from_bytes(stream.read(2), "big")
    elif length == 127:
        length = int.from_bytes(stream.read(8), "big")
    mask = stream.read(4) if head[1] & 0x80 else b"\0\0\0\0"
    data = stream.read(length)
    return opcode, bytes(b ^ mask[i % 4] for i, b in enumerate(data))


def ws_write(stream, payload, opcode=1):
    data = payload.encode() if isinstance(payload, str) else payload
    head = bytes([0x80 | opcode])
    if len(data) < 126:
        head += bytes([len(data)])
    elif len(data) < 65536:
        head += bytes([126]) + len(data).to_bytes(2, "big")
    else:
        head += bytes([127]) + len(data).to_bytes(8, "big")
    stream.write(head + data)
    stream.flush()


def ulaw_to_linear(byte):
    u = ~byte & 0xFF
    magnitude = (((u & 0x0F) << 3) + 0x84) << ((u >> 4) & 0x07)
    return (0x84 - magnitude) if u & 0x80 else (magnitude - 0x84)


class RealtimeTranscriber:
    """The transcription side of a Realtime session: speech stretches -> MEETING_LINES."""

    def __init__(self, send):
        self.send = send
        self.rate, self.encoding = 24000, "pcm"
        self.ms = 0.0
        self.speaking_since = None
        self.quiet_ms = 0.0
        self.items = 0
        self.line = 0

    def configure(self, session):
        fmt = ((session.get("audio") or {}).get("input") or {}).get("format") or {}
        kind = fmt.get("type", "audio/pcm")
        self.encoding = "pcmu" if kind == "audio/pcmu" else "pcm"
        self.rate = 8000 if self.encoding == "pcmu" else int(fmt.get("rate") or 24000)

    def append(self, raw):
        if self.encoding == "pcmu":
            samples = [ulaw_to_linear(b) for b in raw]
        else:
            samples = [int.from_bytes(raw[i:i + 2], "little", signed=True)
                       for i in range(0, len(raw) - 1, 2)]
        frame = max(1, self.rate // 50)
        for start in range(0, len(samples), frame):
            chunk = samples[start:start + frame]
            rms = (sum(x * x for x in chunk) / max(1, len(chunk))) ** 0.5
            step = len(chunk) * 1000.0 / self.rate
            if rms >= REALTIME_LOUD_RMS:
                if self.speaking_since is None:
                    self.speaking_since = self.ms
                self.quiet_ms = 0.0
            elif self.speaking_since is not None:
                self.quiet_ms += step
                if self.quiet_ms >= REALTIME_SILENCE_MS:
                    self.finish(self.ms - self.quiet_ms + step)
            self.ms += step

    def finish(self, end_ms):
        start = self.speaking_since
        self.speaking_since, self.quiet_ms = None, 0.0
        if start is None or end_ms - start < REALTIME_MIN_SPEECH_MS:
            return
        self.items += 1
        item = f"item_stub_{self.items}"
        text = MEETING_LINES[self.line % len(MEETING_LINES)]
        self.line += 1
        count("realtime-segments")
        self.send({"type": "input_audio_buffer.speech_started", "item_id": item,
                   "audio_start_ms": int(start)})
        self.send({"type": "input_audio_buffer.speech_stopped", "item_id": item,
                   "audio_end_ms": int(end_ms)})
        self.send({"type": "conversation.item.input_audio_transcription.completed",
                   "item_id": item, "content_index": 0, "transcript": text})

    def commit(self):
        if self.speaking_since is not None:
            self.finish(self.ms)


# ---- Gemini native wire ------------------------------------------------------------------------

def record_gemini(request, model):
    """Keep what reached the model: each inline part's media type, size and SHA-256 (never the
    bytes themselves)."""
    parts = []
    for content in request.get("contents") or []:
        for part in content.get("parts") or []:
            inline = part.get("inlineData")
            if not isinstance(inline, dict):
                continue
            try:
                data = base64.b64decode(inline.get("data") or "", validate=True)
            except ValueError:
                data = b""
            parts.append({"mimeType": inline.get("mimeType"), "bytes": len(data),
                          "sha256": hashlib.sha256(data).hexdigest()})
    with LOCK:
        STATE["gemini"].append({"model": model, "parts": parts})


def gemini_answer(request, model=""):
    parts = [part for content in request.get("contents") or []
             for part in (content.get("parts") or [])]
    kinds = [str((part.get("inlineData") or {}).get("mimeType", "")) for part in parts]
    images = sum(1 for kind in kinds if kind.startswith("image/"))
    audio = any(kind.startswith("audio/") for kind in kinds)
    json_mode = (request.get("generationConfig") or {}).get("responseMimeType") == \
        "application/json"
    if json_mode and "vision" in model:
        # examples/ai-advanced vision.bpmn: a neutral scene description that echoes how many
        # images (frames) and whether a clip reached the model.
        clips = sum(1 for kind in kinds if kind.startswith("video/"))
        text = json.dumps({
            "scene": "STUB SCENE: a bicycle leans against a blue door.",
            "objects": ["bicycle", "door"],
            "framesSeen": images,
            "clipsSeen": clips,
        })
    elif json_mode:
        text = json.dumps({
            "expression": "attentive" if images else "not visible",
            "engagement": "high" if audio else "medium",
            "attention": "on the meeting",
            "bodyLanguage": ["nodding", "open posture"] if images else [],
            "emotion": {"label": "interested", "confidence": 0.6,
                        "modalities": {"voice": audio, "face": images > 0,
                                       "body": images > 0, "words": True}},
            "disagreement": {"flagged": False, "detail": None},
            "reactions": [],
            "framesSeen": images,
            "audioSeen": audio,
        })
    else:
        text = SUMMARY
    return {"candidates": [{"content": {"role": "model", "parts": [{"text": text}]},
                            "finishReason": "STOP"}],
            "usageMetadata": {"promptTokenCount": USAGE_IN, "candidatesTokenCount": USAGE_OUT}}


# ---- Anthropic wire --------------------------------------------------------------------------


def wants_json(request):
    system = request.get("system") or ""
    if isinstance(system, list):
        system = " ".join(block.get("text", "") for block in system)
    return "JSON object" in system


def message_answer(request):
    model = request.get("model", "test-model")
    tools = request.get("tools") or []
    if tools:
        content = [{"type": "tool_use", "id": "toolu_stub_1", "name": tools[0].get("name", ""),
                    "input": tool_arguments(tools[0].get("input_schema"))}]
        stop = "tool_use"
    else:
        content = [{"type": "text", "text": JSON_ANSWER if wants_json(request) else SUMMARY}]
        stop = "end_turn"
    return {"id": "msg_stub_1", "type": "message", "role": "assistant", "model": model,
            "content": content, "stop_reason": stop, "stop_sequence": None,
            "usage": {"input_tokens": USAGE_IN, "output_tokens": USAGE_OUT}}


def message_stream_events(request):
    model = request.get("model", "test-model")
    yield "message_start", {"type": "message_start", "message": {
        "id": "msg_stub_1", "type": "message", "role": "assistant", "model": model,
        "content": [], "stop_reason": None,
        "usage": {"input_tokens": USAGE_IN, "output_tokens": 1}}}
    yield "content_block_start", {"type": "content_block_start", "index": 0,
                                  "content_block": {"type": "text", "text": ""}}
    for i, word in enumerate(SUMMARY.split(" ")):
        piece = word if i == 0 else " " + word
        yield "content_block_delta", {"type": "content_block_delta", "index": 0,
                                      "delta": {"type": "text_delta", "text": piece}}
    yield "content_block_stop", {"type": "content_block_stop", "index": 0}
    yield "message_delta", {"type": "message_delta",
                            "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                            "usage": {"output_tokens": USAGE_OUT}}
    yield "message_stop", {"type": "message_stop"}


def batch_object(batch_id, status, host):
    body = {"id": batch_id, "type": "message_batch", "processing_status": status,
            "request_counts": {"processing": 0 if status == "ended" else 1,
                               "succeeded": 1 if status == "ended" else 0,
                               "errored": 0, "canceled": 0, "expired": 0},
            "results_url": None}
    if status == "ended":
        body["results_url"] = f"http://{host}/v1/messages/batches/{batch_id}/results"
    return body


# ---- the server ------------------------------------------------------------------------------


class Handler(BaseHTTPRequestHandler):
    def _body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))

    def _send(self, code, payload=None, content_type="application/json"):
        if payload is None:
            data = b""
        elif isinstance(payload, (bytes, str)):
            data = payload.encode() if isinstance(payload, str) else payload
        else:
            data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _unavailable(self):
        self._send(503, {"type": "error", "error": {
            "type": "overloaded_error", "message": "model-stub: simulated outage"}})

    def _stream(self, events):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        for name, data in events:
            frame = "" if name is None else f"event: {name}\n"
            frame += f"data: {data if isinstance(data, str) else json.dumps(data)}\n\n"
            self.wfile.write(frame.encode())
            self.wfile.flush()
        self.close_connection = True

    def _gate(self, path):
        """Strip a /flaky or /relay prefix. Returns (path, relay-base) or None if answered."""
        if path.startswith("/flaky/"):
            count("flaky")
            if STATE["outage"]:
                self._unavailable()
                return None
            return path[len("/flaky"):], None
        if path.startswith("/relay/"):
            target, _, rest = path[len("/relay/"):].partition("/")
            count("relay")
            if target not in RELAY_TARGETS:
                self._send(404)
                return None
            if STATE["outage"]:
                self._unavailable()
                return None
            return "/" + rest, RELAY_TARGETS[target]
        return path, None

    def _relay(self, base, path, raw):
        """Forward to a real provider, unchanged, and pass its answer back."""
        keep = ("content-type", "x-api-key", "anthropic-version", "anthropic-beta",
                "authorization", "accept")
        headers = {k: v for k, v in self.headers.items() if k.lower() in keep}
        request = urllib.request.Request(base + path, data=raw if raw else None,
                                         method=self.command, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=120) as answer:
                status, kind, data = answer.status, answer.headers.get("Content-Type"), answer.read()
        except urllib.error.HTTPError as error:
            status, kind, data = error.code, error.headers.get("Content-Type"), error.read()
        except OSError as error:
            log(f"relay to {base} failed: {error}")
            self._send(502, {"type": "error", "error": {"type": "api_error",
                                                        "message": "model-stub: relay failed"}})
            return
        log(f"relay {self.command} {path} -> HTTP {status}")
        self._send(status, data, kind or "application/json")

    def do_GET(self):
        gated = self._gate(self.path)
        if gated is None:
            return
        path, relay = gated
        if relay:
            self._relay(relay, path, b"")
        elif path == "/health":
            self._send(200, {"status": "ok"})
        elif path.split("?")[0] == "/v1/realtime" and \
                self.headers.get("Upgrade", "").lower() == "websocket":
            self._realtime()
        elif path == "/sink":
            with LOCK:
                self._send(200, list(STATE["sink"]))
        elif path == "/stats":
            with LOCK:
                self._send(200, dict(STATE["stats"]))
        elif path == "/gemini/requests":
            with LOCK:
                self._send(200, list(STATE["gemini"]))
        elif path == "/control/outage":
            self._send(200, {"on": STATE["outage"]})
        elif path.startswith("/v1/responses/"):
            self._poll_response(path[len("/v1/responses/"):])
        elif path.startswith("/v1/messages/batches/"):
            rest = path[len("/v1/messages/batches/"):]
            if rest.endswith("/results"):
                self._batch_results(rest[:-len("/results")])
            else:
                self._poll_batch(rest)
        else:
            self._send(404)

    def do_POST(self):
        raw = self._body()
        gated = self._gate(self.path)
        if gated is None:
            return
        path, relay = gated
        if relay:
            self._relay(relay, path, raw)
            return
        path = path.rstrip("/")
        if path == "/sink":
            text = raw.decode(errors="replace")
            with LOCK:
                STATE["sink"].append(text)
            log(f"SINK received {text}")
            self._send(200, {"received": True})
            return
        if path == "/control/outage":
            STATE["outage"] = bool(json.loads(raw or b"{}").get("on"))
            log(f"outage switch {'on' if STATE['outage'] else 'off'}")
            self._send(200, {"on": STATE["outage"]})
            return
        if path == "/v1/audio/transcriptions":
            self._transcribe(raw)
            return
        request = json.loads(raw or b"{}")
        if path == "/v1/chat/completions":
            count("chat")
            count(f"chat:{request.get('model')}")
            log(f"chat completion model={request.get('model')!r} "
                f"stream={bool(request.get('stream'))} tools={len(request.get('tools') or [])}")
            problem = input_audio_problem(request)
            if problem:
                self._send(400, {"error": {"message": f"model-stub: {problem}"}})
            elif request.get("stream"):
                self._stream(chat_stream_events(request))
            else:
                self._send(200, chat_answer(request))
        elif path == "/v1/audio/speech":
            self._speak(request)
        elif path == "/v1/responses":
            self._submit_response(request)
        elif path == "/v1/messages":
            count("messages")
            log(f"anthropic message model={request.get('model')!r} "
                f"stream={bool(request.get('stream'))} tools={len(request.get('tools') or [])}")
            if request.get("stream"):
                self._stream(message_stream_events(request))
            else:
                self._send(200, message_answer(request))
        elif path == "/v1/messages/batches":
            self._submit_batch(request)
        elif path.startswith("/v1beta/models/") and path.endswith(":generateContent"):
            count("gemini-generate")
            log(f"gemini generateContent model={path[len('/v1beta/models/'):-16]!r}")
            record_gemini(request, path[len('/v1beta/models/'):-16])
            self._send(200, gemini_answer(request, path[len('/v1beta/models/'):-16]))
        else:
            self._send(404)

    # -- OpenAI Realtime --

    def _realtime(self):
        count("realtime-sessions")
        key = self.headers.get("Sec-WebSocket-Key", "")
        accept = base64.b64encode(hashlib.sha1((key + WS_GUID).encode()).digest()).decode()
        self.wfile.write(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                          "Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept +
                          "\r\n\r\n").encode())
        self.wfile.flush()
        self.close_connection = True
        lock = threading.Lock()

        def send(event):
            with lock:
                ws_write(self.wfile, json.dumps(event))

        transcriber = RealtimeTranscriber(send)
        log(f"realtime session {self.path.split('?', 1)[-1]!r}")
        while True:
            opcode, payload = ws_read(self.rfile)
            if opcode is None or opcode == 8:
                if opcode == 8:
                    with lock:
                        ws_write(self.wfile, b"", 8)
                break
            if opcode == 9:
                with lock:
                    ws_write(self.wfile, payload, 10)
                continue
            if opcode != 1:
                continue
            event = json.loads(payload or b"{}")
            kind = event.get("type")
            if kind == "session.update":
                session = event.get("session") or {}
                transcriber.configure(session)
                send({"type": "session.updated", "session": session})
            elif kind == "input_audio_buffer.append":
                transcriber.append(base64.b64decode(event.get("audio", "")))
            elif kind == "input_audio_buffer.commit":
                transcriber.commit()
        log(f"realtime session closed after {transcriber.items} segments")

    # -- OpenAI speech --

    def _transcribe(self, raw):
        count("transcriptions")
        kind = self.headers.get("Content-Type", "")
        if not kind.startswith("multipart/form-data") or b'name="model"' not in raw \
                or b'name="file"' not in raw:
            self._send(400, {"error": {"message": "model-stub: expected a multipart form "
                                                  "with model and file"}})
            return
        log("transcription")
        self._send(200, {"text": STT_TRANSCRIPT})

    def _speak(self, request):
        count("speech")
        fmt = request.get("response_format", "mp3")
        log(f"speech model={request.get('model')!r} format={fmt!r}")
        if not request.get("input"):
            self._send(400, {"error": {"message": "model-stub: speech needs input"}})
        elif fmt == "wav":
            self._send(200, FIXED_WAV, "audio/wav")
        elif fmt == "pcm":
            self._send(200, FIXED_SAMPLES, "audio/pcm")
        else:
            self._send(400, {"error": {"message": f"model-stub: speech format {fmt} not stubbed"}})

    # -- OpenAI background jobs --

    def _submit_response(self, request):
        count("responses")
        if not request.get("background"):
            self._send(400, {"error": {"message": "model-stub: only background responses"}})
            return
        job_id = next_id("resp_stub_")
        model = request.get("model", "test-model")
        json_mode = ((request.get("text") or {}).get("format") or {}).get("type") == "json_object"
        secret = os.environ.get("STUB_WEBHOOK_SECRET", "")
        by_webhook = model.endswith("-webhook") and bool(secret)
        with LOCK:
            STATE["jobs"][job_id] = {"model": model, "polls": 0, "completed": False,
                                     "json": json_mode, "by_webhook": by_webhook}
        log(f"background job {job_id} model={model!r}")
        if by_webhook:
            target = os.environ.get("STUB_WEBHOOK_URL") or \
                f"http://{self.client_address[0]}:8080/llm/openai-hooks"
            threading.Thread(target=send_webhook, args=(job_id, target, secret),
                             daemon=True).start()
        self._send(200, response_object(job_id, STATE["jobs"][job_id], "queued"))

    def _poll_response(self, job_id):
        count("responses-poll")
        with LOCK:
            job = STATE["jobs"].get(job_id)
            if job is None:
                self._send(404, {"error": {"message": "no such response"}})
                return
            job["polls"] += 1
            if not job["by_webhook"] and job["polls"] >= 2:
                job["completed"] = True
            status = "completed" if job["completed"] else "in_progress"
        self._send(200, response_object(job_id, job, status))

    # -- Anthropic Message Batches --

    def _submit_batch(self, request):
        count("batches")
        entries = request.get("requests") or [{}]
        params = entries[0].get("params") or {}
        batch_id = next_id("msgbatch_stub_")
        with LOCK:
            STATE["batches"][batch_id] = {"custom_id": entries[0].get("custom_id", ""),
                                          "params": params, "polls": 0}
        log(f"message batch {batch_id} model={params.get('model')!r}")
        self._send(200, batch_object(batch_id, "in_progress", self.headers.get("Host")))

    def _poll_batch(self, batch_id):
        count("batches-poll")
        with LOCK:
            batch = STATE["batches"].get(batch_id)
            if batch is None:
                self._send(404, {"type": "error", "error": {"type": "not_found_error",
                                                            "message": "no such batch"}})
                return
            batch["polls"] += 1
            status = "ended" if batch["polls"] >= 2 else "in_progress"
        self._send(200, batch_object(batch_id, status, self.headers.get("Host")))

    def _batch_results(self, batch_id):
        count("batches-results")
        with LOCK:
            batch = STATE["batches"].get(batch_id)
        if batch is None:
            self._send(404)
            return
        line = {"custom_id": batch["custom_id"],
                "result": {"type": "succeeded", "message": message_answer(batch["params"])}}
        self._send(200, json.dumps(line) + "\n", "application/binary")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    log(f"listening on {port}, outage switch {'on' if STATE['outage'] else 'off'}")
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
