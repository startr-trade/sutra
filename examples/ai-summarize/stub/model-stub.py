#!/usr/bin/env python3
"""A stand-in for the model providers, shared by the ai-summarize and ai-advanced examples.
Python 3 stdlib only. Every answer is fixed, so a run is deterministic and needs no key.

OpenAI wire (also every Chat Completions service: Gemini, Mistral, ...)
  POST /v1/chat/completions      a fixed answer; `stream: true` as server-sent events; `tools`
                                 -> a call of the first tool; `response_format: json_object` ->
                                 a JSON object as the text
  POST /v1/responses             a background job (`background: true`): answers `queued`
  GET  /v1/responses/<id>        `in_progress` on the first poll, `completed` after it. A job
                                 whose model id ends in `-webhook` is instead completed by a
                                 signed webhook (see WEBHOOKS) and stays `in_progress` until then,
                                 when STUB_WEBHOOK_SECRET is set

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
  GET  /stats                    requests served, by route
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
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SUMMARY = "STUB SUMMARY: the customer reports a problem and asks for it to be looked at."
JSON_ANSWER = '{"category": "outage", "urgency": "high"}'
TOOL_ARGUMENT = "Paris"
USAGE_IN, USAGE_OUT = 42, 17
RELAY_TARGETS = {"anthropic": "https://api.anthropic.com"}

STATE = {
    "outage": os.environ.get("STUB_OUTAGE", "1") not in ("0", "false", "off", ""),
    "sink": [],
    "stats": {},
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


def chat_answer(request):
    model = request.get("model", "test-model")
    message = {"role": "assistant", "content": SUMMARY}
    finish = "stop"
    tools = request.get("tools") or []
    if tools:
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
        elif path == "/sink":
            with LOCK:
                self._send(200, list(STATE["sink"]))
        elif path == "/stats":
            with LOCK:
                self._send(200, dict(STATE["stats"]))
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
        request = json.loads(raw or b"{}")
        if path == "/v1/chat/completions":
            count("chat")
            log(f"chat completion model={request.get('model')!r} "
                f"stream={bool(request.get('stream'))} tools={len(request.get('tools') or [])}")
            if request.get("stream"):
                self._stream(chat_stream_events(request))
            else:
                self._send(200, chat_answer(request))
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
        else:
            self._send(404)

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
