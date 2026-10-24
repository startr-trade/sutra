#!/usr/bin/env python3
"""A tiny stand-in for a model provider, for the ai-summarize example. Python 3 stdlib only.

  POST /v1/chat/completions   answers with a fixed completion (OpenAI Chat Completions shape);
                              with "stream": true, as server-sent events, one word per chunk
  POST /sink                  logs the body it receives (the async flow's hand-off target)
  GET  /health                200

It never reads the prompt for meaning: the point is to exercise the engine's llm transport and
the flow around it deterministically, not to model anything.
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SUMMARY = "STUB SUMMARY: the customer reports a problem and asks for it to be looked at."


class Handler(BaseHTTPRequestHandler):
    def _body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))

    def _send(self, code, payload=None):
        data = json.dumps(payload).encode() if payload is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _stream(self, model):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        def event(obj):
            self.wfile.write(f"data: {json.dumps(obj)}\n\n".encode())
            self.wfile.flush()

        words = SUMMARY.split(" ")
        for i, word in enumerate(words):
            piece = word if i == 0 else " " + word
            event({"id": "stub-completion-1", "object": "chat.completion.chunk", "model": model,
                   "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}]})
        event({"id": "stub-completion-1", "object": "chat.completion.chunk", "model": model,
               "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]})
        event({"id": "stub-completion-1", "object": "chat.completion.chunk", "model": model,
               "choices": [], "usage": {"prompt_tokens": 42, "completion_tokens": 17,
                                        "total_tokens": 59}})
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()
        self.close_connection = True

    def do_GET(self):
        self._send(200, {"status": "ok"}) if self.path == "/health" else self._send(404)

    def do_POST(self):
        raw = self._body()
        if self.path.rstrip("/") == "/v1/chat/completions":
            request = json.loads(raw or b"{}")
            print(f"model-stub: completion for model={request.get('model')!r} "
                  f"messages={len(request.get('messages', []))} stream={bool(request.get('stream'))}",
                  flush=True)
            if request.get("stream"):
                self._stream(request.get("model", "test-model"))
                return
            self._send(200, {
                "id": "stub-completion-1",
                "object": "chat.completion",
                "model": request.get("model", "test-model"),
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": SUMMARY},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 42, "completion_tokens": 17, "total_tokens": 59},
            })
        elif self.path.rstrip("/") == "/sink":
            print(f"model-stub: SINK received {raw.decode(errors='replace')}", flush=True)
            self._send(200, {"received": True})
        else:
            self._send(404)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
