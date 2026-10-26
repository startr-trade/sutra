#!/usr/bin/env bash
# Run the ai-summarize and ai-advanced examples against the REAL providers and print one PASS/FAIL
# line per check (SKIP for a check whose key is missing). Costs a few cents of tokens. Keys are read
# from your environment, handed to the engine container by docker compose through ITS environment
# (never an override file), and never printed: anything the script shows has every key masked.
#
#   export OPENAI_API_KEY=...        # each key is optional; checks needing a missing key are skipped
#   export ANTHROPIC_API_KEY=...
#   export GEMINI_API_KEY=...
#   examples/ai-advanced/live-check.sh
#
# Just the Gemini paths (engine 0.6), with the engine image and CLI built from this tree:
#
#   make image                                   # sutra-rust-engine:dev from HEAD
#   (cd rust && cargo build -p sutra-cli)
#   GEMINI_API_KEY=... ONLY=gemini \
#       SUTRA_ENGINE_IMAGE=sutra-rust-engine:dev SUTRA_BIN=$PWD/rust/target/debug/sutra \
#       examples/ai-advanced/live-check.sh
#
# Just the OpenAI paths (engine 0.6), likewise — everything that needs only OPENAI_API_KEY:
#
#   OPENAI_API_KEY=... ONLY=openai \
#       SUTRA_ENGINE_IMAGE=sutra-rust-engine:dev SUTRA_BIN=$PWD/rust/target/debug/sutra \
#       examples/ai-advanced/live-check.sh
#
# ENVIRONMENT
#   OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY   the provider keys (each optional)
#   ONLY=gemini           run only the Gemini checks (ai-summarize/gemini, the Gemini checks of
#                         ai-advanced/live and the Gemini Live session); everything else is SKIP
#   ONLY=openai           run only the checks that need nothing but OPENAI_API_KEY (ai-summarize/
#                         openai, JSON mode, the background jobs, audio, the voice pipeline, the
#                         media reference, the Realtime session); the cross-provider checks
#                         (fallback, tool call, Message Batch) and Gemini are SKIP
#   OPENAI_AUDIO_MODEL    the audio chat model of the `voice` flow (default gpt-audio-1.5; it must
#                         contain `audio`, which is how the adapter knows it hears and speaks)
#   OPENAI_TRANSCRIBE_MODEL  speech to text, in the voice pipeline and the Realtime session
#                         (default gpt-transcribe)
#   OPENAI_TTS_MODEL      text to speech in the voice pipeline (default gpt-4o-mini-tts)
#   OPENAI_REALTIME_MODEL the Realtime model (default gpt-realtime, the model the llm-channels
#                         guide's live-session example names; DEFAULT_REALTIME_MODEL in
#                         rust/crates/sutra-llm-openai/tests/all/live_api.rs)
#                         The four defaults are what variants/live/channels.yaml and that test
#                         name; your account may offer other ids. Each variable is substituted into
#                         a copy of the channels at deploy time (the committed YAML is unchanged).
#                         A model-not-found failure names the variable to set.
#   SUTRA_ENGINE_IMAGE    the engine image (default ghcr.io/startr-trade/sutra:0.6.0; to check this
#                         tree instead, use sutra-rust-engine:dev built by `make image`)
#   SUTRA_BIN             the `sutra` CLI, 0.5.0 or later (default: a build in rust/target, else PATH)
#   GEMINI_LIVE_MODEL     the Gemini Live model (default gemini-3.8-live, the model the
#                         adapter's guide names; it is set in rust/crates/sutra-llm-gemini/tests/
#                         all/live_api.rs, DEFAULT_LIVE_MODEL). The server-turn-detection round
#                         trip defaults to gemini-2.5-flash-native-audio-latest instead: the sample
#                         question is synthetic speech, and gemini-3.8-live's voice-activity
#                         detection hears only its first word (a real voice works). To check
#                         gemini-3.8-live there, record your own question (16 kHz mono WAV) and set
#                         GEMINI_LIVE_WAV to it as well. If Google renames or retires it the
#                         Live checks FAIL with a model-not-found reason: export a current Live
#                         model id from Google's model list instead. Pick one that answers in
#                         audio (a "-transcribe-" or "-translate-" model is refused); list them:
#                           curl -s -H "x-goog-api-key: $GEMINI_API_KEY" \
#                             "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1000" |
#                             python3 -c 'import json,sys; [print(m["name"][7:]) for m in
#                               json.load(sys.stdin)["models"] if "bidiGenerateContent" in
#                               m.get("supportedGenerationMethods", [])]'
#                         The other Gemini checks use gemini-2.5-flash, a plain string in
#                         variants/live/channels.yaml.
#   SKIP_BATCH, BATCH_WAIT_MIN, OPENAI_WEBHOOK_SECRET, ENGINE_HOST_PORT   see below
#
# Needs docker compose, curl, python3 and the `sutra` CLI; the Gemini Live and OpenAI Realtime
# checks also need cargo (they compile and run the `#[ignore = "live"]` tests in
# sutra-llm-gemini and sutra-llm-openai, through this tree's rust/ workspace). Every container runs
# under an explicit compose project (sutra-live-<example>) and is removed on exit, failure and
# Ctrl-C included.
#
# What runs:
#   ai-summarize   for each key you exported: the openai / anthropic / gemini variant —
#                  sync, SSE-streamed and async summaries
#   ai-advanced    with OPENAI_API_KEY: the `live` variant, with a MinIO object store
#                  (deploy/compose.media.yaml) —
#                  JSON mode, an OpenAI background job (polled) and, when OPENAI_WEBHOOK_SECRET
#                  is set, one completed by webhook; audio understanding with a spoken answer
#                  (`voice`: sample/spoken-question.wav to the audio model, the answer must say
#                  Paris, audio back, audioPreserved true); the voice pipeline (`voice-pipeline`:
#                  transcribed, answered in text, spoken; audioPreserved false — its text model is
#                  Anthropic when ANTHROPIC_API_KEY is set and ONLY is not, else OpenAI
#                  gpt-4o-mini); the same audio as a media reference {ref, sha256} read from MinIO,
#                  and with a wrong SHA-256 (refused before any provider call); and an OpenAI
#                  Realtime session through the adapter (the WAV up, audio and transcripts back,
#                  a clean close; and, through a logging hop, the key in the Authorization
#                  header and in no URL).
#                  With ANTHROPIC_API_KEY too (and no ONLY): cross-provider fallback (Anthropic
#                  taken down and up again through the stub's relay, so BOTH answers are real), a
#                  tool call (Anthropic) and an Anthropic Message Batch (polled; set SKIP_BATCH=1
#                  to skip, BATCH_WAIT_MIN to change the 30-minute wait)
#   gemini         with GEMINI_API_KEY: the `live` variant's native-Gemini flows, with a MinIO
#                  object store (deploy/compose.media.yaml) —
#                  image understanding (sample/frame-1.png), video understanding (two sampled
#                  frames, and a 2 s MP4 clip, sample/moving-square.mp4), audio understanding
#                  (sample/spoken-question.wav asks "What is the capital of France?"; the answer
#                  must say Paris), a structured answer (`json: true`, parsed), the same audio as a
#                  media reference {ref, sha256} read from MinIO, a media reference with a wrong
#                  SHA-256 (refused before any provider call), and a Gemini Live session (an
#                  ephemeral token, the constrained WebSocket, the WAV up, audio and transcripts
#                  back, a clean close; and, through a logging hop, no key or token in any URL).
#                  About two minutes, plus the first cargo build of the live tests.
#
# The webhook check needs the engine reachable from the internet. Run a tunnel to
# 127.0.0.1:$ENGINE_HOST_PORT (default 18080), register https://<tunnel-host>/llm/openai-hooks as a
# webhook for `response.completed` in your OpenAI project, and export its signing secret:
#   export OPENAI_WEBHOOK_SECRET=whsec_...
#   ENGINE_HOST_PORT=18080 examples/ai-advanced/live-check.sh
# The webhook job's safety-net poll is 10 minutes, so a delivery within the 3-minute wait is the
# webhook.
set -uo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
examples="$(cd "$here/.." && pwd)"
repo="$(cd "$examples/.." && pwd)"
# The CLI built from this tree first (it matches these packages), then one on PATH.
sutra_bin=${SUTRA_BIN:-}
for candidate in "$repo/rust/target/release/sutra" "$repo/rust/target/debug/sutra" "$(command -v sutra)"; do
    [ -n "$sutra_bin" ] && break
    [ -n "$candidate" ] && [ -x "$candidate" ] && sutra_bin=$candidate
done

have() { [ -n "${!1:-}" ]; }
ONLY=${ONLY:-}
case "$ONLY" in ""|gemini|openai) ;; *) echo "ONLY=$ONLY: the values are 'gemini' and 'openai'" >&2; exit 2 ;; esac
only_gemini() { [ "$ONLY" = gemini ]; }
only_openai() { [ "$ONLY" = openai ]; }

# The OpenAI model ids, each overridable (see the header); substituted into a copy of the live
# variant's channels. Checked here: each goes into YAML and a URL query.
audio_model=${OPENAI_AUDIO_MODEL:-gpt-audio-1.5}
transcribe_model=${OPENAI_TRANSCRIBE_MODEL:-gpt-transcribe}
tts_model=${OPENAI_TTS_MODEL:-gpt-4o-mini-tts}
realtime_model=${OPENAI_REALTIME_MODEL:-gpt-realtime}
for m in "$audio_model" "$transcribe_model" "$tts_model" "$realtime_model"; do
    [[ "$m" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "model id '$m': only letters, digits, '.', '_' and '-'" >&2; exit 2; }
done

pass=0; fail=0; skip=0
# Every key is masked in anything the script prints (provider error bodies, engine logs).
mask() {
    local text=$1 name
    for name in OPENAI_API_KEY ANTHROPIC_API_KEY GEMINI_API_KEY OPENAI_WEBHOOK_SECRET; do
        if have "$name"; then text=${text//"${!name}"/<$name>}; fi
    done
    printf '%s' "$text"
}
check() {  # check <name> ok|no [detail]
    if [ "$2" = ok ]; then echo "PASS  $1${3:+  — $(mask "$3")}"; pass=$((pass + 1));
    else echo "FAIL  $1${3:+  — $(mask "$3")}"; fail=$((fail + 1)); fi
}
skipped() { echo "SKIP  $1  — $2"; skip=$((skip + 1)); }
result() {
    echo
    echo "RESULT: $pass passed, $fail failed, $skip skipped"
    [ "$fail" = 0 ]
}

GEMINI_CHECKS=("gemini: image understanding" "gemini: video understanding, frames"
    "gemini: video understanding, clip" "gemini: audio understanding" "gemini: structured answer"
    "gemini: media reference" "gemini: media reference, wrong sha256"
    "gemini live: session round trip, server turn detection" "gemini live: session round trip, manual turns"
    "gemini live: no credential in any URL")
skip_gemini() { local c; for c in "${GEMINI_CHECKS[@]}"; do skipped "$c" "$1"; done; }
# The ai-advanced checks that need only OPENAI_API_KEY (ai-summarize/openai is its own loop).
OPENAI_CHECKS=("JSON mode (OpenAI)" "OpenAI background job, polled" "OpenAI background job, webhook"
    "openai: audio understanding, spoken answer" "openai: voice pipeline" "openai: media reference"
    "openai: media reference, wrong sha256" "openai realtime: session round trip"
    "openai realtime: no key in any URL")
# The ones that need ANTHROPIC_API_KEY as well.
CROSS_CHECKS=("fallback: Anthropic up, Anthropic answers" "fallback: Anthropic down, OpenAI answers"
    "tool call (Anthropic)" "Anthropic Message Batch, polled")
skip_openai() { local c; for c in "${OPENAI_CHECKS[@]}"; do skipped "$c" "$1"; done; }
skip_cross() { local c; for c in "${CROSS_CHECKS[@]}"; do skipped "$c" "$1"; done; }
# Why the cross-provider checks do not run, or nothing when they do.
cross_skip_reason() {
    if only_openai; then echo "needs ANTHROPIC_API_KEY too; ONLY=openai runs only the OpenAI-only checks"
    elif ! have ANTHROPIC_API_KEY; then echo "needs ANTHROPIC_API_KEY too (not set)"
    elif ! have OPENAI_API_KEY; then echo "needs OPENAI_API_KEY too (not set)"
    fi
}

# Nothing to run: list what is skipped and stop before needing docker, the CLI or the image.
if { only_gemini && ! have GEMINI_API_KEY; } || { only_openai && ! have OPENAI_API_KEY; } ||
    { ! have OPENAI_API_KEY && ! have ANTHROPIC_API_KEY && ! have GEMINI_API_KEY; }; then
    for v in openai:OPENAI_API_KEY anthropic:ANTHROPIC_API_KEY gemini:GEMINI_API_KEY; do
        if [ -z "$ONLY" ] || [ "$ONLY" = "${v%%:*}" ]; then skipped "ai-summarize/${v%%:*}" "${v#*:} not set"; fi
    done
    if ! only_gemini; then
        skip_openai "OPENAI_API_KEY not set"
        only_openai || skip_cross "needs OPENAI_API_KEY and ANTHROPIC_API_KEY"
    fi
    only_openai || skip_gemini "GEMINI_API_KEY not set"
    echo "no key for what was asked: nothing to run (see the header of $0)"
    result; exit
fi

[ -n "$sutra_bin" ] || { echo "no sutra CLI: set SUTRA_BIN or put sutra on PATH" >&2; exit 2; }
# The examples use 0.5.0 features (sync calls without <q:alias>, q:stream); an older CLI rejects them.
cli_version=$("$sutra_bin" --version | awk '{print $NF}')
if [ "$(printf '%s\n' 0.5.0 "$cli_version" | sort -V | head -1)" != 0.5.0 ]; then
    echo "sutra CLI $cli_version at $sutra_bin is older than 0.5.0; build one (cargo build -p sutra-cli)" >&2
    echo "and point SUTRA_BIN at it, e.g. SUTRA_BIN=\$PWD/rust/target/debug/sutra" >&2
    exit 2
fi
echo "using $("$sutra_bin" --version) ($sutra_bin); engine image ${SUTRA_ENGINE_IMAGE:-ghcr.io/startr-trade/sutra:0.6.0}"
image=${SUTRA_ENGINE_IMAGE:-ghcr.io/startr-trade/sutra:0.6.0}
if ! docker image inspect "$image" >/dev/null 2>&1 && ! docker pull -q "$image" >/dev/null 2>&1; then
    echo "engine image $image is not available locally and cannot be pulled; build one (make image)" >&2
    echo "and set SUTRA_ENGINE_IMAGE, e.g. SUTRA_ENGINE_IMAGE=sutra-rust-engine:dev" >&2
    exit 2
fi
BATCH_WAIT_MIN=${BATCH_WAIT_MIN:-30}
if have OPENAI_WEBHOOK_SECRET; then export ENGINE_HOST_PORT=${ENGINE_HOST_PORT:-18080}; fi

work=$(mktemp -d)
# One line of a failed reply: its problem `detail` (where the provider's own words are) when it
# is a problem document, else the body; whitespace collapsed, truncated. Replies go to python on
# stdin, never as an argument: a spoken answer carries its audio as base64, past the kernel's
# argument size limit.
why() {
    python3 -c 'import json, sys
t = sys.stdin.read()
try:
    d = json.loads(t)
    t = d.get("detail") or t if isinstance(d, dict) else t
except ValueError:
    pass
print(" ".join(str(t).split())[:400])' <<<"$1"
}
json_ok() { python3 -c "import json,sys; d=json.load(sys.stdin); assert $2" <<<"$1" 2>/dev/null; }

# ---- one stack at a time ------------------------------------------------------------------------

project=""; compose_files=()
dc() { docker compose -p "$project" "${compose_files[@]}" "$@"; }
# Down by the explicit project name, then remove any container still labelled with it.
down() {
    [ -n "$project" ] || return 0
    dc down -v --remove-orphans >/dev/null 2>&1
    docker ps -aq --filter "label=com.docker.compose.project=$project" | xargs -r docker rm -f >/dev/null 2>&1
    project=""
}
trap 'down; rm -rf "$work"' EXIT
trap 'exit 130' INT TERM

# up <example> <variant> [extra compose file] [variant directory]: package the variant (or the
# given copy of it) over the shared flows and start the example's stack (plus the extra file,
# e.g. the object store).
up() {
    local example=$1 variant=$2 extra=${3:-} source=${4:-$examples/$1/variants/$2}
    down
    export DEPLOYMENTS_DIR="$work/$example-$variant"
    rm -rf "$DEPLOYMENTS_DIR"; mkdir -p "$DEPLOYMENTS_DIR"
    local out
    if ! out=$("$sutra_bin" package "$source" \
            --overlay "$examples/$example/shared" --out "$DEPLOYMENTS_DIR" 2>&1); then
        check "$example/$variant packages" no "$(grep -m1 ERROR <<<"$out" | cut -c1-300)"
        return 1
    fi
    chmod -R a+rX "$DEPLOYMENTS_DIR"
    project="sutra-live-$example"
    compose_files=(-f "$examples/$example/deploy/compose.yaml")
    if [ -n "$extra" ]; then x=${extra%.yaml}; project="$project-${x#compose.}"; compose_files+=(-f "$examples/$example/deploy/$extra"); fi
    local log
    log=$(dc up -d 2>&1) || { check "$example/$variant starts" no "docker compose up failed: $(tail -1 <<<"$log" | cut -c1-200)"; return 1; }
    url="http://$(dc port engine 8080)"
    for _ in $(seq 1 90); do curl -sf "$url/sutra/health/ready" >/dev/null && return 0; sleep 1; done
    check "$example/$variant starts" no "engine not ready; warnings:"
    mask "$(dc logs engine 2>&1 | grep -E '"level":"(WARN|ERROR)"' | cut -c1-300)"; echo
    return 1
}

post() { curl -s -X POST "$url/channels/$1" -H 'Content-Type: application/json' -H "X-Api-Key: $api_key" -d "$2"; }
post_code() { curl -s -o /dev/null -w '%{http_code}' -X POST "$url/channels/$1" -H 'Content-Type: application/json' -H "X-Api-Key: $api_key" -d "$2"; }
stub() { dc exec -T model-stub wget -qO- "$@" 2>/dev/null; }
sink_has() {  # sink_has <ticketId> <seconds>
    local deadline=$((SECONDS + $2))
    while [ $SECONDS -lt $deadline ]; do
        stub http://127.0.0.1:8000/sink | python3 -c 'import json, sys
sys.exit(0 if any(json.loads(b).get("ticketId") == sys.argv[1] for b in json.load(sys.stdin)) else 1)' "$1" \
            2>/dev/null && return 0
        sleep 2
    done
    return 1
}
# The first failed model call the engine logged since <timestamp> for the outbound channel
# <channel> (its outbox `destination` ends in /<channel>), as one masked line: the provider's own
# refusal. This is how an async check (a reply, a job) learns of a failure instead of waiting
# out its deadline.
engine_failure() {  # engine_failure <since> <channel>
    mask "$(dc logs --no-log-prefix --since "$1" engine 2>&1 | python3 -c 'import json, sys
for line in sys.stdin:
    try:
        d = json.loads(line)
    except ValueError:
        continue
    text = " ".join(str(d.get("error") or d.get("detail") or d.get("message") or "").split())
    if (str(d.get("destination", "")).endswith("/" + sys.argv[1]) and d.get("error")) or \
            f"llm channel \x27{sys.argv[1]}\x27" in text:
        print(text[:400])
        break' "$2")"
}
# await_sink <ticketId> <seconds> <since> <outbound channel>: 0 once the sink has the ticket; 1 on
# a failure the engine reports for the channel (its reason in $failure) or at the deadline.
await_sink() {
    local deadline=$((SECONDS + $2))
    failure=""
    while [ $SECONDS -lt $deadline ]; do
        sink_has "$1" 1 && return 0
        failure=$(engine_failure "$3" "$4")
        [ -n "$failure" ] && return 1
    done
    failure="no result within $2 s"
    return 1
}
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

outage() { stub --post-data "{\"on\": $1}" http://127.0.0.1:8000/control/outage >/dev/null; }

# ---- ai-summarize: one variant per key ----------------------------------------------------------

summarize() {  # summarize <variant>
    local v=$1 r since
    api_key=dev-only-summarize-key
    up ai-summarize "$v" || return
    r=$(post ticket "{\"ticketId\":\"LIVE-$v-1\",\"subject\":\"Cannot log in\",\"description\":\"The password reset email never arrives, I tried three times since yesterday.\"}")
    json_ok "$r" 'd["ticketId"] and len(d["summary"]) > 20' && check "ai-summarize/$v sync" ok || check "ai-summarize/$v sync" no "$(why "$r")"
    r=$(curl -s -N -X POST "$url/channels/ticket-stream" -H 'Content-Type: application/json' -H "X-Api-Key: $api_key" \
        -d "{\"ticketId\":\"LIVE-$v-2\",\"subject\":\"App is slow\",\"description\":\"Every page takes about ten seconds to load since the last update.\"}")
    if [ "$(grep -c '^event: chunk' <<<"$r")" -ge 2 ] && grep -q '^event: reply' <<<"$r"; then
        check "ai-summarize/$v streamed ($(grep -c '^event: chunk' <<<"$r") chunks)" ok
    else
        # A stream's error event carries only the engine's code, never the provider's text: the
        # sync line above (the same model channel) has the provider's reason.
        check "ai-summarize/$v streamed" no "$(why "$(grep -A1 '^event: error' <<<"$r" | tr '\n' ' ')") (a stream carries only the code; the sync check shows the provider's reason)"
    fi
    since=$(now)
    if [ "$(post_code ticket-async "{\"ticketId\":\"LIVE-$v-3\",\"subject\":\"Photo upload fails\",\"description\":\"The app shows an error every time I upload a profile photo.\"}")" != 202 ]; then
        check "ai-summarize/$v async" no "the async channel did not accept the request"
    elif await_sink "LIVE-$v-3" 60 "$since" summarize-async; then check "ai-summarize/$v async" ok
    else check "ai-summarize/$v async" no "$failure"; fi
}

for pair in openai:OPENAI_API_KEY anthropic:ANTHROPIC_API_KEY gemini:GEMINI_API_KEY; do
    if [ -n "$ONLY" ] && [ "${pair%%:*}" != "$ONLY" ]; then continue; fi
    if have "${pair#*:}"; then summarize "${pair%%:*}"
    else skipped "ai-summarize/${pair%%:*}" "${pair#*:} not set"; fi
done

# ---- request bodies ---------------------------------------------------------------------------

sample="$here/sample"
# body <file> <python expression over b64(name), sha(name)>: a request body, written to a file
# (the base64 media is too large for one command-line argument). Only sample data goes in it.
body() {
    python3 - "$sample" "$1" "$2" <<'EOF'
import base64, hashlib, json, os, sys
sample, out, expr = sys.argv[1:4]
read = lambda name: open(os.path.join(sample, name), "rb").read()
b64 = lambda name: base64.b64encode(read(name)).decode()
sha = lambda name: hashlib.sha256(read(name)).hexdigest()
json.dump(eval(expr), open(out, "w"))
EOF
}
# post_file <channel> <file>: the reply body, then a last line with the HTTP status.
post_file() {
    curl -s -m 120 -w '\n%{http_code}' -X POST "$url/channels/$1" -H 'Content-Type: application/json' \
        -H "X-Api-Key: $api_key" -d @"$2"
}
# live_tests <label> <crate> <check name:test name>...: run each `#[ignore = "live"]` test of the
# crate's `all` suite on its own; its `LIVE:` line is the check's reason.
live_tests() {
    local label=$1 crate=$2 t line log="$work/live.log"
    shift 2
    if ! command -v cargo >/dev/null; then
        for t in "$@"; do skipped "$label: ${t%%:*}" "cargo not found"; done
        return
    fi
    echo "      compiling and running the $label tests (cargo; the first build takes a few minutes)"
    for t in "$@"; do
        if (cd "$repo/rust" && timeout 900 cargo test -p "$crate" --test all -- \
                --ignored --nocapture --exact "live_api::${t#*:}") >"$log" 2>&1; then
            check "$label: ${t%%:*}" ok "$(grep -m1 '^LIVE: ' "$log" | cut -c7-400)"
        elif line=$(grep -m1 '^LIVE: ' "$log"); then
            check "$label: ${t%%:*}" no "$(cut -c7-400 <<<"$line")"
        else
            check "$label: ${t%%:*}" no "did not run: $(grep -m1 -E '^error|panicked' "$log" | cut -c1-200)"
        fi
    done
    rm -f "$log"
}

# ---- ai-advanced: the live variant --------------------------------------------------------------

# The live variant as deployed: a copy of variants/live with the OpenAI model ids substituted
# (the committed channels.yaml is never edited) and, when the voice pipeline is to need only
# OPENAI_API_KEY, its text candidate swapped from Anthropic to OpenAI gpt-4o-mini.
live_variant() {  # live_variant <pipeline text provider: anthropic|openai>
    local dst="$work/variant-live"
    rm -rf "$dst"; cp -R "$examples/ai-advanced/variants/live" "$dst"
    python3 - "$dst/channels.yaml" "$audio_model" "$transcribe_model" "$tts_model" "$1" <<'EOF'
import sys
path, audio, transcribe, tts, text_provider = sys.argv[1:6]
text = open(path).read()
def swap(within, old, new):
    head, sep, rest = text.partition(within)
    body, nxt, tail = rest.partition("\n  - name: ")
    assert sep and body.count(old) == 1, f"{old!r} is not in {within!r} exactly once"
    return head + sep + body.replace(old, new) + nxt + tail
text = swap("- name: voice-model\n", "model: gpt-audio-1.5\n", f"model: {audio}\n")
text = swap("- name: voice-pipeline-model\n", "transcribe-model: gpt-transcribe\n", f"transcribe-model: {transcribe}\n")
text = swap("- name: voice-pipeline-model\n", "speech-model: gpt-4o-mini-tts\n", f"speech-model: {tts}\n")
if text_provider == "openai":
    text = swap("- name: voice-pipeline-model\n",
                "- provider: anthropic\n          model: claude-haiku-5-5\n          api-key: env:ANTHROPIC_API_KEY\n",
                "- provider: openai\n          model: gpt-4o-mini\n          api-key: env:OPENAI_API_KEY\n")
open(path, "w").write(text)
EOF
}

# A model-not-found reason gets the variable that changes that model: the one whose id the
# provider named, else every one the check uses.
model_hint() {  # model_hint <reason> <VAR>...
    local reason=$1 var model named=""; shift
    grep -qiE 'model_not_found|does not exist|model[^.]*not found|invalid model|unknown model|not supported|unsupported model' <<<"$reason" || return 0
    for var in "$@"; do
        case $var in
            OPENAI_AUDIO_MODEL) model=$audio_model ;; OPENAI_TRANSCRIBE_MODEL) model=$transcribe_model ;;
            OPENAI_TTS_MODEL) model=$tts_model ;; OPENAI_REALTIME_MODEL) model=$realtime_model ;;
        esac
        grep -qF "$model" <<<"$reason" && named="$named $var (now $model)"
    done
    if [ -z "$named" ]; then for var in "$@"; do named="$named $var"; done; fi
    printf ' — the model is not available to this key: set%s to a model id your account offers' "$named"
}

# openai_call <check name> <channel> <body expression> <python assertion over d> [VAR...]
openai_call() {
    local name=$1 channel=$2 expression=$3 assertion=$4 r status reply
    shift 4
    body "$work/body.json" "$expression"
    r=$(post_file "$channel" "$work/body.json")
    status=${r##*$'\n'}; reply=${r%$'\n'*}
    if [ "$status" = 200 ] && json_ok "$reply" "$assertion"; then
        check "$name" ok "$(python3 -c 'import json, sys
d = json.load(sys.stdin)
a = d.get("audio") or {}
print("text: %r, transcript: %r, audio: %d bytes %s, audioPreserved: %s, provider: %s" % (
    str(d.get("text"))[:80], str(d.get("transcript"))[:80], len(a.get("data") or "") * 3 // 4,
    a.get("format"), d.get("audioPreserved"), d.get("provider")))' <<<"$reply")"
    else
        local reason
        reason="HTTP $status: $(why "$reply")"
        check "$name" no "$reason$(model_hint "$reason" "$@")"
    fi
}

advanced() {
    local r since cross text_provider=openai
    cross=$(cross_skip_reason)
    [ -z "$cross" ] && text_provider=anthropic
    api_key=dev-only-advanced-key
    live_variant "$text_provider"
    if ! up ai-advanced live compose.media.yaml "$work/variant-live"; then
        skip_openai "the stack did not start (FAIL above)"
        [ -z "$cross" ] && skip_cross "the stack did not start (FAIL above)"
        return
    fi

    if [ -z "$cross" ]; then
        outage false
        r=$(post fallback '{}')
        json_ok "$r" 'd["provider"] == "anthropic" and d["text"]' && check "fallback: Anthropic up, Anthropic answers" ok \
            || check "fallback: Anthropic up, Anthropic answers" no "$(why "$r")"
        outage true
        r=$(post fallback '{}')
        json_ok "$r" 'd["provider"] == "openai" and d["text"]' && check "fallback: Anthropic down, OpenAI answers" ok \
            || check "fallback: Anthropic down, OpenAI answers" no "$(why "$r")"
        outage false
    fi

    r=$(post classify '{"ticketId":"LIVE-JSON-1","description":"The checkout page crashes for every customer since this morning."}')
    json_ok "$r" 'isinstance(json.loads(d["text"]), dict)' && check "JSON mode (OpenAI)" ok "$(cut -c1-160 <<<"$r")" \
        || check "JSON mode (OpenAI)" no "$(why "$r")"

    if [ -z "$cross" ]; then
        r=$(post weather '{}')
        json_ok "$r" 'd["finish"] == "tool-calls" and d["toolCalls"][0]["name"] == "get_weather" and "city" in d["toolCalls"][0]["arguments"]' \
            && check "tool call (Anthropic)" ok || check "tool call (Anthropic)" no "$(why "$r")"
    fi

    since=$(now)
    if [ "$(post_code summarize-job '{"ticketId":"LIVE-JOB-1","description":"My parcel was marked delivered but never arrived."}')" != 202 ]; then
        check "OpenAI background job, polled" no "the job channel did not accept the request"
    elif await_sink LIVE-JOB-1 180 "$since" job-model; then check "OpenAI background job, polled" ok
    else check "OpenAI background job, polled" no "$failure"; fi

    if have OPENAI_WEBHOOK_SECRET; then
        since=$(now)
        if [ "$(post_code summarize-webhook '{"ticketId":"LIVE-HOOK-1","description":"The search page shows no results for any query since this morning."}')" != 202 ]; then
            check "OpenAI background job, webhook" no "the job channel did not accept the request"
        elif await_sink LIVE-HOOK-1 180 "$since" webhook-model; then check "OpenAI background job, webhook" ok
        else check "OpenAI background job, webhook" no "$failure (tunnel to 127.0.0.1:$ENGINE_HOST_PORT? secret?)"; fi
    else
        skipped "OpenAI background job, webhook" "set OPENAI_WEBHOOK_SECRET and run a tunnel (see the header)"
    fi

    # Audio: the sample asks "What is the capital of France?".
    local says_paris='"paris" in (str(d.get("text") or "") + " " + str(d.get("transcript") or "")).lower()'
    local has_audio='isinstance(d.get("audio"), dict) and len(d["audio"].get("data") or "") > 100'
    openai_call "openai: audio understanding, spoken answer" voice \
        '{"requestId": "O-AUD-1", "audio": {"data": b64("spoken-question.wav"), "format": "audio/wav"}, "audioOutput": "audio/wav"}' \
        "$says_paris and $has_audio and d.get(\"audioPreserved\") is True and d.get(\"provider\") == \"openai\"" \
        OPENAI_AUDIO_MODEL
    openai_call "openai: voice pipeline" voice-pipeline \
        '{"requestId": "O-PIPE-1", "audio": {"data": b64("spoken-question.wav"), "format": "audio/wav"}, "audioOutput": "audio/wav"}' \
        "str(d.get(\"transcript\") or \"\").strip() and str(d.get(\"text\") or \"\").strip() and \"paris\" in str(d[\"text\"]).lower() and $has_audio and d.get(\"audioPreserved\") is False and d.get(\"provider\") == \"$text_provider\"" \
        OPENAI_TRANSCRIBE_MODEL OPENAI_TTS_MODEL

    # The same audio as a media reference, read by voice-model from the MinIO store `media`.
    if dc exec -T store mc mb -p local/media >/dev/null 2>&1 &&
        dc exec -T store mc pipe local/media/clips/question.wav <"$sample/spoken-question.wav" >/dev/null 2>&1; then
        openai_call "openai: media reference" voice \
            '{"requestId": "O-REF-1", "audio": {"ref": "s3://media/clips/question.wav", "sha256": sha("spoken-question.wav"), "format": "audio/wav"}, "audioOutput": "audio/wav"}' \
            "$says_paris and $has_audio and d.get(\"audioPreserved\") is True" OPENAI_AUDIO_MODEL
        # A digest that is not the object's: refused by the transport before any provider call,
        # so the reason is the refusal, never OpenAI's (with a bad key: not its auth refusal).
        local status reply
        body "$work/body.json" '{"requestId": "O-REF-2", "audio": {"ref": "s3://media/clips/question.wav", "sha256": "0" * 64, "format": "audio/wav"}, "audioOutput": "audio/wav"}'
        r=$(post_file voice "$work/body.json"); status=${r##*$'\n'}; reply=${r%$'\n'*}
        if [ "$status" != 200 ] && grep -qiE 'MEDIA\.REFUSED|sha-?256|digest' <<<"$reply" &&
            ! grep -qiE 'api key|api_key|openai\.com' <<<"$reply"; then
            check "openai: media reference, wrong sha256" ok "refused before any provider call, HTTP $status: $(why "$reply")"
        else
            check "openai: media reference, wrong sha256" no "HTTP $status: $(why "$reply")"
        fi
    else
        check "openai: media reference" no "could not put the sample in the MinIO store"
        check "openai: media reference, wrong sha256" no "could not put the sample in the MinIO store"
    fi

    if [ -z "$cross" ]; then
        if [ -n "${SKIP_BATCH:-}" ]; then
            skipped "Anthropic Message Batch, polled" "SKIP_BATCH set"
        else
            echo "      Anthropic Message Batch submitted; batches usually end in minutes (waiting up to $BATCH_WAIT_MIN min)"
            since=$(now)
            if [ "$(post_code summarize-batch '{"ticketId":"LIVE-BATCH-1","description":"The app logs me out every few minutes on my phone."}')" != 202 ]; then
                check "Anthropic Message Batch, polled" no "the batch channel did not accept the request"
            elif await_sink LIVE-BATCH-1 $((BATCH_WAIT_MIN * 60)) "$since" batch-model; then check "Anthropic Message Batch, polled" ok
            else check "Anthropic Message Batch, polled" no "$failure"; fi
        fi
    else
        skip_cross "$cross"
    fi
    down
    openai_realtime
}

# The OpenAI Realtime API, through the adapter itself (no engine): three #[ignore = "live"] tests.
openai_realtime() {
    export OPENAI_REALTIME_MODEL=$realtime_model OPENAI_TRANSCRIBE_MODEL=$transcribe_model
    live_tests "openai realtime" sutra-llm-openai \
        "session round trip, manual turns:live_session_answers_the_sample_wav" \
        "session round trip, server turn detection:live_session_answers_with_server_turn_detection" \
        "no key in any URL:live_urls_carry_no_key"
}

if only_gemini; then :
elif have OPENAI_API_KEY; then advanced
else
    skip_openai "OPENAI_API_KEY not set"
    skip_cross "$(cross_skip_reason)"
fi

# ---- gemini: the native Gemini paths (engine 0.6) -----------------------------------------------

# gemini_call <check name> <channel> <body expression> <python assertion over d>
gemini_call() {
    local name=$1 channel=$2 r status reply
    body "$work/body.json" "$3"
    r=$(post_file "$channel" "$work/body.json")
    status=${r##*$'\n'}; reply=${r%$'\n'*}
    if [ "$status" = 200 ] && json_ok "$reply" "$4"; then check "$name" ok "$(cut -c1-160 <<<"$reply")"
    else check "$name" no "HTTP $status: $(why "$reply")"; fi
}

gemini() {
    api_key=dev-only-advanced-key
    if ! up ai-advanced live compose.media.yaml; then
        local c
        for c in "${GEMINI_CHECKS[@]:0:7}"; do skipped "$c" "the stack did not start (FAIL above)"; done
        gemini_live
        return
    fi
    local scene='isinstance(d["scene"], str) and d["scene"] and isinstance(d["objects"], list) and d["provider"] == "gemini"'

    gemini_call "gemini: image understanding" vision \
        '{"requestId": "G-IMG-1", "image": {"data": b64("frame-1.png"), "format": "image/png"}}' "$scene"
    gemini_call "gemini: video understanding, frames" vision \
        '{"requestId": "G-VID-1", "video": {"frames": [{"data": b64("frame-1.png"), "format": "image/png", "atMs": 0}, {"data": b64("frame-2.png"), "format": "image/png", "atMs": 500}]}}' "$scene"
    gemini_call "gemini: video understanding, clip" vision \
        '{"requestId": "G-VID-2", "video": {"data": b64("moving-square.mp4"), "format": "video/mp4"}}' "$scene"
    gemini_call "gemini: audio understanding" hear \
        '{"requestId": "G-AUD-1", "audio": {"data": b64("spoken-question.wav"), "format": "audio/wav"}}' \
        '"paris" in d["text"].lower() and d["provider"] == "gemini"'
    gemini_call "gemini: structured answer" extract \
        '{"ticketId": "G-JSON-1", "description": "The checkout page crashes for every customer since this morning."}' \
        'isinstance(d["json"], dict) and {"category", "urgency"} <= set(d["json"]) and d["provider"] == "gemini"'

    # The same audio as a media reference, read by hear-model from the MinIO store `media`.
    if dc exec -T store mc mb -p local/media >/dev/null 2>&1 &&
        dc exec -T store mc pipe local/media/clips/question.wav <"$sample/spoken-question.wav" >/dev/null 2>&1; then
        gemini_call "gemini: media reference" hear \
            '{"requestId": "G-REF-1", "audio": {"ref": "s3://media/clips/question.wav", "sha256": sha("spoken-question.wav"), "format": "audio/wav"}}' \
            '"paris" in d["text"].lower()'
        # A digest that is not the object's: refused by the transport before any provider call,
        # so the reason is the refusal, never a provider's answer (with a bad key: not Google's
        # auth refusal either).
        local r status reply
        body "$work/body.json" '{"requestId": "G-REF-2", "audio": {"ref": "s3://media/clips/question.wav", "sha256": "0" * 64, "format": "audio/wav"}}'
        r=$(post_file hear "$work/body.json"); status=${r##*$'\n'}; reply=${r%$'\n'*}
        if [ "$status" != 200 ] && grep -qiE 'MEDIA\.REFUSED|sha-?256|digest' <<<"$reply" &&
            ! grep -qiE 'api key|API_KEY|generativelanguage' <<<"$reply"; then
            check "gemini: media reference, wrong sha256" ok "refused before any provider call, HTTP $status: $(why "$reply")"
        else
            check "gemini: media reference, wrong sha256" no "HTTP $status: $(why "$reply")"
        fi
    else
        check "gemini: media reference" no "could not put the sample in the MinIO store"
        check "gemini: media reference, wrong sha256" no "could not put the sample in the MinIO store"
    fi
    down
    gemini_live
}

# Gemini Live, through the adapter itself (no engine): three #[ignore = "live"] tests.
gemini_live() {
    live_tests "gemini live" sutra-llm-gemini \
        "session round trip, server turn detection:live_session_answers_the_sample_wav" \
        "session round trip, manual turns:live_session_answers_with_manual_turns" \
        "no credential in any URL:live_urls_carry_neither_key_nor_token"
}

if only_openai; then :
elif have GEMINI_API_KEY; then gemini
else skip_gemini "GEMINI_API_KEY not set"; fi

down
result
