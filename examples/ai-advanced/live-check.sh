#!/usr/bin/env bash
# Run the ai-summarize and ai-advanced examples against the REAL providers and print one PASS/FAIL
# line per check. Costs a few cents of tokens. Keys are read from your environment, handed to the
# engine container by docker compose, and never printed.
#
#   export OPENAI_API_KEY=...        # each key is optional; checks needing a missing key are skipped
#   export ANTHROPIC_API_KEY=...
#   export GEMINI_API_KEY=...
#   examples/ai-advanced/live-check.sh
#
# What runs:
#   ai-summarize   for each key you exported: the openai / anthropic / gemini variant —
#                  sync, SSE-streamed and async summaries
#   ai-advanced    with OPENAI_API_KEY and ANTHROPIC_API_KEY: the `live` variant —
#                  cross-provider fallback (Anthropic taken down and up again through the stub's
#                  relay, so BOTH answers are real), JSON mode (OpenAI), a tool call (Anthropic),
#                  an OpenAI background job (polled), an Anthropic Message Batch (polled; set
#                  SKIP_BATCH=1 to skip, BATCH_WAIT_MIN to change the 30-minute wait) and, when
#                  OPENAI_WEBHOOK_SECRET is set, an OpenAI background job completed by webhook
#
# The webhook check needs the engine reachable from the internet. Run a tunnel to
# 127.0.0.1:$ENGINE_HOST_PORT (default 18080), register https://<tunnel-host>/llm/openai-hooks as a
# webhook for `response.completed` in your OpenAI project, and export its signing secret:
#   export OPENAI_WEBHOOK_SECRET=whsec_...
#   ENGINE_HOST_PORT=18080 examples/ai-advanced/live-check.sh
# The webhook job's safety-net poll is 10 minutes, so a delivery within the 3-minute wait is the
# webhook.
#
# Needs docker compose and the `sutra` CLI, 0.5.0 or later (SUTRA_BIN, else a build in rust/target,
# else `sutra` on PATH). The engine image is SUTRA_ENGINE_IMAGE, default ghcr.io/startr-trade/sutra:0.5.0.
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
[ -n "$sutra_bin" ] || { echo "no sutra CLI: set SUTRA_BIN or put sutra on PATH" >&2; exit 2; }
# The examples use 0.5.0 features (sync calls without <q:alias>, q:stream); an older CLI rejects them.
cli_version=$("$sutra_bin" --version | awk '{print $NF}')
if [ "$(printf '%s\n' 0.5.0 "$cli_version" | sort -V | head -1)" != 0.5.0 ]; then
    echo "sutra CLI $cli_version at $sutra_bin is older than 0.5.0; build one (cargo build -p sutra-cli)" >&2
    echo "and point SUTRA_BIN at it, e.g. SUTRA_BIN=\$PWD/rust/target/debug/sutra" >&2
    exit 2
fi
echo "using $("$sutra_bin" --version) ($sutra_bin); engine image ${SUTRA_ENGINE_IMAGE:-ghcr.io/startr-trade/sutra:0.5.0}"
image=${SUTRA_ENGINE_IMAGE:-ghcr.io/startr-trade/sutra:0.5.0}
if ! docker image inspect "$image" >/dev/null 2>&1 && ! docker pull -q "$image" >/dev/null 2>&1; then
    echo "engine image $image is not available locally and cannot be pulled; build one (make image)" >&2
    echo "and set SUTRA_ENGINE_IMAGE, e.g. SUTRA_ENGINE_IMAGE=sutra-rust-engine:dev" >&2
    exit 2
fi

have() { [ -n "${!1:-}" ]; }
if ! have OPENAI_API_KEY && ! have ANTHROPIC_API_KEY && ! have GEMINI_API_KEY; then
    echo "export at least one of OPENAI_API_KEY, ANTHROPIC_API_KEY, GEMINI_API_KEY" >&2
    exit 2
fi
BATCH_WAIT_MIN=${BATCH_WAIT_MIN:-30}
if have OPENAI_WEBHOOK_SECRET; then export ENGINE_HOST_PORT=${ENGINE_HOST_PORT:-18080}; fi

work=$(mktemp -d)
pass=0; fail=0; skip=0
check() {  # check <name> ok|no [detail]
    if [ "$2" = ok ]; then echo "PASS  $1"; pass=$((pass + 1));
    else echo "FAIL  $1${3:+  — $3}"; fail=$((fail + 1)); fi
}
skipped() { echo "SKIP  $1  — $2"; skip=$((skip + 1)); }
json_ok() { python3 -c "import json,sys; d=json.loads(sys.argv[1]); assert $2" "$1" 2>/dev/null; }

# ---- one stack at a time ------------------------------------------------------------------------

project=""; compose=""
dc() { docker compose -p "$project" -f "$compose" "$@"; }
down() { [ -n "$project" ] && dc down -v >/dev/null 2>&1; project=""; }
trap 'down; rm -rf "$work"' EXIT

# up <example> <variant>: package the variant over the shared flows and start the example's stack.
up() {
    local example=$1 variant=$2
    down
    export DEPLOYMENTS_DIR="$work/$example-$variant"
    mkdir -p "$DEPLOYMENTS_DIR"
    local out
    if ! out=$("$sutra_bin" package "$examples/$example/variants/$variant" \
            --overlay "$examples/$example/shared" --out "$DEPLOYMENTS_DIR" 2>&1); then
        check "$example/$variant packages" no "$(grep -m1 ERROR <<<"$out" | cut -c1-300)"
        return 1
    fi
    chmod -R a+rX "$DEPLOYMENTS_DIR"
    project="sutra-live-$example"
    compose="$examples/$example/deploy/compose.yaml"
    dc up -d >/dev/null 2>&1 || { check "$example/$variant starts" no "docker compose up failed"; return 1; }
    url="http://$(dc port engine 8080)"
    for _ in $(seq 1 90); do curl -sf "$url/sutra/health/ready" >/dev/null && return 0; sleep 1; done
    check "$example/$variant starts" no "engine not ready; warnings:"
    dc logs engine 2>&1 | grep -E '"level":"(WARN|ERROR)"' | cut -c1-300
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
outage() { stub --post-data "{\"on\": $1}" http://127.0.0.1:8000/control/outage >/dev/null; }

# ---- ai-summarize: one variant per key ----------------------------------------------------------

summarize() {  # summarize <variant>
    local v=$1 r
    api_key=dev-only-summarize-key
    up ai-summarize "$v" || return
    r=$(post ticket "{\"ticketId\":\"LIVE-$v-1\",\"subject\":\"Cannot log in\",\"description\":\"The password reset email never arrives, I tried three times since yesterday.\"}")
    json_ok "$r" 'd["ticketId"] and len(d["summary"]) > 20' && check "ai-summarize/$v sync" ok || check "ai-summarize/$v sync" no "$(echo "$r" | cut -c1-200)"
    r=$(curl -s -N -X POST "$url/channels/ticket-stream" -H 'Content-Type: application/json' -H "X-Api-Key: $api_key" \
        -d "{\"ticketId\":\"LIVE-$v-2\",\"subject\":\"App is slow\",\"description\":\"Every page takes about ten seconds to load since the last update.\"}")
    if [ "$(grep -c '^event: chunk' <<<"$r")" -ge 2 ] && grep -q '^event: reply' <<<"$r"; then
        check "ai-summarize/$v streamed ($(grep -c '^event: chunk' <<<"$r") chunks)" ok
    else check "ai-summarize/$v streamed" no "$(echo "$r" | tail -3 | cut -c1-200)"; fi
    [ "$(post_code ticket-async "{\"ticketId\":\"LIVE-$v-3\",\"subject\":\"Photo upload fails\",\"description\":\"The app shows an error every time I upload a profile photo.\"}")" = 202 ] \
        && sink_has "LIVE-$v-3" 60 && check "ai-summarize/$v async" ok || check "ai-summarize/$v async" no "no sink delivery within 60 s"
}

for pair in openai:OPENAI_API_KEY anthropic:ANTHROPIC_API_KEY gemini:GEMINI_API_KEY; do
    if have "${pair#*:}"; then summarize "${pair%%:*}"
    else skipped "ai-summarize/${pair%%:*}" "${pair#*:} not set"; fi
done

# ---- ai-advanced: the live variant --------------------------------------------------------------

advanced() {
    local r
    api_key=dev-only-advanced-key
    up ai-advanced live || return

    outage false
    r=$(post fallback '{}')
    json_ok "$r" 'd["provider"] == "anthropic" and d["text"]' && check "fallback: Anthropic up, Anthropic answers" ok \
        || check "fallback: Anthropic up, Anthropic answers" no "$(echo "$r" | cut -c1-200)"
    outage true
    r=$(post fallback '{}')
    json_ok "$r" 'd["provider"] == "openai" and d["text"]' && check "fallback: Anthropic down, OpenAI answers" ok \
        || check "fallback: Anthropic down, OpenAI answers" no "$(echo "$r" | cut -c1-200)"

    r=$(post classify '{"ticketId":"LIVE-JSON-1","description":"The checkout page crashes for every customer since this morning."}')
    json_ok "$r" 'isinstance(json.loads(d["text"]), dict)' && check "JSON mode (OpenAI)" ok || check "JSON mode (OpenAI)" no "$(echo "$r" | cut -c1-200)"

    r=$(post weather '{}')
    json_ok "$r" 'd["finish"] == "tool-calls" and d["toolCalls"][0]["name"] == "get_weather" and "city" in d["toolCalls"][0]["arguments"]' \
        && check "tool call (Anthropic)" ok || check "tool call (Anthropic)" no "$(echo "$r" | cut -c1-200)"

    [ "$(post_code summarize-job '{"ticketId":"LIVE-JOB-1","description":"My parcel was marked delivered but never arrived."}')" = 202 ] \
        && sink_has LIVE-JOB-1 180 && check "OpenAI background job, polled" ok || check "OpenAI background job, polled" no "no result within 3 min"

    if have OPENAI_WEBHOOK_SECRET; then
        [ "$(post_code summarize-webhook '{"ticketId":"LIVE-HOOK-1","description":"The search page shows no results for any query since this morning."}')" = 202 ] \
            && sink_has LIVE-HOOK-1 180 && check "OpenAI background job, webhook" ok \
            || check "OpenAI background job, webhook" no "no webhook within 3 min (tunnel to 127.0.0.1:$ENGINE_HOST_PORT? secret?)"
    else
        skipped "OpenAI background job, webhook" "set OPENAI_WEBHOOK_SECRET and run a tunnel (see the header)"
    fi

    if [ -n "${SKIP_BATCH:-}" ]; then
        skipped "Anthropic Message Batch" "SKIP_BATCH set"
    else
        echo "      Anthropic Message Batch submitted; batches usually end in minutes (waiting up to $BATCH_WAIT_MIN min)"
        [ "$(post_code summarize-batch '{"ticketId":"LIVE-BATCH-1","description":"The app logs me out every few minutes on my phone."}')" = 202 ] \
            && sink_has LIVE-BATCH-1 $((BATCH_WAIT_MIN * 60)) && check "Anthropic Message Batch, polled" ok \
            || check "Anthropic Message Batch, polled" no "no result within $BATCH_WAIT_MIN min"
    fi
}

if have OPENAI_API_KEY && have ANTHROPIC_API_KEY; then advanced
else skipped "ai-advanced/live" "needs both OPENAI_API_KEY and ANTHROPIC_API_KEY"; fi

down
echo
echo "RESULT: $pass passed, $fail failed, $skip skipped"
[ "$fail" = 0 ]
