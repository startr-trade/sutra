#!/usr/bin/env bash
# generated-by: sutra create app (edit freely — this file is yours)
#
# Health-gated smoke: waits for /sutra/health/ready, checks /sutra/health/live, then POSTs a
# SampleRequest to the sample channel and expects the <Accepted…> reply the validation
# gateway renders.
#
# Usage:
#   ./smoke.sh                       # engine from compose (dynamic host port discovered)
#   ENGINE_URL=http://host:port ./smoke.sh   # engine anywhere else (e.g. a k8s port-forward)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"

base_url="${ENGINE_URL:-}"
if [[ -z "$base_url" ]]; then
  hostport="$(docker compose -f "$here/compose.yaml" port engine 8080)"
  [[ -n "$hostport" ]] || { echo "smoke: engine not running (docker compose up -d first)" >&2; exit 2; }
  base_url="http://${hostport}"
fi
echo "smoke: engine at ${base_url}"

ready=""
for _ in $(seq 1 60); do
  if curl -fsS "${base_url}/sutra/health/ready" >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
[[ -n "$ready" ]] || { echo "smoke: engine never became ready (/sutra/health/ready)" >&2; exit 1; }
echo "smoke: ready"

curl -fsS "${base_url}/sutra/health/live" >/dev/null
echo "smoke: live"

# The channel declares apikey auth, so the request carries the header. Without it the engine
# answers 401 and this script fails — which is the point of declaring it.
api_key="${SAMPLE_API_KEY:-%%APIKEY%%}"
reply="$(curl -fsS -X POST "${base_url}/channels/sample-in" \
  -H 'Content-Type: application/xml' \
  -H "X-Api-Key: ${api_key}" \
  --data '<SampleRequest><note>smoke</note></SampleRequest>')"
echo "smoke: reply ${reply}"

case "$reply" in
  *"<Accepted"*) echo "smoke: OK" ;;
  *) echo "smoke: unexpected reply (wanted <Accepted…>)" >&2; exit 1 ;;
esac
