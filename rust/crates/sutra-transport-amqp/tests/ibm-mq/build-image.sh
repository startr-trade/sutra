#!/usr/bin/env bash
# Build the IBM MQ developer image with AMQP that the transport's IBM MQ suite runs against.
#
#   tests/ibm-mq/build-image.sh [path/to/IBM-MQ-Advanced-for-Developers-Non-Install-LinuxX64.tar.gz]
#
# Without an archive the script downloads the developer archive of the image's release from IBM's
# public download site (about 550 MB, fetched in parallel ranges because a single connection is
# slow). Only its amqp/ directory goes into the image. The image is tagged $SUTRA_IBM_MQ_IMAGE,
# default sutra-test/ibm-mq-amqp:10.0.1.0. Running this means accepting the IBM MQ Advanced for
# Developers licence, as LICENSE=accept does when the container starts.
set -euo pipefail

TAG="${SUTRA_IBM_MQ_IMAGE:-sutra-test/ibm-mq-amqp:10.0.1.0}"
MQ_RELEASE="${MQ_RELEASE:-10.0.1.0}"
MQ_IMAGE="${MQ_IMAGE:-icr.io/ibm-messaging/mq:${MQ_RELEASE}-r1}"
URL="https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqadv/${MQ_RELEASE}-IBM-MQ-Advanced-for-Developers-Non-Install-LinuxX64.tar.gz"
HERE="$(cd "$(dirname "$0")" && pwd)"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
archive="${1:-}"
if [ -z "$archive" ]; then
  archive="$work/mq.tar.gz"
  size="$(curl -fsSIL "$URL" | tr -d '\r' | awk 'tolower($1)=="content-length:"{n=$2} END{print n}')"
  [ -n "$size" ] || { echo "cannot read the archive size from $URL" >&2; exit 1; }
  parts=12
  chunk=$(( (size + parts - 1) / parts ))
  echo "downloading $URL ($size bytes, $parts ranges)"
  pids=()
  for i in $(seq 0 $((parts - 1))); do
    start=$((i * chunk)); end=$((start + chunk - 1)); [ "$end" -ge "$size" ] && end=$((size - 1))
    curl -fsS --retry 5 -r "$start-$end" -o "$work/part.$(printf %02d "$i")" "$URL" &
    pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
  cat "$work"/part.* > "$archive"
  rm -f "$work"/part.*
  [ "$(stat -c %s "$archive")" = "$size" ] || { echo "download incomplete" >&2; exit 1; }
fi

mkdir -p "$work/context"
# Only amqp/ is needed and it sits near the start of the archive; tar reports a read error if the
# rest of a download is damaged, so check for the component's files instead of tar's exit code.
tar xzf "$archive" -C "$work/context" amqp 2>/dev/null || true
for f in amqp/lib/AMQPListener.jar amqp/bin/controlAMQPChannel.sh amqp/config/amqp_unix.properties; do
  [ -f "$work/context/$f" ] || { echo "the archive holds no $f; download it again" >&2; exit 1; }
done
cp "$HERE/Dockerfile" "$work/context/Dockerfile"
docker build --build-arg MQ_IMAGE="$MQ_IMAGE" -t "$TAG" "$work/context"
echo "built $TAG"
