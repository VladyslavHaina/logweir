#!/usr/bin/env bash
# Read back the Kafka broker the e2e compose stack is actually running.
#
# Prints three lines for `.github/workflows/engine-matrix.yml` to put into its
# step output:
#
#     image=<the container's configured image, e.g. apache/kafka:4.3.1>
#     image_id=<the image id the container runs, sha256:...>
#     version=<the version the broker logged: "INFO Kafka version: X">
#
# The matrix used to record the broker a row DECLARED (KAFKA_VERSION) without
# checking the stack had honoured it (review L6 of PROD-00.1); the row now
# records this measured version and fails when it differs from the
# declaration (scripts/engine-matrix-outcome.sh).
#
# With --retention (run after the suite), prints what the broker's
# time-retention check deleted while the row ran:
#
#     retention_deletions=<segments deleted "due to log retention time">
#     retention_topics=<their topics, space-separated; at most five named>
#
# A fixture whose CreateTime stamps are older than the broker's retention
# (7 days by default, checked every 5 minutes) loses its records to that
# check if it runs between the produce and the engine's capture. That is how
# run 36542777892's v0.22.0 row failed (the engine route decision record,
# section 5.6). A failed suite names it; it never changes an outcome.
#
# Exit 1, printing nothing on stdout, when the broker container or (without
# --retention) its version line cannot be found.
set -euo pipefail
cd "$(dirname "$0")/.."

die() { echo "engine-matrix-broker: $*" >&2; exit 1; }

mode=broker
case "${1:-}" in
  "") ;;
  --retention) mode=retention ;;
  *) die "unknown argument: $1 (the only option is --retention)" ;;
esac

cid=$(docker compose -f e2e/compose/docker-compose.yml ps -q kafka-broker-1)
[ -n "$cid" ] || die "the stack has no kafka-broker-1 container"
logs=$(docker logs "$cid" 2>&1)

if [ "$mode" = retention ]; then
  deleted=$(grep -F 'due to log retention time' <<<"$logs" || true)
  count=0 topics=""
  if [ -n "$deleted" ]; then
    count=$(grep -c . <<<"$deleted")
    all=$(sed -n 's/.*partition=\([^,]*\),.*/\1/p' <<<"$deleted" | sed 's/-[0-9][0-9]*$//' | sort -u)
    named=$(grep -c . <<<"$all" || true)
    topics=$(head -5 <<<"$all" | paste -sd' ' -)
    [ "$named" -le 5 ] || topics="$topics (+$((named - 5)) more)"
  fi
  printf 'retention_deletions=%s\nretention_topics=%s\n' "$count" "$topics"
  exit 0
fi

image=$(docker inspect --format '{{.Config.Image}}' "$cid")
image_id=$(docker inspect --format '{{.Image}}' "$cid")
version=$(sed -n '/INFO Kafka version: /{s/.*INFO Kafka version: \([0-9][0-9.]*[0-9]\).*/\1/p;q;}' <<<"$logs")
[ -n "$version" ] || die "the broker logged no 'INFO Kafka version:' line"
printf 'image=%s\nimage_id=%s\nversion=%s\n' "$image" "$image_id" "$version"
