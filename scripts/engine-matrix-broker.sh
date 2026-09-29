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
# Exit 1, printing nothing on stdout, when the broker container or its version
# line cannot be found.
set -euo pipefail
cd "$(dirname "$0")/.."

die() { echo "engine-matrix-broker: $*" >&2; exit 1; }

cid=$(docker compose -f e2e/compose/docker-compose.yml ps -q kafka-broker-1)
[ -n "$cid" ] || die "the stack has no kafka-broker-1 container"
image=$(docker inspect --format '{{.Config.Image}}' "$cid")
image_id=$(docker inspect --format '{{.Image}}' "$cid")
logs=$(docker logs "$cid" 2>&1)
version=$(sed -n '/INFO Kafka version: /{s/.*INFO Kafka version: \([0-9][0-9.]*[0-9]\).*/\1/p;q;}' <<<"$logs")
[ -n "$version" ] || die "the broker logged no 'INFO Kafka version:' line"
printf 'image=%s\nimage_id=%s\nversion=%s\n' "$image" "$image_id" "$version"
