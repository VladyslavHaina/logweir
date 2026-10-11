#!/usr/bin/env bash
# `logweir` from the PUBLISHED runner image, for a local trial with no Rust
# toolchain (PROD-14.1's compose sandbox: `just sandbox`). Every argument goes
# to the image's own `logweir`, which runs the image's own engine
# (/usr/local/bin/kafka-backup, named by /etc/logweir/engine-identity).
#
#     LOGWEIR_IMAGE=docker.io/vladyslavhaina/logweir:<tag> scripts/logweir-image.sh --version
#
# - The current directory is mounted at the same path, so every path under it
#   means the same thing inside the container.
# - `localhost` inside the container is the Docker host, where the compose
#   stack publishes its broker and MinIO ports (the broker advertises
#   `localhost:<port>`), as in e2e/fixtures/engine-docker.sh. Its IPv4
#   address: glibc answers 127.0.0.1 for an IPv4 `localhost` lookup when
#   /etc/hosts maps the name to IPv6 only.
# - The container starts as root only to rewrite /etc/hosts, then runs
#   `logweir` as the calling user, so the files it writes are yours.
set -euo pipefail

image=${LOGWEIR_IMAGE:?set LOGWEIR_IMAGE to a published runner image, for example docker.io/vladyslavhaina/logweir:latest}

exec docker run --rm -i \
  --user 0:0 \
  --add-host host.docker.internal:host-gateway \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION -e RUST_LOG \
  -e LOGWEIR_SOURCE_PASSWORD -e LOGWEIR_TARGET_PASSWORD \
  -e LW_UID="$(id -u)" -e LW_GID="$(id -g)" -e HOME=/tmp \
  -v "$PWD:$PWD" \
  -w "$PWD" \
  --entrypoint /bin/bash \
  "$image" \
  -c 'gw=$(getent ahostsv4 host.docker.internal | cut -d" " -f1 | head -1)
      if [ -z "$gw" ]; then echo "logweir-image: no host.docker.internal" >&2; exit 1; fi
      printf "%s\tlocalhost\n%s\thost.docker.internal\n" "$gw" "$gw" > /etc/hosts
      exec setpriv --reuid "$LW_UID" --regid "$LW_GID" --clear-groups logweir "$@"' logweir "$@"
