#!/usr/bin/env bash
# A stand-in for `.engine/kafka-backup` on a host that cannot EXECUTE it.
#
# Upstream publishes `osodevops/kafka-backup` for linux/amd64 ONLY, so the
# binary `scripts/extract-engine.sh` pulls out of the pinned image is a Linux
# ELF. On darwin/arm64 — the machine this repo is developed on — exec'ing it
# fails with ENOEXEC (`logweir doctor` reports exit 126). On a linux/amd64 CI
# runner the native binary runs directly and this script is never used;
# `e2e/tests/harness/mod.rs::engine_bin` probes `--version` (permitted by
# global ruling GR8: `--version` prints a string and acts on no cluster or
# bucket) and picks the native binary whenever it can actually run.
#
# GLOBAL CONSTRAINT 3. This is a TEST HARNESS, not the `logweir` binary, and it
# invokes NOTHING itself: every argument is forwarded verbatim from
# `logweir_engine_oso::subprocess::run_engine`, so the only subcommands that
# ever reach it are the ones Logweir itself renders. `scripts/check-no-oso.sh`
# scans `crates/`, which is where the runtime constraint lives.
#
# THE `localhost` PROBLEM, and why /etc/hosts is rewritten.
#   The drill spec is consumed by TWO processes with different network views:
#   `logweir` on the macOS host, and the engine inside a container. The spec
#   says `localhost:9092` / `http://localhost:9000`, which is correct for the
#   host (compose publishes both ports) and meaningless inside a container.
#   Rewriting the rendered restore.yaml is not an option — it is the document
#   phase 5 validated and phase 6 executes, and their byte-identity is what
#   binds the approved plan to the executed one. Joining the compose network
#   does not help either: the broker advertises its EXTERNAL listener as
#   `localhost:9092`, so a bootstrap connection would be told to reconnect to
#   `localhost` whatever address it dialled first.
#   So the container is given the HOST's meaning of `localhost`: /etc/hosts is
#   rewritten to point `localhost` at the Docker host gateway, where compose
#   publishes 9092 and 9000. The engine then resolves exactly what `logweir`
#   resolves, including the advertised listener it is handed back.
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
digest=$(tr -d '[:space:]' < "$root/third_party/kafka-backup-binary.digest")
if [ -z "$digest" ]; then
  echo "engine-docker: third_party/kafka-backup-binary.digest is empty" >&2
  exit 1
fi

# The one host directory the engine reads and writes: the rendered
# restore.yaml / validation.yaml and the restore checkpoint all live under the
# TMPDIR the harness sets for the `logweir` child. Mounted at the SAME path so
# every absolute path Logweir rendered is valid inside the container too.
# `LOGWEIR_E2E_ENGINE_MOUNT` overrides it; the default is the same directory
# `harness::engine_mount()` computes, so a bare `--version` probe needs no
# environment at all.
mount=${LOGWEIR_E2E_ENGINE_MOUNT:-"$root/.e2e/tmp"}
mkdir -p "$mount"

exec docker run --rm -i \
  --platform linux/amd64 \
  --user 0:0 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION -e RUST_LOG \
  -v "$mount:$mount" \
  -w "$mount" \
  --entrypoint /bin/bash \
  "osodevops/kafka-backup@$digest" \
  -c 'gw=$(getent hosts host.docker.internal | cut -d" " -f1 | head -1)
      if [ -z "$gw" ]; then echo "engine-docker: no host.docker.internal" >&2; exit 1; fi
      printf "%s\tlocalhost\n%s\thost.docker.internal\n::1\tip6-localhost ip6-loopback\n" \
        "$gw" "$gw" > /etc/hosts
      exec kafka-backup "$@"' kafka-backup "$@"
