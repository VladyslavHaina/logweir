#!/usr/bin/env bash
# Resolve the pinned OSO image BY DIGEST (never by tag — Docker Hub tags are
# mutable, Global Constraint 7), copy the kafka-backup binary out of it, and
# record what we took. Run once in CI and once by `just engine` locally.
set -euo pipefail

# GC14 governs what Logweir publishes under its OWN name; pulling upstream's
# published image is required by Global Constraints 7, 8, 10 and 15 (controller
# ruling GR6). Logweir publishes nothing under osodevops/.
TAG="${OSO_TAG:-v0.21.0}"
IMAGE="osodevops/kafka-backup:${TAG}"

# The commit `docs/UPSTREAM-VERSIONS.md` (in the planning repo that produced
# this task, not part of this repo's own tree) pins for kafka-backup v0.21.0.
# Keep this in lockstep with TAG above; a mismatch means the tag has been
# re-pointed to a different commit than the one this plan was verified
# against, and extraction must refuse to proceed on an unverified build.
EXPECTED_REVISION="ae5a102f93b5270927d95d4ccec184b577febb10"

# `osodevops/kafka-backup` publishes linux/amd64 ONLY (never arm64, across
# all tags). `--platform linux/amd64` is REQUIRED on Apple Silicon: without
# it, `docker pull`/`docker create` ask the daemon's default platform
# (linux/arm64 here) and fail with "no matching manifest". It is a no-op on
# an amd64 CI runner, so this flag is safe on every host this script runs on.
docker pull --platform linux/amd64 "$IMAGE" >/dev/null
DIGEST=$(docker image inspect "$IMAGE" --format '{{index .RepoDigests 0}}' | cut -d@ -f2)

# Provenance check: prove tag/digest/commit agreement rather than trust it.
# A tag can be re-pointed at any time; this is the one place that would
# notice before a stale or wrong binary ever reaches third_party/.
ACTUAL_REVISION=$(docker image inspect "$IMAGE" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
if [ "$ACTUAL_REVISION" != "$EXPECTED_REVISION" ]; then
  echo "REFUSING TO EXTRACT: ${IMAGE} (${DIGEST}) carries org.opencontainers.image.revision=${ACTUAL_REVISION}, expected ${EXPECTED_REVISION}. The tag has moved since this plan was verified against docs/UPSTREAM-VERSIONS.md — re-verify upstream before re-running this script." >&2
  exit 1
fi

echo "$DIGEST" > third_party/kafka-backup-binary.digest
echo "resolved $IMAGE -> $DIGEST (revision $ACTUAL_REVISION verified)"

mkdir -p .engine
CID=$(docker create --platform linux/amd64 "osodevops/kafka-backup@${DIGEST}")
trap 'docker rm -f "$CID" >/dev/null' EXIT
docker cp "$CID:/usr/local/bin/kafka-backup" .engine/kafka-backup
chmod +x .engine/kafka-backup

# Provenance for the relicensing hedge (Global Constraint 15).
docker cp "$CID:/LICENSE" third_party/LICENSE-MIT 2>/dev/null \
  || curl -sSL "https://raw.githubusercontent.com/osodevops/kafka-backup/${TAG}/LICENSE" \
       -o third_party/LICENSE-MIT
# The image ships no LICENSE file at all (verified against the pinned digest
# above), so the docker-cp branch above always misses and the curl fallback
# always runs; either way, refuse to leave third_party/ GC15-incomplete.
if [ ! -s third_party/LICENSE-MIT ]; then
  echo "REFUSING TO CONTINUE: third_party/LICENSE-MIT is missing or empty after extraction (Global Constraint 15)." >&2
  exit 1
fi
curl -sSL "https://github.com/osodevops/kafka-backup/archive/refs/tags/${TAG}.tar.gz" \
  -o "third_party/kafka-backup-${TAG}.tar.gz"
shasum -a 256 "third_party/kafka-backup-${TAG}.tar.gz" \
  > "third_party/kafka-backup-${TAG}.tar.gz.sha256"

# Resolve BOTH digest markers here, so no artifact ships with a placeholder.
# `e2e/compose/.env` is GENERATED, never checked in (it is in .gitignore); the
# Dockerfile's marker is substituted in place.
# Task 21c owns e2e/compose/, which does not exist yet when this script first
# runs; create it so `set -e` does not abort on the redirection below.
mkdir -p e2e/compose
# Task 7b addendum A6 fixes this file's exact content: the KAFKA_VERSION pin the
# compose stack reads, and the GR6 note beside the upstream image reference so a
# reader hitting Global Constraint 14 does not re-raise it. `.env` is generated,
# so the comment has to be emitted here — it is the only writer.
cat > e2e/compose/.env <<ENV
KAFKA_VERSION=3.7.1
# Upstream image reference, permitted by GR6: GC14 governs what Logweir publishes,
# not what it pulls. Keep in lockstep with third_party/kafka-backup-binary.digest.
OSO_DIGEST=${DIGEST}
ENV
# The Dockerfile pins the SAME digest, and it is REWRITTEN here rather than
# templated: this line used to `sed` for a `REPLACE_WITH_PINNED_DIGEST` marker
# that the Dockerfile has not contained for several tasks, so re-running this
# script silently updated the digest file and `e2e/compose/.env` and left the
# image build on the OLD digest. The two happened to agree, which is exactly
# why nobody noticed. `e2e-seed.sh` already carries a drift check for `.env`;
# the Dockerfile had none.
#
# Rewrite the whole `FROM osodevops/kafka-backup@sha256:…` reference, then
# ASSERT the file now names this digest — a `sed` that matched nothing must
# not be reported as an update.
sed -i.bak -E "s|(FROM osodevops/kafka-backup@)sha256:[0-9a-f]{64}|\1${DIGEST}|" Dockerfile \
  && rm -f Dockerfile.bak
if ! grep -q "FROM osodevops/kafka-backup@${DIGEST}" Dockerfile; then
  echo "FAIL: Dockerfile does not pin ${DIGEST} after the rewrite." >&2
  echo "      Expected a line 'FROM osodevops/kafka-backup@<digest> AS engine'." >&2
  grep -n 'FROM osodevops/kafka-backup' Dockerfile >&2 || true
  exit 1
fi
echo "ok: Dockerfile, third_party/kafka-backup-binary.digest and e2e/compose/.env all pin ${DIGEST}"

# The extracted binary is a linux/amd64 ELF (upstream never publishes
# arm64). On Linux (CI, and any real deployment host) it is exec'd directly
# here, proving the exact LOCAL bytes that ship. On a non-Linux dev host
# (e.g. an Apple Silicon workstation) a direct exec is impossible (ENOEXEC —
# Rosetta translates amd64 *inside* a Linux container, it does not let Darwin
# exec a foreign ELF), so verification instead bind-mounts this SAME local
# file (not a fresh registry pull — this is the actual `docker cp` output,
# so a corrupted or truncated extraction is still caught) into a throwaway
# `debian:bookworm-slim` container and execs it there: a real execution of
# the real extracted bytes, never a skip.
if [ "$(uname -s)" = "Linux" ]; then
  .engine/kafka-backup --version
else
  echo "NOTE: $(uname -s) cannot exec a linux/amd64 ELF directly; verifying" \
       ".engine/kafka-backup --version by bind-mounting the SAME local file" \
       "into a debian:bookworm-slim container instead of a native exec" \
       "(see comment above)." >&2
  docker run --rm --platform linux/amd64 \
    -v "$(pwd)/.engine/kafka-backup:/kafka-backup:ro" \
    debian:bookworm-slim /kafka-backup --version
fi
echo "glibc floor: $(objdump -T .engine/kafka-backup 2>/dev/null | grep -o 'GLIBC_[0-9.]*' | sort -V | tail -1 || echo 'unknown (not ELF on this host)')"
