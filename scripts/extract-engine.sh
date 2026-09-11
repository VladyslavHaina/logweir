#!/usr/bin/env bash
# Copy the pinned kafka-backup binary out of the upstream OSO image, and record
# what we took. Run once in CI and once by `just engine` locally.
#
# TWO MODES, and the difference between them is the point of this script:
#
#   DEFAULT — DIGEST-PINNED. Reads third_party/kafka-backup-binary.digest and
#     pulls `osodevops/kafka-backup@<that digest>`. Docker Hub tags are mutable,
#     so a tag pull is not a pin (Global Constraint 7). This mode NEVER writes
#     the digest file, and it does NOT re-fetch third_party/LICENSE-MIT or the
#     source tarball while they are present and non-empty: both are checked in,
#     both are fetched BY TAG, and a digest-pinned mode that still curls a
#     moving tag is only half pinned. With no digest file it REFUSES, exit 1,
#     naming the file and naming OSO_REFRESH=1 — it does not quietly fall back
#     to a tag.
#
#   OSO_REFRESH=1 — THE ONLY PATH THAT MAY CHANGE THE PIN. Resolves
#     `osodevops/kafka-backup:${OSO_TAG:-v0.21.0}` BY TAG, reads RepoDigests,
#     verifies org.opencontainers.image.revision, and rewrites
#     third_party/kafka-backup-binary.digest, e2e/compose/.env and the
#     Dockerfile's `FROM` line. It also re-fetches the MIT licence and the
#     source tarball.
#
# `crates/logweir/tests/extract_engine.rs` asserts both halves: that the default
# pull is by digest and still carries `--platform linux/amd64`, and that the
# write to the digest file lives only inside the OSO_REFRESH branch.
set -euo pipefail

# LOGWEIR_ROOT exists so the tests can point this at a temp workspace overlay;
# it defaults to the repository root, exactly like scripts/check-one-signer.sh.
cd "${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"

# GC14 governs what Logweir publishes under its OWN name; pulling upstream's
# published image is required by Global Constraints 7, 8, 10 and 15 (controller
# ruling GR6). Logweir publishes nothing under osodevops/.
TAG="${OSO_TAG:-v0.21.0}"
IMAGE="osodevops/kafka-backup:${TAG}"
DIGEST_FILE="third_party/kafka-backup-binary.digest"
LICENSE_FILE="third_party/LICENSE-MIT"
TARBALL="third_party/kafka-backup-${TAG}.tar.gz"
REFRESH="${OSO_REFRESH:-0}"

# The commit `docs/UPSTREAM-VERSIONS.md` (in the planning repo that produced
# this task, not part of this repo's own tree) pins for kafka-backup v0.21.0.
# Keep this in lockstep with TAG above; a mismatch means the tag has been
# re-pointed to a different commit than the one this plan was verified
# against, and extraction must refuse to proceed on an unverified build.
EXPECTED_REVISION="ae5a102f93b5270927d95d4ccec184b577febb10"

# PROVENANCE, AND IT IS NOT REDUNDANT NOW THAT THE PULL IS BY DIGEST — it is a
# DIGEST→COMMIT BINDING. Pulling `@sha256:…` proves the bytes are the bytes
# this repository pinned; comparing their
# `org.opencontainers.image.revision` label against EXPECTED_REVISION proves
# those bytes are the build of the upstream COMMIT this plan was verified
# against. It is the only tag-drift detector in the tree, and in the refresh
# mode below it runs BEFORE the pin is rewritten, so an unverified digest is
# never recorded.
assert_revision() {
  ACTUAL_REVISION=$(docker image inspect "$1" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
  if [ "$ACTUAL_REVISION" != "$EXPECTED_REVISION" ]; then
    echo "REFUSING TO EXTRACT: $1 carries org.opencontainers.image.revision=${ACTUAL_REVISION}, expected ${EXPECTED_REVISION}. The upstream build behind this reference is not the commit this plan was verified against (docs/UPSTREAM-VERSIONS.md) — re-verify upstream before re-running this script." >&2
    exit 1
  fi
}

# `osodevops/kafka-backup` publishes linux/amd64 ONLY (never arm64, across
# all tags). `--platform linux/amd64` is REQUIRED on Apple Silicon: without
# it, `docker pull`/`docker create` ask the daemon's default platform
# (linux/arm64 here) and fail with "no matching manifest". It is a no-op on
# an amd64 CI runner, so this flag is safe on every host this script runs on.
if [ "$REFRESH" = "1" ]; then
  docker pull --platform linux/amd64 "$IMAGE" >/dev/null
  DIGEST=$(docker image inspect "$IMAGE" --format '{{index .RepoDigests 0}}' | cut -d@ -f2)
  assert_revision "osodevops/kafka-backup@${DIGEST}"
  echo "$DIGEST" > "$DIGEST_FILE"
  echo "refreshed the pin: $IMAGE -> $DIGEST (revision $ACTUAL_REVISION verified)"
else
  if [ ! -s "$DIGEST_FILE" ]; then
    echo "REFUSING TO EXTRACT: ${DIGEST_FILE} is missing or empty, so there is no pinned digest to pull, and this mode will NOT fall back to the mutable tag ${IMAGE} (Global Constraint 7). Re-run with OSO_REFRESH=1 to resolve the tag, verify org.opencontainers.image.revision and create ${DIGEST_FILE}: OSO_REFRESH=1 is the only path that may create or change the pin." >&2
    exit 1
  fi
  DIGEST="$(cat "$DIGEST_FILE")"
  docker pull --platform linux/amd64 "osodevops/kafka-backup@${DIGEST}" >/dev/null
  assert_revision "osodevops/kafka-backup@${DIGEST}"
  echo "digest-pinned: osodevops/kafka-backup@${DIGEST} (revision $ACTUAL_REVISION verified; ${DIGEST_FILE} unchanged)"
fi

mkdir -p .engine
CID=$(docker create --platform linux/amd64 "osodevops/kafka-backup@${DIGEST}")
trap 'docker rm -f "$CID" >/dev/null' EXIT
docker cp "$CID:/usr/local/bin/kafka-backup" .engine/kafka-backup
chmod +x .engine/kafka-backup

# Provenance for the relicensing hedge (Global Constraint 15). BOTH of these
# are fetched BY TAG, so the default mode does not touch them while the
# checked-in copies are present and non-empty — re-curling a moving tag under a
# digest-pinned mode is exactly the half-pinning this script now refuses.
if [ "$REFRESH" = "1" ] || [ ! -s "$LICENSE_FILE" ]; then
  docker cp "$CID:/LICENSE" "$LICENSE_FILE" 2>/dev/null \
    || curl -sSL "https://raw.githubusercontent.com/osodevops/kafka-backup/${TAG}/LICENSE" \
         -o "$LICENSE_FILE"
fi
# The image ships no LICENSE file at all (verified against the pinned digest
# above), so the docker-cp branch above always misses and the curl fallback
# always runs; either way, refuse to leave third_party/ GC15-incomplete. The
# check runs in BOTH modes: an empty checked-in licence is still a breach.
if [ ! -s "$LICENSE_FILE" ]; then
  echo "REFUSING TO CONTINUE: ${LICENSE_FILE} is missing or empty after extraction (Global Constraint 15)." >&2
  exit 1
fi
if [ "$REFRESH" = "1" ] || [ ! -s "$TARBALL" ]; then
  curl -sSL "https://github.com/osodevops/kafka-backup/archive/refs/tags/${TAG}.tar.gz" \
    -o "$TARBALL"
  shasum -a 256 "$TARBALL" > "${TARBALL}.sha256"
fi

# Resolve BOTH digest markers here, so no artifact ships with a placeholder.
# `e2e/compose/.env` is GENERATED, never checked in (it is in .gitignore), so
# it is written in BOTH modes — it is the only writer, and a fresh worktree has
# no copy to preserve. The Dockerfile's marker is substituted in place in the
# refresh mode and ASSERTED in the default mode: in the default mode the digest
# has not changed, so a rewrite would be a no-op and an assertion is the
# stronger statement.
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
# The Dockerfile pins the SAME digest, and in the refresh mode it is REWRITTEN
# here rather than templated: this line used to `sed` for a
# `REPLACE_WITH_PINNED_DIGEST` marker that the Dockerfile has not contained for
# several tasks, so re-running this script silently updated the digest file and
# `e2e/compose/.env` and left the image build on the OLD digest. The two
# happened to agree, which is exactly why nobody noticed. `e2e-seed.sh` already
# carries a drift check for `.env`; the Dockerfile had none.
#
# Rewrite the whole `FROM osodevops/kafka-backup@sha256:…` reference, then
# ASSERT the file now names this digest — a `sed` that matched nothing must
# not be reported as an update. The assertion runs in both modes.
if [ "$REFRESH" = "1" ]; then
  sed -i.bak -E "s|(FROM osodevops/kafka-backup@)sha256:[0-9a-f]{64}|\1${DIGEST}|" Dockerfile \
    && rm -f Dockerfile.bak
fi
if ! grep -q "FROM osodevops/kafka-backup@${DIGEST}" Dockerfile; then
  echo "FAIL: Dockerfile does not pin ${DIGEST}." >&2
  echo "      Expected a line 'FROM osodevops/kafka-backup@<digest> AS engine'." >&2
  echo "      In the default (digest-pinned) mode this script does not rewrite the" >&2
  echo "      Dockerfile: re-run with OSO_REFRESH=1 to move the pin everywhere at once." >&2
  grep -n 'FROM osodevops/kafka-backup' Dockerfile >&2 || true
  exit 1
fi
echo "ok: Dockerfile, ${DIGEST_FILE} and e2e/compose/.env all pin ${DIGEST}"

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
