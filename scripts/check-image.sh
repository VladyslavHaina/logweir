#!/usr/bin/env bash
# THE IMAGE SMOKE GATE — the single implementation. Phase 1 line item 1f,
# Tier 0 item T0-17 (the refactor half).
#
# These five assertions were lifted VERBATIM IN BEHAVIOUR out of
# `.github/workflows/release.yml:136-182`, where they had lived since Task 22
# and where they have NEVER EXECUTED on any commit, including the v0.1.0 tag —
# there is no git remote and not one of the five workflows has ever run. A gate
# that has never run is not a gate, so they now live in a file a human can run
# on a laptop and whose exit code `just` reads directly.
#
# WHY IT TAKES THE REFERENCE AS AN ARGUMENT (T0-17). The release job used to
# assert a locally built tag and then push a SEPARATELY built image, so the
# image that was asserted was not the image that was pushed. Taking the
# reference as an argument is what lets the release job assert the exact
# reference it pushes — a local tag (`logweir:check`) or a digest reference
# (`ghcr.io/<owner>/logweir@sha256:…`) — so the job becomes
# build-once → assert → push-THAT-digest.
#
# TWO CALLERS, ONE IMPLEMENTATION:
#   * `just smoke`                      (this task)
#   * `.github/workflows/release.yml`   (Task 9 — do not add it here)
# Neither may keep a private copy of the logic; that is the whole point of the
# extraction.
#
# WHAT IT PROVES about the image, in fail-fast order:
#   1. every dynamic dependency of `/usr/local/bin/logweir` RESOLVES (GC10);
#   2. the engine answers `kafka-backup --version`;
#   3. the CLI answers `logweir --version`;
#   4. the image alone can mint a signed approval over `examples/drill.yaml`;
#   5. both redistributed licences are present (GC15).
#
# WHAT IT PROVES ABOUT A DRILL: nothing. No broker, no bucket, no cluster and
# no archive is touched. See docs/stability.md.
#
# EXIT CODES ARE 0 AND 1 ONLY. GC11's 0/1/2/3/4 contract governs the `logweir`
# BINARY, not shell scripts; do not reuse those values here. 0 = all five
# checks passed. 1 = a check failed, the reference is not in the local daemon,
# or a prerequisite is missing. A missing prerequisite is a FAILURE WITH A
# NAMED REASON, never a green "skipped" run — a check that cannot fail is the
# defect class this gate exists to remove, not to repeat.
#
# COSTS: one `docker run` per check plus one for the round-trip, all against an
# image that must already be in the local daemon. It builds nothing and pushes
# nothing (GC17): `just image` is the named producer of `logweir:check`.
set -euo pipefail

# Root resolution differs deliberately from scripts/check-one-signer.sh (Task 7),
# which resolves ${LOGWEIR_ROOT:-...} so its tests can point it at a temp workspace
# overlay. This script must read the REAL examples/drill.yaml, so it takes no
# override and always runs against the repo it lives in — the check-pure-core.sh:13-14
# form. Arity differs for the same reason: check-one-signer.sh takes no positional
# argument, this script takes exactly one image reference.
cd "$(dirname "$0")/.."

# The toolchain pin, exported for the same reason and in the same shape as
# scripts/check-one-signer.sh: a `cargo` reached through rustup's shim with no
# override in scope resolves rustup's DEFAULT channel and syncs it from the
# network, and no gate may reach the network. STATED PLAINLY: this script runs
# no cargo and no rustc today, so the export is inert here — it is kept so the
# two sibling gate scripts have ONE toolchain-resolution idiom rather than two,
# and so a later check added to this file inherits it instead of rediscovering
# the failure. The `docker build` inside the image uses the toolchain pinned in
# the Dockerfile, which this variable does not and must not reach.
if [ -z "${RUSTUP_TOOLCHAIN:-}" ] && [ -f rust-toolchain.toml ]; then
  pinned="$(sed -n 's/^[[:space:]]*channel[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' rust-toolchain.toml | head -1)"
  if [ -n "$pinned" ]; then
    export RUSTUP_TOOLCHAIN="$pinned"
  fi
fi

# Each argument on its own line, so a check that hands over multi-line evidence
# (`ldd`'s output, docker's stderr) stays readable instead of being folded onto
# one line.
fail() {
  printf '%s\n' "$@" >&2
  exit 1
}

# EXACTLY ONE ARGUMENT. Zero must not silently default to `logweir:check` and a
# second must not be ignored: both are how a caller that meant to assert a
# pushed digest ends up asserting something else, which is T0-17 all over again.
if [ "$#" -ne 1 ]; then
  fail "usage: check-image.sh <image-ref>" \
       "  <image-ref> is a local tag (logweir:check) or a digest reference" \
       "  (ghcr.io/<owner>/logweir@sha256:...). Exactly one, never zero and" \
       "  never two. Build the local tag with \`just image\`."
fi
ref="$1"

# Refuse, naming the missing tool. `openssl` is on the HOST on purpose: the
# runtime image is debian:bookworm-slim plus three libraries and carries no
# openssl CLI, which is exactly the situation an operator minting an approval
# is in.
for tool in docker openssl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    fail "check-image: REFUSING to run — \`$tool\` is not on PATH." \
         "  This gate needs \`docker\` (to run the image) and \`openssl\` (to mint" \
         "  the approver key on the host, because the image carries no openssl)." \
         "  Install the missing tool and re-run; this gate does not skip checks."
  fi
done

# The Mac has no `sha256sum`; a GitHub runner has no `shasum` guarantee. Prefer
# the coreutils name and fall back, and REFUSE if neither is present rather
# than letting check 4's digest comparison quietly compare nothing.
if command -v sha256sum >/dev/null 2>&1; then
  sha256_of() { sha256sum "$1" | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha256_of() { shasum -a 256 "$1" | cut -d' ' -f1; }
else
  fail "check-image: REFUSING to run — neither \`sha256sum\` nor \`shasum\` is on PATH." \
       "  Check 4 compares the approval's recorded spec digest against the spec's" \
       "  own sha256; without a hasher that comparison would compare nothing."
fi

# BEFORE ANY CHECK. If the reference is absent, every `docker run` below would
# fail for that one reason and check 1 would take the blame for it.
if ! docker image inspect "$ref" >/dev/null 2>&1; then
  fail "check-image: \`$ref\` is not in the local daemon." \
       "  This gate asserts an image that is already built; it never builds or" \
       "  pulls one. Run \`just image\` (which builds \`logweir:check\`), or pass" \
       "  a reference the daemon holds."
fi

# EVERY `docker run` BELOW CARRIES `--platform linux/amd64`, and the reason is
# not tidiness: the engine layer (Dockerfile:46) has no arm64 manifest, so the
# image is linux/amd64 and on the arm64 development host every one of these
# runs is emulated. Stating the platform makes the emulation intentional rather
# than a warning nobody reads, and it makes a run against an accidentally
# host-architecture image fail here instead of somewhere later.
PLATFORM="linux/amd64"

echo "== check-image: $ref =="

# ------------------------------------------------------------------ check 1
# EVERY DYNAMIC DEPENDENCY MUST RESOLVE (GC10). Task 22 shipped an image whose
# ENGINE ran, whose licences were present and whose `docker images` row looked
# healthy — while `logweir` itself died at the dynamic loader on a missing
# `libsasl2.so.2`, because the runtime stage installed `libssl3` but not
# `libsasl2-2`. Check 3 catches it too; THIS line NAMES the missing library.
#
# TWO EXIT CODES ARE IN PLAY AND ONLY ONE OF THEM IS IGNORED. `ldd` exits 0
# even when libraries are unresolved, which is exactly why the TEXT is what is
# inspected and why the `|| true` on the grep is deliberate (grep exits 1 on no
# match, which is the GOOD case). DOCKER's exit code is still read, on its own
# line: without that, an image that cannot start a shell at all would produce
# no output, match nothing, and pass its own linkage check.
echo "-- check 1 (ldd): the logweir binary's dynamic dependencies resolve"
if ! ldd_out=$(docker run --rm --platform "$PLATFORM" --entrypoint /bin/sh "$ref" \
                 -c 'ldd /usr/local/bin/logweir' 2>&1); then
  fail "check 1 (ldd): could not run ldd inside $ref:" "$ldd_out"
fi
missing=$(printf '%s\n' "$ldd_out" | grep 'not found' || true)
if [ -n "$missing" ]; then
  fail "check 1 (ldd): the image's logweir binary has unresolved libraries:" "$missing"
fi

# ------------------------------------------------------------------ check 2
# GR8 / GC3: `--version` is a FLAG, not a kafka-backup subcommand, so the
# three-subcommand contract (restore, validate-restore, validation run) does
# not reach it, and scripts/check-no-oso.sh:54 matches subcommand tokens only,
# never the binary name or a flag. Reaching the engine needs an explicit
# --entrypoint because Dockerfile:69 makes `logweir` the entrypoint.
echo "-- check 2: kafka-backup --version"
docker run --rm --platform "$PLATFORM" --entrypoint /usr/local/bin/kafka-backup "$ref" --version \
  || fail "check 2: kafka-backup --version failed inside $ref" \
          "  The image carries the engine at /usr/local/bin/kafka-backup" \
          "  (Dockerfile:63) and LOGWEIR_ENGINE_BIN points at it (Dockerfile:67)."

# ------------------------------------------------------------------ check 3
echo "-- check 3: logweir --version"
docker run --rm --platform "$PLATFORM" "$ref" --version \
  || fail "check 3: logweir --version failed inside $ref" \
          "  This is the check the shipped libsasl2 defect failed; check 1" \
          "  above names the library when the cause is the dynamic loader."

# ------------------------------------------------------------------ check 4
# THE IMAGE ALONE MUST BE ABLE TO MINT THE APPROVAL `drill run` REFUSES TO
# START WITHOUT. `--approval` is mandatory and, until `drill approve` existed,
# the only producer in the tree was a cargo EXAMPLE the image does not contain:
# an operator holding nothing but the image could not mint one at all.
#
# THE WORK DIRECTORY IS A `mktemp -d`, NOT `.imgcheck/` IN THE REPOSITORY. That
# is the one deliberate divergence from release.yml's behaviour: this recipe
# runs on a developer's tree and must leave `git status --porcelain` empty
# after a successful AND after a failed run, which an in-repo scratch directory
# plus a fail-fast `exit` cannot do.
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "-- check 4 (drill approve): the image can mint a signed approval"
# 0777 on the directory and 0644 on the key are LOAD-BEARING, not sloppy:
# Dockerfile:68 runs the container as uid 65532, which cannot read a
# 0600 root-owned key nor write into a 0700 host directory.
chmod 0777 "$work"

# The key is generated ON THE HOST and lives only inside this mktemp -d, which
# the trap above removes on every exit path. No key material is ever written
# into the repository. Split into two invocations rather than piped, so each
# openssl's exit status is read on its own line.
openssl ecparam -genkey -name prime256v1 -noout -out "$work/ec.pem" \
  || fail "check 4 (drill approve): openssl could not generate a P-256 key"
openssl pkcs8 -topk8 -nocrypt -in "$work/ec.pem" -out "$work/approver.pem" \
  || fail "check 4 (drill approve): openssl could not write the PKCS#8 approver key"
rm -f "$work/ec.pem"
chmod 0644 "$work/approver.pem"

cp examples/drill.yaml "$work/drill.yaml" \
  || fail "check 4 (drill approve): examples/drill.yaml is not readable from $(pwd)"

docker run --rm --platform "$PLATFORM" -v "$work:/w" -w /w "$ref" \
  drill approve --spec drill.yaml --key approver.pem \
  --approver ci@example.com --ticket REL-CHECK --out approval.json \
  || fail "check 4 (drill approve): the image could not mint an approval over examples/drill.yaml"

test -s "$work/approval.json" \
  || fail "check 4 (drill approve): approval.json is missing or empty"
test -s "$work/approval.sig" \
  || fail "check 4 (drill approve): approval.sig is missing or empty"
grep -q 'drill-approval' "$work/approval.sig" \
  || fail "check 4 (drill approve): approval.sig does not carry the drill-approval payload type"
spec_digest="$(sha256_of "$work/drill.yaml")"
grep -q "sha256:$spec_digest" "$work/approval.json" \
  || fail "check 4 (drill approve): approval.json does not bind the spec it was minted over" \
          "  expected the digest sha256:$spec_digest of $work/drill.yaml"

# ------------------------------------------------------------------ check 5
# GC15, and release.yml:178-179 says so: the upstream MIT licence must be IN
# the image that redistributes the upstream binary. Both paths, because the
# image redistributes two licensed things — upstream's engine and Logweir.
echo "-- check 5 (licence): both redistributed licences are present"
docker run --rm --platform "$PLATFORM" --entrypoint /bin/sh "$ref" -c \
  'test -s /usr/share/licenses/kafka-backup/LICENSE && test -s /usr/share/licenses/logweir/LICENSE' \
  || fail "check 5 (licence): $ref is missing a redistributed licence" \
          "  Expected both /usr/share/licenses/kafka-backup/LICENSE (upstream MIT," \
          "  Dockerfile:65) and /usr/share/licenses/logweir/LICENSE (Dockerfile:66)." \
          "  GC15: the licence ships in the image that redistributes the binary."

echo "ok: engine, CLI, approval minting and both licences are present in $ref"
