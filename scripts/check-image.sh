#!/usr/bin/env bash
# THE IMAGE SMOKE GATE — the single implementation. Phase 1 line item 1f,
# Tier 0 item T0-17 (the refactor half).
#
# Five of these assertions were lifted VERBATIM IN BEHAVIOUR out of
# `.github/workflows/release.yml:136-182`, where they had lived since Task 22
# and where they have NEVER EXECUTED on any commit, including the v0.1.0 tag —
# release.yml has never run because no tag has been pushed (ci.yml, no-oso.yml
# and kind-demo.yml have run green since 2026-09-12). A gate
# that has never run is not a gate, so they now live in a file a human can run
# on a laptop and whose exit code `just` reads directly.
#
# WHY IT TAKES THE REFERENCE AS AN ARGUMENT (T0-17). The release job used to
# assert a locally built tag and then push a SEPARATELY built image, so the
# image that was asserted was not the image that was pushed. Taking the
# reference as an argument is what lets the release job assert the exact
# reference it pushes — a local tag (`logweir:check`) or a digest reference
# (`docker.io/<namespace>/logweir@sha256:…`) — so the job becomes
# build-once → assert → push-THAT-digest.
#
# TWO CALLERS, ONE IMPLEMENTATION:
#   * `just smoke`                      (this task)
#   * `.github/workflows/release.yml`   (Task 9 — do not add it here)
# Neither may keep a private copy of the logic; that is the whole point of the
# extraction.
#
# WHAT IT PROVES about the image, in the order the checks RUN:
#   6. BOTH shipped binaries are x86-64 ELFs (STANDING RULE 10) —
#      `/usr/local/bin/logweir` and `/usr/local/bin/kafka-backup`;
#   1. every dynamic dependency of `/usr/local/bin/logweir` RESOLVES (GC10);
#   2. the engine answers `kafka-backup --version`;
#   3. the CLI answers `logweir --version`;
#   4. the image alone can mint a signed approval over `examples/drill.yaml`;
#   5. both redistributed licences are present (GC15) — TWO assertions, one per
#      file, so a failure names WHICH licence is missing.
#
# CHECK 6 IS SIXTH BY NUMBER AND FIRST BY POSITION, and the reason is written
# out beside it below. In short: the numbers 1-5 are matched in stderr by
# `e2e/tests/check_image.rs`, so renumbering them would silently re-point five
# tests; and the ELF read is both the cheapest check here and the only one that
# gives the right answer when the binary is the wrong architecture.
#
# WHAT IT PROVES ABOUT A DRILL: nothing. No broker, no bucket, no cluster and
# no archive is touched. See docs/stability.md.
#
# EXIT CODES ARE 0 AND 1 ONLY. GC11's 0/1/2/3/4 contract governs the `logweir`
# BINARY, not shell scripts; do not reuse those values here. 0 = all five
# checks passed. 1 = a check failed, the reference is not in the local daemon,
# or a prerequisite is missing. A missing prerequisite is a FAILURE WITH A
# NAMED REASON, never a green "skipped" run — a check that cannot fail is the
# defect class this gate exists to remove, not to repeat. Every check here has
# a test in `e2e/tests/check_image.rs` that BREAKS an image and watches this
# script reject it, check 6 included.
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
       "  (docker.io/<namespace>/logweir@sha256:...). Exactly one, never zero and" \
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
# not tidiness: the engine layer (Dockerfile:164) has no arm64 manifest, so the
# image is linux/amd64 and on the arm64 development host every one of these
# runs is emulated. Stating the platform makes the emulation intentional rather
# than a warning nobody reads, and it makes a run against an accidentally
# host-architecture image fail here instead of somewhere later.
PLATFORM="linux/amd64"

echo "== check-image: $ref =="

# ------------------------------------------------------------------ check 6
# THE SHIPPED BINARY IS x86-64. STANDING RULE 10 made the builder stage
# cross-compile (`FROM --platform=$BUILDPLATFORM`, `--target
# x86_64-unknown-linux-gnu`), which removed a 50-minute emulated compile and
# introduced exactly one new way to be wrong: a builder that quietly produced a
# HOST-architecture binary, or a `COPY --from=builder` pointed back at
# `target/release/`. Task 8 verified this property BY HAND — `od` on the first
# 20 bytes of the shipped binary — and its review asked for the assertion. This
# is the assertion.
#
# WHY IT IS NUMBERED 6 AND RUNS FIRST. The NUMBER is 6 because checks 1-5 are
# pinned to their symptoms by `e2e/tests/check_image.rs`, which matches the
# check number in stderr; renumbering them would silently re-point five tests
# for a cosmetic gain. The POSITION is first because this is the only check
# that gives the right answer when the binary is the wrong architecture: `ldd`
# on a foreign ELF reports "not a dynamic executable" and `logweir --version`
# dies with "exec format error", so checks 1 and 3 both fail for a reason that
# names neither the architecture nor the cause. It is also the cheapest check
# in the file — one `docker run` that reads 20 bytes and EXECUTES NOTHING from
# the image, which is the right thing to do before running anything from it.
#
# `--entrypoint /usr/bin/od`, NOT A SHELL, and that is deliberate: an image
# with no `/bin/sh` must still be caught by CHECK 1, which is where
# `check_image_rejects_an_image_with_no_shell` says the missing shell is found.
# Reading the header through `sh -c` would move that failure up here and
# quietly re-point that test.
#
# THE FIELDS, from the ELF header (little-endian, class 64):
#   bytes 0-3    7f 45 4c 46   the magic, "\x7fELF"
#   byte  4      02            EI_CLASS = ELFCLASS64
#   bytes 18-19  3e 00         e_machine = 0x003e = EM_X86_64
echo "-- check 6 (ELF): logweir and kafka-backup are x86-64 ELFs"
if ! elf_out=$(docker run --rm --platform "$PLATFORM" --entrypoint /usr/bin/od "$ref" \
                 -An -tx1 -N20 /usr/local/bin/logweir 2>&1); then
  fail "check 6 (ELF): could not read the ELF header of /usr/local/bin/logweir in $ref:" \
       "$elf_out" \
       "  This check runs \`od\` from the image itself (coreutils, present in" \
       "  debian:bookworm-slim) rather than a shell, so that an image with no" \
       "  /bin/sh is still reported by check 1."
fi
# od prints 16 bytes per line, space-separated, with a leading indent; flatten
# to one space-separated list so the fields can be addressed by position.
elf_bytes="$(printf '%s' "$elf_out" | tr -s '[:space:]' ' ' | sed -e 's/^ //' -e 's/ $//')"
elf_magic="$(printf '%s' "$elf_bytes" | cut -d' ' -f1-5)"
elf_machine="$(printf '%s' "$elf_bytes" | cut -d' ' -f19-20)"
if [ "$elf_magic" != "7f 45 4c 46 02" ]; then
  fail "check 6 (ELF): /usr/local/bin/logweir in $ref is not a 64-bit ELF." \
       "  expected the first five bytes to be \`7f 45 4c 46 02\` (\\x7fELF, ELFCLASS64)" \
       "  header read: $elf_bytes"
fi
if [ "$elf_machine" != "3e 00" ]; then
  fail "check 6 (ELF): /usr/local/bin/logweir in $ref is NOT an x86-64 binary." \
       "  e_machine at offset 18 is \`$elf_machine\`, expected \`3e 00\` (0x003e," \
       "  EM_X86_64). The builder stage cross-compiles to x86_64-unknown-linux-gnu" \
       "  (Dockerfile:158) and the runtime stage copies from" \
       "  target/x86_64-unknown-linux-gnu/release (Dockerfile:185); a" \
       "  host-architecture binary here means one of those two was changed." \
       "  header read: $elf_bytes"
fi

# THE ENGINE'S ARCHITECTURE IS ASSERTED TOO (Task 9, carried from Task 8b's
# review). Check 6 read the CLI's e_machine and stopped there, which left the
# OTHER shipped ELF unread. The image is meant to be uniformly x86-64: the CLI
# is cross-compiled to x86_64-unknown-linux-gnu and the engine is copied out of
# an amd64-only image pinned BY DIGEST (Dockerfile:164) into
# /usr/local/bin/kafka-backup (Dockerfile:181). Repin that line at a multi-arch
# tag, or point the COPY at another stage, and a foreign binary can land beside
# a correct one. Check 2 does fail on it — with "exec format error", which names
# neither the architecture nor the cause. That is exactly the argument that put
# the CLI's header read first, and it applies to both binaries or to neither.
#
# AN UNREADABLE HEADER IS NOT THIS ARM'S FAILURE, AND THAT IS DELIBERATE. A
# MISSING engine is check 2's to report: `e2e/tests/check_image.rs`'s
# check_image_rejects_an_image_whose_engine_is_missing pins that message to
# "check 2" and "kafka-backup --version", and failing here on an absent file
# would move the failure up and silently re-point that test. So this arm says
# only what it can see — it refuses a header that IS readable and IS the wrong
# machine, and says out loud when it could not read one. It is not a check that
# cannot fail: `check_image_rejects_an_image_whose_engine_is_not_x86_64` breaks
# an image exactly this way and watches this arm reject it.
if engine_elf_out=$(docker run --rm --platform "$PLATFORM" --entrypoint /usr/bin/od "$ref" \
                      -An -tx1 -N20 /usr/local/bin/kafka-backup 2>&1); then
  engine_bytes="$(printf '%s' "$engine_elf_out" | tr -s '[:space:]' ' ' | sed -e 's/^ //' -e 's/ $//')"
  engine_machine="$(printf '%s' "$engine_bytes" | cut -d' ' -f19-20)"
  if [ "$engine_machine" != "3e 00" ]; then
    fail "check 6 (ELF): /usr/local/bin/kafka-backup in $ref is NOT an x86-64 binary." \
         "  e_machine at offset 18 is \`$engine_machine\`, expected \`3e 00\` (0x003e," \
         "  EM_X86_64). The engine is copied from the amd64-only image pinned by" \
         "  digest at Dockerfile:164 into /usr/local/bin/kafka-backup" \
         "  (Dockerfile:181); a foreign binary here means that pin or that COPY" \
         "  was changed." \
         "  header read: $engine_bytes"
  fi
else
  echo "-- check 6 (ELF): /usr/local/bin/kafka-backup's header could not be read;" \
       "check 2 below is where a missing or unrunnable engine is reported"
fi

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
# --entrypoint because Dockerfile:190 makes `logweir` the entrypoint.
echo "-- check 2: kafka-backup --version"
docker run --rm --platform "$PLATFORM" --entrypoint /usr/local/bin/kafka-backup "$ref" --version \
  || fail "check 2: kafka-backup --version failed inside $ref" \
          "  The image carries the engine at /usr/local/bin/kafka-backup" \
          "  (Dockerfile:181) and LOGWEIR_ENGINE_BIN points at it (Dockerfile:188)."

# ------------------------------------------------------------------ check 3
echo "-- check 3: logweir --version"
docker run --rm --platform "$PLATFORM" "$ref" --version \
  || fail "check 3: logweir --version failed inside $ref" \
          "  This is the check the shipped libsasl2 defect failed; check 1" \
          "  above names the library when the cause is the dynamic loader."

# PLAT-02.1: the exact candidate digest must carry the bootstrap CLI before
# promotion. This runs in the existing image publication check, after the
# registry candidate is pulled by digest and before any public tag moves.
echo "-- check 3b: logweir identity bootstrap --help"
docker run --rm --platform "$PLATFORM" "$ref" identity bootstrap --help \
  || fail "check 3b: identity bootstrap --help failed inside $ref" \
          "  Do not publish or pin this runner for identity-enabled Helm installs;" \
          "  it predates or breaks the PLAT-02.1 bootstrap CLI contract."

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
# 0777 on the directory is LOAD-BEARING, not sloppy: Dockerfile:189 runs the
# container as uid 65532, which cannot write its output into a 0700 host
# directory.
chmod 0777 "$work"

# The key is generated ON THE HOST and lives only inside this mktemp -d, which
# the trap above removes on every exit path. No key material is ever written
# into the repository. Split into two invocations rather than piped, so each
# openssl's exit status is read on its own line.
#
# MINTED UNDER `umask 077` (Task 8b, carried from Task 8's review). Until then
# openssl created both files 0644 inside a 0777 directory. On macOS the
# per-user 0700 $TMPDIR hides that, but on a shared Linux CI runner `mktemp -d`
# lands under a world-traversable /tmp and any local user could read a P-256
# private key for as long as the gate ran.
#
# `umask 077` IS NOT THE WHOLE STORY AND MUST NEVER BE QUOTED AS IF IT WERE
# (Task 9, carried from Task 8b's review, whose report stated the umask without
# the window it leaves). THE KEY'S MODE OVER ITS LIFETIME IS 0600 THEN 0644,
# NOT 0600: it is created 0600 by the umask below, `chmod 0644` widens it for
# the one `docker run` that must read it as uid 65532, and it is `rm -f`ed on
# the next line. The world-readable window is those two lines wide — one
# container start — and on this host it is inside a per-user 0700 $TMPDIR,
# while on a shared CI runner it is inside a world-traversable /tmp.
#
# THE RESIDUAL WINDOW IS STATED, NOT HIDDEN. uid 65532 is not the host user, so
# the key must be world-readable for the ONE `docker run` that consumes it; the
# `chmod 0644` therefore sits immediately before that run and the key is
# removed immediately after it, rather than living until the trap fires.
# Closing the last window would need either `--user "$(id -u)"` on that run —
# which would stop the check exercising the uid the image actually ships with,
# the property Dockerfile:189 is about — or minting the key inside the
# container, which carries no openssl (see the prerequisite loop above).
# Neither is a trade this gate should make silently.
old_umask="$(umask)"
umask 077
openssl ecparam -genkey -name prime256v1 -noout -out "$work/ec.pem" \
  || fail "check 4 (drill approve): openssl could not generate a P-256 key"
openssl pkcs8 -topk8 -nocrypt -in "$work/ec.pem" -out "$work/approver.pem" \
  || fail "check 4 (drill approve): openssl could not write the PKCS#8 approver key"
umask "$old_umask"
rm -f "$work/ec.pem"

cp examples/drill.yaml "$work/drill.yaml" \
  || fail "check 4 (drill approve): examples/drill.yaml is not readable from $(pwd)"

chmod 0644 "$work/approver.pem"
docker run --rm --platform "$PLATFORM" -v "$work:/w" -w /w "$ref" \
  drill approve --spec drill.yaml --key approver.pem \
  --approver ci@example.com --ticket REL-CHECK --out approval.json \
  || fail "check 4 (drill approve): the image could not mint an approval over examples/drill.yaml"
rm -f "$work/approver.pem"

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
# the image that redistributes the upstream binary. The image redistributes TWO
# licensed things — upstream's engine and Logweir — so this is TWO ASSERTIONS,
# one per file, each naming the file it did not find.
#
# THEY WERE ONE `test -s A && test -s B` UNTIL TASK 8b. A combined test tells a
# reader that a licence is missing but not WHICH, and "the licence file is
# missing" is a legal-exposure failure whose remedy differs entirely by file:
# the upstream MIT copy comes from `third_party/LICENSE-MIT` and Logweir's own
# from the repository root. Both keep the number 5 because they are the same
# check on two files, and because `e2e/tests/check_image.rs` matches that
# number.
echo "-- check 5 (licence): both redistributed licences are present"
docker run --rm --platform "$PLATFORM" --entrypoint /bin/sh "$ref" -c \
  'test -s /usr/share/licenses/kafka-backup/LICENSE' \
  || fail "check 5 (licence): $ref is missing the UPSTREAM licence" \
          "  /usr/share/licenses/kafka-backup/LICENSE (upstream MIT, copied from" \
          "  third_party/LICENSE-MIT by Dockerfile:186) is absent or empty." \
          "  GC15: the licence ships in the image that redistributes the binary."
docker run --rm --platform "$PLATFORM" --entrypoint /bin/sh "$ref" -c \
  'test -s /usr/share/licenses/logweir/LICENSE' \
  || fail "check 5 (licence): $ref is missing LOGWEIR'S OWN licence" \
          "  /usr/share/licenses/logweir/LICENSE (Apache-2.0, copied with NOTICE" \
          "  by Dockerfile:187) is absent or empty. GC15 governs Logweir's own" \
          "  redistribution exactly as it governs upstream's."

echo "ok: x86-64 binaries, engine, CLI, approval minting and both licences are present in $ref"
