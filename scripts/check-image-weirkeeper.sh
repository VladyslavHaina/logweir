#!/usr/bin/env bash
# THE CONTROLLER IMAGE GATE — interface I25, Task 23; `--no-exec` by Task 30b.
#
# Two modes, one file:
#
#     check-image-weirkeeper.sh <image-ref>              # FIVE checks, by running the image
#     check-image-weirkeeper.sh --no-exec <image-ref>    # THREE checks, executing nothing
#
# ---------------------------------------------------------------------------
# WHY THIS IS A NEW FILE AND NOT A FLAG ON `scripts/check-image.sh`
# ---------------------------------------------------------------------------
# `check-image.sh weirkeeper:check` does not fail one check. It fails ALL SIX,
# and five of them fail because the thing they look for is not in this image
# and must not be (critique B H15):
#
#   check 1  `ldd /usr/local/bin/logweir`            — that binary is not here.
#   check 2  `/usr/local/bin/kafka-backup --version` — this image carries NO
#                                                      OSO code at all.
#   check 3  runs the entrypoint expecting the `logweir` CLI — the entrypoint
#                                                      here is `weirkeeper`.
#   check 4  mints a signed approval with `drill approve` INSIDE the image —
#            `weirkeeper` has no such subcommand and must never link the signer
#            (Global Constraint 27, guard G-SIGN).
#   check 5  requires `/usr/share/licenses/kafka-backup/LICENSE` — the
#            controller redistributes no MIT-licensed binary and owes no MIT
#            notice. Check 3 below asserts that directory is ABSENT, which is
#            the exact opposite assertion.
#   check 6  hard-asserts the ELF `e_machine` `3e 00` (x86-64) for both of the
#            runner's binaries. The controller image is built for the host's
#            architecture (`LOGWEIR_IMAGE_PLATFORM`, default `linux/arm64`),
#            so an x86-64 assertion is wrong here by construction.
#
# So `check-image.sh` is NEITHER REUSED NOR MODIFIED for this image. Running it
# against `weirkeeper:check` is one of this task's mutants, and its observed
# failure is recorded in the task report.
#
# ---------------------------------------------------------------------------
# WHAT THIS PROVES, IN THE ORDER THE CHECKS RUN
# ---------------------------------------------------------------------------
#   1. every dynamic dependency of `/usr/local/bin/weirkeeper` RESOLVES —
#      no `not found` (Global Constraint 10);
#   2. `/usr/local/bin/weirkeeper --version` exits 0 (Task 15 built that flag
#      FOR THIS CHECK: it installs the rustls provider, matches argv, prints,
#      and builds no Kubernetes client — an image check must touch no cluster);
#   3. `/usr/share/licenses/logweir/LICENSE` and `.../NOTICE` are non-empty
#      (Global Constraint 15) **and `/usr/share/licenses/kafka-backup/` is
#      ABSENT** — an image that shipped a notice for a binary it does not
#      carry would be claiming a redistribution it does not make;
#   4. `/etc/logweir/org-root.fingerprint` inside the image is BYTE-IDENTICAL
#      to the checked-in `third_party/org-root.fingerprint` — the anchor of
#      stage-2 Task 16's T1, baked at build time so that whoever controls the
#      cluster cannot change it without producing a different image.
#
# Check 3 also requires `/usr/share/licenses/logweir/THIRD_PARTY_NOTICES.md`
# (Task 29's interface I30). `Dockerfile.weirkeeper:209` `COPY`s all three files
# in one instruction; before Task 30b this gate knew about two of them, so an
# image built from a Dockerfile that had dropped the inventory would have passed
# a gate whose own header cites Global Constraint 15.
#
# ---------------------------------------------------------------------------
# `--no-exec`: THE SAME IMAGE, ASSERTED WITHOUT RUNNING ANYTHING FROM IT
# ---------------------------------------------------------------------------
# Task 30b. `release.yml` builds the controller image once per architecture, on
# a NATIVE runner of that architecture, and the pushing job then loads both and
# asserts both before assembling the manifest list. The amd64 runner can run the
# amd64 variant — that is the mode above — and CANNOT run the arm64 one. QEMU is
# forbidden (STANDING RULE 10), and a variant nobody looked at is exactly the
# defect `workflow_lint.rs` exists to prevent, so the arm64 leg is asserted HERE,
# with no `docker run` anywhere in the mode:
#
#   1. ARCHITECTURE, FIRST. `/usr/local/bin/weirkeeper` is a 64-bit
#      little-endian ELF whose `e_machine` (header bytes 18-19) is `b7 00`,
#      EM_AARCH64. This mirrors `scripts/check-image.sh`'s check 6
#      (`7f 45 4c 46 02` magic + class, then `3e 00` for EM_X86_64): the same
#      three fields at the same offsets, the constant changed to the
#      architecture this mode exists for.
#   2. The licence set of check 3 above, including the inventory, and
#      `/usr/share/licenses/kafka-backup/` ABSENT.
#   3. The org-root anchor of check 4 above, byte for byte.
#
# WHY THE ARCHITECTURE CHECK IS FIRST AND NOT LAST. It is the check that gives
# the right answer when the reference is the wrong image altogether — the same
# argument `check-image.sh` makes for numbering its ELF check 6 and running it
# first. The acceptance for this mode is a NEGATIVE CONTROL,
# `--no-exec logweir:check`: the runner image is x86-64, and this mode must
# reject it NAMING THE ARCHITECTURE. Ordered second or third it would be
# rejected for a missing inventory file instead, which is true of that image and
# is not what the mode is for.
#
# HOW IT READS THE FILES: `docker create` then `docker cp` — the idiom
# `scripts/extract-engine.sh` already uses — and every assertion then runs on
# the HOST, against files on the host's disk. `docker create` does not start a
# process, so no emulation is involved and none is configured. Nothing from the
# image is executed: no `docker run`, no `--entrypoint`, not even `od` out of the
# image's own coreutils (which is how `check-image.sh` reads its header, and
# which is a `docker run`).
#
# WHAT `--no-exec` CANNOT PROVE, stated rather than glossed: the two EXECUTION
# checks. `ldd` resolving and `--version` exiting 0 are properties of running the
# binary, and this mode does not run it. That is the whole reason
# `release.yml` builds each architecture on its own native runner and asserts it
# THERE, with this mode as the second, execution-free look on the pushing job.
#
# `just check-org-root` runs check 4's comparison for BOTH images; this script
# is the controller image's own gate and carries it too, so a caller that runs
# only this one is not left without the anchor check.
#
# WHAT IT PROVES ABOUT A CLUSTER: nothing. No API server, no kubeconfig, no
# token. It builds nothing and pushes nothing (Global Constraint 17):
# `just image-weirkeeper` is the named producer of `weirkeeper:check`.
#
# EVERY EXIT CODE IS READ ON ITS OWN LINE (STANDING RULE 20). Nothing here is
# piped where the status is load-bearing: `docker run`'s status is read
# directly, and the one `grep` whose status is NOT load-bearing (`ldd`'s text)
# is marked `|| true` with the reason beside it.
#
# EXIT CODES ARE 0 AND 1 ONLY, exactly as `check-image.sh`: Global Constraint
# 11's 0/1/2/3/4 contract governs the `logweir` BINARY, not shell scripts. A
# missing prerequisite is a FAILURE WITH A NAMED REASON, never a green
# "skipped" run.
set -euo pipefail

# The REAL repository, with no `LOGWEIR_ROOT` override: check 4 compares the
# image's bytes against the CHECKED-IN file, and a gate that let its expected
# value be pointed somewhere else would compare nothing. Same form as
# `scripts/check-image.sh`.
cd "$(dirname "$0")/.."

# Each argument on its own line, so multi-line evidence (ldd's output, docker's
# stderr, a diff) stays readable.
fail() {
  printf '%s\n' "$@" >&2
  exit 1
}

# EXACTLY ONE IMAGE REFERENCE, after at most the one flag this script has. Zero
# must not silently default to `weirkeeper:check` and a second must not be
# ignored: both are how a caller that meant to assert a pushed digest ends up
# asserting something else (T0-17).
#
# THE FLAG IS MATCHED EXACTLY AND ONLY IN FIRST POSITION. An unknown flag is a
# usage error naming what it got, never a silently-ignored argument that leaves
# the caller believing a mode ran: `--noexec`, `--no_exec` and a trailing
# `--no-exec` after the reference are all refused here.
no_exec=0
if [ "${1:-}" = "--no-exec" ]; then
  no_exec=1
  shift
fi
if [ "$#" -ne 1 ]; then
  fail "usage: check-image-weirkeeper.sh [--no-exec] <image-ref>" \
       "  <image-ref> is a local tag (weirkeeper:check) or a digest reference" \
       "  (ghcr.io/logweir/weirkeeper@sha256:...). Exactly one, never zero and" \
       "  never two. Build the local tag with \`just image-weirkeeper\`." \
       "  --no-exec asserts a variant this host cannot run: architecture," \
       "  licences and the org-root anchor, executing nothing from the image."
fi
case "$1" in
  -*) fail "check-image-weirkeeper: unknown option \`$1\`." \
           "  The only option is \`--no-exec\`, and it comes FIRST:" \
           "    check-image-weirkeeper.sh --no-exec <image-ref>" \
           "  An option this script does not understand is a usage error, never" \
           "  an argument it quietly treats as an image reference." ;;
esac
ref="$1"

if ! command -v docker >/dev/null 2>&1; then
  fail "check-image-weirkeeper: REFUSING to run — \`docker\` is not on PATH." \
       "  This gate runs the image; it does not skip checks."
fi

# THE ANCHOR MUST EXIST ON DISK BEFORE THE IMAGE IS ASKED FOR IT. If it does
# not, check 4 would compare the image against an empty string and pass.
if [ ! -s third_party/org-root.fingerprint ]; then
  fail "check-image-weirkeeper: third_party/org-root.fingerprint is missing or empty." \
       "  That file is the checked-in half of the org-root anchor and this gate's" \
       "  expected value; without it check 4 would compare the image against nothing."
fi

# BEFORE EITHER MODE. If the reference is absent from the daemon, every command
# below fails for that one reason and whichever check ran first takes the blame
# for it — `check-image.sh`'s own practice, and the reason its check 1 is not
# where a missing image is reported.
if ! docker image inspect "$ref" >/dev/null 2>&1; then
  fail "check-image-weirkeeper: \`$ref\` is not in the local daemon." \
       "  This gate asserts an image that is already built; it never builds or" \
       "  pulls one. Run \`just image-weirkeeper\` (which builds" \
       "  \`weirkeeper:check\`), or pass a reference the daemon holds."
fi

# =========================================================================
# `--no-exec` — TASK 30b. THREE CHECKS, NOTHING FROM THE IMAGE IS EXECUTED.
# =========================================================================
if [ "$no_exec" -eq 1 ]; then
  echo "== check-image-weirkeeper --no-exec: $ref =="

  # THE ARCHITECTURE THIS MODE EXISTS FOR, stated once. `release.yml`'s amd64
  # pushing job can run the amd64 variant and cannot run the arm64 one, so
  # `--no-exec` is the arm64 leg's assertion and AArch64 is not a parameter.
  # Making it one would let the negative control below pass.
  WANT_ARCH="arm64"
  WANT_MACHINE="b7 00"

  # ------------------------------------------------- check 1 (architecture), a
  # THE IMAGE'S OWN DECLARED PLATFORM, read from the daemon's metadata — no
  # container, no process. A reference that is not an arm64 image is rejected
  # HERE, naming both architectures, which is what makes the negative control
  # (`--no-exec logweir:check`, an x86-64 image) fail on the architecture rather
  # than on a licence file it also happens to lack.
  echo "-- check 1 (architecture): is $ref a linux/$WANT_ARCH image carrying an AArch64 ELF?"
  if ! declared=$(docker image inspect "$ref" --format '{{.Os}}/{{.Architecture}}' 2>&1); then
    fail "check 1 (architecture): could not read the platform of $ref:" "$declared"
  fi
  if [ "$declared" != "linux/$WANT_ARCH" ]; then
    fail "check 1 (architecture): $ref is a \`$declared\` image, NOT \`linux/$WANT_ARCH\`." \
         "  --no-exec asserts the AArch64 variant: a 64-bit little-endian ELF whose" \
         "  e_machine (header bytes 18-19) is \`$WANT_MACHINE\` (0x00b7, EM_AARCH64)." \
         "  An x86-64 image carries \`3e 00\` (0x003e, EM_X86_64) there — that is the" \
         "  constant \`scripts/check-image.sh\` check 6 asserts for the RUNNER image," \
         "  and the runner image is not what this mode inspects." \
         "  Pass the arm64 controller image, or use the execution mode (no flag)" \
         "  on a host of the image's own architecture."
  fi

  # THE ONE `--platform` FLAG IN THIS FILE, and it is stated rather than
  # defaulted: `docker create` on a daemon whose default platform differs from
  # the image's is where a variant gets silently swapped for another. It is the
  # platform the check above just proved the image declares, so the flag can
  # never disagree with the reference.
  # The `${TMPDIR:-/tmp}` form, as `scripts/check-unverified-labels.sh` uses:
  # `mktemp -t` means different things to BSD and GNU mktemp and this script
  # runs on both (this laptop, and an ubuntu-24.04 runner).
  work="$(mktemp -d "${TMPDIR:-/tmp}/logweir-noexec.XXXXXX")"
  cid=""
  # A FUNCTION AND NOT AN INLINE TRAP STRING: every command in it is `|| true`d,
  # so a cleanup failure cannot change the exit status this gate reports.
  cleanup_no_exec() {
    if [ -n "$cid" ]; then
      docker rm -f "$cid" >/dev/null 2>&1 || true
    fi
    rm -rf "$work" || true
  }
  trap cleanup_no_exec EXIT
  if ! created=$(docker create --platform "linux/$WANT_ARCH" "$ref" 2>&1); then
    fail "check 1 (architecture): could not create a container from $ref:" "$created" \
         "  \`docker create\` allocates a container and starts NO process, which is" \
         "  why this mode can read an image it cannot run."
  fi
  cid="$created"

  # `docker cp`, the `scripts/extract-engine.sh` idiom. `cp_out <path> <dest>`
  # reads the exit status of `docker cp` on its own line, with no pipe, and
  # returns it so each caller decides whether absence is a failure or the point.
  cp_out() {
    rc=0
    docker cp "$cid:$1" "$2" >/dev/null 2>&1 || rc=$?
    return "$rc"
  }

  # ------------------------------------------------- check 1 (architecture), b
  # THE BYTES, ON THE HOST. `od` here is the HOST's od, never the image's: this
  # mode executes nothing from the image, so reading the header through the
  # image's own coreutils (which is what `check-image.sh` does, deliberately)
  # is not available and is not wanted.
  #
  # THE FIELDS, from the ELF header:
  #   bytes 0-3    7f 45 4c 46   the magic, "\x7fELF"
  #   byte  4      02            EI_CLASS = ELFCLASS64
  #   byte  5      01            EI_DATA  = ELFDATA2LSB (little-endian)
  #   bytes 18-19  b7 00         e_machine = 0x00b7 = EM_AARCH64
  if ! cp_out /usr/local/bin/weirkeeper "$work/weirkeeper"; then
    fail "check 1 (architecture): $ref carries no /usr/local/bin/weirkeeper." \
         "  \`Dockerfile.weirkeeper\` copies the binary there and sets it as the" \
         "  ENTRYPOINT; an image without it is not the controller image."
  fi
  elf_out=$(od -An -tx1 -N20 "$work/weirkeeper")
  elf_bytes="$(printf '%s' "$elf_out" | tr -s '[:space:]' ' ' | sed -e 's/^ //' -e 's/ $//')"
  elf_magic="$(printf '%s' "$elf_bytes" | cut -d' ' -f1-6)"
  elf_machine="$(printf '%s' "$elf_bytes" | cut -d' ' -f19-20)"
  if [ "$elf_magic" != "7f 45 4c 46 02 01" ]; then
    fail "check 1 (architecture): /usr/local/bin/weirkeeper in $ref is not a" \
         "  64-bit LITTLE-ENDIAN ELF." \
         "  expected the first six bytes to be \`7f 45 4c 46 02 01\`" \
         "  (\\x7fELF, ELFCLASS64, ELFDATA2LSB)" \
         "  header read: $elf_bytes"
  fi
  if [ "$elf_machine" != "$WANT_MACHINE" ]; then
    fail "check 1 (architecture): /usr/local/bin/weirkeeper in $ref is NOT an" \
         "  AArch64 binary." \
         "  e_machine at offset 18 is \`$elf_machine\`, expected \`$WANT_MACHINE\`" \
         "  (0x00b7, EM_AARCH64). \`3e 00\` there is x86-64 (0x003e, EM_X86_64)," \
         "  which is what the RUNNER image carries and what" \
         "  \`scripts/check-image.sh\` check 6 asserts for it." \
         "  header read: $elf_bytes"
  fi

  # ------------------------------------------------- check 2 (licence)
  # THE SAME SET AS THE EXECUTION MODE'S CHECK 3, including Task 29's inventory,
  # and the same negative assertion. Non-emptiness is `test -s` on the HOST's
  # copy of the file, which is the same predicate the execution mode runs inside
  # the image.
  echo "-- check 2 (licence): logweir's LICENSE, NOTICE and inventory, and no MIT notice"
  for f in LICENSE NOTICE THIRD_PARTY_NOTICES.md; do
    if ! cp_out "/usr/share/licenses/logweir/$f" "$work/$f"; then
      fail "check 2 (licence): $ref is missing /usr/share/licenses/logweir/$f." \
           "  \`Dockerfile.weirkeeper\` COPYs LICENSE, NOTICE and" \
           "  THIRD_PARTY_NOTICES.md into that directory in one instruction" \
           "  (Global Constraint 15, spec §16 clause 5, interface I30)."
    fi
    if [ ! -s "$work/$f" ]; then
      fail "check 2 (licence): /usr/share/licenses/logweir/$f in $ref is EMPTY." \
           "  A zero-byte licence file satisfies a COPY and satisfies nobody else."
    fi
  done
  # THE NEGATIVE ONE. Absence is the pass, so the FAILING exit of `docker cp` is
  # the good case and its status is read on its own line inside `cp_out`.
  if cp_out /usr/share/licenses/kafka-backup "$work/kafka-backup"; then
    fail "check 2 (licence): $ref CARRIES /usr/share/licenses/kafka-backup/." \
         "  The controller image owes no MIT notice: it carries no kafka-backup" \
         "  binary and no OSO code, so a notice for one is a claim to a" \
         "  redistribution that does not happen. Remove the COPY from" \
         "  Dockerfile.weirkeeper — do not add the licence to match the check."
  fi

  # ------------------------------------------------- check 3 (anchor)
  # BYTE IDENTITY against the checked-in file, `diff`'s status read directly.
  echo "-- check 3 (anchor): /etc/logweir/org-root.fingerprint matches third_party/"
  if ! cp_out /etc/logweir/org-root.fingerprint "$work/org-root.fingerprint"; then
    fail "check 3 (anchor): could not read /etc/logweir/org-root.fingerprint from $ref." \
         "  Dockerfile.weirkeeper must carry" \
         "  \`COPY third_party/org-root.fingerprint /etc/logweir/\`."
  fi
  rc=0
  diff -u third_party/org-root.fingerprint "$work/org-root.fingerprint" >/dev/null || rc=$?
  if [ "$rc" -ne 0 ]; then
    diff -u third_party/org-root.fingerprint "$work/org-root.fingerprint" >&2 || true
    fail "check 3 (anchor): $ref's baked fingerprint is NOT the checked-in one." \
         "  The diff above is third_party/org-root.fingerprint (-) against the" \
         "  image's /etc/logweir/org-root.fingerprint (+). Rebuild the image" \
         "  — never edit the file to match a stale image."
  fi

  echo "ok: --no-exec — AArch64 ELF, Logweir's own licences and inventory with no MIT notice,"
  echo "    and the org-root anchor are correct in $ref. NOTHING was executed from the image,"
  echo "    so linkage and --version are NOT asserted here; a native runner asserts those."
  exit 0
fi

# =========================================================================
# THE EXECUTION MODE — Task 23's four checks, plus Task 29's inventory file.
# =========================================================================
# NO `--platform` FLAG IN THIS MODE, and that is deliberate. `check-image.sh`
# pins `linux/amd64` because the runner image is amd64-only (the engine binary
# is). This image is built for whatever `LOGWEIR_IMAGE_PLATFORM` said —
# `linux/arm64` by default on this host — so naming a platform here would either
# be wrong on the developer machine or wrong on a CI runner. `docker run` with
# no `--platform` runs the image's own. (`--no-exec` above does name one,
# because `docker create` is where a daemon's default platform could silently
# swap one variant for another, and because that mode has already proved which
# platform the image declares.)

# ------------------------------------------------------------------ check 1
# EVERY DYNAMIC DEPENDENCY MUST RESOLVE (Global Constraint 10). `weirkeeper`
# links rustls, not OpenSSL, and no Kafka client — so the runtime stage carries
# `ca-certificates` and neither `libssl3` nor `libsasl2-2`. That is a claim
# about the link graph, and this is the line that MEASURES it: if the binary
# ever grows an OpenSSL or SASL edge, the runtime stage stops being sufficient
# and `ldd` says so by name.
#
# TWO EXIT CODES ARE IN PLAY AND ONLY ONE IS IGNORED. `ldd` exits 0 even when
# libraries are unresolved, which is why the TEXT is inspected and why the
# `|| true` on the grep is deliberate (grep exits 1 on no match, the GOOD
# case). DOCKER's status is still read, on its own line: an image that cannot
# start a shell at all would otherwise produce no output, match nothing and
# pass its own linkage check.
echo "-- check 1 (ldd): the weirkeeper binary's dynamic dependencies resolve"
if ! ldd_out=$(docker run --rm --entrypoint /bin/sh "$ref" \
                 -c 'ldd /usr/local/bin/weirkeeper' 2>&1); then
  fail "check 1 (ldd): could not run ldd inside $ref:" "$ldd_out"
fi
missing=$(printf '%s\n' "$ldd_out" | grep 'not found' || true)
if [ -n "$missing" ]; then
  fail "check 1 (ldd): the image's weirkeeper binary has unresolved libraries:" "$missing"
fi

# ------------------------------------------------------------------ check 2
# `--version` IS THE ONLY THING IN THIS BINARY AN IMAGE CHECK CAN RUN. With no
# argv `weirkeeper` builds a `kube::Client` and watches six kinds until
# SIGTERM; inside `docker run` there is no kubeconfig, no service-account token
# and no API server, so the no-argv path is not a check, it is a hang followed
# by a non-zero exit. `crates/weirkeeper/src/main.rs` puts the argv match
# BEFORE the client for exactly this reason, and the rustls provider install
# before the match so that this path cannot abort at 101 inside rustls.
#
# The entrypoint is `/usr/local/bin/weirkeeper`, so no `--entrypoint` override
# is needed and none is given: this runs the image AS SHIPPED.
echo "-- check 2: weirkeeper --version"
docker run --rm "$ref" --version \
  || fail "check 2: weirkeeper --version failed inside $ref" \
          "  The image's ENTRYPOINT is /usr/local/bin/weirkeeper" \
          "  (Dockerfile.weirkeeper) and \`--version\` must exit 0 without" \
          "  building a Kubernetes client. Check 1 above names the library when" \
          "  the cause is the dynamic loader."

# ------------------------------------------------------------------ check 3
# THREE ASSERTIONS, TWO POSITIVE AND ONE NEGATIVE, each run on its own line so
# a failure names WHICH one.
#
# Global Constraint 15 and spec §16 clause 5: Apache-2.0 requires the notice to
# travel with the redistributed binary, and "this image carries only our own
# code" is a reason to ship OUR licence, not a reason to ship none.
#
# THE NEGATIVE ONE IS THE POINT OF THIS CHECK. `/usr/share/licenses/kafka-backup/`
# is where the runner image puts upstream's MIT licence, because the runner
# image REDISTRIBUTES upstream's binary. This image does not — no
# `kafka-backup`, no OSO code of any kind — so the directory must be absent. An
# image that shipped the MIT notice anyway would be claiming a redistribution
# it does not make, and "fixing" a red `check-image.sh` run by copying the
# licence in is one of this task's mutants.
echo "-- check 3 (licence): logweir's own LICENSE, NOTICE and inventory, and no MIT notice"
docker run --rm --entrypoint /bin/sh "$ref" -c \
  'test -s /usr/share/licenses/logweir/LICENSE' \
  || fail "check 3 (licence): $ref is missing LOGWEIR'S OWN licence" \
          "  /usr/share/licenses/logweir/LICENSE (Apache-2.0) is absent or empty." \
          "  Global Constraint 15: the licence ships in the image that" \
          "  redistributes the binary, and this image redistributes Logweir's."
docker run --rm --entrypoint /bin/sh "$ref" -c \
  'test -s /usr/share/licenses/logweir/NOTICE' \
  || fail "check 3 (licence): $ref is missing LOGWEIR'S NOTICE" \
          "  /usr/share/licenses/logweir/NOTICE is absent or empty. It carries the" \
          "  librdkafka/OpenSSL attributions the crate census misses and the ASF" \
          "  trademark sentence (Global Constraints 14 and 15)."
docker run --rm --entrypoint /bin/sh "$ref" -c \
  'test -s /usr/share/licenses/logweir/THIRD_PARTY_NOTICES.md' \
  || fail "check 3 (licence): $ref is missing LOGWEIR'S THIRD-PARTY INVENTORY" \
          "  /usr/share/licenses/logweir/THIRD_PARTY_NOTICES.md is absent or empty." \
          "  Task 29's interface I30: \`weirkeeper\` carries no OSO code but IS" \
          "  statically linked against the same Rust dependency graph the runner" \
          "  is, and MIT, BSD-2-Clause, BSD-3-Clause and Apache-2.0 each require" \
          "  those copyright notices to travel with the binary." \
          "  \`Dockerfile.weirkeeper\` COPYs all three files in one instruction;" \
          "  an image built before Task 29 landed carries only two of them."
docker run --rm --entrypoint /bin/sh "$ref" -c \
  'test ! -e /usr/share/licenses/kafka-backup' \
  || fail "check 3 (licence): $ref CARRIES /usr/share/licenses/kafka-backup/" \
          "  The controller image owes no MIT notice: it carries no kafka-backup" \
          "  binary and no OSO code, so a notice for one is a claim to a" \
          "  redistribution that does not happen. Remove the COPY from" \
          "  Dockerfile.weirkeeper — do not add the licence to match the check."

# ------------------------------------------------------------------ check 4
# THE ORG-ROOT ANCHOR, BYTE FOR BYTE (stage-2 Task 16's T1, Global Constraint
# 7's parity shape).
#
# `docker run --entrypoint cat` and not `docker cp`: `cat` reads the path the
# RUNNING image resolves, through the same rootfs a pod would see, and needs no
# container to be created and removed. The comparison is `diff` against the
# checked-in file, whose exit status is read DIRECTLY — a `[ "$a" = "$b" ]`
# would be equally correct and would print nothing useful when it failed.
#
# WHAT THE VALUE IS: one line, `sha256:` + 64 hex — the SHA-256 of the
# SubjectPublicKeyInfo DER encoding of the org root's PUBLIC key
# (`third_party/org-root.pub.pem`). A public key's hash. No private key
# material is in the image, in the repository, or in this comparison.
echo "-- check 4 (anchor): /etc/logweir/org-root.fingerprint matches third_party/"
if ! baked=$(docker run --rm --entrypoint cat "$ref" /etc/logweir/org-root.fingerprint 2>&1); then
  fail "check 4 (anchor): could not read /etc/logweir/org-root.fingerprint from $ref:" \
       "$baked" \
       "  Dockerfile.weirkeeper must carry" \
       "  \`COPY third_party/org-root.fingerprint /etc/logweir/\`."
fi
# NO PIPE: the image's bytes go to a temp file and `diff`'s exit status is read
# on its own line, so the status that decides this check is the status of the
# command that made the comparison (STANDING RULE 20).
baked_file="$(mktemp -t logweir-org-root.XXXXXX)"
# shellcheck disable=SC2064 # expand $baked_file now, on purpose.
trap "rm -f '$baked_file'" EXIT
printf '%s\n' "$baked" > "$baked_file"
# `|| rc=$?` AND NOT A BARE `rc=$?` ON THE NEXT LINE: this script runs under
# `set -e`, which would abort at the `diff` itself and never reach the
# comparison. The status is still read directly, from `diff`, with no pipe.
rc=0
diff -u third_party/org-root.fingerprint "$baked_file" >/dev/null || rc=$?
if [ "$rc" -ne 0 ]; then
  diff -u third_party/org-root.fingerprint "$baked_file" >&2 || true
  fail "check 4 (anchor): $ref's baked fingerprint is NOT the checked-in one." \
       "  The diff above is third_party/org-root.fingerprint (-) against the" \
       "  image's /etc/logweir/org-root.fingerprint (+). Rebuild the image" \
       "  (\`just image-weirkeeper\`) — never edit the file to match a stale image."
fi

echo "ok: linkage, --version, Logweir's own licences and inventory with no MIT notice, and the org-root anchor are correct in $ref"
