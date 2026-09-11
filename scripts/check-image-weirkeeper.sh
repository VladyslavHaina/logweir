#!/usr/bin/env bash
# THE CONTROLLER IMAGE GATE — interface I25, Task 23. FOUR checks.
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

# EXACTLY ONE ARGUMENT. Zero must not silently default to `weirkeeper:check`
# and a second must not be ignored: both are how a caller that meant to assert
# a pushed digest ends up asserting something else (T0-17).
if [ "$#" -ne 1 ]; then
  fail "usage: check-image-weirkeeper.sh <image-ref>" \
       "  <image-ref> is a local tag (weirkeeper:check) or a digest reference" \
       "  (ghcr.io/logweir/weirkeeper@sha256:...). Exactly one, never zero and" \
       "  never two. Build the local tag with \`just image-weirkeeper\`."
fi
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

# NO `--platform` FLAG ANYWHERE IN THIS SCRIPT, and that is deliberate.
# `check-image.sh` pins `linux/amd64` because the runner image is amd64-only
# (the engine binary is). This image is built for whatever
# `LOGWEIR_IMAGE_PLATFORM` said — `linux/arm64` by default on this host — so
# naming a platform here would either be wrong on the developer machine or
# wrong on a CI runner. `docker run` with no `--platform` runs the image's own.

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
echo "-- check 3 (licence): logweir's own LICENSE and NOTICE, and no MIT notice"
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

echo "ok: linkage, --version, Logweir's own licences with no MIT notice, and the org-root anchor are correct in $ref"
