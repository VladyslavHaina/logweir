#!/usr/bin/env bash
# THE UI IMAGE GATE — Task 39, the third of the three image gates.
#
# Two modes, one file, the shape `scripts/check-image-weirkeeper.sh` established:
#
#     check-image-ui.sh <image-ref>              # FIVE checks, one of them by running the image
#     check-image-ui.sh --no-exec <image-ref>    # FIVE checks, executing NOTHING from the image
#
# ---------------------------------------------------------------------------
# WHAT THIS REPLACES, AND WHY IT IS STRONGER THAN WHAT IT REPLACES
# ---------------------------------------------------------------------------
# Until Task 39 the page reached the cluster as a ConfigMap built from
# `charts/logweir/ui/` — a byte-identical copy of `ui/` kept in the chart — and
# `scripts/check-chart.sh`'s arm 2 `cmp`'d the two directories. That arm is
# DELETED and this file is what replaced it. The difference is not stylistic:
#
#   arm 2 asserted that TWO DIRECTORIES IN THIS REPOSITORY held the same bytes.
#   Check 1 below asserts that THE BYTES AN OPERATOR'S BROWSER RECEIVES are the
#   bytes in `ui/` — it computes the sha256 of every file inside the image that
#   `kubectl proxy --www=/ui` will serve and of every file under `ui/`, and
#   compares the two sets.
#
# A copy in a chart can be right while the artefact is wrong. The artefact is
# what this gate reads.
#
# ---------------------------------------------------------------------------
# WHAT IT PROVES, IN THE ORDER THE CHECKS RUN
# ---------------------------------------------------------------------------
#   1. THE SERVED FILES. `/ui` inside the image holds exactly the fourteen
#      shipped files (`ui/*.html`, `ui/*.js`, `ui/*.css`, `ui/pages/*`), each
#      one's sha256 EQUAL to the tree's, and NOTHING ELSE — no `README.md`, no
#      `tests/` (which carries a throwaway keypair and fixtures naming a
#      developer's compose stack), no `.pem` of any kind, no symlink, no
#      directory the tree does not have. Global Constraint 28.
#   2. THE LICENCES. `/usr/share/licenses/logweir/LICENSE` and `NOTICE`
#      non-empty (Global Constraint 15, spec §16 clause 5 — the page is
#      Logweir's own Apache-2.0 code); `/usr/share/licenses/kubectl/INVENTORY`
#      and `LICENSE` byte-identical to `third_party/kubectl-image.inventory`
#      and `LICENSE` in this tree, because this image REDISTRIBUTES the whole
#      kubectl base. And two ABSENCES, each of which would be a claim to a
#      redistribution this image does not make:
#      `/usr/share/licenses/kafka-backup/` (no OSO code is here) and
#      `/usr/share/licenses/logweir/THIRD_PARTY_NOTICES.md` (that is the RUST
#      graph's inventory; this image links no Rust binary).
#   3. THE BASE AND THE ENTRYPOINT. `Dockerfile.ui`'s `FROM`, the image's own
#      `org.opencontainers.image.base.name` label and the digest the inventory
#      INSIDE the image names are ONE string; and the entrypoint is
#      `/bin/kubectl` with no `CMD`, because every `kubectl proxy` argument —
#      `--accept-paths` above all — belongs to the chart, where three tests in
#      `crates/logweir/tests/chart_lint.rs` can see it.
#   4. `kubectl version --client` exits 0 (execution mode), or the kubectl
#      binary is an AArch64 ELF (`--no-exec`).
#
# WHAT CHECK 3 DOES NOT DO, STATED RATHER THAN GLOSSED: it does not re-download
# the base image and compare layer digests. It does not need to. A `FROM
# <ref>@sha256:<digest>` cannot resolve to other bytes — that is what a digest
# reference is — so the only drift possible is between the `FROM` line, the
# label and the checked-in inventory, and that three-way agreement is exactly
# what the check asserts. A layer comparison would additionally require the base
# in the daemon of every caller, including the pull-back job, and would fail
# there for a reason that has nothing to do with the image.
#
# ---------------------------------------------------------------------------
# `--no-exec`: THE SAME IMAGE, ASSERTED WITHOUT RUNNING ANYTHING FROM IT
# ---------------------------------------------------------------------------
# `release.yml` publishes this image as a MANIFEST LIST (linux/amd64 and
# linux/arm64 — the base is a list carrying both). The pushing job and the
# pull-back job both run on `ubuntu-24.04`: they can execute the amd64 variant
# and cannot execute the arm64 one, and STANDING RULE 10 forbids emulating it.
# So the arm64 leg is asserted HERE, with no `docker run` anywhere in the mode,
# exactly as `check-image-weirkeeper.sh --no-exec` asserts its own.
#
# ITS ARCHITECTURE CHECK RUNS FIRST, AND FOR THE SAME REASON THAT SCRIPT GIVES:
# it is the check that gives the right answer when the reference is the wrong
# image altogether. The acceptance is a NEGATIVE CONTROL, `--no-exec` against
# the amd64 variant, which must be rejected NAMING THE ARCHITECTURE rather than
# passing (every other check is architecture-independent and would pass).
#
# HOW IT READS THE FILES, IN BOTH MODES: `docker create` then `docker cp` — the
# `scripts/extract-engine.sh` idiom — and every assertion then runs on the HOST.
# `docker create` starts no process. This is not only the `--no-exec` mode's
# technique: the base is DISTROLESS and has no shell at all (measured
# 2026-09-14: `docker run --entrypoint /bin/sh` fails with `stat /bin/sh: no
# such file or directory`), so the `docker run --entrypoint /bin/sh -c 'test
# -s …'` idiom both sibling gates use is simply not available against this
# image. The one thing the execution mode runs is the image AS SHIPPED:
# `kubectl version --client`.
#
# `shasum -a 256` AND NOT `sha256sum`: this gate runs on this laptop (BSD
# userland, no `sha256sum`) and on an `ubuntu-24.04` runner (which has both).
# `scripts/helm-demo.sh` made the same choice for the same reason and has run
# green on a runner since 2026-09-12.
#
# TWO SIBLING SCRIPTS ARE NAMED IN FAILURE MESSAGES WITHOUT THEIR `scripts/`
# PREFIX, and that is not sloppiness. `crates/logweir/tests/gate_lint.rs`
# partitions every `scripts/check-*.sh` into "reached by `just gate`" and
# "reached by a stack/cluster recipe" and requires the two sets to be DISJOINT;
# it finds what a recipe reaches by reading the executed lines of the scripts
# that recipe runs, and a `scripts/check-chart.sh` inside a `fail` string here
# is an executed line. Spelling the two as bare file names keeps the message
# useful and keeps `just smoke-ui` from appearing to invoke half the gate.
#
# EVERY EXIT CODE IS READ ON ITS OWN LINE, NEVER THROUGH A PIPE (STANDING
# RULE 20). EXIT CODES ARE 0 AND 1 ONLY, exactly as both siblings: Global
# Constraint 11's 0/1/2/3/4 contract governs the `logweir` BINARY, not shell
# scripts. A missing prerequisite is a FAILURE WITH A NAMED REASON, never a
# green "skipped" run.
#
# WHAT IT PROVES ABOUT A CLUSTER: nothing. No API server, no kubeconfig, no
# token. It builds nothing and pushes nothing (Global Constraint 17):
# `just image-ui` is the named producer of `logweir-ui:check`.
set -euo pipefail

# The REAL repository, with no `LOGWEIR_ROOT` override: checks 1, 2 and 3
# compare the image against CHECKED-IN files, and a gate that let its expected
# values be pointed somewhere else would compare nothing. Same form as both
# siblings.
cd "$(dirname "$0")/.."

# Each argument on its own line, so multi-line evidence stays readable.
fail() {
  printf '%s\n' "$@" >&2
  exit 1
}

# EXACTLY ONE IMAGE REFERENCE, after at most the one flag this script has.
# Zero must not silently default to `logweir-ui:check` and a second must not be
# ignored: both are how a caller that meant to assert a pushed digest ends up
# asserting something else (T0-17).
no_exec=0
if [ "${1:-}" = "--no-exec" ]; then
  no_exec=1
  shift
fi
if [ "$#" -ne 1 ]; then
  fail "usage: check-image-ui.sh [--no-exec] <image-ref>" \
       "  <image-ref> is a local tag (logweir-ui:check) or a digest reference" \
       "  (docker.io/vladyslavhaina/logweir-ui@sha256:...). Exactly one, never zero and" \
       "  never two. Build the local tag with \`just image-ui\`." \
       "  --no-exec asserts a variant this host cannot run: it adds the AArch64" \
       "  architecture assertion and runs nothing from the image."
fi
case "$1" in
  -*) fail "check-image-ui: unknown option \`$1\`." \
           "  The only option is \`--no-exec\`, and it comes FIRST:" \
           "    check-image-ui.sh --no-exec <image-ref>" \
           "  An option this script does not understand is a usage error, never" \
           "  an argument it quietly treats as an image reference." ;;
esac
ref="$1"

if ! command -v docker >/dev/null 2>&1; then
  fail "check-image-ui: REFUSING to run — \`docker\` is not on PATH." \
       "  This gate inspects a built image; it does not skip checks."
fi
if ! command -v shasum >/dev/null 2>&1; then
  fail "check-image-ui: REFUSING to run — \`shasum\` is not on PATH." \
       "  Check 1 compares the sha256 of every served file against the tree's;" \
       "  without it this gate would assert the file NAMES and nothing about" \
       "  their contents, which is the whole point of the check."
fi

# THE EXPECTED VALUES MUST EXIST ON DISK BEFORE THE IMAGE IS ASKED FOR ANYTHING.
# Without them checks 1-3 would compare the image against empty strings and pass.
if [ ! -d ui ]; then
  fail "check-image-ui: there is no ui/ directory in $(pwd)." \
       "  Check 1 compares the image's /ui against the tree's ui/; without it" \
       "  this gate would be comparing the image against nothing."
fi
if [ ! -s third_party/kubectl-image.inventory ]; then
  fail "check-image-ui: third_party/kubectl-image.inventory is missing or empty." \
       "  It is the checked-in half of check 2's byte-identity comparison and the" \
       "  file that records what this image redistributes."
fi
if [ ! -s LICENSE ] || [ ! -s NOTICE ]; then
  fail "check-image-ui: LICENSE or NOTICE is missing or empty in $(pwd)."
fi
if [ ! -s Dockerfile.ui ]; then
  fail "check-image-ui: Dockerfile.ui is missing or empty." \
       "  Check 3 reads the base digest out of its FROM line."
fi

# BEFORE EITHER MODE. If the reference is absent from the daemon, every command
# below fails for that one reason and whichever check ran first takes the blame
# for it — both siblings' practice.
if ! docker image inspect "$ref" >/dev/null 2>&1; then
  fail "check-image-ui: \`$ref\` is not in the local daemon." \
       "  This gate asserts an image that is already built; it never builds or" \
       "  pulls one. Run \`just image-ui\` (which builds \`logweir-ui:check\`)," \
       "  or pass a reference the daemon holds."
fi

# ---------------------------------------------------------------------------
# THE CONTAINER THE FILES ARE READ OUT OF. `docker create` allocates a container
# and STARTS NO PROCESS, which is why both modes can use it and why `--no-exec`
# can read an image this host could never run.
# ---------------------------------------------------------------------------
work="$(mktemp -d "${TMPDIR:-/tmp}/logweir-ui-check.XXXXXX")"
cid=""
# A FUNCTION AND NOT AN INLINE TRAP STRING: every command in it is `|| true`d,
# so a cleanup failure cannot change the exit status this gate reports.
cleanup() {
  if [ -n "$cid" ]; then
    docker rm -f "$cid" >/dev/null 2>&1 || true
  fi
  rm -rf "$work" || true
}
trap cleanup EXIT

# THE PLATFORM THE IMAGE ITSELF DECLARES, read from the daemon's metadata — no
# container, no process. `docker create` on a daemon whose default platform
# differs from the image's is where a variant gets silently swapped for another,
# so the flag below is always given and is always the image's own.
if ! declared=$(docker image inspect "$ref" --format '{{.Os}}/{{.Architecture}}' 2>&1); then
  fail "check-image-ui: could not read the platform of $ref:" "$declared"
fi

if [ "$no_exec" -eq 1 ]; then
  echo "== check-image-ui --no-exec: $ref (declared $declared) =="
else
  echo "== check-image-ui: $ref (declared $declared) =="
fi

if ! created=$(docker create --platform "$declared" "$ref" 2>&1); then
  fail "check-image-ui: could not create a container from $ref:" "$created" \
       "  \`docker create\` allocates a container and starts NO process, which is" \
       "  why this gate can read an image it cannot run."
fi
cid="$created"

# `cp_out <path-in-image> <dest-on-host>` — the exit status of `docker cp` read
# on its own line, with no pipe, and RETURNED so each caller decides whether an
# absence is a failure or the point.
cp_out() {
  rc=0
  docker cp "$cid:$1" "$2" >/dev/null 2>&1 || rc=$?
  return "$rc"
}

# =========================================================================
# `--no-exec` CHECK 0 (architecture) — FIRST, so a wrong image is rejected for
# the right reason. See the header: the negative control is this mode against
# the amd64 variant, and it must fail HERE.
# =========================================================================
if [ "$no_exec" -eq 1 ]; then
  WANT_ARCH="arm64"
  WANT_MACHINE="b7 00"
  echo "-- check 0 (architecture): is $ref a linux/$WANT_ARCH image carrying an AArch64 kubectl?"
  if [ "$declared" != "linux/$WANT_ARCH" ]; then
    fail "check 0 (architecture): $ref is a \`$declared\` image, NOT \`linux/$WANT_ARCH\`." \
         "  --no-exec asserts the AArch64 variant of the manifest list: a 64-bit" \
         "  little-endian ELF whose e_machine (header bytes 18-19) is \`$WANT_MACHINE\`" \
         "  (0x00b7, EM_AARCH64). An x86-64 image carries \`3e 00\` there." \
         "  Every OTHER check in this file is architecture-independent and would" \
         "  have passed, which is why this one runs first." \
         "  Pass the arm64 variant, or use the execution mode (no flag) on a host" \
         "  of the image's own architecture."
  fi
  # THE BYTES, ON THE HOST. `od` here is the HOST's od: this mode executes
  # nothing from the image, and the base has no shell to execute anything with.
  if ! cp_out /bin/kubectl "$work/kubectl"; then
    fail "check 0 (architecture): $ref carries no /bin/kubectl." \
         "  The base image (registry.k8s.io/kubectl) puts the binary there and sets" \
         "  it as the ENTRYPOINT; an image without it is not this image."
  fi
  elf_out=$(od -An -tx1 -N20 "$work/kubectl")
  elf_bytes="$(printf '%s' "$elf_out" | tr -s '[:space:]' ' ' | sed -e 's/^ //' -e 's/ $//')"
  elf_magic="$(printf '%s' "$elf_bytes" | cut -d' ' -f1-6)"
  elf_machine="$(printf '%s' "$elf_bytes" | cut -d' ' -f19-20)"
  if [ "$elf_magic" != "7f 45 4c 46 02 01" ]; then
    fail "check 0 (architecture): /bin/kubectl in $ref is not a 64-bit LITTLE-ENDIAN ELF." \
         "  expected the first six bytes to be \`7f 45 4c 46 02 01\`" \
         "  (\\x7fELF, ELFCLASS64, ELFDATA2LSB)" \
         "  header read: $elf_bytes"
  fi
  if [ "$elf_machine" != "$WANT_MACHINE" ]; then
    fail "check 0 (architecture): /bin/kubectl in $ref is NOT an AArch64 binary." \
         "  e_machine at offset 18 is \`$elf_machine\`, expected \`$WANT_MACHINE\`" \
         "  (0x00b7, EM_AARCH64). \`3e 00\` there is x86-64 (0x003e, EM_X86_64)." \
         "  header read: $elf_bytes"
  fi
fi

# =========================================================================
# CHECK 1 — THE SERVED FILES. The point of the whole image.
# =========================================================================
echo "-- check 1 (the page): /ui is the fourteen shipped files, sha256 for sha256, and nothing else"
if ! cp_out /ui "$work/ui"; then
  fail "check 1 (the page): $ref carries no /ui directory." \
       "  \`Dockerfile.ui\` COPYs the fourteen shipped files there and the chart's" \
       "  Deployment runs \`kubectl proxy --www=/ui\`; an image without it serves" \
       "  nothing."
fi

# ANYTHING THAT IS NOT A DIRECTORY AND NOT A REGULAR FILE IS A RED, BY ITSELF.
# A symlink under /ui would be followed by the proxy and would escape the
# comparison below, which walks regular files only.
strange="$(find "$work/ui" ! -type d ! -type f)"
if [ -n "$strange" ]; then
  fail "check 1 (the page): $ref's /ui holds entries that are neither directories" \
       "  nor regular files:" "$strange" \
       "  A symlink under /ui is followed by \`kubectl proxy --www\` and would serve" \
       "  bytes this gate never hashed."
fi

# THE TWO SETS, each as `<relative path>  <sha256>`, sorted. The tree's side is
# `scripts/check-ui-offline.sh`'s scope by construction: everything under `ui/`
# except `*.md` and `tests/`.
tree_list="$work/tree.sha256"
image_list="$work/image.sha256"
: > "$tree_list"
: > "$image_list"

tree_count=0
while IFS= read -r f; do
  sum="$(shasum -a 256 "$f" | cut -d' ' -f1)"
  printf '%s  %s\n' "${f#ui/}" "$sum" >> "$tree_list"
  tree_count=$((tree_count + 1))
done < <(find ui -type f ! -name '*.md' ! -path 'ui/tests/*' | LC_ALL=C sort)

image_count=0
while IFS= read -r f; do
  sum="$(shasum -a 256 "$f" | cut -d' ' -f1)"
  printf '%s  %s\n' "${f#"$work"/ui/}" "$sum" >> "$image_list"
  image_count=$((image_count + 1))
done < <(find "$work/ui" -type f | LC_ALL=C sort)

# FOURTEEN, NAMED. A gate whose expected count came from the tree alone would
# stay green if someone deleted seven files from both sides at once.
if [ "$tree_count" -ne 14 ]; then
  fail "check 1 (the page): the tree holds $tree_count shipped UI file(s), not fourteen." \
       "  The shipped page is ui/*.html, ui/*.js, ui/*.css and ui/pages/* — the same" \
       "  set \`check-ui-offline.sh\` scans and shipped_ui_files() asserts in" \
       "  crates/logweir/tests/chart_lint.rs. Fix the tree, never this number."
fi
if [ "$image_count" -ne 14 ]; then
  fail "check 1 (the page): $ref's /ui holds $image_count file(s), not fourteen." \
       "  It must hold the fourteen shipped files and NOTHING else: no README.md," \
       "  no tests/ (a throwaway keypair and fixtures naming a developer's compose" \
       "  stack), no key material of any kind (Global Constraint 28)." \
       "  What is in the image:" \
       "$(cut -d' ' -f1 "$image_list")"
fi

# LC_ALL=C on BOTH sides, so the comparison cannot depend on a locale.
rc=0
diff -u "$tree_list" "$image_list" > "$work/ui.diff" 2>&1 || rc=$?
if [ "$rc" -ne 0 ]; then
  cat "$work/ui.diff" >&2
  fail "check 1 (the page): the files $ref serves are NOT the tree's ui/." \
       "  The diff above is the tree's \`<path>  <sha256>\` list (-) against the" \
       "  image's (+). This is the assertion that replaced the chart's byte-copy" \
       "  arm (the chart gate's old arm 2): it compares the bytes a" \
       "  browser receives with the bytes in this repository." \
       "  Rebuild the image (\`just image-ui\`) — never edit ui/ to match a stale one."
fi
echo "   ok: fourteen files, sha256 for sha256, and nothing else under /ui"

# =========================================================================
# CHECK 2 — THE LICENCES: two present, two byte-identical, two ABSENT.
# =========================================================================
echo "-- check 2 (licence): Logweir's LICENSE and NOTICE, kubectl's inventory and licence, and two absences"
for f in LICENSE NOTICE; do
  if ! cp_out "/usr/share/licenses/logweir/$f" "$work/logweir-$f"; then
    fail "check 2 (licence): $ref is missing /usr/share/licenses/logweir/$f." \
         "  The fourteen files it serves are Logweir's own Apache-2.0 code, and" \
         "  Apache-2.0 requires the licence and the NOTICE to travel with the" \
         "  redistribution (Global Constraint 15, spec §16 clause 5)."
  fi
  if [ ! -s "$work/logweir-$f" ]; then
    fail "check 2 (licence): /usr/share/licenses/logweir/$f in $ref is EMPTY." \
         "  A zero-byte licence file satisfies a COPY and satisfies nobody else."
  fi
done

# KUBECTL'S TWO FILES, BYTE FOR BYTE AGAINST THE TREE. This image redistributes
# the whole kubectl base, so these are not decoration; and comparing rather than
# merely testing for non-emptiness is what catches an image built before the
# inventory was updated.
if ! cp_out /usr/share/licenses/kubectl/INVENTORY "$work/kubectl-INVENTORY"; then
  fail "check 2 (licence): $ref is missing /usr/share/licenses/kubectl/INVENTORY." \
       "  \`Dockerfile.ui\` COPYs third_party/kubectl-image.inventory there. This" \
       "  image redistributes the kubectl base in full; the inventory is where" \
       "  that is written down."
fi
rc=0
diff -u third_party/kubectl-image.inventory "$work/kubectl-INVENTORY" > "$work/inv.diff" 2>&1 || rc=$?
if [ "$rc" -ne 0 ]; then
  cat "$work/inv.diff" >&2
  fail "check 2 (licence): $ref's baked inventory is NOT the checked-in one." \
       "  The diff above is third_party/kubectl-image.inventory (-) against the" \
       "  image's /usr/share/licenses/kubectl/INVENTORY (+). Rebuild the image —" \
       "  never edit the file to match a stale image."
fi
if ! cp_out /usr/share/licenses/kubectl/LICENSE "$work/kubectl-LICENSE"; then
  fail "check 2 (licence): $ref is missing /usr/share/licenses/kubectl/LICENSE." \
       "  kubectl is Apache-2.0 and §4(a) requires a copy of the licence to travel" \
       "  with the redistribution. \`Dockerfile.ui\` COPYs this tree's LICENSE there" \
       "  (the unmodified Apache-2.0 text)."
fi
rc=0
diff -u LICENSE "$work/kubectl-LICENSE" > "$work/lic.diff" 2>&1 || rc=$?
if [ "$rc" -ne 0 ]; then
  cat "$work/lic.diff" >&2
  fail "check 2 (licence): $ref's /usr/share/licenses/kubectl/LICENSE is not this" \
       "  tree's LICENSE, byte for byte. The diff above is LICENSE (-) against the" \
       "  image's copy (+)."
fi

# THE TWO ABSENCES. Absence is the pass, so the FAILING exit of `docker cp` is
# the good case and its status is read on its own line inside `cp_out`.
if cp_out /usr/share/licenses/kafka-backup "$work/kafka-backup"; then
  fail "check 2 (licence): $ref CARRIES /usr/share/licenses/kafka-backup/." \
       "  The UI image owes no MIT notice: it carries no kafka-backup binary and no" \
       "  OSO code, so a notice for one is a claim to a redistribution that does not" \
       "  happen. Remove the COPY from Dockerfile.ui — do not add the licence to" \
       "  match the check."
fi
if cp_out /usr/share/licenses/logweir/THIRD_PARTY_NOTICES.md "$work/tpn"; then
  fail "check 2 (licence): $ref CARRIES /usr/share/licenses/logweir/THIRD_PARTY_NOTICES.md." \
       "  That file is the inventory of Logweir's RUST dependency graph, generated" \
       "  from Cargo.lock. This image links no Rust binary and redistributes no" \
       "  crate — it is fourteen static files over a kubectl — so shipping it would" \
       "  claim a redistribution that does not happen. The other two images carry it" \
       "  because they each ship a statically linked Rust binary; this one does not."
fi
echo "   ok: two licences present, kubectl's inventory and licence byte-identical, two absences held"

# =========================================================================
# CHECK 3 — THE BASE AND THE ENTRYPOINT.
# =========================================================================
echo "-- check 3 (base): the FROM, the image's own label and the baked inventory name ONE digest"
# THE TREE'S PIN, read out of Dockerfile.ui's FROM line. Derived, never spelt
# here: a base bump must propagate without editing this gate.
from_ref="$(sed -n 's/^FROM[[:space:]]\{1,\}\([^[:space:]]*\).*$/\1/p' Dockerfile.ui | head -1)"
case "$from_ref" in
  *@sha256:????????????????????????????????????????????????????????????????) : ;;
  *) fail "check 3 (base): Dockerfile.ui's FROM is \`$from_ref\`, which is not a digest" \
          "  reference. Global Constraint 7 pins every third-party image by digest," \
          "  and a tag here would let the base change under a build nobody re-ran." ;;
esac
if ! label=$(docker image inspect "$ref" \
               --format '{{index .Config.Labels "org.opencontainers.image.base.name"}}' 2>&1); then
  fail "check 3 (base): could not read $ref's labels:" "$label"
fi
if [ "$label" != "$from_ref" ]; then
  fail "check 3 (base): $ref declares its base as" \
       "    $label" \
       "  and Dockerfile.ui's FROM names" \
       "    $from_ref" \
       "  These are one string. The label is what an operator who holds the image" \
       "  and not this repository reads with \`docker image inspect\`; a stale one" \
       "  is a provenance claim that is simply false."
fi
if ! grep -q -- "$from_ref" "$work/kubectl-INVENTORY"; then
  fail "check 3 (base): the inventory inside $ref does not name the base digest" \
       "    $from_ref" \
       "  third_party/kubectl-image.inventory must name the reference Dockerfile.ui" \
       "  builds FROM: it is the file that tells a recipient what they were handed."
fi

echo "-- check 3 (entrypoint): /bin/kubectl, and no CMD — every proxy argument is the chart's"
if ! entrypoint=$(docker image inspect "$ref" --format '{{json .Config.Entrypoint}}' 2>&1); then
  fail "check 3 (entrypoint): could not read $ref's entrypoint:" "$entrypoint"
fi
if [ "$entrypoint" != '["/bin/kubectl"]' ]; then
  fail "check 3 (entrypoint): $ref's entrypoint is \`$entrypoint\`, not \`[\"/bin/kubectl\"]\`." \
       "  The image runs kubectl and takes its arguments from the chart."
fi
if ! cmd=$(docker image inspect "$ref" --format '{{json .Config.Cmd}}' 2>&1); then
  fail "check 3 (entrypoint): could not read $ref's CMD:" "$cmd"
fi
if [ "$cmd" != "null" ] && [ "$cmd" != "[]" ]; then
  fail "check 3 (entrypoint): $ref declares CMD $cmd." \
       "  It must declare none. \`kubectl proxy\`'s flags — and" \
       "  --accept-paths='^/(ui/|apis/logweir\\.dev/v1alpha1/)' above all — are a" \
       "  SECURITY property the chart states and three tests in" \
       "  crates/logweir/tests/chart_lint.rs assert against the rendered" \
       "  Deployment. A default argument vector baked in here would be a second" \
       "  place that boundary could come from, in a layer those tests cannot see."
fi
echo "   ok: base digest agrees three ways; entrypoint is kubectl with no CMD"

# =========================================================================
# CHECK 4 — the one thing the execution mode runs, or the mode's own note.
# =========================================================================
if [ "$no_exec" -eq 1 ]; then
  echo
  echo "ok: --no-exec — an AArch64 image, the fourteen shipped files byte for byte, the licences"
  echo "    and the base and entrypoint are correct in $ref. NOTHING was executed from the image,"
  echo "    so \`kubectl version --client\` is NOT asserted here; a native runner asserts that."
  exit 0
fi

# THE IMAGE AS SHIPPED: no `--entrypoint` override, because the entrypoint IS
# what this check exercises. `version --client` contacts no API server, reads no
# kubeconfig and needs no cluster — an image check must touch none of those.
echo "-- check 4 (kubectl): \`kubectl version --client\` runs inside the image"
docker run --rm "$ref" version --client \
  || fail "check 4 (kubectl): \`kubectl version --client\` failed inside $ref" \
          "  The image's ENTRYPOINT is /bin/kubectl and \`version --client\` must exit" \
          "  0 without contacting an API server. A failure here means the base layer" \
          "  is broken or the image was built for another architecture — use" \
          "  \`--no-exec\` for a variant this host cannot run."

echo
echo "ok: the fourteen shipped files byte for byte at /ui and nothing else, Logweir's licences,"
echo "    kubectl's inventory and licence, no MIT notice and no Rust inventory, the base digest"
echo "    agreeing three ways, the entrypoint kubectl with no CMD, and kubectl runs — in $ref"
