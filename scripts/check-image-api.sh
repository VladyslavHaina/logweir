#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# `check-image-api.sh` — THE CONSOLE IMAGE, ASSERTED. D0 stage 7.
# ---------------------------------------------------------------------------
# WHAT IT ASSERTS ABOUT `logweir-console` (built by `Dockerfile.console`):
#
#   0. `--no-exec` only: the image is the AArch64 variant and its binary is an
#      AArch64 ELF. Every other check here is architecture-independent, so this
#      one runs first or a wrong-variant image passes for the wrong reason.
#   1. /ui is the twenty-six shipped files, sha256 for sha256, and NOTHING else
#      — the same assertion `check-image-ui.sh` makes about the other image
#      that carries the page, against the same one directory in this tree.
#   2. The three licences are present, non-empty and at the path both compiling
#      images use; `THIRD_PARTY_NOTICES.md` is REQUIRED here (this image ships a
#      statically linked Rust binary) where `check-image-ui.sh` requires its
#      ABSENCE (that image ships none).
#   3. The absences: no kafka-backup MIT notice, no org-root anchor, no engine.
#   4. The entrypoint is `/usr/local/bin/logweir-api` and there is no CMD: the
#      console's MODE comes from the chart's ConfigMap and from nowhere else.
#   5. NO KEY MATERIAL ANYWHERE (Global Constraint 28) and no kubeconfig.
#   6. The binary is present, on `$PATH`, runs, and NAMES ITSELF — the shape of
#      `scripts/check-image.sh` check 7, for the same reason it exists there.
#
# WHY CHECK 6 IS THE SHAPE IT IS, WHICH IS A DEFECT'S SHAPE.
# `check-image.sh` check 7 was written after RET-NOIMAGE: the controller ran
# retention Jobs with `command: ["logweir-retention"]` out of an image that
# carried no such binary, and every image gate stayed green because none of them
# looked. Review finding L1 then showed that an exit-code-only check was not
# enough either — a `COPY` whose SOURCE was the wrong binary produced an image
# that exited 0 while printing the other binary's name. So this check runs the
# entrypoint BY BARE NAME through `$PATH`, exactly as the kubelet resolves a
# container's command, and INSPECTS the version string rather than the status:
#
#   * `logweir-api --version` must print a line beginning `logweir-api `;
#   * the version must equal `crates/logweir-api/Cargo.toml`'s resolved version,
#     read from the workspace, so a stale layer carrying last release's binary
#     is a red rather than a green with an old number.
#
# WHAT IT PROVES ABOUT A CLUSTER: nothing. No API server, no kubeconfig, no
# token. It builds nothing and pushes nothing (Global Constraint 17):
# `just image-console` is the named producer of `logweir-console:check`.
#
# EVERY EXIT CODE IS READ ON ITS OWN LINE, NEVER THROUGH A PIPE (STANDING
# RULE 20). EXIT CODES ARE 0 AND 1 ONLY, exactly as all three siblings. A
# missing prerequisite is a FAILURE WITH A NAMED REASON, never a green
# "skipped" run.
set -euo pipefail

# The REAL repository, with no `LOGWEIR_ROOT` override: checks 1 and 2 compare
# the image against CHECKED-IN files, and a gate that let its expected values be
# pointed somewhere else would compare nothing. Same form as all three siblings.
cd "$(dirname "$0")/.."

# Each argument on its own line, so multi-line evidence stays readable.
fail() {
  printf '%s\n' "$@" >&2
  exit 1
}

# EXACTLY ONE IMAGE REFERENCE, after at most the one flag this script has.
no_exec=0
if [ "${1:-}" = "--no-exec" ]; then
  no_exec=1
  shift
fi
if [ "$#" -ne 1 ]; then
  fail "usage: check-image-api.sh [--no-exec] <image-ref>" \
       "  <image-ref> is a local tag (logweir-console:check) or a digest reference" \
       "  (docker.io/vladyslavhaina/logweir-console@sha256:...). Exactly one, never" \
       "  zero and never two. Build the local tag with \`just image-console\`." \
       "  --no-exec asserts a variant this host cannot run: it adds the AArch64" \
       "  architecture assertion and runs nothing from the image."
fi
case "$1" in
  -*) fail "check-image-api: unknown option \`$1\`." \
           "  The only option is \`--no-exec\`, and it comes FIRST:" \
           "    check-image-api.sh --no-exec <image-ref>" \
           "  An option this script does not understand is a usage error, never" \
           "  an argument it quietly treats as an image reference." ;;
esac
ref="$1"

if ! command -v docker >/dev/null 2>&1; then
  fail "check-image-api: REFUSING to run — \`docker\` is not on PATH." \
       "  This gate inspects a built image; it does not skip checks."
fi
if ! command -v shasum >/dev/null 2>&1; then
  fail "check-image-api: REFUSING to run — \`shasum\` is not on PATH." \
       "  Check 1 compares the sha256 of every served file against the tree's;" \
       "  without it this gate would assert the file NAMES and nothing about" \
       "  their contents, which is the whole point of the check."
fi

# THE EXPECTED VALUES MUST EXIST ON DISK BEFORE THE IMAGE IS ASKED FOR ANYTHING.
if [ ! -d ui ]; then
  fail "check-image-api: there is no ui/ directory in $(pwd)." \
       "  Check 1 compares the image's /ui against the tree's ui/; without it" \
       "  this gate would be comparing the image against nothing."
fi
if [ ! -s LICENSE ] || [ ! -s NOTICE ] || [ ! -s THIRD_PARTY_NOTICES.md ]; then
  fail "check-image-api: LICENSE, NOTICE or THIRD_PARTY_NOTICES.md is missing or" \
       "  empty in $(pwd). All three travel with this image's Rust binary."
fi
if [ ! -s Dockerfile.console ]; then
  fail "check-image-api: Dockerfile.console is missing or empty." \
       "  Check 4 reads the entrypoint it declares."
fi
if [ ! -s crates/logweir-api/Cargo.toml ]; then
  fail "check-image-api: crates/logweir-api/Cargo.toml is missing." \
       "  Check 6 compares the binary's version against the workspace's."
fi

# THE VERSION THE WORKSPACE RESOLVES FOR THIS PACKAGE. `logweir-api` takes
# `version.workspace = true`, so the number lives in the ROOT Cargo.toml and is
# read from there — derived, never spelt in this gate, so a release bump
# propagates without editing it.
want_version="$(sed -n '/^\[workspace\.package\]/,/^\[/p' Cargo.toml \
                  | sed -n 's/^version[[:space:]]*=[[:space:]]*"\([^"]*\)".*$/\1/p' | head -1)"
if [ -z "$want_version" ]; then
  fail "check-image-api: could not read [workspace.package] version from Cargo.toml." \
       "  Check 6 holds the binary's \`--version\` to it; without the expected value" \
       "  that check would compare against an empty string and pass on anything."
fi

# BEFORE EITHER MODE. If the reference is absent from the daemon, every command
# below fails for that one reason and whichever check ran first takes the blame.
if ! docker image inspect "$ref" >/dev/null 2>&1; then
  fail "check-image-api: \`$ref\` is not in the local daemon." \
       "  This gate asserts an image that is already built; it never builds or" \
       "  pulls one. Run \`just image-console\` (which builds" \
       "  \`logweir-console:check\`), or pass a reference the daemon holds."
fi

# ---------------------------------------------------------------------------
# THE CONTAINER THE FILES ARE READ OUT OF. `docker create` allocates a container
# and STARTS NO PROCESS, which is why both modes can use it and why `--no-exec`
# can read an image this host could never run.
# ---------------------------------------------------------------------------
work="$(mktemp -d "${TMPDIR:-/tmp}/logweir-api-check.XXXXXX")"
cid=""
cleanup() {
  if [ -n "$cid" ]; then
    docker rm -f "$cid" >/dev/null 2>&1 || true
  fi
  rm -rf "$work" || true
}
trap cleanup EXIT

if ! declared=$(docker image inspect "$ref" --format '{{.Os}}/{{.Architecture}}' 2>&1); then
  fail "check-image-api: could not read the platform of $ref:" "$declared"
fi

if [ "$no_exec" -eq 1 ]; then
  echo "== check-image-api --no-exec: $ref (declared $declared) =="
else
  echo "== check-image-api: $ref (declared $declared) =="
fi

if ! created=$(docker create --platform "$declared" "$ref" 2>&1); then
  fail "check-image-api: could not create a container from $ref:" "$created" \
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
# the right reason.
# =========================================================================
if [ "$no_exec" -eq 1 ]; then
  WANT_ARCH="arm64"
  WANT_MACHINE="b7 00"
  echo "-- check 0 (architecture): is $ref a linux/$WANT_ARCH image carrying an AArch64 binary?"
  if [ "$declared" != "linux/$WANT_ARCH" ]; then
    fail "check 0 (architecture): $ref is a \`$declared\` image, NOT \`linux/$WANT_ARCH\`." \
         "  --no-exec asserts the AArch64 variant: a 64-bit little-endian ELF whose" \
         "  e_machine (header bytes 18-19) is \`$WANT_MACHINE\` (0x00b7, EM_AARCH64)." \
         "  An x86-64 image carries \`3e 00\` there. Every OTHER check in this file" \
         "  is architecture-independent and would have passed, which is why this" \
         "  one runs first."
  fi
  if ! cp_out /usr/local/bin/logweir-api "$work/logweir-api"; then
    fail "check 0 (architecture): $ref carries no /usr/local/bin/logweir-api."
  fi
  elf_out=$(od -An -tx1 -N20 "$work/logweir-api")
  elf_bytes="$(printf '%s' "$elf_out" | tr -s '[:space:]' ' ' | sed -e 's/^ //' -e 's/ $//')"
  elf_magic="$(printf '%s' "$elf_bytes" | cut -d' ' -f1-6)"
  elf_machine="$(printf '%s' "$elf_bytes" | cut -d' ' -f19-20)"
  if [ "$elf_magic" != "7f 45 4c 46 02 01" ]; then
    fail "check 0 (architecture): /usr/local/bin/logweir-api in $ref is not a" \
         "  64-bit LITTLE-ENDIAN ELF." \
         "  expected the first six bytes to be \`7f 45 4c 46 02 01\`" \
         "  header read: $elf_bytes"
  fi
  if [ "$elf_machine" != "$WANT_MACHINE" ]; then
    fail "check 0 (architecture): /usr/local/bin/logweir-api in $ref is NOT an" \
         "  AArch64 binary. e_machine at offset 18 is \`$elf_machine\`, expected" \
         "  \`$WANT_MACHINE\` (0x00b7, EM_AARCH64). \`3e 00\` there is x86-64." \
         "  header read: $elf_bytes"
  fi
fi

# =========================================================================
# CHECK 1 — THE SERVED FILES. `assets.rs` reads this directory once at startup.
# =========================================================================
echo "-- check 1 (the page): /ui is the twenty-six shipped files, sha256 for sha256, and nothing else"
if ! cp_out /ui "$work/ui"; then
  fail "check 1 (the page): $ref carries no /ui directory." \
       "  \`Dockerfile.console\` COPYs the twenty-six shipped files there and the" \
       "  chart's ConfigMap names it as \`uiDirectory: /ui\`;" \
       "  \`logweir_api::assets::StaticAssets::load\` refuses to start without an" \
       "  index.html in it."
fi

# ANYTHING THAT IS NOT A DIRECTORY AND NOT A REGULAR FILE IS A RED, BY ITSELF.
# `assets.rs` skips symbolic links deliberately (`DirEntry::file_type` does not
# follow them), so a link under /ui would be a file the image ships and the
# service silently does not serve — a divergence between these two checks and
# the running process.
strange="$(find "$work/ui" ! -type d ! -type f)"
if [ -n "$strange" ]; then
  fail "check 1 (the page): $ref's /ui holds entries that are neither directories" \
       "  nor regular files:" "$strange"
fi

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

# TWENTY-SIX, NAMED. A gate whose expected count came from the tree alone would
# stay green if someone deleted seven files from both sides at once.
if [ "$tree_count" -ne 26 ]; then
  fail "check 1 (the page): the tree holds $tree_count shipped UI file(s), not twenty-six." \
       "  The shipped page is ui/*.html, ui/*.js, ui/*.css and ui/pages/* — the same" \
       "  set \`check-ui-offline.sh\` scans and shipped_ui_files() asserts in" \
       "  crates/logweir/tests/chart_lint.rs. Fix the tree, never this number."
fi
if [ "$image_count" -ne 26 ]; then
  fail "check 1 (the page): $ref's /ui holds $image_count file(s), not twenty-six." \
       "  It must hold the twenty-six shipped files and NOTHING else: no README.md," \
       "  no tests/ (a throwaway keypair and fixtures naming a developer's compose" \
       "  stack), no key material of any kind (Global Constraint 28)." \
       "  What is in the image:" \
       "$(cut -d' ' -f1 "$image_list")"
fi

rc=0
diff -u "$tree_list" "$image_list" > "$work/ui.diff" 2>&1 || rc=$?
if [ "$rc" -ne 0 ]; then
  cat "$work/ui.diff" >&2
  fail "check 1 (the page): the files $ref serves are NOT the tree's ui/." \
       "  The diff above is the tree's \`<path>  <sha256>\` list (-) against the" \
       "  image's (+). BOTH images that carry the page are held to the same one" \
       "  directory by the same comparison (see scripts/check-image-ui.sh check 1)," \
       "  so the console and the legacy proxy cannot drift apart silently." \
       "  Rebuild the image (\`just image-console\`) — never edit ui/ to match a" \
       "  stale one."
fi
echo "   ok: twenty-six files, sha256 for sha256, and nothing else under /ui"

# =========================================================================
# CHECK 2 — THE LICENCES: three present, three byte-identical.
# =========================================================================
echo "-- check 2 (licence): LICENSE, NOTICE and THIRD_PARTY_NOTICES.md, byte for byte"
for f in LICENSE NOTICE THIRD_PARTY_NOTICES.md; do
  if ! cp_out "/usr/share/licenses/logweir/$f" "$work/logweir-$f"; then
    fail "check 2 (licence): $ref is missing /usr/share/licenses/logweir/$f." \
         "  This image ships a statically linked Rust binary, so all three travel" \
         "  with it: Apache-2.0 requires the licence and the NOTICE (Global" \
         "  Constraint 15, spec §16 clause 5), and MIT, BSD-2-Clause, BSD-3-Clause" \
         "  and Apache-2.0 each require the dependency graph's copyright notices," \
         "  which is what THIRD_PARTY_NOTICES.md is. \`Dockerfile.ui\` carries only" \
         "  the first two, and check-image-ui.sh asserts the third is ABSENT there," \
         "  because that image links no Rust binary at all."
  fi
  rc=0
  diff -u "$f" "$work/logweir-$f" > "$work/lic-$f.diff" 2>&1 || rc=$?
  if [ "$rc" -ne 0 ]; then
    cat "$work/lic-$f.diff" >&2
    fail "check 2 (licence): $ref's /usr/share/licenses/logweir/$f is not this" \
         "  tree's $f, byte for byte. Regenerate THIRD_PARTY_NOTICES.md with" \
         "  \`bash scripts/gen-third-party-notices.sh --write\` and rebuild —" \
         "  never edit the tree to match a stale image."
  fi
done
echo "   ok: three licence files, byte-identical to the tree's"

# =========================================================================
# CHECK 3 — THE ABSENCES. Each one is a claim this image must not make.
# =========================================================================
echo "-- check 3 (absences): no MIT notice, no org-root anchor, no engine"
if cp_out /usr/share/licenses/kafka-backup "$work/kafka-backup"; then
  fail "check 3 (absences): $ref CARRIES /usr/share/licenses/kafka-backup/." \
       "  The console owes no MIT notice: it carries no kafka-backup binary and no" \
       "  OSO code, so a notice for one is a claim to a redistribution that does" \
       "  not happen. A licence notice is a statement about what is inside."
fi
if cp_out /etc/logweir/org-root.fingerprint "$work/org-root"; then
  fail "check 3 (absences): $ref CARRIES /etc/logweir/org-root.fingerprint." \
       "  The runner and the controller each carry that anchor because each is an" \
       "  EXECUTION component a cluster owner is handed. This service verifies no" \
       "  archive, mints no evidence and reads no bundle (\`check-one-signer.sh\`" \
       "  puts logweir-api on neither signing allowlist). An anchor that ships" \
       "  without a reader is a claim, not a precaution."
fi
if cp_out /opt/logweir/engine "$work/engine"; then
  fail "check 3 (absences): $ref CARRIES /opt/logweir/engine." \
       "  The extracted OSO engine belongs to the RUNNER image alone. This service" \
       "  executes no backup or restore: \`weirkeeper\` remains the only execution" \
       "  authority and isolated runner Jobs remain the data-plane boundary."
fi
echo "   ok: three absences held"

# =========================================================================
# CHECK 4 — THE ENTRYPOINT AND THE ABSENT CMD.
# =========================================================================
echo "-- check 4 (entrypoint): /usr/local/bin/logweir-api, and no CMD — the mode is the chart's"
if ! entrypoint=$(docker image inspect "$ref" --format '{{json .Config.Entrypoint}}' 2>&1); then
  fail "check 4 (entrypoint): could not read $ref's entrypoint:" "$entrypoint"
fi
if [ "$entrypoint" != '["/usr/local/bin/logweir-api"]' ]; then
  fail "check 4 (entrypoint): $ref's entrypoint is \`$entrypoint\`, not" \
       "  \`[\"/usr/local/bin/logweir-api\"]\`."
fi
if ! cmd=$(docker image inspect "$ref" --format '{{json .Config.Cmd}}' 2>&1); then
  fail "check 4 (entrypoint): could not read $ref's CMD:" "$cmd"
fi
if [ "$cmd" != "null" ] && [ "$cmd" != "[]" ]; then
  fail "check 4 (entrypoint): $ref declares CMD $cmd." \
       "  It must declare none. \`--config <file>\` is the chart's argument, and a" \
       "  default argument vector here would be a second place the console's MODE —" \
       "  \`localAdmin\` versus \`shared\` — could come from, in a layer no chart" \
       "  test can see. Same argument Dockerfile.ui makes about --accept-paths."
fi
if ! user=$(docker image inspect "$ref" --format '{{.Config.User}}' 2>&1); then
  fail "check 4 (entrypoint): could not read $ref's USER:" "$user"
fi
if [ "$user" != "65532:65532" ]; then
  fail "check 4 (entrypoint): $ref declares USER \`$user\`, not \`65532:65532\`." \
       "  The chart's PodSpec sets runAsNonRoot: true, which the kubelet enforces" \
       "  by REFUSING to start a container whose image declares USER root — so the" \
       "  image and that PodSpec have to agree or the console never starts."
fi
echo "   ok: entrypoint is logweir-api, no CMD, non-root by default"

# =========================================================================
# CHECK 5 — NO KEY MATERIAL, ANYWHERE. Global Constraint 28.
# =========================================================================
# This is the one image in the tree that MOUNTS key material at runtime — a
# session key, a cursor MAC key and an OIDC client secret — so the question
# "could a key have been baked in" is worth asking of it in particular, and the
# answer has to come from the filesystem rather than from reading the COPY
# lines. `docker export` flattens the whole image to a tar and the NAMES are
# read from it; nothing is extracted and nothing is executed.
#
# THE SYSTEM TRUST STORE IS EXCLUDED BY PATH, AND THAT IS A CHECK AND NOT A
# HOLE. `ca-certificates` installs ~150 files under /etc/ssl/certs/*.pem plus
# /usr/lib/ssl/cert.pem, and every one of them is a PUBLIC root certificate —
# a certificate is a public key with a signature over it, and a trust store
# with a private key in it would be a different defect with a different name.
# The three directories below are the base image's, none of them is written by
# any COPY in Dockerfile.console, and the arm immediately after this one
# asserts the bundle IS there — because a console whose CA bundle went missing
# fails shared-mode OIDC discovery at the first HTTPS handshake. So the
# exclusion is paired with the requirement, rather than being a name this gate
# quietly stops looking at.
echo "-- check 5 (no keys): no PEM, no *.key, no kubeconfig outside the system trust store"
if ! docker export "$cid" > "$work/image.tar" 2>"$work/export.err"; then
  fail "check 5 (no keys): could not export $ref:" "$(cat "$work/export.err")"
fi
if ! tar -tf "$work/image.tar" > "$work/paths.txt" 2>"$work/tar.err"; then
  fail "check 5 (no keys): could not list the exported filesystem:" "$(cat "$work/tar.err")"
fi
grep -v -E '^(etc/ssl/|usr/lib/ssl/|usr/share/ca-certificates/)' "$work/paths.txt" > "$work/scanned.txt" || true
if [ ! -s "$work/scanned.txt" ]; then
  fail "check 5 (no keys): the exported filesystem is empty once the trust store is" \
       "  excluded, which cannot be true of this image. The scan would be asserting" \
       "  nothing; investigate \`docker export\` rather than trusting this pass."
fi
suspicious="$(grep -E '(\.pem|\.key|\.p8|\.pfx|_rsa|id_ed25519|kubeconfig)$' "$work/scanned.txt" || true)"
if [ -n "$suspicious" ]; then
  fail "check 5 (no keys): $ref carries files whose names are key material:" \
       "$suspicious" \
       "  Global Constraint 28: no key material in the page or in anything that" \
       "  serves it. The console's session key, cursor key and OIDC client secret" \
       "  are MOUNTED from Secrets at paths the chart's ConfigMap names" \
       "  (charts/logweir/templates/ui/api-config.yaml); none of the three is ever" \
       "  built into a layer. \`ui/tests/\` in particular carries a throwaway" \
       "  keypair and must never be COPYed."
fi
bundle="$(grep -c -E '^etc/ssl/certs/.*\.pem$' "$work/paths.txt" || true)"
if [ "${bundle:-0}" -lt 50 ]; then
  fail "check 5 (no keys): $ref carries only ${bundle:-0} root certificate(s) under" \
       "  /etc/ssl/certs/. \`Dockerfile.console\` installs \`ca-certificates\` and the" \
       "  bundle is not optional: rustls's native-tokio root store is what validates" \
       "  the identity provider's certificate at OIDC discovery, JWKS and token" \
       "  exchange. An image without it fails shared-mode startup with an error that" \
       "  names nothing useful. (The in-cluster API server is verified with the" \
       "  projected ServiceAccount CA and is unaffected, which is why this can go" \
       "  unnoticed until the first SSO login.)"
fi
echo "   ok: $bundle public roots in the trust store, and no key-shaped path anywhere else"

# =========================================================================
# CHECK 6 — THE BINARY RUNS, ON $PATH, AND NAMES ITSELF.
# =========================================================================
if [ "$no_exec" -eq 1 ]; then
  echo
  echo "ok: --no-exec — an AArch64 image, the twenty-six shipped files byte for byte, three"
  echo "    licences, three absences, the entrypoint and USER, and no key-shaped path in $ref."
  echo "    NOTHING was executed from the image, so \`logweir-api --version\` is NOT asserted"
  echo "    here; a native runner asserts that."
  exit 0
fi

echo "-- check 6 (the binary): \`logweir-api --version\` runs by bare name and is version $want_version"
if ! bin_version=$(docker run --rm --platform "$declared" \
                     --entrypoint logweir-api "$ref" --version 2>&1); then
  fail "check 6 (the binary): \`logweir-api\` did not run inside $ref:" \
       "$bin_version" \
       "  THE BARE NAME IS DELIBERATE: a path would prove a file exists, while the" \
       "  bare name makes the runtime resolve it through \$PATH exactly as the" \
       "  kubelet resolves a container's command. \`executable file not found in" \
       "  \$PATH\` here is exitCode 127 in the cluster — the shape of defect" \
       "  RET-NOIMAGE, which shipped once already (scripts/check-image.sh check 7)." \
       "  It also RUNS the binary, so a wrong-architecture or unlinkable copy fails" \
       "  here too; use --no-exec for a variant this host cannot run."
fi
case "$bin_version" in
  "logweir-api "*) ;;
  *)
    fail "check 6 (the binary): /usr/local/bin/logweir-api in $ref does not name itself." \
         "  \`--version\` printed: $bin_version" \
         "  Expected a line beginning \`logweir-api \`. A COPY whose SOURCE is a" \
         "  different binary produces exactly this — review finding L1 proved that" \
         "  an exit-code-only check passes on such an image."
    ;;
esac
if [ "${bin_version#logweir-api }" != "$want_version" ]; then
  fail "check 6 (the binary): $ref carries logweir-api" \
       "  ${bin_version#logweir-api }, and this tree's [workspace.package] version is" \
       "  $want_version." \
       "  The package takes \`version.workspace = true\`, so one build cannot produce" \
       "  two versions: a mismatch means the binary came from a stale layer or a" \
       "  different tree. Rebuild with \`just image-console\`."
fi

echo
echo "ok: the twenty-six shipped files byte for byte at /ui and nothing else, three licences,"
echo "    no MIT notice, no org-root anchor and no engine, the entrypoint logweir-api with no"
echo "    CMD and USER 65532, no key-shaped path anywhere, and the binary runs by bare name"
echo "    reporting $bin_version — in $ref"
