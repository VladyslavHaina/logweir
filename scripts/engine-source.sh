#!/usr/bin/env bash
# Logweir's build of the kafka-backup engine, from the vendored OSO source plus
# the ordered patch folder (OD-3, decided 2026-10-07; PROD-00.2).
#
#   engine-source.sh check            verify the inputs and print the identity
#   engine-source.sh digest           print the build-input digest the inputs give
#   engine-source.sh prepare <dir>    check, extract, apply the patches, stamp the version
#   engine-source.sh build [<out>]    prepare and `cargo build --locked --release` natively,
#                                     copying the binary to <out> (default .engine/kafka-backup)
#
# THE INPUTS, all under third_party/:
#   kafka-backup-build.env            ENGINE_VERSION=<release>+logweir.<n> and
#                                     ENGINE_DIGEST=sha256:<hex>
#   kafka-backup-builds.txt           the append-only ledger of every build:
#                                     `<version> <digest>` per line, the last
#                                     line being the build env's pair
#   kafka-backup-v<release>.tar.gz    the vendored OSO source, and its .sha256
#   kafka-backup-patches/             NNNN-<slug>.patch, applied in name order
#
# THE DIGEST is the sha256 of these lines, each ending in a newline:
#   logweir-engine-inputs/v1
#   source kafka-backup-v<release>.tar.gz <sha256 hex of the tarball>
#   patch <file name> <sha256 hex of the patch file>     (one per patch, in order)
#   version <ENGINE_VERSION>
# `crates/logweir/tests/engine_build.rs` computes the same digest in Rust and
# runs this script over planted trees, so the two definitions cannot drift.
#
# The Dockerfile's `engine-logweir` stage runs `prepare` and then cross-compiles
# the prepared tree for the image's platform; `build` is the native route CI's
# e2e job and engine-matrix use. Every refusal exits 1 and names its input.
set -euo pipefail

# LOGWEIR_ROOT lets the tests point this at a planted tree, exactly as
# scripts/extract-engine.sh does.
cd "${LOGWEIR_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"

BUILD_ENV="third_party/kafka-backup-build.env"
LEDGER="third_party/kafka-backup-builds.txt"
PATCH_DIR="third_party/kafka-backup-patches"

die() {
  printf 'engine-source: %s\n' "$@" >&2
  exit 1
}

if command -v sha256sum >/dev/null 2>&1; then
  sha256_file() { sha256sum "$1" | cut -d' ' -f1; }
  sha256_stdin() { sha256sum | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha256_file() { shasum -a 256 "$1" | cut -d' ' -f1; }
  sha256_stdin() { shasum -a 256 | cut -d' ' -f1; }
else
  die "neither sha256sum nor shasum is on PATH; the inputs cannot be verified"
fi

# One `KEY=value` line of the build env, read as text and never sourced.
env_value() {
  local v
  v=$(sed -n "s/^$1=//p" "$BUILD_ENV")
  [ "$(printf '%s\n' "$v" | grep -c .)" = 1 ] || die "$BUILD_ENV must set $1 exactly once"
  printf '%s\n' "$v"
}

[ -f "$BUILD_ENV" ] || die "$BUILD_ENV is missing"
VERSION=$(env_value ENGINE_VERSION)
RECORDED_DIGEST=$(env_value ENGINE_DIGEST)
if ! [[ "$VERSION" =~ ^([0-9]+\.[0-9]+\.[0-9]+)\+logweir\.([1-9][0-9]*)$ ]]; then
  die "ENGINE_VERSION is \`$VERSION\`; it must be <release>+logweir.<n>, n from 1, so" \
      "Logweir's build is never mistaken for OSO's release"
fi
RELEASE="${BASH_REMATCH[1]}"
[[ "$RECORDED_DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] \
  || die "ENGINE_DIGEST is \`$RECORDED_DIGEST\`; it must be sha256:<64 lowercase hex>"

TARBALL="third_party/kafka-backup-v${RELEASE}.tar.gz"
[ -s "$TARBALL" ] || die "$TARBALL is missing: ENGINE_VERSION names release $RELEASE"
[ -s "$TARBALL.sha256" ] || die "$TARBALL.sha256 is missing"
TARBALL_SHA=$(sha256_file "$TARBALL")
[ "$(cat "$TARBALL.sha256")" = "$TARBALL_SHA  $TARBALL" ] \
  || die "$TARBALL does not match $TARBALL.sha256 (the bytes give $TARBALL_SHA)"

# THE LEDGER: ONE VERSION, ONE ENGINE (review M1). A change to the inputs gives
# a new digest, and a new digest is a new build: it is APPENDED as
# `<release>+logweir.<n+1> <digest>`. So the last line must be the build env's
# pair, no version and no digest may appear twice, and `n` must rise within a
# release. Re-recording the digest without bumping `n` leaves either a
# duplicated version (appended) or a last line that disagrees with the env;
# both are refused. Rewriting the last line in place is what
# `crates/logweir/tests/engine_pin.rs` refuses: it pins every build that has
# shipped, as a prefix of this file.
[ -s "$LEDGER" ] || die "$LEDGER is missing: the ledger of Logweir's engine builds"
ledger_versions=" "
ledger_digests=" "
last_version=""
last_digest=""
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue ;; esac
  [[ "$line" =~ ^([0-9]+\.[0-9]+\.[0-9]+)\+logweir\.([1-9][0-9]*)\ (sha256:[0-9a-f]{64})$ ]] \
    || die "$LEDGER: \`$line\` is not \`<release>+logweir.<n> sha256:<hex>\`"
  l_release="${BASH_REMATCH[1]}"; l_n="${BASH_REMATCH[2]}"; l_digest="${BASH_REMATCH[3]}"
  l_version="${l_release}+logweir.${l_n}"
  case "$ledger_versions" in
    *" $l_version "*) die "$LEDGER: version $l_version is recorded twice. Two engines would print one" \
                          "version: bump the <n> of ENGINE_VERSION for a new build." ;;
  esac
  case "$ledger_digests" in
    *" $l_digest "*) die "$LEDGER: digest $l_digest is recorded twice; one build has one version" ;;
  esac
  for prior in $ledger_versions; do
    p_release="${prior%%+logweir.*}"; p_n="${prior##*+logweir.}"
    if [ "$p_release" = "$l_release" ] && [ "$p_n" -ge "$l_n" ]; then
      die "$LEDGER: $l_version follows $prior; <n> must rise within release $l_release"
    fi
  done
  ledger_versions="$ledger_versions$l_version "
  ledger_digests="$ledger_digests$l_digest "
  last_version="$l_version"; last_digest="$l_digest"
done < "$LEDGER"
[ "$last_version $last_digest" = "$VERSION $RECORDED_DIGEST" ] \
  || die "$LEDGER ends with \`$last_version $last_digest\`, but $BUILD_ENV builds" \
         "\`$VERSION $RECORDED_DIGEST\`. A new build is a new LAST line: bump the <n> of" \
         "ENGINE_VERSION and append \`<version> <digest>\`; never rewrite a recorded line."

# THE PATCH FOLDER'S FORMAT (third_party/kafka-backup-patches/README.md).
[ -d "$PATCH_DIR" ] || die "$PATCH_DIR is missing"
[ -s "$PATCH_DIR/README.md" ] || die "$PATCH_DIR/README.md is missing: it states the policy"
PATCHES=()
seen_numbers=" "
while IFS= read -r name; do
  [ -n "$name" ] || continue
  [ "$name" = README.md ] && continue
  [[ "$name" =~ ^([0-9]{4})-[a-z0-9][a-z0-9-]*\.patch$ ]] \
    || die "$PATCH_DIR/$name is not NNNN-<slug>.patch; nothing else lives in the patch folder"
  number="${BASH_REMATCH[1]}"
  case "$seen_numbers" in
    *" $number "*) die "$PATCH_DIR: two patches are numbered $number; the order must be total" ;;
  esac
  seen_numbers="$seen_numbers$number "
  file="$PATCH_DIR/$name"
  [ -f "$file" ] || die "$file is not a regular file"
  reason=$(sed -n '1p' "$file")
  [[ "$reason" =~ ^Reason:\ [^[:space:]].*$ ]] \
    || die "$file: line 1 must be \`Reason: <one line>\`, got \`$reason\`"
  line2=$(sed -n '2p' "$file")
  if [ -n "$line2" ]; then
    [[ "$line2" =~ ^Upstream:\ https://[^[:space:]]+$ ]] \
      || die "$file: line 2 must be blank or \`Upstream: https://…\`, got \`$line2\`"
    [ -z "$(sed -n '3p' "$file")" ] || die "$file: line 3 must be blank, before the diff"
  fi
  grep -q '^+++ b/' "$file" || die "$file carries no unified diff (\`+++ b/…\`)"
  PATCHES+=("$name")
done < <(LC_ALL=C ls -1A "$PATCH_DIR")

# The digest the inputs give, by the definition in the header.
inputs_digest() {
  {
    printf 'logweir-engine-inputs/v1\n'
    printf 'source %s %s\n' "$(basename "$TARBALL")" "$TARBALL_SHA"
    local name
    for name in ${PATCHES[@]+"${PATCHES[@]}"}; do
      printf 'patch %s %s\n' "$name" "$(sha256_file "$PATCH_DIR/$name")"
    done
    printf 'version %s\n' "$VERSION"
  } | sha256_stdin
}
DIGEST="sha256:$(inputs_digest)"

identity() {
  printf 'version=%s\ndigest=%s\n' "$VERSION" "$DIGEST"
}

check_digest() {
  [ "$DIGEST" = "$RECORDED_DIGEST" ] \
    || die "the inputs give $DIGEST, but $BUILD_ENV records $RECORDED_DIGEST." \
           "A change to the tarball, the patch folder or ENGINE_VERSION is a new build:" \
           "bump the <n> of ENGINE_VERSION, record \`$0 digest\` as ENGINE_DIGEST, and" \
           "append the pair to $LEDGER."
}

prepare() {
  local dest="$1" main stamped
  [ -n "$dest" ] || die "prepare needs a destination directory"
  if [ -e "$dest" ] && [ -n "$(ls -A "$dest" 2>/dev/null)" ]; then
    die "$dest is not empty; prepare extracts into an empty directory"
  fi
  mkdir -p "$dest"
  tar -xzf "$TARBALL" -C "$dest" --strip-components=1
  # `git apply` and not `patch`: exact context, no fuzz, the same tool on
  # macOS and in the builder. GIT_CEILING_DIRECTORIES stops it discovering an
  # enclosing repository (this worktree, when the destination is under
  # target/), so it applies the patch to the extracted tree as a plain
  # directory and never through another repository's index or prefix.
  local name patch_abs dest_abs
  dest_abs=$(cd "$dest" && pwd)
  for name in ${PATCHES[@]+"${PATCHES[@]}"}; do
    patch_abs="$PWD/$PATCH_DIR/$name"
    (cd "$dest_abs" && GIT_CEILING_DIRECTORIES="$(dirname "$dest_abs")" \
       git apply -p1 --whitespace=nowarn "$patch_abs") \
      || die "$PATCH_DIR/$name does not apply to $TARBALL. If the release now contains the" \
             "fix, drop the patch in the change that moves the pin (README, rule 4)."
    echo "applied $name: $(sed -n '1s/^Reason: //p' "$PATCH_DIR/$name")"
  done
  # THE VERSION STAMP. clap's `#[command(version)]` prints CARGO_PKG_VERSION;
  # Logweir's build prints its own identity instead, so `kafka-backup
  # --version` can never be read as OSO's release. Exactly one attribute is
  # replaced, and the replacement is asserted: an upstream change to that line
  # fails the build instead of shipping OSO's version string.
  main="$dest/crates/kafka-backup-cli/src/main.rs"
  [ "$(grep -c '^#\[command(version)\]$' "$main")" = 1 ] \
    || die "$main must carry exactly one \`#[command(version)]\` line to stamp"
  sed -i.orig "s/^#\[command(version)\]$/#[command(version = \"$VERSION\")]/" "$main"
  rm -f "$main.orig"
  stamped=$(grep -c "^#\[command(version = \"$VERSION\")\]$" "$main" || true)
  [ "$stamped" = 1 ] || die "the version stamp did not land in $main"
  identity > "$dest/LOGWEIR-ENGINE-IDENTITY"
  echo "prepared $dest: kafka-backup $VERSION ($DIGEST)"
}

case "${1:-}" in
  check)
    check_digest
    identity
    ;;
  digest)
    echo "$DIGEST"
    ;;
  prepare)
    check_digest
    prepare "${2:-}"
    ;;
  build)
    check_digest
    out="${2:-.engine/kafka-backup}"
    src="target/engine-src"
    rm -rf "$src"
    prepare "$src"
    # A target directory of its own, outside the extracted tree, so a fresh
    # extraction keeps the compiled dependencies.
    CARGO_TARGET_DIR="$PWD/target/engine-build" \
      cargo build --locked --release --manifest-path "$src/Cargo.toml" --bin kafka-backup
    mkdir -p "$(dirname "$out")"
    cp target/engine-build/release/kafka-backup "$out"
    chmod +x "$out"
    "$out" --version
    ;;
  *)
    echo "usage: engine-source.sh check | digest | prepare <dir> | build [<out>]" >&2
    exit 2
    ;;
esac
