#!/usr/bin/env bash
# The emergency recovery kit (PROD-14.1): everything a clean installation needs
# to recover from an existing archive, with no public network once it is built.
#
#   recovery-kit.sh <version> --release-assets <dir> [options]   a release's assets (gh release download)
#   recovery-kit.sh <version> --local-build [options]            a DRY RUN: this host's logweir build
#
#   <version>              v<semver>, or sha-<commit> for a main publication (--local-build only)
#   --trust <dir>          public material you exported (docs/install.md), copied into trust/
#   --save-images <os/arch>  also `docker save` the four images for that platform
#   --out <dir>            where the kit goes (default target/recovery-kit)
#
# It writes <out>/logweir-recovery-kit-<version>/ and the same as a .tar.gz with
# a .sha256 sidecar. MANIFEST.sha256 lists every other file with its sha256, and
# kit.json says whether the binaries came from the release or a local build.
#
# NO PRIVATE KEY AND NO PASSWORD, EVER. Before anything is packed, every file is
# checked: a private-key PEM block (plain or base64), a Kubernetes Secret, or a
# file named like a key, a password or a credential is refused with exit 3, and
# nothing is left behind.
set -euo pipefail
cd "$(dirname "$0")/.."
export LC_ALL=C

NS="${NS:-vladyslavhaina}"
PRODUCTS=(weirkeeper logweir logweir-console)
ROLES=(controller runner console)
TARGETS=(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin)
GUIDE=docs/install.md
GUIDE_SECTION='## Recover onto a clean installation with the kit'

die() { echo "recovery-kit: $*" >&2; exit 1; }
usage() { sed -n '4,13p' "$0" >&2; exit 2; }
sha256_of() {
  local line
  if command -v sha256sum >/dev/null 2>&1; then line=$(sha256sum "$1"); else line=$(shasum -a 256 "$1"); fi
  printf '%s\n' "${line%% *}"
}

version="${1:-}"
[[ -n "$version" && "$version" != -* ]] || usage
shift
mode="" assets="" trust="" platform="" out=target/recovery-kit
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release-assets) assets="${2:?}"; mode=release; shift 2 ;;
    --local-build) mode=local-build; shift ;;
    --trust) trust="${2:?}"; shift 2 ;;
    --save-images) platform="${2:?}"; shift 2 ;;
    --out) out="${2:?}"; shift 2 ;;
    *) usage ;;
  esac
done
[[ -n "$mode" ]] || die "name the binaries' source: --release-assets <dir> or --local-build"
if [[ "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then :
elif [[ "$version" =~ ^sha-[0-9a-f]{40}$ && "$mode" == local-build ]]; then :
else die "'$version' is not v<semver> (or sha-<commit> with --local-build)"; fi
[[ -z "$trust" || -d "$trust" ]] || die "--trust $trust is not a directory"

name="logweir-recovery-kit-$version"
kit="$out/$name"
[[ ! -e "$kit" && ! -e "$kit.tar.gz" ]] || die "$kit exists; remove it or choose another --out"
mkdir -p "$kit"/{bin,chart,images,schemas,verify}
done_ok=false
trap '[[ "$done_ok" == true ]] || rm -rf "$kit" "$kit.tar.gz" "$kit.tar.gz.sha256" "$out/.$name.manifest"' EXIT

# ---------------------------------------------------------------- binaries, chart, image references
images="$kit/images/images.txt"
: > "$images"
ref() { awk -v r="$1" '$1 == r { print $2 }' "$images"; }   # role -> image reference
if [[ "$mode" == release ]]; then
  [[ -f "$assets/SHA256SUMS" && -f "$assets/release.json" ]] || die "$assets holds no SHA256SUMS and release.json"
  while read -r want file; do
    [[ -f "$assets/$file" && "$(sha256_of "$assets/$file")" == "$want" ]] || die "$assets/$file does not match SHA256SUMS"
  done < "$assets/SHA256SUMS"
  [[ "$(jq -r .tag "$assets/release.json")" == "$version" ]] || die "$assets/release.json is not $version"
  commit=$(jq -er .commit "$assets/release.json")
  git cat-file -e "$commit^{commit}" 2>/dev/null || die "the release commit $commit is not in this checkout; fetch it"
  git diff --quiet "$commit" HEAD -- schemas || die "this checkout's schemas/ differ from $version's; check out $version"
  for target in "${TARGETS[@]}"; do
    for file in "logweir-$target.tar.xz" "logweir-$target.tar.xz.sha256"; do
      awk -v f="$file" '$2 == f { n++ } END { exit n != 1 }' "$assets/SHA256SUMS" || die "$version has no $file"
      cp "$assets/$file" "$kit/bin/"
    done
  done
  chart=$(jq -er .chart.package "$assets/release.json")
  cp "$assets/$chart" "$kit/chart/"
  cp "$assets/verify_scorecard.py" "$kit/verify/"
  cp "$assets/release.json" "$kit/"
  for i in 0 1 2; do
    printf '%s %s\n' "${ROLES[$i]}" \
      "$(jq -er --arg p "${PRODUCTS[$i]}" '.images.refs[$p].reference' "$assets/release.json")" >> "$images"
  done
  absent='[]'
else
  bin="${LOGWEIR_BIN:-target/release/logweir}"
  [[ -x "$bin" ]] || die "no logweir at $bin: run \`cargo build --locked --release -p logweir\`, or set LOGWEIR_BIN"
  case "$(uname -s)/$(uname -m)" in
    Darwin/arm64) host=aarch64-apple-darwin ;;
    Linux/x86_64) host=x86_64-unknown-linux-gnu ;;
    Linux/aarch64) host=aarch64-unknown-linux-gnu ;;
    *) die "no release target for $(uname -s)/$(uname -m)" ;;
  esac
  mkdir -p "$kit/bin/$host"
  cp "$bin" "$kit/bin/$host/logweir"
  absent=$(printf '%s\n' "${TARGETS[@]}" | grep -vx "$host" | jq -R . | jq -sc .)
  commit=$(git rev-parse HEAD)
  chart="logweir-chart-local.tgz"
  tar -czf "$kit/chart/$chart" --exclude rendered -C charts logweir
  cp docs/verify_scorecard.py "$kit/verify/"
  # Anonymous registry reads, as release.sh does: "pullable by anyone" is the claim.
  anon=$(mktemp -d "${TMPDIR:-/tmp}/logweir-kit-docker.XXXXXX")
  if [[ -d "${DOCKER_CONFIG:-$HOME/.docker}/cli-plugins" ]]; then
    ln -s "${DOCKER_CONFIG:-$HOME/.docker}/cli-plugins" "$anon/cli-plugins"
  fi
  for i in 0 1 2; do
    tagged="docker.io/$NS/${PRODUCTS[$i]}:$version"
    if ! DOCKER_CONFIG="$anon" docker buildx imagetools inspect "$tagged" --format '{{json .Manifest}}' > "$anon/manifest.json" \
      || ! digest=$(jq -er '.digest | select(test("^sha256:[0-9a-f]{64}$"))' "$anon/manifest.json"); then
      rm -rf "$anon"; die "could not resolve $tagged"
    fi
    printf '%s %s\n' "${ROLES[$i]}" "$tagged@$digest" >> "$images"
  done
  rm -rf "$anon"
fi

# The identity hook's image is the chart's own digest pin, not one of the three.
values=$(tar -tzf "$kit/chart/$chart" | grep -E '^[^/]+/values\.yaml$' | head -1)
printf 'bootstrap %s\n' "$(tar -xzOf "$kit/chart/$chart" "$values" \
  | awk '$1 == "bootstrapImage:" { print $2; exit }')" >> "$images"
for role in controller runner console bootstrap; do
  [[ "$(ref "$role")" =~ @sha256:[0-9a-f]{64}$ ]] || die "the $role image reference '$(ref "$role")' has no digest"
done
cat > "$kit/values-offline.yaml" <<EOF
# Generated by scripts/recovery-kit.sh for $version: the images in this kit, by digest.
# helm install ... -f values-offline.yaml (with your own values file after it).
controllerImage: $(ref controller)
runnerImage: $(ref runner)
imagePullPolicy: IfNotPresent
runnerImagePullPolicy: IfNotPresent
identity:
  bootstrapImage: $(ref bootstrap)
  bootstrapImagePullPolicy: IfNotPresent
api:
  console:
    image: $(ref console)
    imagePullPolicy: IfNotPresent
EOF

# ---------------------------------------------------------------- schemas, trust, instructions
cp schemas/*.json "$kit/schemas/"
if [[ -n "$trust" ]]; then
  mkdir -p "$kit/trust"
  cp -R "$trust"/. "$kit/trust/"
fi
{
  cat <<EOF
# Logweir $version: recovery kit

Built by \`scripts/recovery-kit.sh\` from \`$commit\` (binaries: $mode). \`kit.json\`
and \`images/images.txt\` name what is here. \`MANIFEST.sha256\` holds every file's
sha256: check it first with \`sha256sum -c MANIFEST.sha256\` (or \`shasum -a 256 -c\`).

EOF
  awk -v h="$GUIDE_SECTION" '$0 == h { on = 1; print; next } on && /^## / { exit } on { print }' "$GUIDE"
} > "$kit/RECOVERY.md"
grep -qxF "$GUIDE_SECTION" "$kit/RECOVERY.md" || die "$GUIDE has no '$GUIDE_SECTION' section"
jq -n --arg version "$version" --arg binaries "$mode" --arg commit "$commit" --argjson absent "$absent" \
  --rawfile images "$images" --arg platform "$platform" '
  { version: $version, binaries: $binaries, commit: $commit, absentTargets: $absent,
    images: ($images | split("\n") | map(select(length > 0) | split(" ") | {(.[0]): .[1]}) | add),
    savedImagesPlatform: (if $platform == "" then null else $platform end) }' > "$kit/kit.json"

# ---------------------------------------------------------------- the guard: no private key, no password
refuse() { echo "recovery-kit: REFUSED $1: $2; nothing was packed" >&2; exit 3; }
pem='-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----'
b64='UFJJVkFURSBLRVk|SVZBVEUgS0VZ|UklWQVRFIEtFWS'   # "PRIVATE KEY" at each base64 alignment
while IFS= read -r -d '' file; do
  rel="${file#"$kit"/}"
  base=$(basename "$rel" | tr '[:upper:]' '[:lower:]')
  case "$base" in
    *.key|*password*|*passwd*|*credential*|*secret*) refuse "$rel" "its name says key, password, credential or secret" ;;
  esac
  case "$rel" in
    bin/*|images/*.tar) continue ;;   # release-built or locally built executables and image layers
    chart/*.tgz)
      if tar -xzOf "$file" | grep -aEq -- "$pem|$b64"; then refuse "$rel" "the chart carries private-key material"; fi
      continue ;;
  esac
  if grep -aEq -- "$pem|$b64" "$file"; then refuse "$rel" "it holds a private key"; fi
  if grep -aEq '^[[:space:]]*kind:[[:space:]]*Secret[[:space:]]*$|"kind"[[:space:]]*:[[:space:]]*"Secret"' "$file"; then
    refuse "$rel" "it is a Kubernetes Secret"
  fi
done < <(find "$kit" -type f -print0)

# ---------------------------------------------------------------- image archives, after the guard
if [[ -n "$platform" ]]; then
  for role in controller runner console bootstrap; do
    docker pull -q --platform "$platform" "$(ref "$role")" >/dev/null || die "could not pull $(ref "$role") for $platform"
    if ! docker save --platform "$platform" -o "$kit/images/$role-${platform//\//-}.tar" "$(ref "$role")" 2>/dev/null; then
      # A single-platform image (the identity hook's pin can be one) is saved as it is.
      have=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$(ref "$role")") || die "could not save $(ref "$role")"
      echo "recovery-kit: $(ref "$role") has no $platform variant; saved its $have one, which a $platform node runs only under emulation" >&2
      docker save -o "$kit/images/$role-${have//\//-}.tar" "$(ref "$role")" || die "could not save $(ref "$role")"
    fi
  done
fi

# ---------------------------------------------------------------- manifest and tarball
( cd "$kit" && find . -type f | sed 's|^\./||' | sort | while IFS= read -r f; do
    printf '%s  %s\n' "$(sha256_of "$f")" "$f"
  done ) > "$out/.$name.manifest"
mv "$out/.$name.manifest" "$kit/MANIFEST.sha256"
tar -czf "$kit.tar.gz" -C "$out" "$name"
printf '%s  %s\n' "$(sha256_of "$kit.tar.gz")" "$name.tar.gz" > "$kit.tar.gz.sha256"
done_ok=true
echo "recovery-kit: $kit.tar.gz ($mode binaries, $(wc -l < "$kit/MANIFEST.sha256" | tr -d ' ') files in MANIFEST.sha256)"
