#!/usr/bin/env bash
# The release's steps other than compiling (PROD-14.0). Each arm is one
# `release.yml` step; every arm but `promote` reads the registry anonymously
# and writes local files only.
#
#   release.sh validate                       tag, version, prerelease, publish (for $GITHUB_OUTPUT)
#   release.sh resolve <images.json>          the sha-<commit> publication this release ships
#   release.sh assemble <in> <out> <notes.md> the GitHub Release's assets, checked, and its notes
#   release.sh verify <dir>                   SHA256SUMS and each archive's sidecar, again
#   release.sh promote <images.json>          the version tag on the SAME digests [tag push only]
#
# THE IMAGES ARE NOT REBUILT. A release ships the four images main CI already
# built, checked and published for its commit as `sha-<commit>` — the
# publications the PoC installs and upgrades from — and `promote` gives them
# the version tag without changing a byte: `imagetools create` of ONE source is
# a carbon copy, and the digest under the version tag is asserted equal to the
# commit tag's. A rebuild would ship bytes no main run or PoC round has seen.
#
# WHICH PUBLICATION. The release commit's own, when main CI published it; else
# the newest first-parent ancestor whose publication is complete and from which
# the release commit differs ONLY under docs/ — the classification main CI
# itself uses to skip `publish` (`scripts/ci-changes.sh`), because no binary
# embeds a document and no image copies one. A tracker or release-notes commit
# on top of a published one can be tagged; a code change cannot be skipped.
set -euo pipefail
cd "$(dirname "$0")/.."
export LC_ALL=C

PRODUCTS=(weirkeeper logweir logweir-console logweir-ui)
TARGETS=(x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu aarch64-apple-darwin)
TAG_RE='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?$'
# Ancestors examined for a publication. Each docs-only commit after the newest
# published one costs one; fifty is far more than a release ever has.
MAX_ANCESTORS=50
# A commit no build ever had: `resolve` reads its sha- tag to see the registry
# answer "not found" before relying on that answer.
NEVER_BUILT=0000000000000000000000000000000000000000

die() { echo "release.sh: $*" >&2; exit 1; }

sha256_of() { # file -> hex
  local line
  if command -v sha256sum >/dev/null 2>&1; then line=$(sha256sum "$1"); else line=$(shasum -a 256 "$1"); fi
  printf '%s\n' "${line%% *}"
}

expected_platforms() { # product -> os/arch,... as main CI publishes it (docs/gates.md)
  # All four, since PROD-00.2 built the runner's engine from source for arm64.
  echo linux/amd64,linux/arm64
}

# Anonymous registry reads: an EMPTY Docker configuration, so "exists for the
# publisher" can never pass for "pullable by anyone".
ANONYMOUS_DOCKER=$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-anonymous.XXXXXX")
READS=$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-reads.XXXXXX")
trap 'rm -rf "$ANONYMOUS_DOCKER" "$READS"' EXIT
# Plugins, never credentials: Docker Desktop installs `buildx` as a plugin
# under the user's configuration directory, which an empty DOCKER_CONFIG would
# hide ("unknown command: docker buildx", measured). The link carries plugin
# binaries only; credentials live in config.json, which is not there.
if [[ -d "${DOCKER_CONFIG:-$HOME/.docker}/cli-plugins" ]]; then
  ln -s "${DOCKER_CONFIG:-$HOME/.docker}/cli-plugins" "$ANONYMOUS_DOCKER/cli-plugins"
fi
anonymous() {
  DOCKER_CONFIG="$ANONYMOUS_DOCKER" "$@"
}

# EVERY "DOES IT ALREADY EXIST?" READ BEFORE A PUBLICATION (PROD-14.0 review
# M-1, swept): 0 when the reference resolves, 1 when the registry SAYS it does
# not, and the run stops when it cannot tell. Absent is only the answer buildx
# prints for a reference the registry does not have -- containerd's resolver,
# measured anonymously against Docker Hub on 2026-10-05:
#   ERROR: docker.io/vladyslavhaina/logweir:v0.2.0-rc.1: not found
# A rate limit, a timeout or a refused connection is not an answer, and is
# never taken for an absent image: that would tag a version beside one already
# published, or ship an older publication than the commit's own.
# Never call it inside $(...): its refusal must end the script, not a subshell.
published() { # ref [file for the manifest JSON]
  local ref="$1" out="${2:-/dev/null}"
  if anonymous docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' > "$out" 2> "$READS/err"; then
    return 0
  fi
  if grep -qxF "ERROR: $ref: not found" "$READS/err"; then
    return 1
  fi
  cat "$READS/err" >&2
  die "could not tell whether $ref exists (the registry did not answer 'not found'); nothing was tagged"
}

complete_publication() { # commit -> 0 when all four sha-<commit> images exist, 1 when the registry says one does not
  local product
  for product in "${PRODUCTS[@]}"; do
    published "docker.io/$NS/$product:sha-$1" || return 1
  done
}

validate() {
  local tag publish version prerelease=false tagged
  case "${EVENT:?}" in
    push)
      [[ "${REF:?}" == "refs/tags/${REF_NAME:?}" ]] \
        || die "a publishing run is a tag push; '$REF' is not refs/tags/$REF_NAME"
      # A TAG DELETION IS A PUSH TOO (PROD-14.0 review L-5), and GitHub runs it
      # on the default branch's commit. Every push payload says whether it
      # deleted the ref, so anything but an explicit `false` is refused.
      [[ "${DELETED:-}" == false ]] \
        || die "this push deleted $REF (github.event.deleted is '${DELETED:-}'); a deleted tag publishes nothing"
      tag="$REF_NAME" publish=true ;;
    workflow_dispatch)
      tag="${REHEARSAL_TAG:-}" publish=false ;;
    *) die "release.yml runs on a v<semver> tag push or as a dispatched dry run, not on '$EVENT'" ;;
  esac
  [[ "$tag" =~ $TAG_RE ]] || die "'$tag' is not v<semver> (vMAJOR.MINOR.PATCH or vMAJOR.MINOR.PATCH-PRERELEASE)"
  if [[ "$publish" == true ]]; then
    # AND THE TAG MUST STILL NAME THIS RUN'S COMMIT, read from origin: the
    # checkout's own tag ref is written from GITHUB_SHA and always agrees. A
    # re-run of an older run after the tag was deleted or re-cut would
    # otherwise publish another commit's assets under the tag's name. An
    # annotated tag is listed with its peeled commit (`^{}`), which is the one
    # GITHUB_SHA names; a lightweight tag with its commit alone.
    git ls-remote --tags origin "refs/tags/$tag" "refs/tags/$tag^{}" > "$READS/tag" 2> "$READS/err" \
      || { cat "$READS/err" >&2; die "could not read refs/tags/$tag from origin; nothing is published"; }
    tagged=$(awk -v ref="refs/tags/$tag" '$2 == ref "^{}" { peeled = $1 } $2 == ref { plain = $1 }
      END { print (peeled != "" ? peeled : plain) }' "$READS/tag")
    [[ -n "$tagged" ]] || die "origin has no tag $tag (deleted since this run started); nothing is published"
    [[ "$tagged" == "${GITHUB_SHA:?}" ]] \
      || die "origin's $tag names $tagged, not this run's commit $GITHUB_SHA; a re-cut tag publishes from its own run"
  fi
  version="${tag#v}"
  if [[ "$version" == *-* ]]; then prerelease=true; fi
  printf 'tag=%s\nversion=%s\nprerelease=%s\npublish=%s\n' "$tag" "$version" "$prerelease" "$publish"
}

resolve() {
  local out="${1:?usage: release.sh resolve <images.json>}" commits c changes publication="" how
  local work product ref digest platforms revisions images='{}'
  : "${NS:?}" "${COMMIT:?}"
  [[ "$NS" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || die "NS '$NS' is not a Docker Hub namespace"
  [[ "$COMMIT" =~ ^[0-9a-f]{40}$ ]] || die "COMMIT '$COMMIT' is not a 40-hex commit"
  # THE REGISTRY'S "NOT FOUND" MUST BE READABLE HERE before the walk relies on
  # it: a commit that was never built has no sha- tag, so this read must end in
  # that answer -- and every dry run proves it on the runner's own buildx.
  if published "docker.io/$NS/weirkeeper:sha-$NEVER_BUILT"; then
    die "docker.io/$NS/weirkeeper:sha-$NEVER_BUILT exists; this registry's answers cannot be trusted"
  fi
  if [[ -n "${PUBLICATION:-}" ]]; then
    # A DRY RUN on a branch has no publication of its own: it may name the main
    # commit whose publication it rehearses against. A tag never may.
    [[ "${PUBLISH:-false}" != true ]] \
      || die "a publishing run ships its own commit's publication; PUBLICATION is a dry-run input"
    [[ "$PUBLICATION" =~ ^[0-9a-f]{40}$ ]] || die "PUBLICATION '$PUBLICATION' is not a 40-hex commit"
    git merge-base --is-ancestor "$PUBLICATION" "$COMMIT" \
      || die "PUBLICATION $PUBLICATION is not an ancestor of $COMMIT"
    complete_publication "$PUBLICATION" \
      || die "docker.io/$NS has no complete sha-$PUBLICATION publication (all four images)"
    publication="$PUBLICATION"
    how="named by the dry run, whose own commit has no publication"
  else
    commits=$(git rev-list --first-parent --max-count="$MAX_ANCESTORS" "$COMMIT") \
      || die "git rev-list $COMMIT failed; the release needs the full history (checkout fetch-depth: 0)"
    while IFS= read -r c; do
      if [[ "$c" != "$COMMIT" ]]; then
        changes=$(bash scripts/ci-changes.sh "$c" "$COMMIT")
        [[ "$changes" == "code=false" ]] || break
      fi
      if complete_publication "$c"; then publication="$c"; break; fi
    done <<< "$commits"
    [[ -n "$publication" ]] || die "docker.io/$NS has no complete sha-<commit> publication for $COMMIT, \
nor for an ancestor it differs from only under docs/. Tag a commit main CI has published \
(its ci.yml run's publish job is green), or wait for that job and re-run."
    if [[ "$publication" == "$COMMIT" ]]; then
      how="the release commit's own"
    else
      how="the newest ancestor the release commit differs from only under docs/"
    fi
  fi

  work=$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-resolve.XXXXXX")
  for product in "${PRODUCTS[@]}"; do
    ref="docker.io/$NS/$product:sha-$publication"
    anonymous docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' > "$work/manifest.json"
    anonymous docker buildx imagetools inspect "$ref" --format '{{json .Image}}' > "$work/image.json"
    digest=$(jq -er '.digest | select(test("^sha256:[0-9a-f]{64}$"))' "$work/manifest.json") \
      || die "$ref reports no sha256 digest"
    # One platform's configuration, or a map of them for a manifest list; the
    # attestation entries a list may also carry name no real platform.
    platforms=$(jq -r 'if has("architecture") then [.] else [.[]] end
      | map(select((.os // "unknown") != "unknown") | "\(.os)/\(.architecture)") | unique | join(",")' "$work/image.json")
    [[ "$platforms" == "$(expected_platforms "$product")" ]] \
      || die "$ref is built for '$platforms', not $(expected_platforms "$product")"
    revisions=$(jq -r 'if has("architecture") then [.] else [.[]] end
      | map(select((.os // "unknown") != "unknown") | .config.Labels["org.opencontainers.image.revision"] // "none")
      | unique | join(",")' "$work/image.json")
    [[ "$revisions" == "$publication" ]] \
      || die "$ref carries org.opencontainers.image.revision '$revisions', not $publication"
    images=$(jq -c --arg product "$product" --arg digest "$digest" --arg platforms "$platforms" --arg ref "$ref" \
      '. + {($product): {digest: $digest, platforms: ($platforms | split(",")), published_as: $ref}}' <<< "$images")
  done
  rm -rf "$work"
  jq -n --arg commit "$COMMIT" --arg publication "$publication" --arg how "$how" --arg ns "$NS" \
    --argjson images "$images" \
    '{commit: $commit, publication: $publication, how: $how, namespace: $ns, images: $images}' > "$out"
  echo "release.sh: ships docker.io/$NS sha-$publication ($how)"
}

check_archive() { # dir target -> checks the archive and its sidecar in dir
  local dir="$1" target="$2" name want sidecar_name x doc
  name="logweir-$target"
  [[ -f "$dir/$name.tar.xz" && -f "$dir/$name.tar.xz.sha256" ]] || die "$dir has no $name.tar.xz and its .sha256"
  read -r want sidecar_name < "$dir/$name.tar.xz.sha256"
  [[ "${sidecar_name#\*}" == "$name.tar.xz" && "$(sha256_of "$dir/$name.tar.xz")" == "$want" ]] \
    || die "$name.tar.xz.sha256 does not describe $name.tar.xz"
  x=$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-archive.XXXXXX")
  tar -tJf "$dir/$name.tar.xz" > "$x/listed"
  sort "$x/listed" > "$x/listed.sorted"
  printf '%s\n' "$name/" "$name/LICENSE" "$name/NOTICE" "$name/README.md" \
    "$name/THIRD_PARTY_NOTICES.md" "$name/logweir" | sort > "$x/expected"
  cmp -s "$x/expected" "$x/listed.sorted" || { diff "$x/expected" "$x/listed.sorted" >&2 || true; die "$name.tar.xz does not hold exactly logweir and its four notices"; }
  mkdir "$x/out"
  tar -xJf "$dir/$name.tar.xz" -C "$x/out"
  for doc in LICENSE NOTICE README.md THIRD_PARTY_NOTICES.md; do
    cmp -s "$doc" "$x/out/$name/$doc" || die "$name.tar.xz carries another commit's $doc"
  done
  rm -rf "$x"
}

assemble() {
  local in="${1:?usage: release.sh assemble <in> <out> <notes.md>}" out="${2:?}" notes="${3:?}"
  local target name images version_ok package chart_sha work product digest asset file
  : "${TAG:?}" "${VERSION:?}" "${PRERELEASE:?}" "${COMMIT:?}" "${NS:?}" "${RUN_URL:?}"
  [[ "$TAG" =~ $TAG_RE && "$VERSION" == "${TAG#v}" ]] || die "TAG '$TAG' and VERSION '$VERSION' disagree"
  images="$in/images.json"
  [[ -f "$images" ]] || die "$images is missing (release.sh resolve writes it)"
  [[ "$(jq -r .commit "$images")" == "$COMMIT" ]] || die "$images was resolved for another commit"
  [[ ! -e "$out" ]] || die "$out exists; assemble writes a fresh directory"
  mkdir -p "$out"
  work=$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-assemble.XXXXXX")

  # 1. The three CLI archives, each checked again here, after the artifact
  #    round trip, and for an engine (Global Constraint 10).
  for target in "${TARGETS[@]}"; do
    name="logweir-$target"
    check_archive "$in" "$target"
    [[ -f "$in/$name.linkage.txt" ]] || die "$in has no $name.linkage.txt (scripts/release-build.sh writes it)"
    bash scripts/check-no-engine-in-binary.sh "$in/$name.tar.xz" >&2
    cp "$in/$name.tar.xz" "$in/$name.tar.xz.sha256" "$out/"
  done

  # 2. The independent verifier, the notices and the engine's licence.
  cp docs/verify_scorecard.py LICENSE NOTICE THIRD_PARTY_NOTICES.md "$out/"
  cp third_party/LICENSE-MIT "$out/kafka-backup-LICENSE"

  # 3. The chart, packaged ONCE, its four images pinned by the digests resolved
  #    above. These bytes are the release asset AND what `promote`'s job pushes.
  jq '.images | map_values(.digest)' "$images" > "$work/digests.json"
  package=$(GITHUB_SHA="$COMMIT" TAG="$TAG" NS="$NS" IMAGE_DIGESTS="$work/digests.json" \
    bash scripts/ci-images.sh chart-package "$work/chart")
  [[ "$(basename "$package")" == "logweir-chart-$VERSION.tgz" ]] || die "the chart package is $(basename "$package")"
  helm show values "$package" > "$work/values.yaml"
  helm show chart "$package" > "$work/chart.yaml"
  for product in "${PRODUCTS[@]}"; do
    digest=$(jq -r --arg product "$product" '.images[$product].digest' "$images")
    grep -q -F "docker.io/$NS/$product:$TAG@$digest" "$work/values.yaml" \
      || die "the packaged chart does not pin docker.io/$NS/$product:$TAG@$digest"
  done
  if grep -q ':latest' "$work/values.yaml"; then die "the packaged chart still names a :latest image"; fi
  version_ok=$(awk -v v="$VERSION" -v a="$TAG" '$1 == "version:" && $2 == v { n++ } $1 == "appVersion:" && $2 == a { n++ } END { print n + 0 }' "$work/chart.yaml")
  [[ "$version_ok" == 2 ]] || die "the packaged chart is not version $VERSION, appVersion $TAG"
  cp "$package" "$out/"
  chart_sha=$(sha256_of "$out/logweir-chart-$VERSION.tgz")

  # 4. The page files the console and UI images ship, by digest — the list the
  #    release notes' candidate record asks for (docs/release-notes.md).
  find ui -type f ! -name '*.md' ! -path 'ui/tests/*' > "$work/ui-files"
  sort "$work/ui-files" > "$work/ui-files.sorted"
  while IFS= read -r file; do
    printf '%s  %s\n' "$(sha256_of "$file")" "$file"
  done < "$work/ui-files.sorted" > "$out/ui-files.sha256"

  # 5. release.json: what this release is, machine-readable.
  {
    for target in "${TARGETS[@]}"; do
      jq -n --arg target "$target" --arg file "logweir-$target.tar.xz" \
        --arg sha256 "$(sha256_of "$out/logweir-$target.tar.xz")" \
        --rawfile linkage "$in/logweir-$target.linkage.txt" \
        '{target: $target, file: $file, sha256: $sha256, runtime: ($linkage | split("\n") | map(select(length > 0)))}'
    done
  } | jq -s . > "$work/archives.json"
  jq -n --arg tag "$TAG" --arg version "$VERSION" --argjson prerelease "$PRERELEASE" \
    --arg commit "$COMMIT" --arg run "$RUN_URL" --arg ns "$NS" --arg chart_sha "$chart_sha" \
    --slurpfile images "$images" --slurpfile archives "$work/archives.json" '
    {
      tag: $tag, version: $version, prerelease: $prerelease, commit: $commit, run: $run,
      images: {
        publication: $images[0].publication,
        how: $images[0].how,
        refs: ($images[0].images | with_entries(.value += {reference: "docker.io/\($ns)/\(.key):\($tag)@\(.value.digest)"}))
      },
      chart: {
        repository: "oci://registry-1.docker.io/\($ns)/logweir-chart",
        version: $version, app_version: $tag,
        package: "logweir-chart-\($version).tgz", sha256: $chart_sha
      },
      archives: $archives[0]
    }' > "$out/release.json"

  # 6. The release's notes, from the same data.
  release_notes "$out/release.json" > "$notes"

  # 7. SHA256SUMS over every asset, then the whole directory checked again.
  for file in "$out"/*; do
    asset="${file##*/}"
    [[ "$asset" == SHA256SUMS ]] || printf '%s  %s\n' "$(sha256_of "$file")" "$asset"
  done > "$work/SHA256SUMS"
  mv "$work/SHA256SUMS" "$out/SHA256SUMS"
  rm -rf "$work"
  verify "$out"
}

release_notes() { # release.json -> the GitHub Release body (Markdown)
  jq -r '
    def ref(p): .images.refs[p].reference;
    def plats(p): (.images.refs[p].platforms | join(", "));
    "Logweir `\(.tag)`\(if .prerelease then " (pre-release)" else "" end), built from `\(.commit)` by [this run](\(.run)).",
    "",
    "## Install with Helm",
    "",
    "```bash",
    "helm upgrade --install logweir \(.chart.repository) --version \(.chart.version) \\",
    "  -n logweir-system --create-namespace --wait --timeout 10m",
    "```",
    "",
    "The chart pins its four images by digest. They are main CI'"'"'s `sha-\(.images.publication)` publication (\(.images.how)), given the tag `\(.tag)` without a rebuild, so the commit tag and the version tag name the same bytes:",
    "",
    "| Image | Reference | Platforms |",
    "|---|---|---|",
    "| controller | `\(ref("weirkeeper"))` | \(plats("weirkeeper")) |",
    "| runner | `\(ref("logweir"))` | \(plats("logweir")) |",
    "| console | `\(ref("logweir-console"))` | \(plats("logweir-console")) |",
    "| UI | `\(ref("logweir-ui"))` | \(plats("logweir-ui")) |",
    "",
    "`\(.chart.package)` is the chart package itself (sha256 `\(.chart.sha256)`); `helm pull \(.chart.repository) --version \(.chart.version)` returns the same bytes. Before upgrading an installation, read the upgrade order in [docs/release-notes.md](https://github.com/VladyslavHaina/logweir/blob/\(.commit)/docs/release-notes.md): the CRDs move first.",
    "",
    "## The CLI",
    "",
    "| Archive | Needs at run time (measured on the shipped binary) |",
    "|---|---|",
    (.archives[] | "| `\(.file)` | \(.runtime | map(select((startswith("target:") or startswith("system:")) | not)) | join("; ")) |"),
    "",
    "Each archive holds the `logweir` binary, `LICENSE`, `NOTICE`, `THIRD_PARTY_NOTICES.md` and `README.md`. The CLI does not bundle the `kafka-backup` engine: a drill or restore needs `LOGWEIR_ENGINE_BIN`, `LOGWEIR_ENGINE_VERSION` and `LOGWEIR_ENGINE_DIGEST` ([quickstart](https://github.com/VladyslavHaina/logweir/blob/\(.commit)/docs/quickstart.md)). `logweir --version` prints the workspace version, not the tag; `release.json` ties each archive to this tag and commit.",
    "",
    "**Governed approvals.** An approver countersigns a Governed restore with `logweir drill countersign`, run from the archive for their own machine; the key stays on that machine, and nothing in this release holds one. The release run started every archive on its own platform and had each one countersign a throwaway Governed request, then checked that countersignature with the independent verifier'"'"'s primitives.",
    "",
    "## Verify what you downloaded",
    "",
    "```bash",
    "sha256sum -c SHA256SUMS            # macOS: shasum -a 256 -c SHA256SUMS",
    "python3 verify_scorecard.py scorecard.json scorecard.sig signer.pub.pem   # pip install cryptography",
    "```",
    "",
    "`release.json` lists every asset, image digest and the chart; `ui-files.sha256` lists, by digest, the page files the console and UI images ship.",
    "",
    "## Licences",
    "",
    "Logweir is Apache-2.0 (`LICENSE`, `NOTICE`); `THIRD_PARTY_NOTICES.md` lists the Rust crates linked into the binary, and `NOTICE` the C libraries. The runner image redistributes the MIT-licensed `kafka-backup` engine; its licence is attached as `kafka-backup-LICENSE`.",
    "",
    "Apache Kafka® and Kafka® are registered trademarks of the Apache Software Foundation. Logweir is not affiliated with or endorsed by the ASF."
  ' "$1"
}

verify() {
  local dir="${1:?usage: release.sh verify <dir>}" asset listed want name target work
  [[ -f "$dir/SHA256SUMS" ]] || die "$dir has no SHA256SUMS"
  work=$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-verify.XXXXXX")
  # Every asset listed, nothing listed that is absent, every digest right.
  for asset in "$dir"/*; do
    asset="${asset##*/}"
    [[ "$asset" == SHA256SUMS ]] || printf '%s\n' "$asset"
  done > "$work/present"
  : > "$work/listed"
  while read -r want name; do
    [[ -f "$dir/$name" ]] || die "SHA256SUMS lists $name, which $dir does not hold"
    [[ "$(sha256_of "$dir/$name")" == "$want" ]] || die "$name does not match SHA256SUMS"
    printf '%s\n' "$name" >> "$work/listed"
  done < "$dir/SHA256SUMS"
  sort "$work/present" > "$work/present.sorted"
  sort "$work/listed" > "$work/listed.sorted"
  if ! cmp -s "$work/present.sorted" "$work/listed.sorted"; then
    diff "$work/listed.sorted" "$work/present.sorted" >&2 || true
    die "SHA256SUMS and $dir disagree about which assets exist"
  fi
  rm -rf "$work"
  for target in "${TARGETS[@]}"; do
    check_archive "$dir" "$target"
  done
  for asset in verify_scorecard.py LICENSE NOTICE THIRD_PARTY_NOTICES.md kafka-backup-LICENSE release.json ui-files.sha256; do
    [[ -f "$dir/$asset" ]] || die "$dir has no $asset"
  done
  listed=$(jq -r .chart.package "$dir/release.json")
  [[ -f "$dir/$listed" && "$(sha256_of "$dir/$listed")" == "$(jq -r .chart.sha256 "$dir/release.json")" ]] \
    || die "the chart package $listed does not match release.json"
  echo "release.sh: $dir verified ($(wc -l < "$dir/SHA256SUMS" | tr -d ' ') assets)"
}

promote() {
  local images="${1:?usage: release.sh promote <images.json>}" product digest dst found work attempt
  : "${NS:?}" "${TAG:?}" "${GITHUB_STEP_SUMMARY:?}"
  [[ "$TAG" =~ $TAG_RE ]] || die "TAG '$TAG' is not v<semver>"
  work=$(mktemp -d "${TMPDIR:-/tmp}/logweir-release-promote.XXXXXX")
  # 1. DECIDE FOR THE WHOLE SET BEFORE MOVING ANY TAG. A release tag is never
  #    moved: already there with these bytes is a re-run (nothing to do); there
  #    with other bytes is refused; absent is created — and "absent" only when
  #    the registry SAYS not found, never on a read that merely failed
  #    (`published`, which stops the run when it cannot tell).
  : > "$work/create"
  for product in "${PRODUCTS[@]}"; do
    digest=$(jq -er --arg product "$product" '.images[$product].digest | select(test("^sha256:[0-9a-f]{64}$"))' "$images") \
      || die "$images has no sha256 digest for $product"
    anonymous docker buildx imagetools inspect "docker.io/$NS/$product@$digest" > /dev/null \
      || die "docker.io/$NS/$product@$digest is not publicly readable; nothing was tagged"
    dst="docker.io/$NS/$product:$TAG"
    if published "$dst" "$work/found.json"; then
      found=$(jq -r .digest "$work/found.json")
      [[ "$found" == "$digest" ]] || die "$dst already names $found; a release tag is never moved to $digest. Nothing was tagged."
      echo "release.sh: $dst already names $digest"
    else
      printf '%s %s\n' "$product" "$digest" >> "$work/create"
    fi
  done
  # 2. The tags, each a carbon copy of the commit tag's digest.
  while read -r product digest; do
    docker buildx imagetools create --prefer-index=false --tag "docker.io/$NS/$product:$TAG" "docker.io/$NS/$product@$digest"
  done < "$work/create"
  # 3. Read every one back anonymously; a registry may take a moment to serve
  #    a new tag.
  for product in "${PRODUCTS[@]}"; do
    digest=$(jq -r --arg product "$product" '.images[$product].digest' "$images")
    dst="docker.io/$NS/$product:$TAG"
    found=""
    for attempt in 1 2 3 4 5; do
      if anonymous docker buildx imagetools inspect "$dst" --format '{{json .Manifest}}' > "$work/after.json" 2> /dev/null; then
        found=$(jq -r .digest "$work/after.json")
        break
      fi
      [[ "$attempt" == 5 ]] || sleep 6
    done
    [[ "$found" == "$digest" ]] || die "$dst names '$found' after promotion, not $digest (not a carbon copy)"
    echo "- $dst — \`$digest\` (the same bytes as $(jq -r --arg product "$product" '.images[$product].published_as' "$images"))" >> "$GITHUB_STEP_SUMMARY"
  done
  rm -rf "$work"
}

case "${1:-}" in
  validate) validate ;;
  resolve) resolve "${2:-}" ;;
  assemble) assemble "${2:-}" "${3:-}" "${4:-}" ;;
  verify) verify "${2:-}" ;;
  promote) promote "${2:-}" ;;
  *) echo 'usage: release.sh validate|resolve <images.json>|assemble <in> <out> <notes.md>|verify <dir>|promote <images.json>' >&2; exit 2 ;;
esac
