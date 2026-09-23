#!/usr/bin/env bash
# Build jobs smoke-test native images before uploading candidates. Only the
# promotion job moves public tags, after every platform has passed.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${GITHUB_SHA:?}" "${NS:?}"
[[ "$GITHUB_SHA" =~ ^[0-9a-f]{40}$ && "$NS" =~ ^[a-z0-9][a-z0-9_-]*$ ]] || exit 1
products=(weirkeeper logweir-ui logweir-console)
if [[ "${ARCH:-}" == amd64 ]]; then products+=(logweir); fi

check_image() {
  local product="$1" ref="$2"
  case "$product" in
    logweir) bash scripts/check-image.sh "$ref" ;;
    weirkeeper) bash scripts/check-image-weirkeeper.sh "$ref" ;;
    logweir-ui) bash scripts/check-image-ui.sh "$ref" ;;
    logweir-console) bash scripts/check-image-api.sh "$ref" ;;
  esac
  local revision
  revision=$(docker image inspect "$ref" --format '{{index .Config.Labels "org.opencontainers.image.revision"}}')
  [[ "$revision" == "$GITHUB_SHA" ]] || { echo "Wrong revision in $ref" >&2; exit 1; }
}

# ---------------------------------------------------------------------------
# THE CHART, PUBLISHED BESIDE THE IMAGES IT NAMES (chart gap G4)
# ---------------------------------------------------------------------------
# `chart-package <dir>` writes `logweir-chart-<version>.tgz` into <dir>;
# `chart` packages, pushes it to oci://registry-1.docker.io/$NS and verifies
# the registry serves back the same bytes. Both take TAG — the image tag the
# promote step just published — and version the chart WITH it:
#
#   TAG sha-<commit> (a main push)  version <Chart.yaml version>-sha-<commit>
#   TAG v<semver>    (a release)    version <semver>
#
# and in both cases appVersion = TAG and the four Logweir image defaults are
# rewritten from `:latest` to `docker.io/$NS/<image>:$TAG`, so
# `helm install oci://registry-1.docker.io/$NS/logweir-chart --version <v>`
# installs exactly the images this run published — no `--set` needed. The
# chart is renamed `logweir-chart` in the package only: Docker Hub names a
# chart's repository after the chart, and `$NS/logweir` is the runner image.
# The bootstrap image stays the chart's reviewed digest pin (identity.*).
CHART_SRC=charts/logweir
CHART_NAME=logweir-chart
CHART_REPOSITORY="oci://registry-1.docker.io/$NS"

chart_version() {
  local base
  base=$(awk '/^version:/ { print $2; exit }' "$CHART_SRC/Chart.yaml")
  [[ "$base" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Chart.yaml version '$base' is not X.Y.Z" >&2; return 1; }
  if [[ "$TAG" == "sha-$GITHUB_SHA" ]]; then
    echo "$base-sha-$GITHUB_SHA"
  elif [[ "$TAG" =~ ^v([0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?)$ ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    echo "TAG '$TAG' is neither sha-\$GITHUB_SHA nor a v<semver> release tag" >&2
    return 1
  fi
}

chart_package() {
  local out="$1" version work line from rewritten=0
  version=$(chart_version) || return 1
  work=$(mktemp -d "${TMPDIR:-/tmp}/logweir-chart.XXXXXX")
  cp -R "$CHART_SRC" "$work/$CHART_NAME"
  # Chart.yaml: the package's name. Version and appVersion are set by
  # `helm package` below, the one place both are decided.
  awk -v name="$CHART_NAME" '/^name: / && !done { print "name: " name; done = 1; next } { print }' \
    "$CHART_SRC/Chart.yaml" > "$work/$CHART_NAME/Chart.yaml"
  : > "$work/values.yaml"
  while IFS= read -r line || [[ -n "$line" ]]; do
    for image in weirkeeper logweir logweir-console logweir-ui; do
      from="docker.io/vladyslavhaina/$image:latest"
      if [[ "$line" == *"$from"* ]]; then
        # Prefix + replacement + suffix: literal on every bash, with no
        # pattern or escape rules in the replacement.
        line="${line%%"$from"*}docker.io/$NS/$image:$TAG${line#*"$from"}"
        rewritten=$((rewritten + 1))
      fi
    done
    printf '%s\n' "$line" >> "$work/values.yaml"
  done < "$CHART_SRC/values.yaml"
  # FOUR, EXACTLY: controllerImage, runnerImage, api.console.image, ui.image.
  # A fifth would be an image this chart does not own; three would be one left
  # at `:latest`, which the published chart must never install.
  [[ "$rewritten" -eq 4 ]] || { echo "rewrote $rewritten image defaults, expected 4" >&2; rm -rf "$work"; return 1; }
  if grep -q ':latest' "$work/values.yaml"; then
    echo "the packaged values.yaml still names a :latest image" >&2; rm -rf "$work"; return 1
  fi
  mv "$work/values.yaml" "$work/$CHART_NAME/values.yaml"
  helm lint "$work/$CHART_NAME" >/dev/null || { rm -rf "$work"; return 1; }
  helm template logweir "$work/$CHART_NAME" -n logweir-system >/dev/null || { rm -rf "$work"; return 1; }
  mkdir -p "$out"
  helm package "$work/$CHART_NAME" --version "$version" --app-version "$TAG" -d "$out" >/dev/null \
    || { rm -rf "$work"; return 1; }
  rm -rf "$work"
  [[ -f "$out/$CHART_NAME-$version.tgz" ]] || return 1
  echo "$out/$CHART_NAME-$version.tgz"
}

case "${1:-}" in
  chart-package)
    : "${TAG:?}" "${2:?usage: ci-images.sh chart-package <dir>}"
    chart_package "$2"
    ;;
  chart)
    : "${TAG:?}" "${GITHUB_OUTPUT:?}" "${GITHUB_STEP_SUMMARY:?}"
    : "${DOCKERHUB_USERNAME:?}" "${DOCKERHUB_TOKEN:?}"
    [[ "$TAG" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || exit 1
    # The images this chart names must already be PUBLIC under TAG: the chart
    # is published AFTER the promote step, never before, so no published
    # chart can point at a tag that does not exist. Asked ANONYMOUSLY -- an
    # empty Docker config, not the credentials the login step left behind --
    # so "exists for the publisher" cannot pass for "pullable by anyone".
    anonymous_docker="$(mktemp -d)"
    for product in logweir weirkeeper logweir-ui logweir-console; do
      DOCKER_CONFIG="$anonymous_docker" docker buildx imagetools inspect "docker.io/$NS/$product:$TAG" >/dev/null
    done
    rm -rf "$anonymous_docker"
    dir=$(mktemp -d)
    package=$(chart_package "$dir")
    version=$(chart_version)
    # The SAME credentials the image steps use, handed to Helm's own registry
    # client on stdin; nothing is written to the job log.
    printf '%s' "$DOCKERHUB_TOKEN" | helm registry login registry-1.docker.io \
      --username "$DOCKERHUB_USERNAME" --password-stdin
    helm push "$package" "$CHART_REPOSITORY"
    # Verify by content, not by exit code, and ANONYMOUSLY: pull what the
    # registry now serves under that version with an empty Helm registry
    # configuration and compare the bytes with what was pushed. A chart
    # repository Docker Hub created private on its first push fails here,
    # loudly, instead of publishing a chart nobody can install.
    mkdir -p "$dir/pulled" "$dir/anonymous"
    HELM_REGISTRY_CONFIG="$dir/anonymous/config.json" \
      helm pull "$CHART_REPOSITORY/$CHART_NAME" --version "$version" -d "$dir/pulled"
    pushed=$(sha256sum "$package" | awk '{print $1}')
    served=$(sha256sum "$dir/pulled/$CHART_NAME-$version.tgz" | awk '{print $1}')
    [[ "$pushed" == "$served" ]] || { echo "the registry serves different chart bytes" >&2; exit 1; }
    echo "chart_version=$version" >> "$GITHUB_OUTPUT"
    echo "chart_sha256=$pushed" >> "$GITHUB_OUTPUT"
    echo "- $CHART_REPOSITORY/$CHART_NAME --version $version (appVersion $TAG) — package sha256 \`$pushed\`" >> "$GITHUB_STEP_SUMMARY"
    ;;
  check|candidates)
    [[ "${ARCH:-}" == amd64 || "${ARCH:-}" == arm64 ]] || exit 1
    for product in "${products[@]}"; do
      if [[ "$1" == check ]]; then
        check_image "$product" "$product:check"
        continue
      fi
      : "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}"
      repo="docker.io/$NS/$product"
      ref="$repo:build-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-$ARCH"
      docker tag "$product:check" "$ref"
      docker push "$ref"
      digest=$(docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' | jq -er .digest)
      [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || exit 1
      # Execute the exact digest returned by the registry on its native host.
      docker pull --platform "linux/$ARCH" "$repo@$digest"
      check_image "$product" "$repo@$digest"
      mkdir -p image-digests
      jq -n --arg product "$product" --arg arch "$ARCH" --arg digest "$digest" \
        --arg sha "$GITHUB_SHA" '{product:$product,arch:$arch,digest:$digest,sha:$sha}' \
        > "image-digests/$product-$ARCH.json"
    done
    ;;
  promote)
    : "${TAG:?}" "${GITHUB_OUTPUT:?}" "${GITHUB_STEP_SUMMARY:?}"
    [[ "$TAG" =~ ^[a-zA-Z0-9_][a-zA-Z0-9_.-]{0,127}$ ]] || exit 1
    if [[ "${PROMOTE_LATEST:-false}" == true ]]; then
      [[ "${GITHUB_REF:-}" == refs/heads/main && "$TAG" == "sha-$GITHUB_SHA" ]] || exit 1
    fi
    products=(logweir weirkeeper logweir-ui logweir-console)
    # Validate the complete candidate set before moving any public tag.
    for product in "${products[@]}"; do
      arches=(amd64)
      if [[ "$product" != logweir ]]; then arches+=(arm64); fi
      for arch in "${arches[@]}"; do
        file="image-digests/$product-$arch.json"
        jq -e --arg sha "$GITHUB_SHA" --arg product "$product" --arg arch "$arch" \
          '.sha == $sha and .product == $product and .arch == $arch and (.digest | test("^sha256:[0-9a-f]{64}$"))' "$file" >/dev/null
        digest=$(jq -r .digest "$file")
        docker buildx imagetools inspect "docker.io/$NS/$product@$digest" >/dev/null
      done
    done
    for product in "${products[@]}"; do
      repo="docker.io/$NS/$product"
      refs=("$repo@$(jq -r .digest "image-digests/$product-amd64.json")")
      if [[ "$product" != logweir ]]; then
        refs+=("$repo@$(jq -r .digest "image-digests/$product-arm64.json")")
      fi
      docker buildx imagetools create --prefer-index=false --tag "$repo:$TAG" "${refs[@]}"
      digest=$(docker buildx imagetools inspect "$repo:$TAG" --format '{{json .Manifest}}' | jq -er .digest)
      [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || exit 1
      printf '%s\n' "$digest" > "image-digests/$product.published"
      case "$product" in
        logweir) output=runner_digest ;;
        weirkeeper) output=controller_digest ;;
        logweir-ui) output=ui_digest ;;
        logweir-console) output=console_digest ;;
      esac
      echo "$output=$digest" >> "$GITHUB_OUTPUT"
      echo "- $repo:$TAG — \`$digest\`" >> "$GITHUB_STEP_SUMMARY"
    done
    if [[ "${PROMOTE_LATEST:-false}" == true ]]; then
      # A slower, older main run must not roll latest back. Promotion jobs are
      # serialized; version releases never move these rolling tags.
      head=$(git ls-remote origin refs/heads/main | awk '{print $1}')
      if [[ "$head" != "$GITHUB_SHA" ]]; then
        echo 'A newer main commit exists; SHA tags published, rolling tags left unchanged.' >> "$GITHUB_STEP_SUMMARY"
        exit 0
      fi
      for product in "${products[@]}"; do
        repo="docker.io/$NS/$product"
        digest=$(cat "image-digests/$product.published")
        docker buildx imagetools create --prefer-index=false --tag "$repo:main" --tag "$repo:latest" "$repo@$digest"
        for rolling in main latest; do
          actual=$(docker buildx imagetools inspect "$repo:$rolling" --format '{{json .Manifest}}' | jq -er .digest)
          [[ "$actual" == "$digest" ]] || { echo "Tag verification failed: $repo:$rolling" >&2; exit 1; }
        done
      done
      echo 'Verified main and latest tags for all four images.' >> "$GITHUB_STEP_SUMMARY"
    fi
    ;;
  *) echo 'usage: ci-images.sh check|candidates|promote|chart|chart-package <dir>' >&2; exit 2 ;;
esac
