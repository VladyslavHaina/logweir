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

case "${1:-}" in
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
        echo 'A newer main commit exists; immutable SHA tags published, rolling tags left unchanged.' >> "$GITHUB_STEP_SUMMARY"
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
  *) echo 'usage: ci-images.sh check|candidates|promote' >&2; exit 2 ;;
esac
