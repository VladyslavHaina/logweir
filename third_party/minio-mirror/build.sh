#!/usr/bin/env bash
# third_party/minio-mirror/build.sh — build the two MinIO mirror images for
# linux/amd64 and linux/arm64, and publish them.
#
#   bash third_party/minio-mirror/build.sh                # build, load into the local image store
#   bash third_party/minio-mirror/build.sh --push-loaded  # push what the line above loaded
#   bash third_party/minio-mirror/build.sh --push         # build and push in one go
#
# THE DIGESTS THE TREE PINS ARE THE LOADED IMAGES' OWN. The build is not
# bit-reproducible (image `created` times differ per build), so the images are
# built once, loaded, pinned by their index digests, and pushed as they are:
# `--push-loaded` pushes the two local tags and refuses unless the registry
# answers with exactly the local index digest. `--push` is for a new release,
# where the pins are then updated to the digests it prints.
#
# `--load` of a multi-platform image needs Docker's containerd image store
# (Docker Desktop's default); a push needs a `docker login` to the registry.
# Needs `jq`. Override the repositories with MINIO_MIRROR_REPO / MC_MIRROR_REPO.
# Go is cross-compiled on the build host's own platform (Dockerfile.*'s build
# stages are `--platform=$BUILDPLATFORM`); only the final stage's one
# `chmod -R 777 /usr/bin` runs under emulation for the foreign architecture.
#
# A PUSH REFUSES A DIRTY RECIPE: the images carry the Logweir commit their
# recipe came from (label io.logweir.mirror.recipe-revision), and that label is
# only true of a committed, unmodified third_party/minio-mirror/.
set -euo pipefail

H="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MINIO_RELEASE=RELEASE.2025-09-07T16-13-09Z
MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
MINIO_REPO="${MINIO_MIRROR_REPO:-docker.io/vladyslavhaina/minio-mirror}"
MC_REPO="${MC_MIRROR_REPO:-docker.io/vladyslavhaina/mc-mirror}"
PLATFORMS="${MIRROR_PLATFORMS:-linux/amd64,linux/arm64}"
REFS=("$MC_REPO:$MC_RELEASE" "$MINIO_REPO:$MINIO_RELEASE")

mode=load
case "${1:-}" in
  --push) mode=push ;;
  --push-loaded) mode=push-loaded ;;
  "") ;;
  *) echo "usage: build.sh [--push | --push-loaded]" >&2; exit 2 ;;
esac

rev="$(git -C "$H" rev-parse HEAD)"
if ! git -C "$H" diff --quiet HEAD -- . || [ -n "$(git -C "$H" status --porcelain -- .)" ]; then
  if [ "$mode" = push ]; then
    echo "build.sh: third_party/minio-mirror/ has uncommitted changes; commit them before --push" >&2
    exit 1
  fi
  rev="$rev-dirty"
fi

registry_digest() {
  docker buildx imagetools inspect "$1" --format '{{json .Manifest}}' | jq -r .digest
}

if [ "$mode" = push-loaded ]; then
  for ref in "${REFS[@]}"; do
    label="$(docker image inspect "$ref" --format '{{index .Config.Labels "io.logweir.mirror.recipe-revision"}}')"
    case "$label" in
      ""|*-dirty) echo "build.sh: $ref was built from an uncommitted recipe ($label); rebuild it" >&2; exit 1 ;;
    esac
    local_id="$(docker image inspect "$ref" --format '{{.Id}}')"
    docker push "$ref"
    pushed="$(registry_digest "$ref")"
    if [ "$pushed" != "$local_id" ]; then
      echo "build.sh: $ref: the registry holds $pushed, the local image is $local_id" >&2
      exit 1
    fi
    echo "$ref -> ${ref%:*}@$pushed (recipe $label)"
  done
  exit 0
fi

build() {
  local dockerfile="$1" ref="$2"
  echo "build.sh: $ref ($PLATFORMS, recipe $rev)" >&2
  docker buildx build \
    --platform "$PLATFORMS" \
    --label "io.logweir.mirror.recipe-revision=$rev" \
    --provenance=false --sbom=false \
    -f "$H/$dockerfile" -t "$ref" "--$mode" "$H"
}

build Dockerfile.mc "${REFS[0]}"
build Dockerfile.minio "${REFS[1]}"

for ref in "${REFS[@]}"; do
  if [ "$mode" = push ]; then
    echo "$ref -> ${ref%:*}@$(registry_digest "$ref")"
  else
    echo "$ref loaded: index $(docker image inspect "$ref" --format '{{.Id}}')"
  fi
done
