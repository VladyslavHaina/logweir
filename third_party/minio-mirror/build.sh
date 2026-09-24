#!/usr/bin/env bash
# third_party/minio-mirror/build.sh — build the two MinIO mirror images for
# linux/amd64 and linux/arm64, and with --push publish them.
#
#   bash third_party/minio-mirror/build.sh          # build, load into the local image store
#   bash third_party/minio-mirror/build.sh --push   # build, push, print the index digests
#
# `--load` of a multi-platform image needs Docker's containerd image store
# (Docker Desktop's default); `--push` needs a `docker login` to the target
# registry, and `jq`. Override the repositories with MINIO_MIRROR_REPO / MC_MIRROR_REPO.
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

mode=--load
case "${1:-}" in
  --push) mode=--push ;;
  "") ;;
  *) echo "usage: build.sh [--push]" >&2; exit 2 ;;
esac

rev="$(git -C "$H" rev-parse HEAD)"
if [ "$mode" = --push ]; then
  if ! git -C "$H" diff --quiet HEAD -- . || [ -n "$(git -C "$H" status --porcelain -- .)" ]; then
    echo "build.sh: third_party/minio-mirror/ has uncommitted changes; commit them before --push" >&2
    exit 1
  fi
fi

build() {
  local dockerfile="$1" ref="$2"
  echo "build.sh: $ref ($PLATFORMS, recipe $rev)" >&2
  docker buildx build \
    --platform "$PLATFORMS" \
    --label "io.logweir.mirror.recipe-revision=$rev" \
    -f "$H/$dockerfile" -t "$ref" "$mode" "$H"
}

build Dockerfile.mc "$MC_REPO:$MC_RELEASE"
build Dockerfile.minio "$MINIO_REPO:$MINIO_RELEASE"

if [ "$mode" = --push ]; then
  for ref in "$MC_REPO:$MC_RELEASE" "$MINIO_REPO:$MINIO_RELEASE"; do
    digest="$(docker buildx imagetools inspect "$ref" --format '{{json .Manifest}}' | jq -r .digest)"
    echo "$ref -> ${ref%:*}@$digest"
  done
fi
