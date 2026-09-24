# docker.io/vladyslavhaina/mc-mirror:RELEASE.2025-08-13T08-35-41Z
#
# An UNMODIFIED rebuild, from upstream source, of the MinIO Client image
# quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z (index sha256:a7fe349e…), which
# MinIO withdrew from Docker Hub on 2026-09-11 and from anonymous quay.io pulls
# on 2026-09-24. README.md beside this file carries the provenance, the licence
# (AGPL-3.0-only) and how to rebuild it; `build.sh` is the command.
#
# Upstream's own recipe (mc@7394ce0d, Dockerfile.release) ADDs the release
# binary from dl.min.io onto ubi9/ubi-micro. dl.min.io now answers 410 Gone, so
# this file COMPILES that binary from the tagged source instead, the way
# upstream's Makefile does (`go build -tags kqueue -trimpath --ldflags
# "$(go run buildscripts/gen-ldflags.go)"`), and then REFUSES TO FINISH unless
# the result is byte-identical to the binary upstream published and signed for
# this release and architecture (upstream/*.sha256sum, *.minisig, verified with
# MinIO's minisign key). Go is cross-compiled on $BUILDPLATFORM for
# $TARGETARCH: nothing is compiled under emulation.

ARG GO_IMAGE=docker.io/library/golang:1.24.6-alpine@sha256:c8c5f95d64aa79b6547f3b626eb84b16a7ce18a139e3e9ca19a8c078b85ba80d
# The CA bundle only: arch-independent, and byte-identical to the one in the
# upstream image (upstream copied it from ubi9/ubi-minimal after a microdnf update).
ARG CA_IMAGE=registry.access.redhat.com/ubi9/ubi-minimal:9.6@sha256:34880b64c07f28f64d95737f82f891516de9a3b43583f39970f7bf8e4cfa48b7
# The exact base layer of the upstream image: ubi9/ubi-micro 9.6, build 1754467928.
ARG BASE_IMAGE=registry.access.redhat.com/ubi9/ubi-micro:9.6-1754467928@sha256:f5c5213d2969b7b11a6666fc4b849d56b48d9d7979b60a37bb853dff0255c14b

FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
ARG MC_COMMIT=7394ce0dd2a80935aded936b09fa12cbb3cb8096
ARG MINISIGN_VERSION=v0.2.1
ARG MINIO_MINISIGN_PUBKEY=RWTx5Zr1tiHQLwG9keckT0c45M3AGeHD6IvimQHpyRywVWGbP1aVSGav
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
RUN apk add --no-cache git
RUN git -c advice.detachedHead=false clone --quiet --depth 1 --branch "$MC_RELEASE" https://github.com/minio/mc.git /src/mc \
 && test "$(git -C /src/mc rev-parse HEAD)" = "$MC_COMMIT"
RUN --mount=type=cache,target=/go/pkg/mod GOBIN=/usr/local/bin go install "aead.dev/minisign/cmd/minisign@${MINISIGN_VERSION}"
COPY upstream/ /upstream/
# `.mirror-build` is an UNTRACKED file and changes no compiled byte: it makes
# the VCS stamp read `vcs.modified=true` (module version `+dirty`), exactly as
# upstream's release binaries' does. Without it the binary differs from
# upstream's in the embedded build info only, and the checks below would fail.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    set -eu; \
    cd /src/mc; \
    touch .mirror-build; \
    LDFLAGS="$(MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go)"; \
    echo "mc ldflags: $LDFLAGS"; \
    GOOS="$TARGETOS" GOARCH="$TARGETARCH" go build -tags kqueue -trimpath -ldflags "$LDFLAGS" -o /out/mc .; \
    f="/upstream/mc.linux-$TARGETARCH.$MC_RELEASE"; \
    got="$(sha256sum /out/mc | cut -d' ' -f1)"; want="$(cut -d' ' -f1 "$f.sha256sum")"; \
    echo "mc linux/$TARGETARCH sha256 $got (upstream $want)"; \
    test "$got" = "$want"; \
    minisign -Vm /out/mc -x "$f.minisig" -P "$MINIO_MINISIGN_PUBKEY"; \
    install -d -m 0755 /rootfs/usr/bin /rootfs/licenses; \
    install -m 0711 /out/mc /rootfs/usr/bin/mc; \
    install -m 0664 /src/mc/CREDITS /src/mc/LICENSE /rootfs/licenses/

FROM --platform=$BUILDPLATFORM ${CA_IMAGE} AS ca

FROM ${BASE_IMAGE}
ARG MC_RELEASE=RELEASE.2025-08-13T08-35-41Z
ARG MC_COMMIT=7394ce0dd2a80935aded936b09fa12cbb3cb8096
LABEL maintainer="Logweir project mirror (docker.io/vladyslavhaina), not MinIO, Inc." \
      org.opencontainers.image.title="mc-mirror" \
      org.opencontainers.image.description="Unmodified rebuild of the MinIO Client (mc) ${MC_RELEASE} from upstream source (https://github.com/minio/mc, commit ${MC_COMMIT}); the mc binary is byte-identical to upstream's signed release binary. Corresponding source: the upstream repository at that commit. Rebuilt because MinIO withdrew its public images; a stopgap for Logweir's test stacks, not affiliated with MinIO, Inc. Recipe: https://github.com/VladyslavHaina/logweir/tree/main/third_party/minio-mirror" \
      org.opencontainers.image.source="https://github.com/minio/mc" \
      org.opencontainers.image.revision="${MC_COMMIT}" \
      org.opencontainers.image.version="${MC_RELEASE}" \
      org.opencontainers.image.licenses="AGPL-3.0-only" \
      org.opencontainers.image.url="https://github.com/VladyslavHaina/logweir/tree/main/third_party/minio-mirror"
# NO `COPY --chmod` here, on purpose: BuildKit applies --chmod to the parent
# directories a COPY creates as well, and a 0444 /etc/pki/ca-trust/ is a
# directory a non-root container cannot enter. Modes come from the stages
# (upstream's: the bundle 0444 as ubi-minimal ships it, LICENSE and CREDITS
# 0664, mc 0711); a directory a COPY creates is 0755, as upstream's are.
COPY --from=ca /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem /etc/pki/ca-trust/extracted/pem/
COPY --from=build /rootfs/licenses/ /licenses/
COPY --from=build /rootfs/usr/bin/mc /usr/bin/mc
ENTRYPOINT ["mc"]
