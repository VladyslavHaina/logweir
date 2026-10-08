# The Logweir runtime image. NOT distroless and NOT musl: the kafka-backup
# engine is dynamically linked against glibc >= 2.36 and needs a CA bundle,
# and Logweir's own binary links libssl3 and libsasl2 (librdkafka). Upstream
# builds FROM rust:bookworm and runs FROM debian:bookworm-slim
# [VERIFIED U/kafka-backup/Dockerfile:9,34,40]; this file does the same.
#
# TWO PLATFORMS, linux/amd64 AND linux/arm64 (PROD-00.2). Name the platform:
#
#     docker build --platform linux/arm64 -t logweir:check .
#     docker build --platform linux/amd64 -t logweir:check .
#
# TO BUILD *AND VERIFY*, WHICH IS THE ONLY WAY THIS FILE'S DEFECTS HAVE EVER
# BEEN FOUND, run `just smoke` — `just image` builds the tag `logweir:check`
# for `$LOGWEIR_IMAGE_PLATFORM` (this host's own by default), and
# `scripts/check-image.sh logweir:check` then asserts both binaries'
# architecture, the linkage, both CLIs, the engine's identity, the approval
# round-trip and both licences.
# Every comment below records a defect that a successful `docker build` did not
# catch, so do not treat a green build as a verified image.
#
# THE ENGINE IS LOGWEIR'S BUILD OF THE VENDORED OSO SOURCE (OD-3, decided
# 2026-10-07; PROD-00.2). The `engine-logweir` stage verifies the tarball's
# checksum, applies `third_party/kafka-backup-patches/` in order, stamps the
# version identity (`kafka-backup 0.23.3+logweir.1`) and compiles it `--locked`
# for the image's platform. OSO publishes its own image for linux/amd64 only;
# building the source is what gives this image an arm64 variant, engine CVE
# fixes on Logweir's schedule, and a version string nobody can mistake for
# OSO's release.
#
# THE ONE-RELEASE ROLLBACK is `--build-arg ENGINE_SOURCE=oso`: the image then
# carries OSO's released binary, copied from the image pinned BY DIGEST below
# (third_party/kafka-backup-binary.digest), and declares OSO's identity
# instead. It exists for linux/amd64 only, because that is all OSO publishes;
# docs/install.md ("Rolling the engine back") is the operator's procedure.
#
# WHICH ENGINE AN IMAGE CARRIES IS WRITTEN INTO IT. Both engine stages leave
# `/out/engine-identity` (two lines, `version=` and `digest=`), and the
# runtime copies it to `/etc/logweir/engine-identity`. Logweir reads it as the
# engine identity it signs into every scorecard and receipt, ahead of the
# `LOGWEIR_ENGINE_VERSION`/`LOGWEIR_ENGINE_DIGEST` variables a Job carries
# (`crates/logweir/src/engine_identity.rs`): the bytes and their identity
# travel in one immutable image, so a rollback image can never sign under the
# default build's name. `scripts/check-image.sh` check 8 holds the file to
# what the engine itself prints.
ARG ENGINE_SOURCE=logweir

# THE RUST COMPILE IS NOT EMULATED — Task 8b, STANDING RULE 10. Every compiling
# stage derives from `cross`, which runs on `$BUILDPLATFORM` (the machine's own
# architecture) and cross-compiles to the image's `$TARGETARCH`: on the arm64
# development host an amd64 image is a cross build and an arm64 image a native
# one; on a GitHub runner of either architecture both are native. QEMU never
# executes anything heavier than the runtime stage's `apt-get` and its `COPY`s.
# Before Task 8b the emulated `cargo build --release` alone took 3027 s of a
# 3044 s build.
# 1.89, not 1.82: Task 12b raised the workspace MSRV (`rust-version = "1.89"` in
# Cargo.toml, `rust-toolchain.toml` channel 1.89.0) when object_store 0.14's
# dependency tree pulled in edition2024 and a 1.89 floor. This line still read
# 1.82 until Task 22, which means every `docker build` of this image failed with
# "package requires rustc 1.89" — the image job's own MSRV, unchecked, had
# drifted below the workspace's. Update the two together, always. The engine
# compiles with the same toolchain.
#
# `--platform=$BUILDPLATFORM` IS THE WHOLE POINT OF TASK 8b and must not be
# removed. `$BUILDPLATFORM` is BuildKit's predeclared platform of the machine
# doing the building, so this stage is native everywhere.
#
# THE RUNTIME STAGE DELIBERATELY CARRIES NO `--platform` and must not gain one:
# it follows the platform of the build, so the binaries the compiling stages
# produce for `$TARGETARCH` land in a runtime of the same architecture.
FROM --platform=$BUILDPLATFORM rust:1.89-bookworm AS cross
ARG TARGETARCH
WORKDIR /src
# ONE apt LAYER. What it installs, by role:
#
# HOST-ARCHITECTURE packages — these run ON the builder, so they are native:
#   cmake          rdkafka vendors and compiles librdkafka from C (ADR 0004)
#   clang/libclang bindgen (via zstd-sys) panics without libclang
#   pkg-config     how the *-sys build scripts locate the target's libraries
#
# THE CROSS TOOLCHAIN FOR `$TARGETARCH` — Task 8b, generalised by PROD-00.2:
#   gcc-<triplet>  the real cross linker, and the compiler the `cc` crate picks
#                  BY NAME for the Rust target. Chosen over cargo-zigbuild
#                  because it is one apt package from the same Debian release
#                  as the runtime stage's glibc, needs no `cargo install` step
#                  and no second C toolchain in the image, and because the
#                  glibc it links against is by construction the one bookworm
#                  ships — which is the property GC10 and the `ldd` check are
#                  about. On a builder of the target's own architecture the
#                  same package is the native compiler under its triplet name,
#                  and the stage degrades to a native build.
#   g++-<triplet>  librdkafka's CMakeLists enables CXX, so cmake's configure
#                  step fails without a C++ compiler FOR THE TARGET even though
#                  no C++ ends up in the binary.
#
# TARGET-ARCHITECTURE (`:$TARGETARCH`) development libraries, for Logweir's own
# link (the engine needs none of them: its TLS is rustls and its C
# dependencies — ring, aws-lc, zstd, lz4, SQLite — are compiled from source by
# the `cc` crate with the cross compiler above):
#   libsasl2-dev   sasl2-sys: "Unable to find libsasl2 on your system"
#   libssl-dev     rdkafka's TLS support links OpenSSL
#   zlib1g-dev     librdkafka's gzip codec
#   libcurl4-openssl-dev
#                  A HEADER, NOT A LIBRARY, and it is not optional.
#                  rdkafka-sys passes `-DWITH_CURL=0`, so librdkafka's
#                  `WITH_OAUTHBEARER_OIDC` is OFF and no libcurl symbol is
#                  linked (`ldd` on the shipped binary proves it) — but
#                  librdkafka's config.h is generated with
#                  `#cmakedefine01 WITH_OAUTHBEARER_OIDC`, which DEFINES the
#                  macro even when it is 0, and rdkafka_conf.c guards
#                  `#include <curl/curl.h>` with `#ifdef`. The header is
#                  therefore always required. MEASURED: without it the build
#                  dies at "curl/curl.h: No such file or directory".
#
# `dpkg --add-architecture` IS GUARDED: on a builder of the target's own
# architecture dpkg refuses to add it, and the `:$TARGETARCH` suffixes resolve
# to the native packages.
#
# `/etc/logweir-cross.env` is the one place the target's names are derived,
# sourced by every compiling `RUN` below: the Rust triple, and
# `PKG_CONFIG_LIBDIR`, which REPLACES the search path — unlike
# PKG_CONFIG_PATH, which only prepends — so a host `.pc` file can never answer
# a query about the target build. Any other `$TARGETARCH` is refused here, by
# name, rather than half-built.
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) triple=x86_64-unknown-linux-gnu; gnu=x86_64-linux-gnu; pkg=x86-64-linux-gnu ;; \
      arm64) triple=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu; pkg=aarch64-linux-gnu ;; \
      *) echo "Dockerfile: REFUSING TARGETARCH=$TARGETARCH; Logweir builds linux/amd64 and linux/arm64" >&2; exit 1 ;; \
    esac; \
    if [ "$(dpkg --print-architecture)" != "$TARGETARCH" ]; then dpkg --add-architecture "$TARGETARCH"; fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      cmake pkg-config clang libclang-dev \
      "gcc-$pkg" "g++-$pkg" \
      "libsasl2-dev:$TARGETARCH" "libssl-dev:$TARGETARCH" "zlib1g-dev:$TARGETARCH" \
      "libcurl4-openssl-dev:$TARGETARCH"; \
    rm -rf /var/lib/apt/lists/*; \
    printf 'TRIPLE=%s\nexport PKG_CONFIG_LIBDIR=/usr/lib/%s/pkgconfig:/usr/share/pkgconfig\n' \
      "$triple" "$gnu" > /etc/logweir-cross.env; \
    . /etc/logweir-cross.env; \
    rustup target add "$TRIPLE"
# cargo-auditable (PROD-00.2), a host tool: `cargo auditable build` embeds the
# exact list of crates linked into each binary in a section of the binary
# itself. That is what lets an SBOM of the IMAGE name the Rust crates inside
# both shipped binaries — the engine's included — rather than only the Debian
# packages beside them (`scripts/ci-images.sh sbom` requires both crate sets),
# and what lets `cargo audit bin` check a shipped engine against advisories
# without its source. Pinned and `--locked`; a separate layer, so a version
# move does not re-run the apt layer.
RUN cargo install cargo-auditable --locked --version 0.7.7
# HOW THE CROSS BUILD IS WIRED, as ENV rather than a `.cargo/config.toml`, so
# that `docker history` shows it and no file arriving through `COPY . .` can
# shadow it. Both targets are named; cargo and the `cc`/`cmake` crates read
# only the variables of the target they build.
#   CARGO_TARGET_..._LINKER  the real cross linker. Cargo drives the link
#                            through gcc rather than ld, so the target's C
#                            runtime objects and library search paths come with
#                            it. THIS is the line that makes the build a cross
#                            build rather than a failed one.
#   CC_/CXX_/AR_             the per-target names the `cc` and `cmake` crates
#                            read; librdkafka, libzstd, libz, ring, aws-lc and
#                            SQLite are all compiled from C here, for the target.
#   PKG_CONFIG_ALLOW_CROSS   pkg-config refuses to answer for a foreign target
#                            without it, and that refusal is the first thing a
#                            cross build of openssl-sys/sasl2-sys hits.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ \
    AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CXX_aarch64_unknown_linux_gnu=aarch64-linux-gnu-g++ \
    AR_aarch64_unknown_linux_gnu=aarch64-linux-gnu-ar \
    PKG_CONFIG_ALLOW_CROSS=1

# THE ENGINE, BUILT FROM THE VENDORED SOURCE (OD-3, PROD-00.2).
#
# ONLY THE ENGINE'S INPUTS ARE COPIED, never `COPY . .`: an edit to Logweir's
# own source must not re-run the engine compile, and an edit to the engine's
# inputs is exactly what must. `scripts/engine-source.sh prepare` is the
# recipe and its refusals are the gate: the tarball must match its `.sha256`,
# the patch folder must follow its README's format, the inputs must give the
# `ENGINE_DIGEST` that `third_party/kafka-backup-build.env` records, every
# patch must apply with exact context, and the version stamp must land. Then
# `cargo build --locked`: the engine's own Cargo.lock, unchanged, is what
# `cargo deny` checks in `scripts/ci-check.sh`.
#
# `--target "$TRIPLE"` puts the binary under `target/<triple>/release/`, so a
# host-architecture build cannot be copied by accident; `scripts/check-image.sh`
# check 6 asserts the shipped e_machine anyway.
FROM cross AS engine-logweir
WORKDIR /engine
COPY scripts/engine-source.sh scripts/
COPY third_party/kafka-backup-build.env third_party/kafka-backup-v*.tar.gz third_party/kafka-backup-v*.tar.gz.sha256 third_party/
COPY third_party/kafka-backup-patches third_party/kafka-backup-patches
RUN bash scripts/engine-source.sh prepare /engine/src
RUN set -eu; \
    . /etc/logweir-cross.env; \
    cd /engine/src; \
    cargo auditable build --locked --release --target "$TRIPLE" --bin kafka-backup; \
    mkdir -p /out; \
    cp "target/$TRIPLE/release/kafka-backup" /out/kafka-backup; \
    cp LOGWEIR-ENGINE-IDENTITY /out/engine-identity

# THE ROLLBACK: OSO's released binary, pinned BY DIGEST. Update this line and
# third_party/kafka-backup-binary.digest together, never separately
# (`scripts/extract-engine.sh` OSO_REFRESH=1 rewrites both). Naming upstream's
# namespace here is permitted: GC14 forbids Logweir publishing under
# osodevops/, not pulling from it. Only `--build-arg ENGINE_SOURCE=oso` builds
# these two stages; BuildKit never pulls an image no requested stage needs.
FROM osodevops/kafka-backup@sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db AS oso-release

# OSO's identity, derived from the same two files the pin is kept in: the
# release named by `ENGINE_VERSION` (the part before `+logweir.`) and the
# image digest. `engine-pin` in the tests holds both to the pin.
FROM --platform=$BUILDPLATFORM debian:bookworm-slim AS engine-oso
COPY --from=oso-release /usr/local/bin/kafka-backup /out/kafka-backup
COPY third_party/kafka-backup-build.env third_party/kafka-backup-binary.digest /tmp/pin/
RUN set -eu; \
    version=$(sed -n 's/^ENGINE_VERSION=//p' /tmp/pin/kafka-backup-build.env); \
    printf 'version=%s\ndigest=%s\n' "${version%%+logweir.*}" "$(cat /tmp/pin/kafka-backup-binary.digest)" \
      > /out/engine-identity

# The engine this build ships: `engine-logweir` unless the rollback is asked for.
FROM engine-${ENGINE_SOURCE} AS engine

# LOGWEIR ITSELF.
#
# CACHE DISCIPLINE IS UNCHANGED, DELIBERATELY. `COPY . .` is still the last
# thing before the compile, so every layer above survives an edit to any
# tracked file and only the compile is re-run — and the compile is minutes. A
# manifest-first split (`COPY Cargo.toml Cargo.lock` plus a stub build) was
# considered and rejected: it needs a stub `src/` for every workspace member
# and breaks quietly the day a member is added. `.dockerignore` already keeps
# `target/` out of the context.
FROM cross AS builder
COPY . .
# `--target "$TRIPLE"` IS LOAD-BEARING TWICE OVER: it is what cross-compiles,
# and it is what puts the binaries under `target/<triple>/release/`, the only
# directory they are copied from, so a host-architecture binary cannot be
# picked up silently. `scripts/check-image.sh` check 6 asserts the shipped
# binary's ELF e_machine anyway, because "cannot silently" is a claim that
# deserves a machine check.
#
# TWO PACKAGES, ONE COMPILE, AND `-p logweir-retention` IS NOT OPTIONAL.
# `crates/weirkeeper/src/controllers/retention_policy.rs` renders every
# enforcement Job with `command: ["logweir-retention"]` and takes the Job's
# image from the controller's runner image — "retention runs a DIFFERENT
# executable in the same image", as that file says beside the line that sets the
# command. Until 2026-09-18 this build named `-p logweir` only, so the binary
# the controller asks for existed in no image this repository produces and
# `mode: Enforce` died at the kubelet with
# `exec: "logweir-retention": executable file not found in $PATH`, exitCode 127
# — observed live on docker-desktop (defect RET-NOIMAGE). One `cargo build`
# with both packages, not two `RUN`s: they share the whole dependency graph, so
# a second invocation would pay the link twice and cache as a separate layer.
RUN set -eu; \
    . /etc/logweir-cross.env; \
    cargo auditable build --release --target "$TRIPLE" -p logweir -p logweir-retention; \
    mkdir -p /out; \
    cp "target/$TRIPLE/release/logweir" "target/$TRIPLE/release/logweir-retention" /out/

FROM debian:bookworm-slim AS runtime
# `libsasl2-2` is REQUIRED BY LOGWEIR'S OWN BINARY, not by the engine. rdkafka
# links librdkafka with SASL support, so `logweir` has a dynamic dependency on
# `libsasl2.so.2`. Without it the image builds, `docker images` looks healthy,
# the ENGINE runs — and every `logweir` invocation dies at the dynamic loader:
#
#   /usr/local/bin/logweir: error while loading shared libraries:
#   libsasl2.so.2: cannot open shared object file: No such file or directory
#
# Verified 2026-09-05 by building the image and running it, which is the only
# thing that finds this. `ldd /usr/local/bin/logweir` inside the image is the
# check: it must report no "not found".
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates libssl3 libsasl2-2 \
    && rm -rf /var/lib/apt/lists/*
# THE ENGINE AND ITS IDENTITY, from whichever engine stage this build chose.
# `/etc/logweir/engine-identity` is what Logweir signs as `engine.version` and
# `engine.digest` (see the header): it is copied from the same stage as the
# binary, so the two cannot come from different builds.
COPY --from=engine   /out/kafka-backup /usr/local/bin/kafka-backup
COPY --from=engine   /out/engine-identity /etc/logweir/engine-identity
# `/out/` holds what the builder copied out of `target/<triple>/release/`. See
# the `--target` note in the builder stage: the architecture-qualified
# directory is what makes a host-architecture binary impossible to copy by
# accident.
COPY --from=builder  /out/logweir  /usr/local/bin/logweir
# THE ENFORCEMENT BINARY, BESIDE THE EVERYDAY ONE AND NEVER INSTEAD OF IT.
#
# WHY IT IS IN THIS IMAGE AND NOT AN IMAGE OF ITS OWN. The controller names ONE
# image for the enforcement Job — its runner image — and overrides only the
# container's `command`. A second image would need a second value on the chart,
# a second variable on the Deployment, a second publish job and a second digest
# to keep in step with the first; the live wave that found RET-NOIMAGE proved
# the fix by building exactly this image plus this one binary
# (`e2e/k8s/d3/Dockerfile.retention`, a test fixture that this line retires).
#
# IT DOES NOT WIDEN THE DELETION BOUNDARY, and the boundary is a LINKAGE claim,
# not an image one. `scripts/check-no-archive-write.sh` check 3 asserts from
# `cargo metadata` that the set of workspace crates reaching `logweir-reaper` —
# the one crate that can delete from an object store — is exactly
# {logweir-retention}. `/usr/local/bin/logweir`, the image's ENTRYPOINT and the
# binary every backup, restore, verify and check Job runs, still links no delete
# path (`crates/weirkeeper/tests/retention_policy_controller.rs::the_everyday_binary_links_no_delete_path`).
# What a pod can delete is decided by the credential the Job mounts: the
# retention pod is the only one that mounts a delete-capable grant, and the
# controller drops the destination's archive credential from that pod on purpose
# (`retention_policy.rs`, `build_job`). This file ships an executable; it grants
# nothing.
COPY --from=builder  /out/logweir-retention  /usr/local/bin/logweir-retention
COPY third_party/LICENSE-MIT /usr/share/licenses/kafka-backup/LICENSE
COPY LICENSE NOTICE /usr/share/licenses/logweir/
# THE GENERATED THIRD-PARTY INVENTORY — Task 29, interface I30, Global
# Constraint 15. MIT, BSD-2-Clause, BSD-3-Clause and Apache-2.0 each require the
# copyright notice to travel with the redistributed binary, and the binary above
# is statically linked against 392 packages' worth of them. LICENSE says what
# Logweir may be used under; NOTICE says what Logweir owes and cannot be
# generated; this says who the 392 are. `scripts/gen-third-party-notices.sh`
# writes it and `crates/logweir/tests/doc_lint.rs` keeps it honest.
COPY THIRD_PARTY_NOTICES.md /usr/share/licenses/logweir/
# THE ORG-ROOT ANCHOR, BAKED AT BUILD TIME — Task 23, stage-2 Task 16's T1.
#
# WHAT THIS FILE IS. One line, `sha256:` + 64 hex: the SHA-256 of the
# SubjectPublicKeyInfo DER encoding of the org root's PUBLIC key
# (`third_party/org-root.pub.pem`), the same definition of "fingerprint"
# `docs/keys.md` gives for a signing key. It is a PUBLIC key's hash. No private
# key material is in this image, in this repository, or in this line.
#
# WHY IT IS COPIED AND NOT MOUNTED. The point of the anchor is that whoever
# controls the cluster cannot change it without producing a DIFFERENT IMAGE: a
# ConfigMap or a Secret is exactly the projection a compromised control plane
# owns. `COPY` puts it on the read-only rootfs of an image referenced by
# digest, which is the only arrangement in which "the controller may project
# any bytes; it cannot make them match" is true.
#
# NOTHING READS IT YET, AND THAT IS STATED RATHER THAN IMPLIED. Phase 0 does
# not open this path — `crates/logweir/tests/manifest_lint.rs`'s
# `the_fingerprint_is_not_read_at_runtime` asserts no `.rs` under `crates/`
# does — because the anchor must EXIST before the check that verifies against
# it (G5's pod-side `--org-key` refusal, Phase 3). Shipping the file now is
# what makes that later check a one-line comparison instead of a migration.
#
# THE BYTE-IDENTITY GATE is `just check-org-root`, which `cat`s this path out
# of BOTH images and `diff`s each against the checked-in file, plus
# `scripts/check-image-weirkeeper.sh` check 4 for the controller image and
# `scripts/check-dod.sh`'s org-root arm for the file itself.
COPY third_party/org-root.fingerprint /etc/logweir/
ENV LOGWEIR_ENGINE_BIN=/usr/local/bin/kafka-backup
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/logweir"]
