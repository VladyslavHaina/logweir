# The Logweir runtime image. NOT distroless and NOT musl: the extracted
# kafka-backup binary is dynamically linked against glibc >= 2.36 and libssl3
# and needs a CA bundle, because upstream builds FROM rust:bookworm and runs
# FROM debian:bookworm-slim [VERIFIED U/kafka-backup/Dockerfile:9,34,40].
#
# BUILD IT FOR linux/amd64, EXPLICITLY:
#
#     docker build --platform linux/amd64 -t logweir:v0.1.0 .
#
# TO BUILD *AND VERIFY*, WHICH IS THE ONLY WAY THIS FILE'S DEFECTS HAVE EVER
# BEEN FOUND, run `just smoke` — `just image` builds the tag `logweir:check`
# with the platform above, and `scripts/check-image.sh logweir:check` then
# asserts the binary's architecture, the linkage, both CLIs, the approval
# round-trip and both licences.
# Every comment below records a defect that a successful `docker build` did not
# catch, so do not treat a green build as a verified image.
#
# Upstream publishes `osodevops/kafka-backup` for linux/amd64 ONLY. On an arm64
# host a plain `docker build` fails at the `engine` stage below with
# "no match for platform in manifest: not found" — verified on darwin/arm64,
# 2026-09-05. The whole image is built for one platform on purpose: pinning
# only the engine stage to amd64 would drop an amd64 ELF into an arm64 runtime,
# which is worse than a build failure because it fails at drill time instead.
#
# THE RUST COMPILE IS NOT EMULATED — Task 8b, STANDING RULE 10. The builder
# stage runs on `$BUILDPLATFORM` (the machine's own architecture) and
# cross-compiles to `x86_64-unknown-linux-gnu`; only `engine` and `runtime` are
# linux/amd64, so on an arm64 host QEMU never executes anything heavier than
# the runtime stage's `apt-get` and four `COPY`s. Before that change the
# emulated `cargo build --release` alone took 3027 s of a 3044 s build.
# 1.89, not 1.82: Task 12b raised the workspace MSRV (`rust-version = "1.89"` in
# Cargo.toml, `rust-toolchain.toml` channel 1.89.0) when object_store 0.14's
# dependency tree pulled in edition2024 and a 1.89 floor. This line still read
# 1.82 until Task 22, which means every `docker build` of this image failed with
# "package requires rustc 1.89" — the image job's own MSRV, unchecked, had
# drifted below the workspace's. Update the two together, always.
#
# `--platform=$BUILDPLATFORM` IS THE WHOLE POINT OF TASK 8b and must not be
# removed. `$BUILDPLATFORM` is BuildKit's predeclared platform of the machine
# doing the building, so this stage is native everywhere: arm64 on the
# development host, amd64 on a GitHub runner (where the cross build below
# degrades to a native one and costs nothing).
#
# THE OTHER TWO STAGES DELIBERATELY CARRY NO `--platform` and must not gain
# one: they follow the platform of the build, which `just image` and
# release.yml state as `--platform linux/amd64`. That is what keeps a plain
# `docker build` on an arm64 host FAILING at the engine stage instead of
# quietly assembling an arm64 runtime — see the header above.
FROM --platform=$BUILDPLATFORM rust:1.89-bookworm AS builder
WORKDIR /src
# ONE apt LAYER STILL, not two: the cross toolchain is added to the packages
# that were already here rather than to a layer of its own. Task 8's brief said
# "do not add layers"; Task 8b's addendum relaxes that for the cross toolchain
# only, so every addition is named below and the total added is ONE `RUN` (the
# `rustup target add` beneath this one).
#
# HOST-ARCHITECTURE packages — these run ON the builder, so they are native:
#   cmake          rdkafka vendors and compiles librdkafka from C (ADR 0004)
#   clang/libclang bindgen (via zstd-sys) panics without libclang
#   pkg-config     how the *-sys build scripts locate the target's libraries
#
# CROSS TOOLCHAIN — added by Task 8b:
#   gcc-x86-64-linux-gnu  the real cross linker, and the compiler the `cc`
#                         crate picks BY NAME for x86_64-unknown-linux-gnu.
#                         Chosen over cargo-zigbuild because it is one apt
#                         package from the same Debian release as the runtime
#                         stage's glibc, needs no `cargo install` step and no
#                         second C toolchain in the image, and because the
#                         glibc it links against is by construction the one
#                         bookworm ships — which is the property GC10 and the
#                         `ldd` check are about. cargo-zigbuild would have
#                         added a Zig download, a cargo subcommand build and a
#                         second answer to "which glibc" for no gain here.
#   g++-x86-64-linux-gnu  librdkafka's CMakeLists enables CXX, so cmake's
#                         configure step fails without a C++ compiler FOR THE
#                         TARGET even though no C++ ends up in the binary.
#
# TARGET-ARCHITECTURE (`:amd64`) development libraries. These REPLACE the
# host-architecture copies that stood here before Task 8b: the link is now for
# x86_64, so the arm64 ones linked nothing, and leaving them would let a
# misconfigured pkg-config answer a target query with a host library.
#   libsasl2-dev:amd64  sasl2-sys: "Unable to find libsasl2 on your system"
#   libssl-dev:amd64    rdkafka's TLS support links OpenSSL
#   zlib1g-dev:amd64    librdkafka's gzip codec
#   libcurl4-openssl-dev:amd64
#                       A HEADER, NOT A LIBRARY, and it is not optional.
#                       rdkafka-sys passes `-DWITH_CURL=0`, so librdkafka's
#                       `WITH_OAUTHBEARER_OIDC` is OFF and no libcurl symbol is
#                       linked (`ldd` on the shipped binary proves it) — but
#                       librdkafka's config.h is generated with
#                       `#cmakedefine01 WITH_OAUTHBEARER_OIDC`, which DEFINES
#                       the macro even when it is 0, and rdkafka_conf.c guards
#                       `#include <curl/curl.h>` with `#ifdef`. The header is
#                       therefore always required. It came free before Task 8b
#                       because the rust image inherits buildpack-deps'
#                       libcurl4-openssl-dev FOR THE HOST, which under
#                       emulation was also the target; cross-compiling needs
#                       the target's copy. MEASURED: without this the build
#                       dies at "curl/curl.h: No such file or directory".
#
# `dpkg --add-architecture amd64` IS GUARDED: on an amd64 builder (a GitHub
# runner) amd64 is already the native architecture, dpkg refuses to add it, and
# the `:amd64` suffixes resolve to the native packages — the whole stage then
# degrades to a native build, which is exactly what is wanted there.
RUN set -eux; \
    if [ "$(dpkg --print-architecture)" != "amd64" ]; then dpkg --add-architecture amd64; fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      cmake pkg-config clang libclang-dev \
      gcc-x86-64-linux-gnu g++-x86-64-linux-gnu \
      libsasl2-dev:amd64 libssl-dev:amd64 zlib1g-dev:amd64 \
      libcurl4-openssl-dev:amd64; \
    rm -rf /var/lib/apt/lists/*
# THE ONE ADDED LAYER. `cargo build --target` does not install a standard
# library for you; without this the build below stops at "can't find crate for
# `std`". It sits above `COPY . .` on purpose, so a source edit does not re-run
# it — and it is a separate `RUN` from the apt layer above because it is a
# different tool with a different failure mode, and merging them would make one
# cache miss pay for both.
RUN rustup target add x86_64-unknown-linux-gnu
# HOW THE CROSS BUILD IS WIRED, as ENV rather than a `.cargo/config.toml`, so
# that `docker history` shows it and no file arriving through `COPY . .` can
# shadow it.
#   CARGO_TARGET_..._LINKER  the real cross linker. Cargo drives the link
#                            through gcc rather than ld, so the target's C
#                            runtime objects and library search paths come with
#                            it. THIS is the line that makes the build a cross
#                            build rather than a failed one.
#   CC_/CXX_/AR_             the per-target names the `cc` and `cmake` crates
#                            read; librdkafka, libzstd and libz are all
#                            compiled from C here, for x86_64.
#   PKG_CONFIG_ALLOW_CROSS   pkg-config refuses to answer for a foreign target
#                            without it, and that refusal is the first thing a
#                            cross build of openssl-sys/sasl2-sys hits.
#   PKG_CONFIG_LIBDIR        REPLACES the search path — unlike PKG_CONFIG_PATH,
#                            which only prepends — so an arm64 `.pc` file can
#                            never answer a query about the amd64 build.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CXX_x86_64_unknown_linux_gnu=x86_64-linux-gnu-g++ \
    AR_x86_64_unknown_linux_gnu=x86_64-linux-gnu-ar \
    PKG_CONFIG_ALLOW_CROSS=1 \
    PKG_CONFIG_LIBDIR=/usr/lib/x86_64-linux-gnu/pkgconfig:/usr/share/pkgconfig
# CACHE DISCIPLINE IS UNCHANGED, DELIBERATELY. `COPY . .` is still the last
# thing before the compile, so every layer above survives an edit to any
# tracked file and only the compile is re-run — and the compile is now minutes.
# A manifest-first split (`COPY Cargo.toml Cargo.lock` plus a stub build) was
# considered and rejected: it needs a stub `src/` for every workspace member
# and breaks quietly the day a member is added. `.dockerignore` already keeps
# `target/` out of the context.
COPY . .
# `--target` IS LOAD-BEARING TWICE OVER: it is what cross-compiles, and it is
# what puts the binary under `target/x86_64-unknown-linux-gnu/release/` so that
# the `COPY --from=builder` below cannot silently pick up a host-architecture
# binary if someone removes it. `scripts/check-image.sh` check 6 asserts the
# shipped binary's ELF e_machine anyway, because "cannot silently" is a claim
# that deserves a machine check.
RUN cargo build --release --target x86_64-unknown-linux-gnu -p logweir

# The engine, pinned BY DIGEST. Update this line and
# third_party/kafka-backup-binary.digest together, never separately.
# Naming upstream's namespace here is permitted: GC14 forbids Logweir
# publishing under osodevops/, not pulling from it (controller ruling GR6).
FROM osodevops/kafka-backup@sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317 AS engine

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
COPY --from=engine   /usr/local/bin/kafka-backup /usr/local/bin/kafka-backup
# The cross-compiled path, not `target/release/`. See the `--target` note in
# the builder stage: the architecture-qualified directory is what makes a
# host-architecture binary impossible to copy by accident.
COPY --from=builder  /src/target/x86_64-unknown-linux-gnu/release/logweir  /usr/local/bin/logweir
COPY third_party/LICENSE-MIT /usr/share/licenses/kafka-backup/LICENSE
COPY LICENSE NOTICE /usr/share/licenses/logweir/
ENV LOGWEIR_ENGINE_BIN=/usr/local/bin/kafka-backup
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/logweir"]
