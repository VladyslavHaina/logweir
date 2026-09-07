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
# asserts the linkage, both CLIs, the approval round-trip and both licences.
# Every comment below records a defect that a successful `docker build` did not
# catch, so do not treat a green build as a verified image.
#
# Upstream publishes `osodevops/kafka-backup` for linux/amd64 ONLY. On an arm64
# host a plain `docker build` fails at the `engine` stage below with
# "no match for platform in manifest: not found" — verified on darwin/arm64,
# 2026-09-05. The whole image is built for one platform on purpose: pinning
# only the engine stage to amd64 would drop an amd64 ELF into an arm64 runtime,
# which is worse than a build failure because it fails at drill time instead.
# The build runs under emulation on an arm64 host and is slow; that is the cost
# of an engine that has no arm64 build.
# 1.89, not 1.82: Task 12b raised the workspace MSRV (`rust-version = "1.89"` in
# Cargo.toml, `rust-toolchain.toml` channel 1.89.0) when object_store 0.14's
# dependency tree pulled in edition2024 and a 1.89 floor. This line still read
# 1.82 until Task 22, which means every `docker build` of this image failed with
# "package requires rustc 1.89" — the image job's own MSRV, unchecked, had
# drifted below the workspace's. Update the two together, always.
FROM rust:1.89-bookworm AS builder
WORKDIR /src
# Every package here is REQUIRED by a C dependency `rdkafka` and the zstd codec
# drag in, and every one of them was found by a build that failed without it
# (verified 2026-09-05). This line read `cmake` alone until Task 22, which means
# `docker build` had never succeeded:
#   cmake          rdkafka vendors and compiles librdkafka from C (ADR 0004)
#   libsasl2-dev   sasl2-sys: "Unable to find libsasl2 on your system"
#   clang/libclang bindgen (via zstd-sys) panics without libclang
#   pkg-config     how the *-sys build scripts locate all of the above
#   libssl-dev     rdkafka's TLS support links OpenSSL
#   zlib1g-dev     librdkafka's gzip codec
RUN apt-get update && apt-get install -y --no-install-recommends \
      cmake pkg-config clang libclang-dev libsasl2-dev libssl-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p logweir

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
COPY --from=builder  /src/target/release/logweir  /usr/local/bin/logweir
COPY third_party/LICENSE-MIT /usr/share/licenses/kafka-backup/LICENSE
COPY LICENSE NOTICE /usr/share/licenses/logweir/
ENV LOGWEIR_ENGINE_BIN=/usr/local/bin/kafka-backup
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/logweir"]
