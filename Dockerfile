# The Logweir runtime image. NOT distroless and NOT musl: the extracted
# kafka-backup binary is dynamically linked against glibc >= 2.36 and libssl3
# and needs a CA bundle, because upstream builds FROM rust:bookworm and runs
# FROM debian:bookworm-slim [VERIFIED U/kafka-backup/Dockerfile:9,34,40].
FROM rust:1.82-bookworm AS builder
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release -p logweir

# The engine, pinned BY DIGEST. Update this line and
# third_party/kafka-backup-binary.digest together, never separately.
# Naming upstream's namespace here is permitted: GC14 forbids Logweir
# publishing under osodevops/, not pulling from it (controller ruling GR6).
FROM osodevops/kafka-backup@sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317 AS engine

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=engine   /usr/local/bin/kafka-backup /usr/local/bin/kafka-backup
COPY --from=builder  /src/target/release/logweir  /usr/local/bin/logweir
COPY third_party/LICENSE-MIT /usr/share/licenses/kafka-backup/LICENSE
COPY LICENSE NOTICE /usr/share/licenses/logweir/
ENV LOGWEIR_ENGINE_BIN=/usr/local/bin/kafka-backup
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/logweir"]
