# ADR 0003: Rust, with Go as the named rejected alternative

## Status

Accepted.

## Context

Logweir is a CLI that renders a configuration, spawns a subprocess, reads an
S3-compatible bucket, decodes a proprietary segment container, talks the Kafka
protocol, and produces a **signed** document whose bytes an auditor re-verifies
with an independent tool. The language choice is mostly determined by four of
those, not by taste:

1. **It decodes `.kbak` segments itself** (ADR 0001). That is byte-level work
   over untrusted input read from a bucket. A memory-safety class of bug here
   is reachable by an attacker who can write to the archive.
2. **It signs documents.** DSSE over PAE, ECDSA P-256 and Ed25519, with the
   requirement that the signature covers the exact stored bytes and that a
   second implementation (`docs/verify_scorecard.py`) agrees.
3. **It shells out to a Rust binary** whose data shapes it vendors. Vendoring a
   struct definition across a language boundary means hand-translating it and
   re-hand-translating it on every engine bump; `cargo xtask sync-upstream`
   can only diff vendored Rust against upstream Rust.
4. **It must ship as a small static-ish artifact** for three target triples
   plus a `debian:bookworm-slim` image.

## Decision

**Rust.**

The determining factor is (3): upstream is Rust, and Logweir's correctness
depends on its vendored copies of upstream's manifest and segment structs
staying faithful. In Rust, `cargo xtask sync-upstream --tag v0.21.0` compares a
vendored definition against upstream's own source at that tag and reports
agreement or drift mechanically. In any other language the vendored copy is a
hand translation, the comparison is a human reading two files, and the failure
mode is a silently wrong field offset in a document Logweir then signs.

(1) and (2) reinforce it: `#![forbid(unsafe_code)]` in `logweir-evidence`, and a
segment decoder that cannot read out of bounds, are cheap here and are work
elsewhere.

## Alternatives considered

- **Go — rejected.** It is the obvious alternative for a Kafka-adjacent CLI:
  a large Kafka client ecosystem (`franz-go`, `sarama`), trivial static
  cross-compilation to every triple this project needs (which would have made
  the musl target ADR 0004 dropped a non-issue), and a shorter path to a
  Kubernetes operator in SP5. It was rejected on (3): the vendored upstream
  structs would become hand-maintained Go translations of Rust definitions,
  with no mechanical drift check, and the segment decoder — the part that must
  not be wrong — would be the part with the weakest verification story. The
  musl and operator advantages are real and are the cost of this decision;
  ADR 0004 records the musl target being dropped, and SP5 carries the operator.
- **Python — rejected** for the segment decoder and the release artifact
  (startup, packaging, and a signing story that would be one implementation
  rather than two). Python is nevertheless used deliberately for
  `docs/verify_scorecard.py`, precisely because being a *different* language
  and a *different* implementation is what makes the second verifier
  independent. That is not a contradiction of this ADR; it is its complement.

## Consequences

- **MSRV moves with the dependency tree, and is published.** It is 1.89 today
  (raised from 1.82 by `object_store` 0.14's feature set). `docs/stability.md`
  states it, and `rust-toolchain.toml`, `Cargo.toml`'s `rust-version`, the
  CI toolchain and the `Dockerfile`'s builder stage must move together — they
  had drifted apart once, which is why the `Dockerfile` now says so in a
  comment.
- **No musl target in v0.1** (ADR 0004): `rdkafka` vendors and compiles
  `librdkafka` from C.
- **Two signing implementations, on purpose.** The Rust signer and the Python
  verifier are written from the DSSE specification independently. If they ever
  disagree, the format is broken — which is a far more useful signal than one
  implementation agreeing with itself.

Documentation is licensed [CC-BY-4.0](../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
