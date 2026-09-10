# ADR 0002: Logweir shells out to the engine binary, digest-pinned

## Status

Accepted.

## Context

ADR 0001 rules out linking `kafka-backup-core`. Logweir still has to make a
restore happen, and it has to be *upstream's* restore — a drill that exercised
a reimplementation would measure the reimplementation.

Three ways to run someone else's restore:

1. **Link the library** — refused by ADR 0001.
2. **Reimplement the restore** — measures the wrong thing, and doubles the
   surface a bug can hide in.
3. **Run the published binary as a subprocess**, feeding it a rendered
   configuration and reading its exit status and streams.

## Decision

**Shell out to `kafka-backup`, pinned by image digest, with exactly four
subcommands reachable from shipped code: `backup`, `restore`,
`validate-restore`, `validation run`** (Global Constraint 3).

**Amended 2026-09-09** by the Logweir MVP tag-1 plan
(`docs/mvp/2026-09-09-mvp-tag1-plan.md`, Task 1), spec §5 and spec §16 item 10,
and recorded as Amendment D of `docs/adr/0008-mvp-constraint-amendments.md`:
the original Decision fixed the count at **three** and named **no
`--from-cluster` exception anywhere in this ADR**, so admitting `backup` is a
CHANGE to what this ADR decided and not a reading of it — GC18's
`--from-cluster` path renders a `backup.yaml` and runs it, which three
subcommands cannot express.

The details that make this safe rather than merely convenient:

- **Digest, never tag.** `third_party/kafka-backup-binary.digest` holds a
  `sha256:` image digest. Docker Hub tags are mutable; a tag pin would let the
  engine change under a signed scorecard that names it.
- **The binary is extracted, not run in place.** `scripts/extract-engine.sh`
  resolves the digest, extracts `/usr/local/bin/kafka-backup`, and vendors the
  matching source tarball and MIT licence into `third_party/`.
- **The configuration is rendered, then validated, then executed — byte for
  byte the same document.** `validate-restore` runs over the exact file
  `restore` is then given. Rendering a second document for execution would
  break the binding between what was checked and what ran.
- **Three keys are refused, in two independent places.** `purge_topics`,
  `dry_run` and `header_preflight_external` are refused by the admission guard
  on parsed input (`logweir_core::guard`) and asserted absent from rendered
  output (`logweir_engine_oso`). Global Constraints 4 and 7 (GR7): they never
  appear in an argv or a rendered document, with no exception.
- **Warnings are read back off the stream the engine actually uses.** Upstream
  emits `Ignoring unknown config key ...` when a rendered key is dropped.
  Logweir parses both stdout and stderr for it, because a key Logweir rendered
  and the engine ignored is a silently degraded restore. This is why
  `kafka-backup` **0.16.0** is the floor for the mechanism at all.

## Consequences

- **The standalone binary carries no engine.** A `cargo install logweir` or a
  release tarball binary needs `kafka-backup` of the pinned digest on `$PATH`
  (or `$LOGWEIR_ENGINE_BIN`). The container image is the only artifact that
  ships both. This is stated in the README's install section because it is the
  first thing a new adopter trips over.
- **An empty engine version or digest is a hard refusal.** A signed scorecard
  that names no engine is not a scorecard an auditor can act on, so `drill run`
  exits 1 rather than signing one.
- **`engine_subreport` is `null` in v0.1.** `OsoCliEngine` does not override
  `DataEngine::validation_run`, so the permitted `validation run` subcommand is
  not actually invoked yet. The default implementation refuses rather than
  fabricating a report — see `docs/stability.md`.
- **Cross-architecture is the adopter's problem, and it is documented.**
  Upstream publishes linux/amd64 only. On arm64 the extracted ELF cannot exec;
  the demo and the e2e harness both PROBE the route and print which one they
  took, rather than assuming.

## Alternatives considered

- **A Kubernetes Job per restore**, rather than a local subprocess. Deferred to
  SP5 (`weirkeeper`) — it needs a kubeconfig, which makes the guard suite
  unrunnable in CI.
- **Upstream's operator / `KafkaRestore` CR write path.** Refused outright, not
  deferred: it would gate restores on a dry run that reports success
  unconditionally.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
