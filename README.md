# Logweir

Logweir produces a **signed drill scorecard** in which `measured.rto_seconds`
and `measured.rpo_seconds` are real numbers, produced by actually restoring a
sampled point-in-time window from a Kafka backup archive into a segregated
scratch cluster and reconciling it **per record**.

Not a policy document. Not a dry run. A restore that happened, timed, checked
byte-for-byte against the archive, and signed so an auditor can verify it
without trusting the machine that produced it.

## What it looks like

This is `logweir drill show` over the scorecard `scripts/demo.sh` produced on a
laptop — real output, pasted unedited:

```
logweir drill scorecard  (01M1RZW1F5KQE6HANC41M91CS9 v1.0.0)
------------------------------------------------------------------------
  outcome                     pass
  engine                      oso-cli 0.21.0 sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317
  levers                      header_preflight=honoured  dry_run_check_segments=unknown-not-observable
  target                      5L6g3nShT-eMCtK--X86sw  marker=logweir.scratch  2 mapping entry/ies
  approval                    demo@example.com (DEMO-1)

  rto requested→verified      7s
  rto approval→verified       7s
  rto restore only            0s
* rto excluding preflight     7s   <- compared against objectives.rto_seconds
  rpo                         12s   archive coverage gap at the requested point (NOT source-relative loss)

  integrity                   byte-fingerprint/pass  150/150 matched, 0 mismatch(es)
  target diff                 0 collision(s), 2 would-create (full)
  topic parity                intended []  unexpected []
  evidence                    immutable=false  create_only_enforced=false

  no capture gap or retention-pruned range overlaps the sampled window

  objectives (from the approved plan)
    rto_seconds                  900   compared against the starred row above
    rpo_seconds                  300
    pass_rate                      1   measured 1
    met                          yes

  qualifiers the fourteen rows above do not carry
    integrity.partial_reason  —
    engine_subreport          null — no engine sub-report was retained; this is NOT "the engine reported nothing wrong"

  This table is a SUMMARY of a signed document, not the document. `--format json`
  prints the signed bytes; docs/verify-a-scorecard.md lists what the summary omits.
```

The table is a summary. The signed document is the JSON, and
[docs/verify-a-scorecard.md](docs/verify-a-scorecard.md) is written for the
person who has to decide how much weight to give it.

## Quickstart

One command, on a laptop, with no cloud resources:

```bash
./scripts/demo.sh
```

It needs `docker`, `cargo`, `openssl`, `jq` and `python3` with the
`cryptography` package (`pip install cryptography`; or point `LOGWEIR_PYTHON`
at an interpreter that has it). All five are checked at second zero, before
anything is started. It takes about four minutes and does six things:

1. Extracts the digest-pinned `kafka-backup` engine.
2. Brings up Kafka (KRaft) and MinIO with `docker compose`.
3. Produces records and takes a real backup with that engine.
4. Mints a signing key **and a separate approver key**, derives the cluster
   allowlist from the running broker, and runs `logweir doctor`.
5. Approves the exact plan by hash, then runs the drill.
6. Shows the scorecard and verifies it **twice** — once with `logweir drill
   verify`, once with `docs/verify_scorecard.py`, which shares no code with
   Logweir.

Tear it down with `just e2e-down`.

**It does not modify your working tree.** Everything it writes goes to `.demo/`
and `.engine/`, both gitignored, and it checks `git status` for you at the end
and says so. (It seeds the stack with `LOGWEIR_SEED_REFRESH_FIXTURES=0`, so it
does not refresh the two checked-in archive fixtures that `just e2e-seed`
deliberately refreshes — that is a maintainer action, not part of the demo.)

The demo rebinds one field of `examples/drill.yaml`: `sample.window_start` /
`sample.window_end`, which name the point-in-time range you are recovering to
and are therefore specific to your archive, not to Logweir. A window that
overlaps no segment is refused rather than reported as a pass. See
[docs/quickstart.md](docs/quickstart.md) to run it against a cluster you
already have.

## What Logweir is **not**

| Non-goal | Why, and what does it instead |
|---|---|
| **It does not back up.** | `osodevops/kafka-backup` does. Logweir consumes an archive that already exists and never creates one. |
| **It does not write to the source cluster, on any path.** | Not a topic, not an offset commit, not a config. The drill restores into a *scratch* cluster, proved segregated by a marker topic before anything runs. |
| **It does not consume OSO CRDs.** | No `KafkaRestore`, no `KafkaBackup`, no operator objects read or written. Logweir shells out to the engine binary and nothing else. |
| **It does not require Kubernetes.** | v0.1 is a CLI that spawns a local subprocess. A `CronJob` example is provided ([examples/cronjob-drill.yaml](examples/cronjob-drill.yaml)) because that is how most people will schedule it — but nothing needs a cluster. |
| **It ships no web UI.** | No HTTP surface at all in v0.1: no `/metrics`, no `/healthz`, no `/readyz`. Metrics are a Prometheus textfile at `--metrics-file`. |
| **It reads no metadata in v0.1.** | No ACLs, no client quotas, no broker configs. Metadata snapshot and diff are SP2. |

## Relationship to `osodevops/kafka-backup`

Logweir **drives** upstream's engine. It does not fork it, link it, or modify it.

- **MIT, redistributed.** Upstream is MIT-licensed; the licence and the source
  tarball for the pinned version are vendored in
  [third_party/](third_party/README.md) and the licence ships inside the
  container image.
- **Shelled out to, by digest.** `third_party/kafka-backup-binary.digest` pins
  an immutable image digest, never a tag. The allowlist `scripts/check-no-oso.sh`
  enforces is exactly three subcommands — `restore`, `validate-restore`,
  `validation run` — and that is a *ceiling*, not a description: **v0.1 actually
  invokes two.** `OsoCliEngine` does not override `DataEngine::validation_run`,
  so the engine's own validation run is never executed and
  `engine_subreport` is `null` in every scorecard v0.1 produces. See
  [ADR 0002](docs/adr/0002-shell-out.md) and
  [stability.md](docs/stability.md).
- **Never linked.** No crate in this workspace depends on `kafka-backup-core`,
  under any feature or target; `scripts/check-no-oso.sh` proves it on every
  build with `cargo tree`, `cargo metadata --all-features` and a narrow linkage
  grep. See [ADR 0001](docs/adr/0001-no-core-link.md) and
  [ADR 0002](docs/adr/0002-shell-out.md).
- **Naming.** Logweir publishes nothing under the `osodevops/` namespace, the
  `kafkabackup.com` or `oso.sh` domains, or those API groups. Naming upstream's
  published image in order to *pull* it is a different thing from publishing
  under it, and is what the digest pin and the MIT redistribution above
  require.

Which engine versions are supported, and which are not:
[docs/stability.md](docs/stability.md) and
[docs/support-matrix.md](docs/support-matrix.md).

## Stability, in one paragraph each

- **Engine floor.** `kafka-backup` **0.16.0** is the floor for the
  unknown-config-key warning mechanism Logweir parses off the engine's streams;
  **0.21.0** is the floor for the full drill as shipped, and is the digest
  pinned here. `strimzi-backup-operator`'s hard-coded default `v0.19.1` is
  *below* that floor and is reported `unsupported (lever-absent)` — an operator
  default that has not caught up, never a fault Logweir raises.
- **Format policy.** `format_version` on the scorecard and the put-receipt is
  semver. A **minor** adds optional fields only; a reader ignores unknown
  fields within a major and **refuses** a higher major rather than guessing.
  v0.1.0 is the point at which this starts being a promise rather than a
  draft — see [docs/stability.md](docs/stability.md), "The v0.1.0 tag is the
  compatibility boundary".
- **Explicitly not a contract.** The Rust crates in this workspace are **not** a
  stable API before 1.0. Only the signed document formats and the `logweir`
  CLI's flags and exit codes are covered.

Read [docs/stability.md](docs/stability.md) before depending on any of it. It
also lists what v0.1 does **not** do — including `--from-cluster`, compacted
targets, `sample.anchor: tail|random`, and the fact that `engine_subreport` is
always `null`.

## Install

**Logweir is not on crates.io at v0.1.0.** `cargo install logweir` does not work
and this README will not pretend otherwise: the workspace's internal
dependencies are declared by path with no version, so `cargo publish --dry-run`
refuses with *"all dependencies must have a version specified when
publishing"*. Publishing the five crates in dependency order is release work
that has not been done. Until it is:

```bash
# From a clone:
cargo install --path crates/logweir --locked
```

**The standalone binary carries no engine.** A `cargo install --path` or a release
tarball gives you `logweir` alone; it needs `kafka-backup` of the pinned digest
on `$PATH` (or at `$LOGWEIR_ENGINE_BIN`), plus `$LOGWEIR_ENGINE_VERSION` and
`$LOGWEIR_ENGINE_DIGEST` set to that image's version and digest. Logweir
refuses to sign a scorecard that names no engine, so an unset digest is an
error at run time, not a silently unattributed document.

The container image carries both:

```bash
docker build --platform linux/amd64 -t logweir:v0.1.0 .
```

**`--platform linux/amd64` is required, not optional.** Upstream publishes the
engine image for linux/amd64 only, so on an arm64 host a plain `docker build`
fails at the engine stage with `no match for platform in manifest: not found`.
The whole image is built for one platform deliberately: pinning only the engine
stage would put an amd64 binary inside an arm64 runtime, which fails at drill
time instead of at build time. On arm64 the build runs under emulation and is
slow.

It is built `FROM debian:bookworm-slim` with `ca-certificates` and `libssl3` —
never musl or distroless, because the extracted engine is dynamically linked
against **glibc >= 2.36** and makes TLS connections.

Running it under Kubernetes has a small number of facts that will otherwise
cost you an afternoon — in particular, **the exit code that says "a drill ran
and did not pass" is nearly invisible to a Kubernetes operator** unless the Job
is shaped correctly. They are all in
[docs/kubernetes.md](docs/kubernetes.md).

## Documentation

| Document | For |
|---|---|
| [docs/quickstart.md](docs/quickstart.md) | Running a drill against a cluster you already have. |
| [docs/verify-a-scorecard.md](docs/verify-a-scorecard.md) | The auditor who received a scorecard and has to decide what it proves. |
| [docs/formats/drill-scorecard.md](docs/formats/drill-scorecard.md) | Every field, its type, and its formula. |
| [docs/stability.md](docs/stability.md) | Version floors, format policy, and every known limitation of v0.1. |
| [docs/support-matrix.md](docs/support-matrix.md) | Which engine versions are tested green. |
| [docs/kubernetes.md](docs/kubernetes.md) | Scheduling drills on Kubernetes, and the exit-code trap. |
| [docs/adr/](docs/adr/0001-no-core-link.md) | Why the architecture is the way it is. |
| [SECURITY.md](SECURITY.md) | Reporting a vulnerability, and what is in scope. |
| [CONTRIBUTING.md](CONTRIBUTING.md) | DCO sign-off, no CLA, inbound = outbound Apache-2.0. |
| [MAINTAINERS.md](MAINTAINERS.md) | Who reviews, and what a format change needs. |

---

Licensed under [Apache-2.0](LICENSE); see [NOTICE](NOTICE). Contributions
require a DCO sign-off (`git commit -s`) and no CLA.

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
