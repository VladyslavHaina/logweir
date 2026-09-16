# Architecture and decisions

Logweir runs an upstream Kafka backup engine and independently checks the
result before producing signed evidence. This document consolidates ADRs
0001–0005, 0007 and 0008. Their identifiers remain stable for source comments;
the original records and superseded planning detail remain in Git history.
For supported behavior and release limitations, see [stability](stability.md)
and the [support matrix](support-matrix.md).

## Workspace boundaries

| Component | Responsibility |
| --- | --- |
| `logweir` | CLI, admission, backup/restore orchestration, evidence signing. |
| `logweir-core` | Format types, deterministic checks and the engine interface; no I/O, clock or entropy reads. |
| `logweir-kafka` | Fingerprints and broker interfaces; the `client` feature provides the `rdkafka` implementation. |
| `logweir-engine-oso` | Upstream wire types, independent `.kbak` decoding, configuration rendering and subprocess execution. |
| `logweir-store` | Object storage, including restricted evidence writes and read-only controller handles. |
| `logweir-evidence` | Signing keys and signing API; re-exports the verification API. |
| `logweir-verify` | DSSE envelope types, PAE and signature verification. |
| `weirkeeper` | Kubernetes reconciliation and runner Jobs; links storage and verification without linking the engine wrapper or signing API. |
| `logweir-api` | The bounded product API: serves the static UI and `/api/v1` on one origin and creates the existing custom resources through one Kubernetes adapter. Not a proxy, not an execution authority, not yet packaged. See [the product API](api.md). |
| `ui` | Static interface displaying controller state and verification results. |
| `xtask`, `e2e` | Upstream synchronization and integration validation. |

The dependency gate's **pure layer** is `logweir-core`, `logweir-evidence`,
`logweir-kafka` and `logweir-verify`, built with `--no-default-features` and
without AWS, Kubernetes or upstream-engine dependencies. That dependency
boundary is broader than `logweir-core`'s stricter no-I/O rule.

## ADR 0001: independent engine boundary

No workspace crate may depend on `kafka-backup-core`, under any feature or
target. Linking upstream's internal library would couple releases to its
unstable Rust API and make archive verification share the writer's decoder.
Independent `.kbak` decoding makes a fingerprint disagreement useful evidence.
Keeping the library outside the artifact also keeps upstream contribution and
licensing provenance separate from Logweir's Apache-2.0 code.

Upstream data shapes are vendored under
`crates/logweir-engine-oso/src/vendored/`, with upstream source citations.
`cargo xtask sync-upstream --tag <tag> --upstream /path/to/kafka-backup`
checks them against a local upstream checkout. Check out the requested tag
first; `--tag` labels diagnostics and does not select or fetch that revision.
`scripts/check-no-oso.sh` checks the dependency graph, all-feature metadata and
source imports; source-text agreement alone is not proof of absent linkage.

Pulling upstream's digest-pinned image is permitted. The proposed, separately
released `logweir-sasl-msk-iam` integration was the historical exception for
linking upstream; it is not a dependency of Logweir's released artifact.

## ADR 0002: digest-pinned subprocess execution

Logweir shells out to the published `kafka-backup` binary. The contract permits
**exactly four subcommands reachable from shipped code: `backup`, `restore`,
`validate-restore`, `validation run`**. Amendment D records the addition of
`backup` on 2026-09-09. Permission to invoke a command does not imply that every
engine trait method implements it.

- `third_party/kafka-backup-binary.digest` pins an image digest, never a mutable
  tag. `scripts/extract-engine.sh` extracts the binary and vendors its matching
  source archive and MIT license.
- Restore validation and execution must use byte-identical configuration.
  Parsed admission and rendered-output checks independently refuse
  `purge_topics`, `dry_run` and `header_preflight_external` at any value.
- Both engine output streams are inspected for ignored-config-key warnings.
  Upstream 0.16.0 introduced the required warning mechanism; that floor alone
  is not the supported-version matrix.
- Missing engine version or digest refuses a drill rather than creating
  unsigned provenance inside signed evidence.
- Standalone Logweir binaries need a separately installed engine on `PATH` or
  at `LOGWEIR_ENGINE_BIN`; the container image carries both. The pinned upstream
  engine is linux/amd64, so alternate architectures need a verified execution
  route.

`OsoCliEngine` currently inherits the refusing default for `validation_run`;
its `engine_subreport` is therefore null. The upstream operator's restore CR
write path was rejected because its dry-run gate did not establish a real
restore. Logweir's own runner Jobs were subsequently admitted by Amendment C.

## ADR 0003: Rust

Rust keeps vendored upstream definitions comparable in their original language
and supports memory-safe parsing of untrusted segments. Go was rejected despite
its straightforward static cross-compilation and Kafka ecosystem: translated
upstream structs would require a different, manually maintained drift check.
Python was rejected for the primary decoder and release artifact, but is used
for the deliberately independent [scorecard verifier](verify_scorecard.py).

The Rust signer and Python verifier must agree on DSSE PAE and the exact stored
payload bytes. Agreement between independently implemented paths is stronger
evidence than having one implementation verify itself.

The current Rust floor is 1.89. Keep `Cargo.toml`, `rust-toolchain.toml`, CI and
container builders aligned when changing it. See ADR 0004 for the C dependency
and the absence of a musl release target.

## ADR 0004: Kafka client

`logweir-kafka` uses `rdkafka` with vendored `librdkafka`. The client must support
metadata, end offsets, per-topic `DescribeConfigs` and record consumption;
target-topic deletion also uses its admin API. Do not implement a custom Kafka
protocol path for operations an existing client provides.

The original selection inspected `rskafka` **0.5.0**: its protocol messages and
client methods lacked `DescribeConfigs`. This is a finding about that version,
not all future releases. The live-broker leg was blocked by the original
environment, so the selection establishes API availability, not runtime
compatibility. [UNVERIFIED — run the broker integration suite on the intended
Kafka and engine versions before asserting live compatibility]

`client` is **enabled by default in this crate** and disabled by the pure-layer
build's `--no-default-features`. It enables `rdkafka` and Tokio. Preserve the
combination of rdkafka's `tokio` feature and Tokio's `rt`, `time` and `net`
features: per-call current-thread runtimes drive admin futures. Removing these
can block for the configured timeout or leave the runtime without an I/O
reactor; workspace feature unification can hide the latter in tests.

The C/cmake build complicates musl cross-compilation, so v0.1 has no musl release
target and one Kafka reader implementation. Reconsidering a pure-Rust reader
requires checking the new client's full admin API and release targets.

## ADR 0005: estimation history

The original week-one velocity calibration was **not performed**. Estimates
assumed a human builder at nine focused hours per calendar week, whereas the
initial work was executed by AI agents. Agent wall time and test counts are not
a valid substitute for actual focused human hours. No schedule was validated
or recalibrated by that exercise; future estimates need comparable measurements
and must include review and rework.

## ADR 0007: source capture scope

The 2026-09-03 decision restored `--from-cluster` and phase `-1` to v0.1 scope
because requiring adopters to already own an upstream archive obstructed the
intended entry path. Its earlier deferral was revoked, but the initial CLI did
not implement that flag. **The current CLI still has no `--from-cluster`
flag**; it instead provides a separate `backup run` command. A scope decision
must not be presented as shipped functionality.

The scorecard domain remains `-1..=9`. Its `captured_by_logweir` and source-RPO
fields enforce their measured/unmeasured pairing; the restore orchestrator does
not execute phase `-1`. Any future combined source-capture path must bind the
source cluster identity, refuse source=target, require explicit named topics,
prevent source mutations and preserve forbidden-key checks in `backup.yaml`.

## ADR 0008: MVP constraint amendments

Accepted from 2026-09-09. These are explicit changes to earlier constraints,
not reinterpretations of them. Lettered sections preserve the original
decision references used by gates and source comments.

## Amendment A — kind list

The kinds are `KafkaCluster`, `BackupSchedule`, `Backup`, `Restore`, `Approval`
and `TrustRoster`, plus `Switchover` for tag 2 and reserved `MetadataSnapshot`.
`RestoreDrill` is retired in favor of `Restore`. Kind names must not encode a
particular cluster, fleet, topic, connector or backup; new kinds require a
recorded architectural decision.

## Amendment B — naming boundaries

GC14's exclusions remain: Logweir does not publish under `kafkabackup.com`,
`oso.sh`, upstream API groups, the `osodevops/` Docker Hub namespace or upstream
crates.io names. Its API group remains `logweir.dev/v1alpha1`, and its ASF
trademark disclaimer remains required. The deferral of that API group to SP5
was lifted; the naming exclusions were not changed.

## Amendment C — operator and UI scope

The owner overrode the roadmap's adopter-request gate: the operator, CRDs and
static UI became scheduled tag-1 work. This is the authority for the control
plane work and does not remove its security boundaries.

## Amendment D — engine command allowlist

`backup` joined `restore`, `validate-restore` and `validation run` so source
capture could invoke the real engine. `list`, `restore-status`, `offset`,
`evidence-verify` and `validation evidence-verify` remain denied.

`scripts/check-no-oso.sh` separates the runtime allowlist from legal argv
tokens: the latter also includes `run`, `--config`, `--format` and `json`.
Its primary check inspects literal argv tokens inside engine invocation
expressions. A secondary source scan catches denied tokens elsewhere; an
`engine-token-ok` escape needs a reason of at least ten characters and does
not bypass the primary invocation check.

## Amendment E — storage and verification extraction

`logweir-store` was extracted from the engine wrapper so the controller could
fetch evidence, inspect manifest windows and report retention without linking
the subprocess wrapper. It uses `object_store` with `aws`, `azure`, `gcp` and
`http`, so it is **outside the pure layer**. The engine wrapper re-exports it
as `storage`. Evidence writes remain restricted to the literal `logweir/`
prefix; read-only handles reject writes before any other put-side check.

`logweir-verify` was extracted from `logweir-evidence` so the controller could
verify signatures without linking the signing API. It owns PAE, verification,
`VerifyingKey`, envelope types and payload constants; the evidence crate
re-exports those while retaining `SigningKey`, `KeyAlg` and signing. Parsing a
public key from a string supports `TrustRoster` entries without a file.

The verification crate is **inside the pure layer** and appears in both its
build list and forbidden-dependency loop. It has no direct entropy-source
dependency or signing API, but the shared cryptography dependencies still
bring transitive `rand_core`/`getrandom`. The claim is about declared dependency
edges and APIs, not absence of every cryptographic capability.

The single-signer gate therefore keeps four distinct checks:

| Check | Allowed workspace consumers |
| --- | --- |
| Signing API linkage | `logweir`, `e2e` |
| Crypto primitives through normal dependencies | `logweir-evidence`, `logweir`, `logweir-verify`, `weirkeeper` |
| Signing API source references | `logweir-evidence`, `logweir`, `e2e` |
| Verification API linkage | `logweir-evidence`, `logweir`, `weirkeeper`, `e2e` |

Checks 1 and 3 retained their original scope when verification was extracted;
check 2 was narrowed to state which crates reach shared primitives. These
allowlists and their distinct dependency walks are enforced by
`scripts/check-one-signer.sh` and its regression tests.

This is a **linkage boundary**, not a Kubernetes capability boundary.
`weirkeeper` has Job CRUD in the runner namespace and can create a pod that
mounts the signing key. Restricting Secret reads does not prevent that. This
residual risk is accepted by the current design. The corpus gate
`scripts/check-withdrawn-claim.sh` checks documentation, configuration, UI,
workflows, scripts and Rust source for the withdrawn stronger guarantee;
only the two scripts that explicitly prohibit it are exempt.

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
