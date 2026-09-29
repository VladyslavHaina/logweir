# Engine support matrix

<!--
HAND-WRITTEN, except the section between the `engine-matrix:rows` markers,
which `.github/workflows/engine-matrix.yml` renders weekly and publishes as the
`support-matrix` artifact (a pull request only when the `ENGINE_MATRIX_OPEN_PR`
repository variable is `true`). Everything else carries exactly the rows that
were ACTUALLY EXERCISED, and says so for every other version rather than
projecting a result. A matrix that lists an untested version with a verdict is
worse than a short matrix.
-->

**Installing Logweir is [install.md](install.md)**, which leads with the engine
floor below. This file is the row-by-row evidence behind it.

## The floors, stated first

| Floor | Version | What it gates |
|---|---|---|
| **Warning-mechanism floor** | `kafka-backup` **0.16.0** | The `Ignoring unknown config key ...` message Logweir parses off the engine's streams. Below it, a rendered key the engine dropped fails **silently** instead of surfacing in `engine.levers.unknown_key_warnings`. |
| **Full-drill floor** | `kafka-backup` **0.21.0** | The full drill as shipped. This is the version every vendored struct and CLI behaviour was verified against, and the version pinned by digest in `third_party/kafka-backup-binary.digest`. |

Anything below the full-drill floor is reported **`unsupported (lever-absent)`**
— an engine that predates a lever Logweir needs. **That is never a fault Logweir
raises against that engine or against an operator that defaults to it.**

## The five outcomes

| Outcome | Meaning |
|---|---|
| `pass` | The full drill ran and passed. |
| `pass-degraded` | The drill passed at a reduced integrity level (e.g. `consume-only`, because the KBAK decoder returned `Unsupported`). |
| `fail(reason)` | The drill ran and did not pass, for the stated reason. The weekly job also records these reasons, each derived from what ran (`scripts/engine-matrix-outcome.sh`): `fail(seed)` when the stack could not be seeded with that engine; `fail(build)` when `logweir` did not build; `fail(floor-not-enforced)` when a drill accepted an engine below the full-drill floor, or failed without Logweir's floor refusal; and `fail(setup)` when a step did not run, the tag did not resolve to a digest whose revision label is the tag's commit, or the broker the stack ran is not the one the row declares. |
| `fail(lever-not-honoured)` | The engine accepted a lever and did not act on it. The deleted-segment positive control catches this: a **non-oldest** segment is removed from the live archive, so `validate-restore` must report it unrestorable and the drill must block at phase 5 with `outcome: preflight-failed` and exit 2. A version where the drill sails past lands here. The control is the e2e test `a_corrupted_segment_yields_exit_2_and_a_signed_preflight_failed_scorecard`. |
| `unsupported(lever-absent)` | The engine predates a lever Logweir needs. Reported, never treated as a fault. |

## Rows

| Engine version | Image digest | Outcome | Evidence |
|---|---|---|---|
| **0.21.0** | `sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317` | **`pass`** | **The engine floor: `0.21.0` is the minimum supported `kafka-backup` version, the version this row was run against, and the version the shipped digest pins. [install.md](install.md) leads with it.** Full drill, 2026-09-05, against the compose stack (Kafka 3.7.1 KRaft + MinIO) via `scripts/demo.sh`: `outcome: pass`, `integrity: byte-fingerprint/pass`, 150/150 records reconciled, `pass_rate_measured: 1.0`, `rto_excluding_preflight_seconds: 6`, `rpo_seconds: 9`, all three objectives met, signature VALID under both `logweir drill verify` and `docs/verify_scorecard.py`. `header_preflight: honoured`. |

That is **one green row at the declared floor**, which is the release
requirement. It is also the only row that has been run.

## Versions with no row yet, and why

Listed so that "absent from the matrix" is never mistaken for "known bad", and
so nobody quotes a projected verdict as a tested one.

| Engine version | Status | Note |
|---|---|---|
| **0.22.0** | **evaluated, not the shipped pin; no weekly row recorded yet** | Upstream's current release (2026-09-07). PROD-00.1 ran it against the compose stack (Kafka 3.7.1, the `linux/amd64` image `sha256:1c3432c9399dbdd59fea7b6cf386135656a73bb6dd60841212ddbfe5ba26b1fe` under emulation, 2026-09-29). The demo drill scored `outcome: pass` and `integrity: byte-fingerprint/pass` (150/150), and both verifiers returned VALID. `just pitr` passed (six of nine records, boundary included). The `.kbak` fixture tests passed on 0.22.0 bytes. `doctor` refuses it, because it accepts exactly 0.21.0. Moving the pin is proposed as PROD-00.3f, subject to OD-3 ([decision record](to-do/decisions/PROD-00-engine-route.md) §4). Its `path_style` change does **not** make VirtualHosted addressing with a custom endpoint possible, so that refusal stays. It also newly treats an `http://` endpoint as `allow_http: true`. |
| 0.20.x | **unsupported by the full-drill floor** | Matrix compatibility probes do not override the 0.21.0 runtime floor. The weekly job runs v0.20.0 as a below-floor row. |
| 0.19.x | **unsupported by the full-drill floor** | Same floor as 0.20.x. v0.19.2 is the `kafka-backup-core` that `kafka-backup-operator` 1.3.0 links as a library (its `Cargo.lock`). v0.19.1 is the default of `strimzi-backup-operator` v0.2.22–v0.2.25; v0.2.21 defaulted to v0.19.0. Engines before 0.21 write no segment sha256, so a drill over an archive one of them wrote reports `integrity.result: partial`, never `pass`. Measured on 2026-09-29 over a v0.19.2 archive, drilled with the pinned engine: `outcome: fail-integrity`, `integrity: byte-fingerprint/partial`, exit 2, and both verifiers VALID. Logweir refuses to drive a below-floor engine itself: v0.19.2 as the restore engine exits 1 with "ignored the config key `restore.header_preflight` that logweir rendered; this tag is below the declared floor". The weekly job runs v0.19.2 and v0.19.1 as below-floor rows, which record `unsupported (lever-absent)` and are never a fault. |
| 0.16.0 – 0.18.x | **unsupported**, by floor | Below the full-drill floor; only the unknown-key warning mechanism works. |
| < 0.16.0 | **unsupported (lever-absent)**, by floor | The warning mechanism this project depends on does not exist. |

`strimzi-backup-operator` has defaulted to engine **v0.22.0** since its v0.3.0
(2026-09-07, `DEFAULT_BACKUP_IMAGE` in its `src/engine.rs`). An earlier
revision of this page said its default was v0.19.1, which is true only of its
v0.2.22–v0.2.25 releases.

## Authentication modes, and what each one has actually been run against

The recorded matrix row above is a **PLAINTEXT** drill. Authentication test
coverage is separate from that recorded result: the Compose stack now has
SCRAM listeners and `e2e/tests/scram.rs` exercises both clients with real
brokers when the `e2e` feature and its infrastructure are enabled.

| `auth.mode` | Logweir's client | The engine's client | Exercised |
|---|---|---|---|
| `plaintext` (default) | `security.protocol: PLAINTEXT` | no `security:` block rendered — the engine's own default | **Yes**, by the 0.21.0 row above and by every e2e drill. |
| `scramSha512`, `tls: false` | `security.protocol: SASL_PLAINTEXT`, `sasl.mechanism: SCRAM-SHA-512` | `security_protocol: SASL_PLAINTEXT`, `sasl_mechanism: SCRAM-SHA512` | **Automated e2e coverage exists.** `e2e/tests/scram.rs` exercises Logweir's librdkafka client and engine-backed backups against the Compose SCRAM listeners, plus an in-cluster pod. It requires the live stack and Kubernetes; it is not part of the default unit suite or an additional result row above. |
| `scramSha512`, `tls: true` | `security.protocol: SASL_SSL` | `security_protocol: SASL_SSL` | **Exercised on docker-desktop, not in CI.** The compose stack speaks no TLS, so no automated e2e row covers it; PLAT-07.1's live run (2026-09-16, an in-namespace broker with a private CA) succeeded with `KafkaCluster.spec.auth.tlsCa` and failed at the handshake — never a plaintext dial — without the CA or with the wrong one. `auth.tlsCa` hands one CA file to both trust stores (Global Constraint 29; [kubernetes.md](kubernetes.md) §20.2). |
| OAUTHBEARER / MSK IAM | `AuthConfig::Token` — constructing it returns an error | not rendered. The engine's YAML offers only PLAIN, SCRAM-SHA-256/512 and GSSAPI; other mechanisms need a programmatic plugin its CLI does not expose ([PROD-00.1](to-do/decisions/PROD-00-engine-route.md) C9) | **Not in tag 1.** |

**Nothing here is an MSK row.** `[UNVERIFIED — needs an MSK cluster]`: MSK
holds SCRAM credentials in AWS Secrets Manager and requires TLS on its
`:9096` SASL endpoint, so the sentence that would verify it is *"point
`auth.tls: true` and `bootstrap_servers` at an MSK cluster's
`*.kafka.<region>.amazonaws.com:9096` endpoint, project the Secrets Manager
value into `LOGWEIR_TARGET_PASSWORD`, and record that `drill run` reaches
phase 2"*. It needs a provisioned cluster, which Global Constraint 17 forbids,
so it is recorded as **blocked, never as closed** — and the two `tls: true`
claims above are deliberately about rendered bytes and not about a handshake.

## Broker versions (Apache Kafka)

The engine rows above were run against Apache Kafka **3.7.1**, a line Apache
no longer supports and MSK stopped supporting on 2026-09-01. Each row below
names its broker line. All were run on 2026-09-29 against the compose stack's
single-node KRaft broker, engine 0.21.0 (the pinned digest), plaintext, MinIO,
each line on its own compose slot (`e2e/compose/stack-env.sh --slot N --kafka
LINE`, [`e2e/README.md`](../e2e/README.md)). Three paths per line: the demo
drill (`scripts/demo.sh`), `just pitr` (the G-PITR boundary row) and the
receipt path (`just mvp-demo`: `logweir backup run`, the receipt verified by
both readers, a point-in-time `restore run` into new topics, the scorecard
verified by both readers).

| Broker (Apache Kafka) | Image digest | Engine | Demo drill | `just pitr` | Receipt path | Status |
|---|---|---|---|---|---|---|
| **3.7.1** | `sha256:ed74d7d115968d5e8b00ba6822ac6a384cbaaf54ca38991828647000d7089b68` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | **legacy** — still the default fixture (`e2e/compose/.env`) |
| **3.9.2** | `sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | supported line, measured |
| **4.1.2** | `sha256:5cc2a2fd93fa2687b44015eee04fb2c3edd9e526bd64bf8bec5ff1e268772e0e` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | supported line, measured |
| **4.3.1** | `sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837` | 0.21.0 | `pass`, VALID ×2 | 1 passed | `pass`, 2000 records, VALID ×2 | supported line, measured |

The digests are the pins: `stack-env.sh --kafka LINE` runs exactly these
images (`KAFKA_IMAGE`), and a test fails if the two disagree.

No row yet: **4.0.x** and **4.2.x** (supported by Apache, not run), and
anything with authentication, several brokers or another object store — the
optional profiles in [`e2e/README.md`](../e2e/README.md) provide those fixtures;
their rows belong to the tasks that use them.

**The engine does not negotiate protocol versions, and 4.x accepts what it
sends.** `kafka-backup` 0.21.0 sends every request at a fixed version and never
sends ApiVersions (`crates/kafka-backup-core/src/kafka/client.rs:588-611` in the
pinned source tarball; any API not in that table goes out at version 0). Kafka
4.0 removed old versions (KIP-896). Measured on each line with
`kafka-broker-api-versions.sh`, every version the engine can send is inside the
broker's range; 4.x raised the floors below them:

| API | Engine sends | 3.7.1 | 3.9.2 | 4.1.2 | 4.3.1 |
|---|---|---|---|---|---|
| Produce | v8 | v0–v10 | v0–v11 | v0–v13 | v0–v13 |
| Fetch | v11 | v0–v16 | v0–v17 | v4–v18 | v4–v18 |
| ListOffsets | v5 | v0–v8 | v0–v9 | v1–v10 | v1–v11 |
| Metadata | v9 | v0–v12 | v0–v12 | v0–v13 | v0–v13 |
| OffsetCommit | v5 | v0–v9 | v0–v9 | v2–v9 | v2–v10 |
| OffsetFetch | v5 | v0–v9 | v0–v9 | v1–v9 | v1–v10 |
| FindCoordinator | v2 | v0–v4 | v0–v6 | v0–v6 | v0–v6 |
| DescribeGroups | **v0** (the `_ => 0` fallback) | v0–v5 | v0–v5 | v0–v6 | v0–v6 |
| ListGroups | v2 | v0–v4 | v0–v5 | v0–v5 | v0–v5 |
| CreateTopics | v5 | v0–v7 | v0–v7 | v2–v7 | v2–v7 |
| DeleteRecords | v1 | v0–v2 | v0–v2 | v0–v2 | v0–v2 |
| DescribeConfigs | **v1** (the 4.x floor) | v0–v4 | v0–v4 | v1–v4 | v1–v4 |
| IncrementalAlterConfigs | v1 | v0–v1 | v0–v1 | v0–v1 | v0–v1 |
| SaslHandshake / SaslAuthenticate | v1 / v2 | v0–v1 / v0–v2 | same | same | same |

On 4.3.1 the broker's own request log (`kafka.request.logger` at DEBUG) showed
the engine (client id `kafka-backup`) sending exactly Metadata v9, ListOffsets
v5, Fetch v11, DescribeConfigs v1 and Produce v8 on both the demo drill and the
receipt path, all accepted, and no ApiVersions; Logweir's own client negotiated
(ApiVersions v3, then Fetch v16, Metadata v13) and created the target topics
itself. The engine's CreateTopics, consumer-group APIs (FindCoordinator,
OffsetFetch, OffsetCommit, ListGroups, DescribeGroups), DeleteRecords,
IncrementalAlterConfigs and, on these plaintext runs, SASL requests were never
sent, so they are in range but unexercised. **The risk is structural:**
DescribeConfigs already sits on the 4.x floor and DescribeGroups on v0, and a
future release that raises either floor fails the engine with an unsupported
version instead of a downgrade. So do the table's DescribeAcls, CreateAcls and
DeleteAcls entries (v1, the 4.x floor), which 0.21.0 never sends. That route
(negotiate, or pin higher) belongs to PROD-00.1's capability table.

## Object stores: conditional create is required

Since RECEIPT-DUP was fixed, `logweir backup run` claims each execution with a
create-only put (`If-None-Match: *`) under `logweir/backups/<backup_id>/`
before the engine starts, and proves the store refused a second create. A store
that does not enforce conditional create is **unsupported**: every backup to it
exits 4 `ExecutionClaimUnproven` before any data is written, and a destination
with `writeProbe` on reports it `notReady / ConditionalCreateUnsupported`
beforehand ([kubernetes.md §21.5](kubernetes.md)).

| Object store | Status |
|---|---|
| MinIO `RELEASE.2025-09-07T16-13-09Z` | **Supported, measured** (private container: the claim, the refused second run, and the readiness probe's double create). The project's rebuild of this release (`third_party/minio-mirror/`), which the compose stack and the chart's demo MinIO now run, answers a second `If-None-Match: *` create with the same `412 PreconditionFailed` as the upstream image (smoke of 2026-09-24) |
| older MinIO releases | `[UNVERIFIED — needs a run against an older MinIO release]` |
| AWS S3 | `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` |
| GCS, Azure Blob | `[UNVERIFIED — native conditional create in object_store, not run against either provider]` |
| local filesystem (standalone CLI) | **Supported, measured in process** |
| `AWS_CONDITIONAL_PUT=disabled`, or an S3-compatible store that ignores `If-None-Match` | **Unsupported** — refused, never silently accepted |

## The weekly engine-matrix job

`.github/workflows/engine-matrix.yml` runs every Monday, and on demand, over a
declared set of rows: the newest four engine minors (`v0.22.0`, `v0.21.0`,
`v0.20.0`, `v0.19.2`), `v0.19.1` (the default of `strimzi-backup-operator`
v0.2.22–v0.2.25), and the pinned engine once more on the newest supported
Apache Kafka line (`KAFKA_VERSION`). Each row does the following:

- pins the tag to a digest whose revision label is the tag's commit;
- sets the stack up and runs the suite exactly as the CI e2e job does
  (`just e2e-up`, then `cargo test --locked -p e2e --features e2e`);
- reads back the version the running broker logged, which is what the Kafka
  broker column records;
- runs the deleted-segment positive control;
- records one of the outcomes above.

A row is green only when it records the outcome it declares, and a row whose
broker differs from its declaration fails. Rows below the full-drill floor seed
with segment digests optional, because those engines write none. They record
`unsupported(lever-absent)` only when Logweir is seen refusing the engine
("below the declared floor") in both the reduced row and the control.

The job renders its rows between the two markers below and changes nothing else
in this file. It publishes the page as the `support-matrix` artifact and in the
run summary, and opens a pull request only when the repository variable
`ENGINE_MATRIX_OPEN_PR` is `true`: GitHub Actions may not create pull requests
in this repository (checked 2026-09-28).

<!-- engine-matrix:rows:begin -->
No repaired run has been published here yet. The three scheduled runs before
the repair (2026-09-14, 2026-09-21 and 2026-09-28) produced no usable row; the
[engine route decision record](to-do/decisions/PROD-00-engine-route.md) says
why.
<!-- engine-matrix:rows:end -->

Filter-based checks use `scripts/run-named-tests.sh`, which resolves test
names against `--list` and fails if a requested test does not exist. A bare
`cargo test <filter>` can exit successfully after running zero tests.

Only recorded runs belong in the result table; workflow definitions and test
coverage alone do not establish a green version or authentication combination.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
