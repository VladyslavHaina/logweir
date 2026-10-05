# Engine support matrix

<!--
HAND-WRITTEN. `.github/workflows/engine-matrix.yml` regenerates this file weekly
and opens a PR when it changes. Until that workflow has run for the first time,
this file carries exactly the rows that were ACTUALLY EXERCISED, and says so for
every other version rather than projecting a result. A matrix that lists an
untested version with a verdict is worse than a short matrix.
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
| `fail(reason)` | The drill ran and did not pass, for the stated reason. |
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
| 0.20.x | **unsupported by the full-drill floor; no recorded run** | Matrix compatibility probes do not override the 0.21.0 runtime floor. |
| 0.19.x (except v0.19.1, below) | **unsupported by the full-drill floor; no recorded run** | Same floor as 0.20.x. |
| **v0.19.1** | **`unsupported (lever-absent)`, by inspection — not by a run** | This is `strimzi-backup-operator`'s hard-coded `DEFAULT_BACKUP_IMAGE` [VERIFIED-SPEC `U/strimzi-backup-operator/src/engine.rs:17`]. It predates **both** levers and is **below** the 0.21.0 full-drill floor, so it **can never be green** and the matrix job runs it against a reduced row set (restore succeeds, `pass-degraded`, `integrity.level: consume-only`) rather than the full one. An operator whose default has not caught up — not a fault. |
| 0.16.0 – 0.18.x | **unsupported**, by floor | Below the full-drill floor; only the unknown-key warning mechanism works. |
| < 0.16.0 | **unsupported (lever-absent)**, by floor | The warning mechanism this project depends on does not exist. |

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
| OAUTHBEARER / MSK IAM | `AuthConfig::Token` — constructing it returns an error | not rendered | **Not in tag 1.** |

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

**Versioned buckets pin the manifest (FX-7).** On a bucket with versioning
enabled, a backup receipt records the version id of the manifest it attests
(`archive.manifest_version_id`, receipt format `1.1.0`), and a point-bound
restore and the catalog compare it with the manifest's current version; when
they differ they read the pinned version by id, so a byte-for-byte copy of the
archive in another bucket — which carries the pin and not the version — is
checked by its digest and says the pin could not be checked there
([backup-receipt.md](formats/backup-receipt.md#the-pinned-manifest-version-versioned-buckets)).

| Object store | Manifest version pinned |
|---|---|
| SeaweedFS 4.48, bucket with versioning (Object Lock) enabled | **Yes, measured** (compose slot 3, 2026-09-29; again on slot 2, 2026-10-05): the receipt pins the version the read-back was answered with; after a `v0.1.5` runner rewrote the set (identical manifest bytes, a rewritten segment), the catalog's deep check reported the point `Conflict` by version, and a point-bound `restore run` of it exited 3 `PointBindingMismatch` (2026-10-05) |
| A byte-for-byte copy of a pinned point (`aws s3 cp`) into a MinIO unversioned bucket, and into a SeaweedFS 4.48 versioned bucket with its own version ids | **The same point, measured** (slot 2, 2026-10-05): the deep check reports it `Available` with the note that the pin could not be checked in that bucket, and a point-bound `restore run` gets past the binding and logs `PointPinUnchecked`. Each store answered the foreign version id `404 NoSuchVersion`, or MinIO `400 InvalidArgument` for an id that is not a UUID |
| MinIO `RELEASE.2025-09-07T16-13-09Z` and SeaweedFS 4.48, unversioned buckets | **No pin, measured**: the store answers no version id, and the receipt is the `1.0.0` document |
| AWS S3 with versioning | `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` |
| Versioning suspended (S3's `null` version) | no pin by design: a `null` version is replaced in place. A point pinned BEFORE versioning was suspended is still checked: a write after the suspension, over a pinned version the bucket still holds, is `Conflict` (in process) |

## What the weekly job will add

`.github/workflows/engine-matrix.yml` runs the full compose drill against each
tag in the declared window — the newest four minors plus `v0.19.1` — records one
of the five outcomes, and opens a PR when this file changes. It also carries the
deleted-segment positive control described above.

Filter-based checks use `scripts/run-named-tests.sh`, which resolves test
names against `--list` and fails if a requested test does not exist. A bare
`cargo test <filter>` can exit successfully after running zero tests.

Only recorded runs belong in the result table; workflow definitions and test
coverage alone do not establish a green version or authentication combination.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
