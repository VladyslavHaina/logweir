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
