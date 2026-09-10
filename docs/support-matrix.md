# Engine support matrix

<!--
HAND-WRITTEN. `.github/workflows/engine-matrix.yml` regenerates this file weekly
and opens a PR when it changes. Until that workflow has run for the first time,
this file carries exactly the rows that were ACTUALLY EXERCISED, and says so for
every other version rather than projecting a result. A matrix that lists an
untested version with a verdict is worse than a short matrix.
-->

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
| **0.21.0** | `sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317` | **`pass`** | Full drill, 2026-09-05, against the compose stack (Kafka 3.7.1 KRaft + MinIO) via `scripts/demo.sh`: `outcome: pass`, `integrity: byte-fingerprint/pass`, 150/150 records reconciled, `pass_rate_measured: 1.0`, `rto_excluding_preflight_seconds: 6`, `rpo_seconds: 9`, all three objectives met, signature VALID under both `logweir drill verify` and `docs/verify_scorecard.py`. `header_preflight: honoured`. |

That is **one green row at the declared floor**, which is the release
requirement. It is also the only row that has been run.

## Versions with no row yet, and why

Listed so that "absent from the matrix" is never mistaken for "known bad", and
so nobody quotes a projected verdict as a tested one.

| Engine version | Status | Note |
|---|---|---|
| 0.20.x | **not yet run** | Inside the declared two-minor deprecation window in `docs/stability.md`. `engine-matrix.yml` covers it on its first run. |
| 0.19.x (except v0.19.1, below) | **not yet run** | Same. |
| **v0.19.1** | **`unsupported (lever-absent)`, by inspection — not by a run** | This is `strimzi-backup-operator`'s hard-coded `DEFAULT_BACKUP_IMAGE` [VERIFIED-SPEC `U/strimzi-backup-operator/src/engine.rs:17`]. It predates **both** levers and is **below** the 0.21.0 full-drill floor, so it **can never be green** and the matrix job runs it against a reduced row set (restore succeeds, `pass-degraded`, `integrity.level: consume-only`) rather than the full one. An operator whose default has not caught up — not a fault. |
| 0.16.0 – 0.18.x | **unsupported**, by floor | Below the full-drill floor; only the unknown-key warning mechanism works. |
| < 0.16.0 | **unsupported (lever-absent)**, by floor | The warning mechanism this project depends on does not exist. |

## Authentication modes, and what each one has actually been run against

Task 6 made SASL/SCRAM-SHA-512 reachable end to end. The matrix rows above are
all **PLAINTEXT** drills, and that is stated here rather than left to be
inferred from a table that does not mention auth at all.

| `auth.mode` | Logweir's client | The engine's client | Exercised |
|---|---|---|---|
| `plaintext` (default) | `security.protocol: PLAINTEXT` | no `security:` block rendered — the engine's own default | **Yes**, by the 0.21.0 row above and by every e2e drill. |
| `scramSha512`, `tls: false` | `security.protocol: SASL_PLAINTEXT`, `sasl.mechanism: SCRAM-SHA-512` | `security_protocol: SASL_PLAINTEXT`, `sasl_mechanism: SCRAM-SHA512` | **Rendered bytes and engine config-load only.** The digest-pinned engine loads the rendered document with **no** unknown-key warning and **no** parse error; no automated gate completes a SCRAM handshake, because the compose stack's SASL listeners land in a later task (STANDING RULE 15). |
| `scramSha512`, `tls: true` | `security.protocol: SASL_SSL` | `security_protocol: SASL_SSL` | **Rendered bytes only.** TLS is not exercised locally at all — the compose stack speaks no TLS — and `tls` flips exactly one match arm on each side. A private-CA adopter configures **two** trust stores (Global Constraint 29; `docs/stability.md`). |
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

**That control was vacuous until 2026-09-05.** The step filtered `cargo test` on
the string `dry_run_check_segments`, which is the name of a struct field and of a
rendered-YAML key and has never been the name of a test: it matched **0 of the
475 tests**, and `cargo test` exits 0 on a filter that matches nothing. So
`fail(lever-not-honoured)` — the one outcome of the five that detects upstream
accepting a lever and ignoring it — was unreachable by construction, and the
weekly job would have published a green verdict about a check that never ran.
Both filter-based steps now go through `scripts/run-named-tests.sh`, which
resolves every name against `--list` first and **exits 1 on a name that matches
nothing**.

Until it has run, this file is short on purpose.

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
