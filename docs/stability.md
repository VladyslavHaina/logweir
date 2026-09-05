# Stability

A fresh clone must run `just engine` before `cargo test --workspace`; CI does
this in the `build` job.

## Known limitations of v0.1

- **S3 credentials come from `object_store`'s own chain, not the AWS SDK's.**
  `logweir-engine-oso`'s `Store::from_url`/`Store::read_only_from_url` build
  the S3 client with `object_store` 0.14's `AmazonS3Builder::from_env()`,
  which applies `object_store`'s OWN credential chain (static keys, then web
  identity / IRSA, ECS, EKS Pod Identity, IMDS). That is **not** the AWS SDK
  chain: `~/.aws/credentials` profiles, `AWS_PROFILE` and SSO are
  unsupported. State it plainly here because an adopter discovering it at
  drill time is a support ticket.

- **MSRV is 1.89**, raised from 1.82 by `object_store` 0.14's aws/azure/gcp/http
  feature set (Global Constraint 9 fixes that crate, version and feature set,
  so the toolchain floor moves instead). Rust edition 2024, required by that
  dependency tree's RustCrypto crates (`digest`/`crypto-common`/
  `block-buffer`), is only supported starting rustc 1.85; several other
  transitive dependencies (`crc-fast`, the `icu_*` crates, `idna_adapter`,
  pulled in through `reqwest`) then raise the floor further to 1.89.

- **Create-only evidence puts: MinIO is the only S3-compatible backend Logweir
  has actually tested.** Three facts follow, and they are stated separately
  because only the second is verified here:
  1. `object_store` 0.14 implements `PutMode::Create` on AWS S3 via the
     conditional `If-None-Match` put.
  2. MinIO is the only S3-compatible backend Logweir has tested. **Real AWS S3
     is NOT exercised in v0.1** — Global Constraint 17 forbids provisioning a
     cloud resource, and no task in this plan supplies a bucket, a region or a
     credential source. The AWS leg is `[UNVERIFIED against real S3]` and is
     confirmed by the first adopter run, not by this plan.
  3. Any other S3-compatible backend that answers `NotSupported` (or
     `NotImplemented`) to `PutMode::Create` takes the HEAD-then-PUT fallback in
     `Store::put_create_only`, which reports `create_only_enforced: false` to
     its caller. That fallback is not equivalent: between the HEAD and the PUT
     there is a window in which a concurrent writer can create the object, so
     it is a genuinely weaker guarantee, not a cosmetic difference. **The
     signed scorecard does not distinguish the two cases** — it publishes
     `create_only_enforced: false` either way, for the reason in the next
     bullet — so the only way to know which path a given backend takes is to
     test that backend, as MinIO was tested here.

- **The signed scorecard's `evidence` block makes NO claim about the upload.**
  Read this before quoting a scorecard's `evidence` block to an auditor.

  Four fields — `create_only_enforced`, `immutable`, `retain_until`,
  `version_id` — describe facts that only exist *after* the scorecard has been
  uploaded. The scorecard is signed *before* that upload (it must be: a
  signature covers bytes, and the bytes have to exist first), and it is never
  re-serialised afterwards, because re-serialising would invalidate the
  signature. So at signing time those four fields cannot be known.

  Rather than sign whatever value happened to be in them, Logweir **zeroes all
  four before signing**. Every scorecard Logweir emits in v0.1 therefore reads:

  ```json
  "evidence": { "create_only_enforced": false, "immutable": false,
                "retain_until": null, "version_id": null }
  ```

  What that does and does not mean:

  - It means **"no proof was obtainable at signing time."** It is *not* a claim
    that the object is mutable, was overwritten, or is unversioned.
  - The document **deliberately under-claims.** A run whose upload really was
    conditional still publishes `create_only_enforced: false`, because phase 8
    cannot know that yet and a signed document is not the place to guess.
  - Conversely — and this is the point — **a signed scorecard can never assert
    WORM protection or create-only enforcement that Logweir did not establish.**
    A valid signature over `immutable: true` would be indistinguishable from a
    verified fact, and nothing downstream (`logweir drill verify` included)
    could tell the difference.
  - The real post-upload readback *is* captured — `Store::put_create_only`
    reports whether the conditional put was used, and
    `Store::object_lock_readback` is consulted — and since Task 21a it **is**
    published, in a **second signed document written after the upload**:

    ```
    logweir/drills/<run_id>.receipt.json   # the readback
    logweir/drills/<run_id>.receipt.sig    # its DSSE sidecar
    ```

    The receipt carries `create_only_enforced`, `version_id`, `immutable`,
    `retain_until` and the time they were observed, plus `scorecard_sha256` —
    the sha256 of the **exact signed scorecard bytes** it describes, so a
    reader can tell which document the readback belongs to. Its payload type is
    `application/vnd.logweir.drill-put-receipt+json;version=1.0.0`; verify it
    the same way as the scorecard, against that type. This is the same shape as
    the teardown attestation, and for the same reason: a fact that only becomes
    true after signing needs its own signature rather than a second bite at the
    first one.

    Read the two documents together. The scorecard is the measurement and
    under-claims about storage; the receipt is the storage evidence and claims
    only what the store actually answered. A receipt that fails to upload is a
    logged warning and never changes the drill's outcome — so its **absence**
    means "no storage evidence was published for this run", never "the upload
    was not create-only".

  Independently of the above: `object_store` 0.14 — the crate, version and
  feature set Global Constraint 9 fixes — models no Object Lock / WORM API on
  any of its `aws`/`azure`/`gcp`/`http` backends, so
  `Store::object_lock_readback` returns `None` on every backend Logweir can
  build. Even the in-memory readback is therefore always "no proof" for
  `immutable`/`retain_until` today. A bucket genuinely under Object Lock will
  not be recognised as such until that API exists.

### A compacted target topic is reported as a mismatch, not as `partial`

`integrity.result: partial` exists for a sample the drill could not fully
examine, and a compacted topic — which legitimately holds fewer records than
the archive — reads like the obvious case for it. **v0.1 does not classify it
that way.** Distinguishing "compaction removed this record on purpose" from
"the restore or the archive lost it" needs a specific mismatch cross-referenced
against the target's `cleanup.policy`, and phase 7 does not attempt it. A
compacted target is reported through the same path a real mismatch takes:
`integrity.result: fail`, with the offsets logged.

This is the safe direction — a drill over a compacted topic FAILS rather than
passing — but it means you cannot run a meaningful drill against a compacted
scratch topic in v0.1. Restore into a scratch topic with
`cleanup.policy=delete`, which is what phase 6 creates by default.

### `engine_subreport` is always null in v0.1

The scorecard reserves `engine_subreport` for the upstream engine's own
verbatim evidence report. **v0.1 never populates it.** `OsoCliEngine` does not
override `DataEngine::validation_run`, so Logweir never runs the engine's
`validation run` subcommand and nothing is ever written under
`logweir/<run_id>/engine-validation/` for phase 8 to retain. The default
implementation refuses rather than fabricating a report, so there is no risk of
a scorecard claiming an engine validation that never happened — the field is
simply `null`, and `phases[8].notes` records why on the in-memory document.

Two consequences worth stating plainly:

- Reading `engine_subreport: null` means "no engine sub-report was retained",
  not "the engine reported nothing wrong".
- The checked-in examples (`e2e/fixtures/signed/*.json`,
  `e2e/fixtures/scorecard-pass.json`) show a POPULATED block. They document the
  format; they are not output the shipping code can produce. Same class of
  divergence as `evidence.create_only_enforced: true` in those same fixtures.

`crates/logweir-engine-oso/tests/engine.rs` carries an `#[ignore]`d marker test
that CI runs on every build so the missing override cannot be forgotten, and
`e2e/tests/full_drill.rs`'s
`the_engine_subreport_is_absent_until_oso_cli_engine_overrides_validation_run`
turns RED the day it lands.

### A crashed restore is not resumable in v0.1

The rendered `restore.checkpoint_state` path is **pod-local and is never uploaded**. If the
restore process dies mid-run, there is no checkpoint to resume from: the drill re-runs from
phase 0. The presence of the `checkpoint_state` key in the rendered `restore.yaml` does not
imply resumability, and Logweir does not offer it in v0.1.

## Engine compatibility and support policy

- **Engine version floors, and why each exists (spec §7.2).** `kafka-backup`
  **0.16.0** is the floor for the unknown-config-key warning mechanism
  `OsoCliEngine` parses off stderr/stdout (`Ignoring unknown config key
  ...`); below it, a dropped rendered key fails silently instead of aborting
  the run. `kafka-backup` **0.21.0** is the floor for the full drill as
  shipped — it is the version this plan verified every vendored struct and
  CLI behaviour against (`docs/UPSTREAM-VERSIONS.md` in the planning repo;
  `ae5a102f93b5270927d95d4ccec184b577febb10`), and it is the version pinned
  by digest in `third_party/kafka-backup-binary.digest`.

- **Runtime image floor.** The extracted `kafka-backup` binary is dynamically
  linked against **glibc >= 2.36** and **libssl3**, and performs TLS
  connections that need a CA bundle — hence `debian:bookworm-slim` plus
  `ca-certificates` and `libssl3` in the runtime stage, never a musl or
  distroless base (see the `Dockerfile`'s own comment).

- **Format policy.** `format_version` in the signed scorecard and the
  put-receipt is semver. A **minor** bump adds optional fields only —
  existing readers keep working by ignoring what they don't recognise. A
  reader **ignores unknown fields** on any document whose major matches what
  it supports, and **refuses** a document whose major is higher than it
  supports, rather than guessing at a shape it has never seen.

- **CLI flags and exit codes are stable within a major.** A given Logweir
  major version does not remove or repurpose a flag, nor change the meaning
  of an exit code, without a major bump. New flags and new exit codes may be
  added in a minor.

- **Supported OSO digests.** Logweir supports the digest currently pinned in
  `third_party/kafka-backup-binary.digest`, plus the two minor versions
  before it, on a two-minor deprecation window:

  | Engine version | Status |
  | --- | --- |
  | 0.21.x | supported (currently pinned) |
  | 0.20.x | supported (deprecation window) |
  | 0.19.x | supported (deprecation window) |
  | 0.16.0 – 0.18.x | unsupported (below the full-drill floor; only the unknown-key warning mechanism works) |
  | < 0.16.0 | unsupported (lever-absent: the warning mechanism this plan depends on does not exist) |

  `strimzi-backup-operator` hard-codes `DEFAULT_BACKUP_IMAGE =
  "osodevops/kafka-backup:v0.19.1"` [VERIFIED-SPEC
  `U/strimzi-backup-operator/src/engine.rs:17`], which is **below** the
  0.21.0 full-drill floor. That is a support-matrix row reading `unsupported
  (lever-absent)` — an operator whose default has not caught up yet — not a
  fault Logweir raises against that operator.

- **Explicit non-contracts.** The Rust crates in this workspace
  (`logweir-core`, `logweir-engine-oso`, `logweir-evidence`, `logweir-kafka`,
  `logweir`) are **not** a stable API before 1.0. Only the signed document
  formats (scorecard, put-receipt, teardown attestation) and the `logweir`
  CLI's flags and exit codes are covered by the stability promises above;
  internal crate APIs may change in any release before 1.0.
