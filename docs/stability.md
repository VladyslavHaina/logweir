# Stability

A fresh clone must run `just engine` before `cargo test --workspace`; CI does
this in the `build` job.

## The v0.1.0 tag is the compatibility boundary

Read this before adding a field to the scorecard.

`format_version` is `1.0.0` and has been throughout the build. Up to and
including the v0.1.0 tag, adding a field to
`schemas/logweir-drill-scorecard-1.0.0.json` was an internal edit: no document
had been published, no reader existed, and the schema could be regenerated
freely. **The tag ends that.** From v0.1.0 onward, every scorecard in an
adopter's evidence bucket is a document some reader may already parse, so:

- **Adding an optional field is a MINOR bump** — `1.1.0` — with a new schema
  file beside the old one. It is not a free edit and it is not "still 1.0.0".
- **Changing a field's type, its meaning, or an identity rule
  (`validate_invariants`) is a MAJOR bump** — `2.0.0` — and needs two maintainer
  approvals ([MAINTAINERS.md](../MAINTAINERS.md)).
- **Removing or renaming a field is a MAJOR bump**, including a rename that
  merely fixes a spelling. `integrity.restoredPrincipalCouldConsume` is
  camelCase on the wire *permanently* for exactly this reason: it was frozen
  before SP3 fills it in.

The reason this is written down rather than left to judgement: the first change
after a release is the one most likely to be treated as if the release had not
happened. It has. **Do not let the first post-release addition inherit a
pre-release ruling by accident.**

## Known limitations of v0.1

- **`--from-cluster` (phase −1) has no execution path in v0.1.0.** This is the
  most consequential gap on this page, and it is a *scheduling* gap, not a
  scope decision: `--from-cluster` **is** in v0.1's scope, funded and decided —
  the 2026-09-03 deferral was refuted 3-0 and revoked
  ([ADR 0007](adr/0007-from-cluster-in-v0.1.md)). What is deferred is when in
  the task order the code lands, and it lands in a follow-up task authored after
  the main line is green.

  What that means concretely, and each item is checkable:

  - There is **no `--from-cluster` flag on the CLI**.
    `logweir drill run --from-cluster ...` is a usage error and exits **1**,
    naming the unknown argument. It is not silently ignored.
  - Every scorecard v0.1.0 emits carries `source.captured_by_logweir: false`, a
    null `measured.rpo_source_relative_seconds`, and
    `rpo_source_relative_unmeasured_reason: "source cluster never contacted"`.
    `last_phase_completed` never takes the value `-1`.
  - **The format is already bound**, so this costs no compatibility event later:
    the three fields are in the frozen 1.0.0 schema and
    `Scorecard::validate_invariants` enforces their pairing in **both**
    directions today, before the first writer exists.
  - **The consequence for an adopter is real and is the point of recording it:**
    you must already possess a `kafka-backup` archive to run a drill. Logweir
    cannot take the backup for you in v0.1.0.

- **No musl release target.** `rdkafka` vendors and compiles `librdkafka` from
  C, which does not cross-compile to musl without substantially more work than
  v0.1 has ([ADR 0004](adr/0004-kafka-client.md)). `release.yml` *attempts* the
  build in a `continue-on-error` job and the release does not block on it, so a
  musl binary may or may not be attached to a given release. Do not assume one.

- **Three release workflows have never been executed.**
  `.github/workflows/release.yml`, `release-drill.yml` and `engine-matrix.yml`
  were authored for the v0.1.0 tag against a repository with **no GitHub remote
  configured**, so none of them has ever run. They are checked in, their YAML
  parses, and their content is reviewed — but "the release workflow produced
  binaries and an image" and "the drill ran from the released binary artifact"
  are **UNVERIFIED**, not green. Run them via `workflow_dispatch` before
  announcing the tag. `docs/support-matrix.md` says the same about its own rows.

- **The sample window in `examples/drill.yaml` is illustrative and will not
  match your archive.** `sample.window_start` / `sample.window_end` name the
  point-in-time range you are recovering to, so a checked-in example cannot
  carry a correct one. A window that overlaps no segment is REFUSED with exit
  1 — "a drill over an empty window would report a pass that means nothing" —
  rather than reported as a pass over nothing. `scripts/demo.sh` and the e2e
  harness both rebind those two fields at run time, and only those two.

- **The exit-code contract is nearly invisible under Kubernetes** unless the
  Job is shaped with `restartPolicy: Never` and `backoffLimit: 0`. Exit 2 ("a
  drill ran and did not pass — a scorecard WAS signed") otherwise renders as a
  generic `Error`, indistinguishable from exit 1 ("nothing ran, no artifact").
  Full details, and the retry behaviour that is actively harmful, in
  [kubernetes.md](kubernetes.md).

- **The checked-in fixtures show one value the code cannot emit.**
  `e2e/fixtures/signed/*.json` carry a POPULATED `engine_subreport`, which no
  v0.1 scorecard has; they are not regenerated because doing so would invalidate
  the signatures they exist to exercise. `e2e/fixtures/scorecard-pass.json` is
  unsigned and **was** corrected: its `evidence.create_only_enforced` now reads
  `false`, matching what phase 8 emits. Both facts are stated beside the files
  in [`e2e/fixtures/README.md`](../e2e/fixtures/README.md).


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

### `sample.anchor` accepts only `head` in v0.1; `tail` and `random` are refused

`sample.anchor` chooses WHICH records in the sampled window a drill reconciles.
The spec vocabulary is `head`, `tail` and `random`, and **v0.1 implements only
`head`.** A plan naming either of the other two is REFUSED at phase 0 with
**exit 3** — before anything runs, no scorecard written, nothing uploaded — and
the message names the limitation.

Why they are refused rather than run: phase 4 honours the anchor when choosing
which ARCHIVE records to fingerprint, while phase 7 reconciles by reading the
restored topic's FIRST `sample.records_per_partition` records. For `head` those
are the same records. For `tail` and `random` they are not, so the drill would
compare two different sets and report a healthy backup as a failure.

The two are refused for different reasons, which matters if you are wondering
how hard they are to add. `tail` is not expensive — reading the last N records
of the target is a bounded read — it is **unsound**: the target's last N records
are the last N of the RESTORED window, which match the archive's last N in the
sampled window only if the restore wrote every in-window record contiguously,
and that is the very property the drill is measuring. `random` is unsound in the
same way and additionally unbounded, since reaching offsets spread across the
window means reading the whole span between them. Measured on
this repository's own compose stack (338 records per partition, 25 sampled):
`random` overlapped in 2 offsets and scored `pass_rate_measured: 0.08` with
`outcome: fail-integrity` against a byte-for-byte correct restore.

Refusing is deliberate, and it is not the same as quietly substituting `head`:
running a different sample than the approved plan named would make the signed
scorecard record an anchor the drill never applied.

**What to do:** set `sample.anchor: head`, or omit the field — `head` is the
default. If you were relying on `random` to rotate coverage across runs, rotate
the sampled WINDOW between drills instead; that varies which records are
examined while keeping the anchor at `head`.

An unrecognised spelling (`sample.anchor: sideways`) is a different failure: the
spec does not parse, and the drill exits **1** naming the offending value. It
can never degrade to head-like behaviour, because `anchor` is a closed enum
rather than a free-form string.

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
simply `null`.

**The reason is NOT in the scorecard.** This paragraph used to say
`phases[8].notes` records it; it cannot. Phase 8 signs a frozen copy of the
document, so the document's `phases` array ends before phase 8's own record —
there is nowhere in the signed bytes for a phase-8 warning to go. The note is
emitted on the structured log instead (`target: logweir::score`, with the run
id), and it says in the line that it is not in the signed document. What the
artifact carries is `engine_subreport: null`, and this section is what tells
you how to read it.

Two consequences worth stating plainly:

- Reading `engine_subreport: null` means "no engine sub-report was retained",
  not "the engine reported nothing wrong".
- The checked-in examples (`e2e/fixtures/signed/*.json`,
  `e2e/fixtures/scorecard-pass.json`) show a POPULATED block. They document the
  format; they are not output the shipping code can produce. The `signed/` pair
  cannot be regenerated without invalidating the signatures they exist to
  exercise, which is why they stay as they are; `scorecard-pass.json` is
  unsigned, so its OTHER divergence — `evidence.create_only_enforced: true` —
  was corrected to `false` in Task 22 rather than documented. See
  [`e2e/fixtures/README.md`](../e2e/fixtures/README.md).

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

---

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
