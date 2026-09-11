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

- **The PagerDuty `dedup_key` is an interim key, not a durable drill
  identity.** PagerDuty's Events v2 API keys an incident by `dedup_key`, and
  v0.1's key was `logweir-drill-{cluster_id}` — so two drill specs pointed at
  one scratch cluster shared a single incident, and because a passing drill
  sends `event_action: resolve`, a nightly smoke drill passing at 03:00 closed
  the weekly full drill's open page. The key is now
  `logweir-drill-{name-or-plan-hash-prefix}-{cluster_id}`, built from the drill
  spec's **optional** `name` (`notifications` is documented in
  [`docs/formats/drill-spec.md`](formats/drill-spec.md)) falling back to the
  first 12 hex characters of the approval's `plan_hash` when the spec has no
  name — a sha256 over the approved plan bytes, so it is distinct per spec and
  stable across re-runs.

  Two residuals, both deliberate:

  - **The durable fix is an artifact-side `drill_id`** that travels on the
    scorecard itself (backlog **T1-8**), which §12 assigns to decision **O16**
    with default *not funded*. Until that lands, a drill's identity for
    alerting purposes is spec-side only: rename a spec and its open incidents
    are orphaned; copy a spec to a second file without changing its `name` and
    the two share an incident again. Give every spec a distinct, stable `name`.
  - **The operational-failure key is separate from the drill-result key, on
    purpose.** Exits 1, 3 and 4 page under
    `logweir-drill-{name-or-"unnamed"}-preflight`, never under the result key,
    so a later passing run's `resolve` does **not** automatically close an
    incident that says "logweir could not run this drill at all" — those are
    different facts about different things. The cost is that a spec with no
    `name` collapses every operational failure on the install to one incident,
    which is the second reason to set `name`.

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

- **The YAML parser is archived.** The drill spec and both rendered engine
  documents are YAML, parsed by `serde_yaml` 0.9, which resolves to
  `0.9.34+deprecated` — an archived crate (RUSTSEC-2024-0370). It is kept for
  v0.1 deliberately: no alternative was evaluated, and swapping the parser under
  the two renderers that Global Constraints 4 and 7 pin is not a release-week
  change. **Revisit before v0.2.** `deny.toml` carries the matching `ignore`
  entry and points here; this paragraph is what it points at, and it was missing
  until 2026-09-05 — `deny.toml`'s comment claimed a sentence in this document
  that did not exist.

- **No musl release target.** `rdkafka` vendors and compiles `librdkafka` from
  C, which does not cross-compile to musl without substantially more work than
  v0.1 has ([ADR 0004](adr/0004-kafka-client.md)). This is not contradicted by
  the `Dockerfile` cross-compiling to `x86_64-unknown-linux-gnu`: that target
  has a one-package Debian toolchain and multiarch `:amd64` copies of every
  C library librdkafka wants, and musl has neither. `release.yml` *attempts* the
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

- **The release image is built once and pushed only after the gate returns 0 —
  a lint-proved shape, not an observed run.** `.github/workflows/release.yml`
  produces the runtime image a single time, into the runner's own daemon
  (`docker/build-push-action` with `load: true`, `push: false`, tagged
  `logweir:check`); runs `scripts/check-image.sh logweir:check` against it; and
  only then tags and pushes those same bytes to `ghcr.io`. Nothing is
  recompiled between the assertion and the upload, so the bytes the gate
  interrogated are the bytes an operator pulls. The repository digest is read
  back from the daemon after the upload and republished in the release notes,
  so a consumer can pin by digest instead of by a mutable tag (Global
  Constraint 7). What is machine-checked is what the workflow *says*:
  `crates/logweir/tests/workflow_lint.rs` parses the YAML on a laptop and
  asserts the shape, in the default `cargo test`. **The workflow itself has
  still never executed on any commit, including the v0.1.0 tag** (the entry
  above), so its first run will also be its first debugging session. The digest
  in the release notes is also **not** the digest the Kubernetes manifests pin:
  those name a locally built image, which is a different artifact from a
  different machine.

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

- **A drill that verified correctly and could not clean up still exits `0`.**
  Phase 9 deletes the scratch topics the restore created on your cluster, and it
  runs *after* phase 8 has signed and uploaded the drill result. A broker that
  refuses a deletion — `TOPIC_DELETION_DISABLED`, an ACL, a topic held open —
  therefore leaves real topics on a real cluster on a run the exit code calls a
  pass. What v0.1 guarantees is that it **says so**, on all three surfaces, on
  the run that left them:
  - the metric `logweir_drill_teardown_topics_failed{cluster}`, emitted on every
    scorecard-carrying path, `0` included, so `> 0` is a valid alert
    ([metrics.md](metrics.md));
  - a `WARN` on that run's log naming every failed topic, carrying the run id as
    a field on the event; and
  - a clause on `drill run`'s summary line naming the count and the topics. A
    clean run's line is unchanged.

  The signed teardown attestation
  (`logweir/drills/{run_id}.teardown.json`) has always been honest about this —
  a refused topic goes in `topics_failed` and never in `topics_deleted` — and
  remains the durable record. **Whether a leftover topic should also change the
  process exit code is an open question this release does not answer** (stage-2
  ruling R-C): making it exit 4 would be a behaviour change after the result has
  already been signed and uploaded, and it is deferred to its own decision
  rather than made in passing. `teardown_failure_does_not_change_the_exit_code`
  pins the current behaviour in both directions, so the question cannot be
  answered by accident.

  The same holds, with one surface fewer, for a teardown attestation that could
  not be **persisted**: it is a `WARN` at the call site and nothing else, the
  exit code is unchanged, and **the durable signal is the document's absence
  from the bucket** — there is no metric for it in v0.1. An operator who wants
  to detect it checks for the `.teardown.json` key beside each drill's
  scorecard.

- **The checked-in fixtures show one value the code cannot emit.**
  `e2e/fixtures/signed/*.json` carry a POPULATED `engine_subreport`, which no
  v0.1 scorecard has; it is kept because it is the only checked-in example of
  that block. Their two OTHER divergences are closed, not documented:
  `last_phase_completed` reads `7` and the `evidence` block is fully zeroed in
  both. Regenerating them no longer invalidates anything — the fixture keypair
  is pinned and read, so `just fixtures-sign` re-signs under the same key.
  `e2e/fixtures/scorecard-pass.json` is unsigned and was corrected the same way:
  its `evidence.create_only_enforced` reads `false`, matching what phase 8
  emits. All of it is stated beside the files in
  [`e2e/fixtures/README.md`](../e2e/fixtures/README.md).


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
  format; they are not output the shipping code can produce. This is the LAST
  remaining divergence in those files: the `signed/` pair was re-minted under
  the pinned keypair, so `last_phase_completed` reads `7` and the `evidence`
  block is zeroed in both, and `scorecard-pass.json` — which is unsigned — was
  corrected the same way in Task 22. See
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

### SASL/SCRAM-SHA-512: two clients, two trust stores, and one password variable

Logweir dials a cluster with **two different clients**, and a SCRAM adopter
configures both.

1. **Logweir's own client** is librdkafka (`rdkafka`, the `ssl` and `sasl`
   features). It sets `security.protocol` to `SASL_SSL` when `auth.tls` is true
   and `SASL_PLAINTEXT` otherwise, with `sasl.mechanism: SCRAM-SHA-512`.
2. **The engine's client** is `kafka-backup`'s own, driven by the
   `security:` block Logweir renders into `backup.yaml`, `restore.yaml` and
   `validation.yaml` — `security_protocol`, `sasl_mechanism`, `sasl_username`,
   `sasl_password`.

**The trust stores are separate, and this is Global Constraint 29.** The
engine falls back to bundled `webpki-roots` unless `ssl_ca_location` is set
[U:crates/kafka-backup-core/src/kafka/tls.rs:22-23,126-129]; Logweir's rdkafka
path uses the runtime image's `ca-certificates`. So a **private-CA adopter
configures two things**, not one: the CA file for the engine, and a CA bundle
the image trusts for Logweir. Tag 1 renders no `ssl_ca_location` — an adopter
with a private CA is a case tag 1 does not configure for them.

**The mechanism has two spellings and they are both correct.** The engine's
wire value is `SCRAM-SHA512`, with **one** hyphen: its `SaslMechanism` is
`#[serde(rename_all = "SCREAMING-KEBAB-CASE")]` over
`Plain, ScramSha256, ScramSha512, Gssapi`
[U:crates/kafka-backup-core/src/config.rs:318-331]. librdkafka's is
`SCRAM-SHA-512`, with **two**. Upstream's own `config/example-backup.yaml`
comments the librdkafka form and is wrong for its own parser. Verified by
execution against the digest-pinned engine: the two-hyphen value in an engine
config is a serde **type** error that aborts config load —
`Failed to parse config: source.security.sasl_mechanism: unknown variant
"SCRAM-SHA-512", expected one of "PLAIN", "SCRAM-SHA256", "SCRAM-SHA512",
"GSSAPI"` — and **not** an unknown key, so the stderr unknown-key readback
cannot catch it. Both spellings are read out of source by
`crates/logweir-engine-oso/tests/render_scram.rs::the_engine_spelling_is_one_hyphen_and_librdkafkas_is_two`,
so neither can drift alone.

**The four keys are nested under `security:`, and the nesting was measured.**
They are fields of `SecurityConfig`, reached through `KafkaConfig.security`
[U:config.rs:173-208] — not fields of `KafkaConfig`. Rendered one level too
high, the pinned engine returns four *"Ignoring unknown config key
`source.security_protocol`"* warnings and `OsoCliEngine`'s
`assert_no_dropped_logweir_key` aborts the run at exit 1 **after** the document
was written. Short of that abort, a plan that named SCRAM would have dialled
the cluster unauthenticated.

**The password is never in a spec, a plan, a rendered document, a receipt or a
plan hash.** It is projected into the runner's environment as
`LOGWEIR_SOURCE_PASSWORD` (source/backup) or `LOGWEIR_TARGET_PASSWORD`
(target/restore); the rendered documents carry the literal
`sasl_password: ${LOGWEIR_*_PASSWORD}` and the engine substitutes it out of its
own process environment. There is **no CLI flag** for it, at any command.

Three consequences an operator should know:

- **An unset variable under `auth.mode: scramSha512` exits 1, not 3.** Nothing
  was refused; project the Secret and re-run. The check happens in Logweir,
  before the engine is spawned, and that ordering is load-bearing: with the
  variable unset the engine substitutes the **empty string** behind nothing
  but a `WARN Environment variable 'LOGWEIR_SOURCE_PASSWORD' is not set, using
  empty string` and **still loads the config and starts the run** — verified by
  execution against the pinned engine.
- **Five characters make a password unrenderable and are refused (exit 3,
  `refusal-reason=CredentialNotRenderable`):** newline, carriage return,
  double quote, single quote and `$`. The refusal names the character *class*
  and the variable, never the value.

  On `$`, specifically, the reason was settled by execution rather than left
  as a reading. The engine's `expand_env_vars`
  [U:crates/kafka-backup-cli/src/commands/config.rs:6-34] is a **single**
  left-to-right pass over the input: substituted text is appended to the
  output and never re-scanned. Probed against the pinned engine with
  `OUTER='${INNER}'`, the engine logged
  ``Ignoring unknown config key `probe_${INNER}` `` — the literal, unexpanded.
  So a `$`, and even a whole `${VAR}`, inside a projected password provably
  **cannot** name or read an environment variable at engine 0.21.0. The
  refusal is kept anyway, and the reason is now forward-defence rather than
  ambiguity: **GC8's 0.21.0 is a floor, not a ceiling**, upstream describes its
  own approach as *"this simple approach is sufficient"*, and the failure mode
  of a future recursive pass is a password that reads an environment variable
  — while the cost of the refusal is one refused password and a message that
  says why. Narrowing it to `${` would buy `pa$$word` and no security.
- **The placeholder is rendered unquoted, so the expanded value is a YAML
  plain scalar.** Its residual hazards — a `#` preceded by a space, a `: `, a
  leading flow indicator — produce a config **parse error** or a **truncated
  password**, i.e. a failed authentication. They cannot open a new YAML key,
  because newline and carriage return are refused before the value is ever
  projected. Double-quoting would be worse, not better: `\` becomes an escape
  introducer, so a password containing a backslash would be silently rewritten
  and a trailing one would unterminate the scalar. A parse error is the right
  way to fail.

**What is not verified.** Nothing in tag 1 authenticates against a real SCRAM
listener in an automated gate: `e2e/compose/docker-compose.yml` gains its
`SASL://kafka-broker-1:9096` + `SASLEXT://localhost:9097` listeners and its
`scram-setup` service in a later task (STANDING RULE 15), and until then every
SCRAM test asserts rendered bytes, exit codes and refusals — never a successful
handshake. **`[UNVERIFIED — needs an MSK cluster]`** for everything
MSK-specific: MSK's SCRAM credentials are held in AWS Secrets Manager and its
brokers require TLS, so the sentence that would verify it is *"point
`auth.tls: true` and `bootstrap_servers` at an MSK cluster's
`*.kafka.<region>.amazonaws.com:9096` endpoint, project the Secrets Manager
value into `LOGWEIR_TARGET_PASSWORD`, and record that `drill run` reaches
phase 2"* — which needs a provisioned MSK cluster and is therefore forbidden by
Global Constraint 17 (zero cloud spend). It is recorded as blocked, never as
closed. **`[UNVERIFIED — needs an MSK cluster]`** likewise for MSK IAM /
OAUTHBEARER, which is `AuthConfig::Token` and is not in tag 1 at all.

### The unit suite dials nothing; the e2e suite dials

Recorded rulings from Task 5b. They bind every later task in this repository.

**`cargo test --workspace` must open no connection, and no single test in it
may take more than five seconds.** The default feature set is the run every
contributor and every agent does dozens of times a day; a test in it that waits
on a broker, a bucket or a webhook that is not there is paying for a dependency
it does not have. The two bounds — 120 s for the whole suite, 5 s for any one
test — are enforced by `just time-unit-suite`, which also refuses to run while
9092 or 9000 answers, because a timing number taken against a live stack is
about a different machine. `just e2e` is where dialling belongs, and it is a
different check run at a different time.

**An unroutable address is not a fast-failing mechanism for librdkafka.**
MEASURED at Task 5b, through the compiled binary, with a local archive so only
the target check was timed: `127.0.0.1:1`, `localhost:0`, `localhost:99999`, a
syntactically invalid host, and an EMPTY broker list each cost **20.1 s**,
which is `crates/logweir-kafka/src/rdkafka_reader.rs:16`'s
`const T: Duration = Duration::from_secs(20)` to three significant figures. A
refused connect does not shorten `T`; librdkafka retries the connection with
backoff and the metadata *request* waits the constant out. So no test may use
an address as its speed mechanism. Any test that needs "unreachable broker"
behaviour in the default suite uses a `ClusterReader` double. Making `T`
configurable is open decision **O18** (folded into G19) and is not any test's
to take.

**A well-formed S3 config with no credentials does not dial its endpoint
first — it dials `169.254.169.254`.** `AmazonS3Builder::from_env()` falls
through to the EC2 instance-metadata credential provider, and `object_store`
0.14.1's unconfigured retry budget spends ten attempts against that link-local
address — ~6.5 s — before the configured endpoint is ever contacted. On a
workstation that is not an EC2 instance this is traffic off the loopback
interface from a unit test (Global Constraint 17), and it is invisible in the
error message unless you read the URL in it. A unit test that wants an
unreachable *classification* calls the pure `evaluate_storage`; only the
`e2e`-gated form dials.

**A grep audit proves a constructor is absent, not that a socket is closed.**
`crates/logweir/tests/no_network_in_unit_tests.rs` is the cheap always-on half
and `just time-unit-suite`'s per-test bound is the independent second half.
Neither alone is the acceptance.

**Mutation rounds run in an isolated worktree, and build under a throwaway
target directory.** A round leaves mutated source behind whenever it is
interrupted, and "there were backups" is not a property anyone can verify
afterwards — so it does not happen in a working tree that also holds work in
progress. And wherever it runs, it builds somewhere disposable. Five tasks of
mutants built into the shared `target/` and nothing cleaned up after them:
`target/debug/deps` reached **873,349 files / 43.5 GiB**, and at that size
cargo spends ~30 s per test binary fingerprinting the directory — the whole
workspace suite took twenty minutes at 0% CPU and was twice mistaken for a
hang. The pure-core binary cargo took 30 s over runs its tests in 17 ms when
executed directly; the cost was never in the tests. So:

    just mutant "test --workspace --lib doctor"   # CARGO_TARGET_DIR=target/mutants
    just mutant-clean                             # when the round is done

`just deps-count` (part of `just lint`) is the detection: it fails above
**50,000** files in `target/debug/deps` and prints the count on every run so
the trend is visible before it fails. The ceiling is measured, not guessed —
5,608 files for a fresh clean-and-build, 10,026 after one ordinary task.

**Every notification POST is bounded.** `phase7_verify::notify` had no timeout
of any kind: ureq's agentless request builders carry none, so a sink that
accepted the connection and never replied hung the drill indefinitely — after
the scorecard was signed and uploaded. A refused connection was never the risk;
a firewall that DROPs, or a wedged sink, was. Notifications now go through
`notify_agent()` (connect 5 s, overall 10 s) and a timeout is swallowed and
logged exactly like any other transport failure, so the exit-code contract is
unchanged.

- **The image smoke gate (`just smoke`) — what it needs, what it proves, what it costs.**
  `just smoke` needs `docker` and `openssl` on the host and builds a `linux/amd64`
  image. **The Rust compile in that build is not emulated**: the builder stage runs on
  the build machine's own architecture and cross-compiles to
  `x86_64-unknown-linux-gnu`, so on an arm64 host only the runtime stage's `apt-get`
  and its four `COPY`s go through QEMU.
  `scripts/check-image.sh <image-ref>` proves six things about the image: **both**
  shipped binaries — `/usr/local/bin/logweir` and `/usr/local/bin/kafka-backup` — are
  **x86-64 ELFs** (`e_machine` 0x3e, which is what keeps the cross-compile honest for
  the one and the digest pin honest for the other), the CLI's dynamic linkage resolves
  (`ldd`, GC10),
  `kafka-backup --version` runs, `logweir --version` runs, `drill approve` mints a
  signed approval over `examples/drill.yaml`, and both redistributed licences are
  present (GC15 — asserted one file at a time, so a failure names which). It proves
  **nothing** about a real drill: no broker, no bucket, no cluster and no archive is
  touched.

  Measured on the development host on 2026-09-09, arm64, with the host otherwise
  quiet — load average `2.16` at the start of the build and `5.79` at its end. **A
  floor, not a support statement**, and in particular not a budget: build time here
  scales with how many other compiles share the machine. Task 17 sizes CronJob
  `requests` from these numbers and must not read them as a guarantee.
  - `docker build --platform linux/amd64` with the WHOLE builder stage forced cold
    (`--no-cache-filter builder`, i.e. packages, rustup target and compile all re-run):
    **`00:02:01`**, of which `20.8s` was the `apt-get` layer, `7.7s` `rustup target add`,
    `0.1s` `COPY . .` and **`92.1s`** the cargo layer (267 crates, `librdkafka` compiled
    from C among them). An ordinary edit-and-rebuild pays only the last of those,
    because the two layers above `COPY . .` stay cached.
  - Fully warm, nothing recompiled and every layer `CACHED`: `00:00:04`.
  - `bash scripts/check-image.sh logweir:check`: `00:02`.
  - `just smoke` end to end (cached build + gate + the `#[ignore]`d image tests):
    `00:29`, of which the tests were `25.5s`. **That is a RE-RUN figure, and it
    assumes the e2e test binary is already compiled.** `just smoke`'s last line is
    `cargo test -p e2e --features e2e --test check_image`, and on a tree where that
    target has never been built the compile is part of the wall clock: measured
    separately on 2026-09-09 at `37.4s` for the compile alone (`Finished test
    profile ... in 37.40s`), on a host under load average ~9. Quote `00:29` for a
    second run and nothing else. There are now **twelve** such tests, not eleven —
    Task 9 added `check_image_rejects_an_image_whose_engine_is_not_x86_64` with
    check 6's engine arm — and each builds a one-layer overlay image, so the test
    figure moves with the docker daemon and the host's load, not with the code:
    the same twelve took `100.7s` on a loaded host the same day.
  - resulting image size (`docker image inspect --format '{{.Size}}'`): `54453404` bytes.
  - peak RSS: `not observed`.

  Read those figures narrowly, because they are the ones most likely to be quoted at
  something they do not cover.
  - **They are a quiet-host floor.** The cargo layer is a 267-crate release compile
    and takes whatever share of the CPU it is left; on a machine running other builds
    it costs multiples of the figure above. Size against the slow case, never this one.
  - **Any** edit to a tracked file invalidates `COPY . .` and re-runs
    `RUN cargo build --release --target x86_64-unknown-linux-gnu -p logweir` from
    scratch: there is no cargo cache mount in the builder stage, so the compile
    never resumes, it restarts. That is the `92.1s`, not the `00:00:04`.
  - A machine with an empty BuildKit cache pays more than any of these — it also
    downloads both base images — and a machine whose builder architecture is already
    amd64 pays less, because there the "cross" build is a native one.

  **For contrast, and to keep the reason for the shape of the Dockerfile legible:**
  until 2026-09-07 the builder stage was emulated, and the same cargo layer took
  `3027s` of a `3044s` build on this host. That is **33x** the cross-compiled cargo
  layer above, and the emulated run had its `apt-get` layers already cached while the
  run above did not. The `FROM --platform=$BUILDPLATFORM` line and the `--target` flag
  are what removed it; check 6 of the smoke gate is what proves the resulting binary
  is still the right architecture.

## Recorded rulings that have no ADR yet

### Exit 3 has one documented exception: phase 0's `LogAppendTime` override probe

Exit 3 means "refused by a guard, before anything runs", and there is exactly one write that
happens before the approval is verified: on a target broker whose effective
`log.message.timestamp.type` is `LogAppendTime` — and only then — phase 0 creates the first mapped
target topic with `message.timestamp.type=CreateTime`, reads the value back, and deletes the topic
again on both branches. The reason it is a write and not a read is that no read answers the
question: whether a broker on `LogAppendTime` honours a per-topic `CreateTime` override is a
property of the broker, so the only way to find out is to ask this one, and getting it wrong means
every restored timestamp is silently replaced by the restore's wall clock. The probe is confined to
the drill's own scratch namespace (`TopicDeleter` refuses every name outside
`target.topic_mapping_prefix`), it runs only after phase 0 has established that no mapped target
topic exists, so the name it creates was free, and it is deleted before any verdict is returned —
but on a `LogAppendTime` broker an exit-3 run has therefore created and deleted one topic, and this
sentence is the record of it.

### The restore window's end is inclusive; the backup receipt's `covered.to_ms` is exclusive

A `Restore`'s window is a closed interval: the engine's PITR filter is `timestamp >= start &&
timestamp <= end`, so a record whose timestamp equals `restore.point_in_time` exactly **is**
restored. The `BackupReceipt`'s covered range is half-open in the other direction: `covered.from_ms`
is the oldest segment's inclusive start and `covered.to_ms` is the newest segment's end **plus one
millisecond** (`backup/phase_run.rs`), so it is the first instant the archive does *not* cover.
Copying a `covered.to_ms` into a `restore.point_in_time` is therefore harmless — one millisecond
wide of a region that holds nothing — while treating `point_in_time` as exclusive silently drops the
boundary record. Both documents' floors are the minimum segment start over the topics that document
names, so a receipt's `covered.from_ms` and a restore's `time_window_start` agree for the same
archive and the same topics.

### Interface I8's third stdout line is CONDITIONAL: `offset-report-key=` is present exactly when the engine wrote a report

A successful `logweir restore run` (or its `drill run` alias) prints its evidence keys as the last
lines of stdout, in this order:

```
scorecard-key=logweir/drills/<run_id>.json
sidecar-key=logweir/drills/<run_id>.sig
offset-report-key=logweir/drills/<run_id>.offsets.json
```

**The third line is present exactly when the run completed a restore AND the engine wrote its
offset-mapping report to the path the plan named.** The engine writes that file only from a
completed restore, and its own write failure is a warning rather than an error, so a restore that
finished can legitimately leave nothing at the path. When that happens `phase8_score` records no
offset-report key or digest in the signed scorecard, uploads nothing, emits a `tracing::warn!`
naming the run, the path and the error — and the run still **exits 0 with two key lines**. It is
not a finding: exits 1, 3 and 4 write no artifact at all by contract, and a scorecard whose
`evidence.offset_report_*` pair is absent is a well-formed 1.0.0 document that both readers accept
(the pair is present-or-absent together, never half of one).

**So a reader must scan for a key by NAME and treat the third as optional** — never take "the third
line from the end", and never treat its absence as an error. That is the same rule plan erratum E4
draws for `refusal-reason=`: a pod log is stdout and stderr merged in nondeterministic order, so
every reader in this project scans a bounded tail and matches by key name
(`KEY_SCAN_TAIL_LINES = 8`, `controllers::backup::evidence_keys`).

### A phase-5 / phase-6 `restore.yaml` divergence is exit 1, not exit 3

Logweir renders `restore.yaml` twice: once for `kafka-backup validate-restore` at phase 5 and once
for `kafka-backup restore` at phase 6. Since v0.1.x Logweir hashes the exact bytes it writes at
phase 5 and refuses at phase 6 if the re-rendered document does not match, naming both digests.

The refusal is **exit 1** — an operational error, no artifact written. It is deliberately not
exit 3: the exit-code contract reserves `3` for a plan refused by a guard *before anything runs*,
and by phase 6 the admission guard has passed, the plan has been rendered and the engine's own
`validate-restore` has already executed. The ADR that would normally record this decision is gated
on an open question and is deferred; this section is the record.

### Guard G-PITR: upstream's six point-in-time tests contain zero assertions, so Logweir proves the boundary itself

`kafka-backup` 0.21.0 declares six point-in-time tests —
`crates/kafka-backup-core/tests/integration_suite/pitr_accuracy.rs:25,40,54,66,78,86`: accuracy,
boundary-inclusive, multi-partition consistency, millisecond precision, empty window, and
`test_full_restore_no_pitr`. Every one of them is `#[ignore = "requires Docker"]` with a body that
is one `println!` and a comment beginning "This test would:", and the file
**contains zero assertions** (`grep -c assert` → 0). The filter at the centre of this product has no
executable evidence upstream, the `<=` boundary included. The source-level answer is readable and
inclusive — `r.timestamp >= s && r.timestamp <= e`
(`crates/kafka-backup-core/src/restore/helpers.rs:74-82`) — so Logweir's guard **confirms a stated
expectation** rather than discovering one.

**The recorded result.** `e2e/tests/pitr_boundary.rs::pitr_boundary_includes_the_record_whose_timestamp_equals_point_in_time`,
run by `just pitr` with the compose stack live. Measured against engine **0.21.0** at digest
`sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317`, Apache Kafka **3.7.1**
(KRaft, node 1001), on 2026-09-10:

```
[pitr] point_in_time=2025-10-09T08:53:20Z (T=1760000000000) -> target topic restore-20251009T085320Z-pitr-src
[e2e] produced 9 record(s) into pitr-src across 3 partition(s) with explicit CreateTime; end offsets 0 -> 9
[pitr] partition 0: p0-before-1ms@ts=1759999999999 offset=0 x-original-offset=Some(0)  p0-boundary@ts=1760000000000 offset=1 x-original-offset=Some(1)
[pitr] partition 1: p1-before-1ms@ts=1759999999999 offset=0 x-original-offset=Some(0)  p1-boundary@ts=1760000000000 offset=1 x-original-offset=Some(1)
[pitr] partition 2: p2-before-1ms@ts=1759999999999 offset=0 x-original-offset=Some(0)  p2-boundary@ts=1760000000000 offset=1 x-original-offset=Some(1)
[pitr] manifest bound over [1759999999999, 1760000000000] = [0, 9]; restored 6
```

The signed scorecard for that run: `outcome: pass`, `integrity.result: pass`,
`integrity.records_sampled: 6`, `records_sampled_matching: 6`, `mismatches: 0`,
`pass_rate_measured: 1.0`, `sample.records_restored: 6`, `measured.rpo_seconds: 0`,
`target.mode: newTopic`, `target.topic_mapping_prefix: restore-20251009T085320Z-`. Both readers
accept it (`logweir drill verify` and `docs/verify_scorecard.py` 1.13.0).

The fixture is nine records with explicit `CreateTime` — `T − 1 ms`, `T`, `T + 1 ms` on **each** of
three partitions, where `T` is the fixed literal `1_760_000_000_000` (2025-10-09T08:53:20Z) and never
a clock read. A `mode: newTopic` restore at `point_in_time = T` brought back
**six of the nine records**: `T − 1 ms` and `T` on every partition, and none of the three at
`T + 1 ms`. Partitions 0, 1 and 2 each returned exactly two records, on the partition they were
produced to, each carrying an 8-byte little-endian `x-original-offset` decoding to its source
offset. The boundary record — the one whose timestamp equals `point_in_time` exactly — came back on
all three partitions, still stamped `T`.

**What this measurement also found: the canary size cannot exceed what the window really holds.**
`sample.records_per_partition: 25` over this fixture scored `fail-integrity` on **all three**
partitions — not one — with exit code **2** and a **signed** scorecard written for the failing run
(`integrity.records_sampled: 6`, `records_sampled_matching: 6`, `mismatches: 0`). The text is the
same for each selection; partition 1's:

```
selection pitr-src/1  claimed 3  records Unverified { why: "the archive returned 2 fingerprints
  where the manifest claims 3 for this selection; a short sample is coverage the drill did not
  obtain, not a smaller successful sample" }
```

`phase7_verify::verdict_for_selection` claims
`min(records_per_partition, Σ record_count over the segments overlapping the sample window)`, and
the manifest's finest granularity is the SEGMENT — so a recovery point that falls INSIDE a segment
makes the manifest claim the whole segment (3) while the archive side correctly yields only the
records inside the window (2). This is the same segment-granularity fact that makes
`expected_restored_count` a bound rather than an equality.

**It was a known limitation of the sampled reconciliation's `claimed`, not a property of the
restore.** `phase7_verify::verdict_for_selection` summed the WHOLE record count of every segment
that OVERLAPS the sample window, so a straddling segment claimed records the window deliberately
excludes. Nothing was short: the archive side returned every record the window contains, and the
shortfall existed only against a figure derived from records the window excludes — so a signed
document reporting `fail-integrity` with `mismatches: 0` over a restore that was verified
record-by-record off the broker was a false negative, not a correct refusal.

**Fixed by Task 10b, and the fixture now proves it.** `claimed` is
`min(sample.records_per_partition, Σ record_count over the segments WHOLLY inside the sample
window)`, a straddler counting towards an upper bound only, and the short-sample `Unverified` text
names the supportable claim and the straddler count. Task 11 had worked the defect around by
setting this row's `sample.records_per_partition` to **2**; **Task 12 raised it to 25** — the value
`examples/restore.yaml`, `examples/drill.yaml` and `harness::spec_default` all use — so that `just
pitr` is the standing end-to-end proof of the fix rather than a row that would stay green through
its regression. Every segment in this fixture straddles `T`, so the manifest supports a claim of
nothing at all. Measured at `records_per_partition: 25` (2026-09-11, `just pitr` rc **0**, 38 s),
one line per partition, asserted by the row and not merely printed:

```
selection verdict  selection="pitr-src/0"  claimed=0  segments=Verified { checked: 1 }  records=Verified { checked: 2 }  records_restored=2
selection verdict  selection="pitr-src/1"  claimed=0  segments=Verified { checked: 1 }  records=Verified { checked: 2 }  records_restored=2
selection verdict  selection="pitr-src/2"  claimed=0  segments=Verified { checked: 1 }  records=Verified { checked: 2 }  records_restored=2
```

Outcome `pass`, exit 0, both readers VALID. The operator consequence recorded here before the fix —
*set `sample.records_per_partition` at or below the number of records each partition holds inside
the window, or the run reports `fail-integrity` about its own sample* — **no longer holds and is
kept only as history of what was measured.** A canary larger than the window is now ordinary: the
run reports what the manifest can support, which over a straddled recovery point is zero, and the
per-record lane still verifies every record the window does hold.

**The count is bounded, never equated.** The nine records land in one segment per partition
(`segment_max_records: 1000`), and each of those segments straddles `T`: it starts at `T − 1 ms` and
ends at `T + 1 ms`. Nothing is wholly inside `[floor, T]`, so
`logweir_core::engine::expected_restored_count` returns **`[0, 9]`** and the six restored records are
inside it. That is the whole claim a manifest can support: `point_in_time` falls INSIDE a segment on
any real archive and `SegmentFacts` carries no per-record timestamp, so an equality would fail a
correct implementation and would then be "fixed" by weakening it. The boundary property is proved by
the restored payload **set**; the count is proved only to be within the bound.

### A broker on `LogAppendTime` honours a per-topic `CreateTime` override (Task 8, residual 3)

Apache Kafka 3.7.1 (KRaft, node 1001) whose effective `log.message.timestamp.type` is
**`LogAppendTime`** **honours a per-topic `message.timestamp.type=CreateTime` override** — measured
by execution, with the broker setting applied as a dynamic config and reverted (and the revert
asserted) in the same step. Two consequences, and they pull in opposite directions, which is why the
sentence is recorded rather than assumed: phase 0's override probe can therefore succeed on such a
broker, so `TargetTopicConfigRefused`'s binary arm is unreachable on this stack by construction (its
deterministic arm is
`crates/logweir/tests/topic_preflight.rs::a_logappendtime_broker_that_refuses_the_override_is_a_guard_refusal`);
and a topic that states the override keeps the timestamps its producer states, which is what makes
G-PITR's fixture possible at all. MSK is a different broker and is not covered by this measurement —
spec §10's MSK item 4 stays `[UNVERIFIED — needs an MSK cluster]` (Global Constraint 17 forbids the
spend).

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
