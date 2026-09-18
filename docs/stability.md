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

### The product API's OpenAPI document is pre-release, and says so

`schemas/logweir-api-v1.openapi.json` is the third checked-in schema and the
only one the rule above does **not** cover. `info.version` is
`1.0.0-alpha.1`: `logweir-api` is `publish = false`, no image builds it, the
chart does not deploy it, and nothing outside this repository reads the
document. It is therefore still in the "internal edit" state the scorecard left
behind at v0.1.0.

While that holds, adding, retyping or removing a field is a pre-release bump of
the `-alpha.N` suffix and needs no maintainer approval — but it is never a
silent edit, because two gates compare the checked-in bytes against the
generator: `just schema-check` regenerates and `diff -u`s it (as
`scripts/ci-check.sh` does), and
`crates/logweir-api/tests/contract.rs::the_checked_in_openapi_document_is_what_the_types_generate`
does the same comparison in-process. Regenerate with `just schema` and review
the diff.

The rule changes the day anything consumes it — the console image, the typed UI
client (PLAT-18.1), or any client outside this repository. From then on the
scorecard's rule applies verbatim: **adding an optional field is MINOR,
removing or retyping one is MAJOR**, and the asymmetry decides what goes in. It
is why `Operation` carries no `jobName`: PLAT-14.1 owns the final normalized
operation mapping, and a field left out today costs a MINOR bump to add,
whereas a field frozen in today costs a MAJOR bump to remove. Prefer the
reversible direction until the owning task rules.

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

- **Backups and drills are separate commands.** `logweir backup run` captures
  the source cluster and writes a signed backup receipt; `logweir restore run`
  and `logweir drill run` consume an existing archive. The historical
  `--from-cluster` flag is not implemented. See the [quickstart](quickstart.md)
  and [backup receipt format](formats/backup-receipt.md).

  A drill does not contact the source cluster: its scorecard has
  `source.captured_by_logweir: false`, null
  `measured.rpo_source_relative_seconds`, and
  `rpo_source_relative_unmeasured_reason: "source cluster never contacted"`.
  `last_phase_completed` never takes `-1`. The schema already reserves these
  fields and validates their pairing; a separately created backup receipt
  does not turn a drill's archive-relative RPO into source-relative RPO.

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
  v0.1 has ([ADR 0004](architecture.md#adr-0004-kafka-client)). This is not contradicted by
  the `Dockerfile` cross-compiling to `x86_64-unknown-linux-gnu`: that target
  has a one-package Debian toolchain and multiarch `:amd64` copies of every
  C library librdkafka wants, and musl has neither. `release.yml` does not
  attempt a musl build. Earlier tags carried a non-blocking attempt; it failed
  in `openssl-sys`, which finds no musl OpenSSL on the runner, on every tagged
  run (v0.1.1 through v0.1.5), so it was removed rather than kept as a
  permanently failing job. No release ships a musl binary. Do not assume one.

- **Release execution remains an evidence requirement.** The checked-in
  [release checklist](tag1-checklist.md) records which release obligations are
  blocked; [gates.md](gates.md) records dated workflow results. A local test
  of workflow YAML does not prove that a published artifact was pulled and
  executed. Update those records when the corresponding run is observed.

  The runner release image is built once, checked with
  `scripts/check-image.sh`, and those same bytes are pushed only after the
  check succeeds. `crates/logweir/tests/workflow_lint.rs` tests that ordering.
  The published digest and a locally built digest are different artifacts.

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

- **Fixtures document more than the current writer emits.** Some scorecard
  fixtures populate `engine_subreport`, which the current writer always sets
  to null. Their `last_phase_completed` and zeroed evidence fields do match
  runtime output. Keep these signed format examples and their pinned test keys;
  [fixture notes](../e2e/fixtures/README.md) explain regeneration.

- **S3 credentials come from `object_store`'s own chain, not the AWS SDK's.**
  `logweir-store`'s `Store::from_url`/`Store::read_only_from_url` build
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
     credential source. The AWS leg is `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` and is
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

- **The signed scorecard's `evidence` block cannot establish upload facts.**
  The scorecard is signed before upload. Logweir therefore zeroes the fields
  that cannot yet be known:

  ```json
  "evidence": { "create_only_enforced": false, "immutable": false,
                "retain_until": null, "version_id": null }
  ```

  These values mean no storage proof was available at signing time, not that
  the object is mutable or unversioned. The post-upload readback is a separate
  signed document:

  ```text
  logweir/drills/<run_id>.receipt.json
  logweir/drills/<run_id>.receipt.sig
  ```

  The put receipt records `create_only_enforced`, `version_id`, `immutable`,
  `retain_until`, observation time and `scorecard_sha256`, which binds it to
  the exact scorecard bytes. Its payload type is
  `application/vnd.logweir.drill-put-receipt+json;version=1.0.0`.
  A receipt upload failure is a logged warning and does not change the drill
  result; a missing receipt means no storage evidence was published.

  `object_store` 0.14 models no Object Lock/WORM readback API for the enabled
  backends, so `Store::object_lock_readback` returns `None` and the immutable
  fields remain unproven even for a bucket with Object Lock configured.

### Controller image architecture and release evidence

The shared image workflow records digests after native execution and registry
verification. Main publication and versioned releases reuse it; check the
exact Actions run before deploying a digest. The release checklist records
evidence per candidate.

The workflow builds `linux/amd64` and `linux/arm64` controller variants on
native runners, checks each variant, and assembles a manifest list after
checking the resulting artifacts. The runtime supports
`scripts/check-image-weirkeeper.sh --no-exec` for inspecting a foreign
architecture without executing it.

`Dockerfile.weirkeeper` refuses cross-compilation because its `aws-lc-sys`
build reads host headers. A cross attempt failed on `sys/types.h` in the
recorded 2026-09-11 trial; the native arm64 build took 182 s with cached base
layers. These are historical measurements, not build-time guarantees.

Local rebuilds change image digests. `config/manager/deployment.yaml` and
`logweir.yaml` retain local digest pins; the Helm chart uses `:latest` for the
Logweir repositories by the owner's 2026-09-12 decision. Third-party chart
images remain digest-pinned. See the [chart guide](../charts/logweir/README.md)
for the pull-policy and registry consequences.

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

The structured log explains the missing report. The signed scorecard cannot
include that phase-8 warning because signing freezes its phase list before
phase 8's own record is appended. Read null as "no engine sub-report retained",
not as a successful upstream validation. Populated format fixtures are
explained in the [fixture notes](../e2e/fixtures/README.md).

`crates/logweir-engine-oso/tests/engine.rs` and
`e2e/tests/full_drill.rs::the_engine_subreport_is_absent_until_oso_cli_engine_overrides_validation_run`
record this limitation and fail when the implementation changes.

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

**Use each client's mechanism spelling.** librdkafka expects
`SCRAM-SHA-512`; the engine's YAML parser expects `SCRAM-SHA512`. The engine
keys must be nested under `security:`, not directly under `source` or
`target`. A wrong spelling fails parsing; wrongly nested keys trigger the
engine's unknown-key warnings, which Logweir rejects. The rendering tests in
`crates/logweir-engine-oso/tests/render_scram.rs` cover both requirements.

**The password is never in a spec, a plan, a rendered document, a receipt or a
plan hash.** It is projected into the runner's environment as
`LOGWEIR_SOURCE_PASSWORD` (source/backup) or `LOGWEIR_TARGET_PASSWORD`
(target/restore); the rendered documents carry the literal
`sasl_password: ${LOGWEIR_*_PASSWORD}` and the engine substitutes it out of its
own process environment. There is **no CLI flag** for it, at any command.

The credential failure modes are:

- An unset password variable exits **1** before the engine starts.
- Newline, carriage return, double quote, single quote or `$` exits **3** with
  `refusal-reason=CredentialNotRenderable`. Errors name the variable and
  character class, never the password. The pinned engine currently expands
  variables in one pass, but `$` remains refused across future engine changes.
- The placeholder expands into an unquoted YAML plain scalar. A space before
  `#`, a `: ` sequence or a leading flow indicator can cause a parse error or
  truncate the password and fail authentication. Newline refusal prevents
  inserting another YAML key. Double-quoting would introduce backslash escape
  interpretation, so the renderer does not use it.

**Local SCRAM has end-to-end coverage.** The compose stack includes
`SASL://kafka-broker-1:9096`, `SASLEXT://localhost:9097` and `scram-setup`.
`e2e/tests/scram.rs` tests successful and incorrect-password authentication
through librdkafka, backup through the engine's own client, and a full drill.
Run these with `just e2e`; the default offline suite does not dial the stack.

**MSK-specific behavior remains [UNVERIFIED — needs an MSK cluster].**
MSK's TLS endpoints and Secrets Manager credential projection require a real
MSK run. Local SASL_PLAINTEXT tests do not establish private-CA or MSK TLS
compatibility. MSK IAM / OAUTHBEARER remains unimplemented; the
[authentication matrix](support-matrix.md) records that distinction.

### The unit suite dials nothing; the e2e suite dials

The default `cargo test --workspace` suite must not contact a broker, bucket,
cluster or webhook. `just time-unit-suite` enforces 120 s for the whole suite
and **15 s for any individual test**; it refuses while ports 9092 or 9000
answer. Use `just e2e` separately for tests that need the compose stack.
[The gate reference](gates.md) describes all checks and prerequisites.

A refused address is not a fast test double: librdkafka retries for its fixed
20 s metadata timeout even when the endpoint is unreachable. Use a
`ClusterReader` double in the default suite. Likewise, S3 configuration with
no credentials can fall through to IMDS at `169.254.169.254`; test storage
classification through the pure evaluator and reserve real clients for e2e.
The source audit in `no_network_in_unit_tests.rs` complements the timing gate.

Run mutation tests in an isolated worktree with a disposable target directory:

```bash
just mutant "test --workspace --lib doctor"
just mutant-clean
```

`just deps-count` fails above 50,000 files in `target/debug/deps` and prints
the count on each run. This guards against accumulated mutation artifacts
making Cargo's fingerprint checks dominate test time.

Notification POSTs use a 5 s connection timeout and 10 s overall timeout.
Transport failures are logged and do not change a signed drill result.

**Image smoke checks require Docker.** `just smoke` builds the amd64 runner
image and checks both binaries' architecture, dynamic linkage, versions,
approval signing and license files. It does not contact an archive or broker.
The runner builder executes natively and cross-compiles Rust for amd64;
only foreign runtime-stage commands use emulation on arm64 hosts.

Historical development-host measurements (2026-09-09, arm64): the runner's
cold builder took 121 s, including a 92.1 s Cargo layer; a cached rebuild took
4 s, and the direct image check took 2 s. The former emulated Cargo layer
took 3027 s, **33x** the native cross-compile. Cache state, dependency changes
and host load change these figures; they are not release budgets.

## Recorded rulings that have no ADR yet

### A `Backup` frozen against a saved destination is REFUSED by an older controller, not run

A `Backup` that names `spec.destinationRef` freezes a `destination` block into
its immutable `execution-inputs.json`. Every struct in that grammar is
`deny_unknown_fields`, so a controller rolled back to a release that predates
destination-backed execution **refuses such a run** rather than executing it.

That is deliberate and it is the safe direction: an older controller cannot
render the complete explicit `AWS_*` set the frozen block describes, so running
the plan would address the store with whatever happened to be in the
controller's own environment — which is the defect saved destinations exist to
close. The same rollback also meets the sentinel `archive.url`
(`logweir-destination://<name>`), which the old `storage_url_for` refuses as an
unknown scheme, so the run ends terminally as `ArchiveUrlUnreadable` before any
POST.

**Planning a downgrade:** let destination-backed runs reach a terminal phase
first, or expect them to end `Failed` with that reason. Nothing is written to
the wrong place either way, and legacy inline-`archive` objects are unaffected.
See `docs/kubernetes.md` §7e and §10.

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
(`KEY_SCAN_TAIL_LINES = 16`, `controllers::backup::evidence_keys`). It was 8
until D3 W2 raised it: contract v2's `teardown-key=` line left a passing
restore at seven of the eight, and because the scan matches by key NAME an
overrun is a silently absent status field rather than an error. The window is
now a budget with its margin written down — see `docs/kubernetes.md` §10.

### The runner progress channel is OPTIONAL, bounded and contract-versioned (D3 §2.4)

Both runners announce where they are on stdout. The channel exists because a controller reading a
pod log has no other way to tell "the engine is still running" from "the archive exists and the
receipt is being signed": a `tracing` line is JSON on the same merged stream and is filtered by
`RUST_LOG`, so it is not a contract anything can read.

```
progress-contract=2
progress-phase=0:admit
progress-phase=1:approval
…
progress-phase=9:teardown
```

`logweir backup run` has no numbered phases after admission, so every line it emits is at `-1`,
over five named steps in this order:

```
progress-contract=2
progress-phase=-1:admit
progress-phase=-1:engine
progress-phase=-1:readback
progress-phase=-1:sign
progress-phase=-1:upload
```

**`progress-contract=<version>` is the version of the progress grammar that follows. It equals
the execution contract version when one is stamped, and the version the binary implements
otherwise. It is printed once, before the first `progress-phase=` line. Absence is not an error.**

Both readings occur, and the difference is worth naming. `logweir restore run` under a controller
prints the version the controller stamped — so a v1 invocation prints `progress-contract=1`.
`logweir backup run` prints the version this BINARY implements, because a Backup Job carries no
execution-contract environment at all on this build and there is nothing else it could truthfully
say; the same is true of `logweir restore run` invoked standalone. The value is
`logweir_core::execution_contract::VERSION`, `"2"` since decision D3's Amendment I.

**Absence is not an error.** A runner that predates the channel prints nothing; a controller that
sees no progress line simply has no phase to report. That is also what happens when the channel
declines to say something: the line is produced by a filter over a **closed vocabulary** — the ten
restore phase names and the five backup step names, and nothing else is renderable at any phase
number. A caller that reached the formatter with a projected password, a URL with userinfo, a
broker error, a record's bytes or a string containing a newline gets silence rather than a
sanitised half-truth, because a charset rule would not help: `hunter2` is lowercase alphanumeric.
Every line the channel can produce is at most 96 bytes, so the optional channel can never push the
mandatory evidence keys out of the controller's bounded tail.

**Read it by key name, never by position.** Same rule as `refusal-reason=` and
`offset-report-key=`, for the same reason (plan erratum E4).

### `teardown-key=` is printed exactly when phase 9 attested, and it comes BEFORE interface I8's keys

```
teardown-key=logweir/drills/<run_id>.teardown.json
scorecard-key=logweir/drills/<run_id>.json
sidecar-key=logweir/drills/<run_id>.sig
offset-report-key=logweir/drills/<run_id>.offsets.json
```

The line names the signed teardown attestation (`PAYLOAD_TYPE_TEARDOWN`), which is what says
honestly which scratch topics were deleted and which the broker refused. It is **conditional in
exactly the way `offset-report-key=` is**: it is printed when, and only when, phase 9's create-only
put of that object succeeded. A teardown whose attestation could not be written prints no line at
all — a line naming a key nothing was written to would be the worst possible output, and the
warning on the log says what to do instead.

**It goes in front of interface I8's three keys**, exactly as PLAT-15.1's `catalog-key=` goes in
front of interface I7's two. I8's contract is "`scorecard-key=`, `sidecar-key=`,
`offset-report-key=`, as the FINAL stdout lines of a successful run, with nothing after them", and
that is unchanged.

**It is not restricted to exit 0.** A restore that ran every phase and did not pass exits 2 and
prints no evidence keys at all — and that is precisely the run whose leftover topics matter, since
a rehearsal's leftovers block the next scheduled slot. The key is carried on the scorecard's
phase-9 record, which is pushed after phase 8 froze and signed the document, so nothing that
delivers it can reach the signed bytes.

### Execution contract v2: what v1 still buys, and what it may no longer carry

`logweir_core::execution_contract::VERSION` is `"2"` (decision D3 §8, Amendment I). The bump is the
whole reason the version exists: v2 carries the recovery point binding, the standing rehearsal
authorization and the two stdout lines above, so an old binary must refuse the new
`--execution-contract-version` argument *before dispatch* rather than run a plan whose checks it
does not implement.

**v1 is still accepted, for already-created legacy Restores only.** A runner holds no cluster
credential, so it cannot read a `Restore`'s creation timestamp and cannot ask anyone whether an
object is "legacy". What it can see is whether the invocation carries v2 material — a `source.point`
block, a standing rehearsal authorization, an approval-policy snapshot or a confirmation-issuer
public key. **A v1 invocation carrying any of them is refused by name, exit 3**, because those are
exactly the things a Restore created after the rollout has. Let the in-flight legacy Restore finish
(or delete it) and create the new one; a legacy object is not upgraded in place.

**Every v2 addition is additive and absent-tolerant**, and each is all-or-nothing as a block:

| addition | where | absent means |
|---|---|---|
| `source.point` | the plan document | the archive set comes from `source.backup`; no binding is checked. Under a **v1** contract a plan carrying it is refused by name, exit 3 — it is v2 material like the rest |
| `LOGWEIR_EXECUTION_AUTHORIZATION_KIND` | Job environment | `approval` — the per-run `Approval`, tag 1's shape |
| `LOGWEIR_EXECUTION_AUTHORIZATION_SHA256`, `…_AUTHORIZATION_SIDECAR_SHA256`, `…_AUTHORIZATION_KEYS_SHA256`, `…_REHEARSAL_SCHEDULE_UID` | Job environment | no standing authorization; mounted material with no pinned digest is **refused**, never ignored |
| `LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256` | Job environment | the synthesized `legacy-governed-v1` policy |
| `LOGWEIR_EXECUTION_CONFIRMATION_KEY_SHA256` | Job environment | the legacy single-key bundle |

The five optional bundle members (`--standing-authorization` and its derived `.sig` sidecar,
`--authorization-keys`, `--policy-snapshot`, `--confirmation-key`) are digest-pinned in **both**
directions: a pinned digest with nothing mounted is a lost bundle member, and a mounted member with
no pinned digest is material the controller never committed to. Both are refused. Supplying any of
them to a standalone invocation — one with no execution contract at all — is refused too, because
nothing could make unpinned material trustworthy and silently ignoring it would let an operator
believe an authorization was enforced when nothing bound it to the run.

### The standing rehearsal authorization is SIGNED, and the runner checks the signature

D3 §4.3(e) says the bundle carries "the authorization document, its signatures, the trusted public
keys, the scope and the rendered plan". All five are mounted, and the runner verifies them before
any data-plane work — because §4.3's own first sentence names the adversary this exists for: *a
controller that could mint its own authorization*. A scope pinned only by a digest the controller
set would prove nothing against exactly that adversary.

```
--standing-authorization <path>   the signed document; its DSSE sidecar is read from
                                  <path> with the extension replaced by .sig, the same
                                  convention --approval already uses
--authorization-keys <path>       the trusted public keys the signature anchors in
```

The document is canonical JSON with the DSSE payload type
`application/vnd.logweir.standing-rehearsal-authorization+json;version=1.0.0`:

```json
{
  "formatVersion": "1.0.0",
  "kind": "StandingRehearsalAuthorization",
  "subjectRef": {"apiVersion": "logweir.dev/v1alpha1", "kind": "RehearsalSchedule",
                 "namespace": "team-a", "name": "weekly-orders", "uid": "…"},
  "scope": { …D3 §4.3's RehearsalScope, camelCase… },
  "issuedAt": "2026-06-01T00:00:00Z",
  "expiresAt": "2026-07-01T00:00:00Z"
}
```

Unknown fields are ignored on read, so a document PLAT-19.2 later enriches with `policy`,
`requester` or a second signature still verifies against this build. A higher `formatVersion` major
is refused.

**The runner's order, and what each step buys.** The signature is checked over the envelope bytes
*before* they are parsed, so nothing read out of the document is believed until those exact bytes
are known to be signed. The key that verified is then judged on its **usage**: a rehearsal may be
authorised only by a key carrying `GovernedApproval` or `ConsoleConfirmation`. A key carrying only
`EvidenceSigning` is refused even when its signature is perfectly good — the installation's own
evidence identity must never be able to authorise its own rehearsals (D3 §7.3), and that fault is
reported as `KeyUsageMismatch` and never as a bad signature, because an operator told "bad
signature" about a genuinely signed document goes looking at the wrong thing.

**`…_REHEARSAL_SCHEDULE_UID` is a verified binding.** The environment says which schedule this run
claims to be; the signed document's `subjectRef.uid` says which schedule the human authorised; the
runner requires them to agree. What the runner does **not** check is `scope.templateDigest` —
whether the scope matches the schedule's sealed spec. That is recomputed from the
`RehearsalSchedule`'s own spec and is the controller's each-slot check (D3 §4.3(a)); a runner
holding no cluster credential cannot read that spec at all.

**The expiry is checked, against the node's clock, as a second line of defence.** `issuedAt` must
not be in the future, `expiresAt` must not have passed, and `expiresAt - issuedAt` must not exceed
the 90 days D3 §4.3 permits. A Job's node clock is not a trusted time source, so this does not
replace the controller's each-slot expiry check — but it does refuse a bundle replayed weeks later,
which nothing else would. An expired document refuses under `AuthorizationExpired`; every other
authorization fault refuses under `AuthorizationInvalid`. Both are D3 §4.3's own controller skip
reasons, reused so the controller's `status.lastSkipped.reason` and the runner's refusal say the
same word about the same fault.

**Key lifecycle stays the controller's.** The mounted keyring carries `{keyId, publicKeyPem,
usages}` and no lifecycle: `state`, `notBefore`/`notAfter` and revocation are `TrustPolicy`
resolution's to evaluate (`logweir_core::trust::decide`), and they are evaluated before the keyring
is written. The runner re-checks what it can — that the signature verifies under a key the
controller pinned, and that the key may authorise.

**No new exit code.** Both new refusals live inside Global Constraint 11's existing four:

| situation | code | `refusal-reason=` |
|---|---|---|
| a rendered plan outside the signed rehearsal scope | 3 | `GuardRefused`, message opens `RehearsalScopeViolation` |
| a standing authorization that is unsigned, signed by an unpinned key, signed by a wrong-usage key, for another schedule, or malformed | 3 | `GuardRefused`, message opens `AuthorizationInvalid` |
| a standing authorization outside its validity window | 3 | `GuardRefused`, message opens `AuthorizationExpired` |
| a mounted keyring or sidecar that does not parse | 1 | — (structural corruption of a file, not a statement about authorisation) |
| a bound point whose receipt or manifest digest differs | 3 | `GuardRefused`, message opens `PointBindingMismatch` |
| a bound point whose receipt or manifest is missing or unreadable | 1 | — (no refusal line; nothing about the plan was found wanting) |

`logweir_core::guard::TERMINAL_STATES` is still the closed three-element list, so every refusal
above classifies as the general `GuardRefused` and carries its state name as the first token of the
message. Promoting `PointBindingMismatch`, `RehearsalScopeViolation`, `AuthorizationInvalid` and
`AuthorizationExpired` to declared terminal states is a change to that list and to the controller's
mapping, and is not made here.

### `logweir notify deliver`'s exit codes and its `notify-result=` lines (PLAT-14.2)

`logweir notify deliver --event <path>` posts one protection event
(`application/vnd.logweir.protection-event+json;version=1.0.0`,
[docs/formats/protection-event.md](formats/protection-event.md)) to every configured sink. It is the
runner-side half of PLAT-14.2: the protection controller decides what the alert says and has no way
to send it — `config/rbac/role.yaml` gives it no Secret verb, so it cannot read a routing key, and it
is given no HTTP egress — so delivery runs as a short Job built from the runner image, with the sink
credentials projected into that Job and nowhere else.

**Sinks come from the environment, never from a flag.** A routing key or a signed webhook URL on an
argv is visible in `/proc/<pid>/cmdline`, in every process listing on the host, and in the Job spec
anyone with pod read can see — the same reason `logweir cluster-probe` takes its SASL password from
`LOGWEIR_SOURCE_PASSWORD` and offers no `--password`.

| variable | sink name | what is posted |
|---|---|---|
| `PAGERDUTY_ROUTING_KEY` | `pagerduty` | one Events v2 enqueue, `trigger`/`resolve` from `alert.action`, `dedup_key` = `alert.key` |
| `NOTIFY_WEBHOOK_URL` | `webhook` | one POST of the event document |
| `NOTIFY_SLACK_WEBHOOK_URL` | `slack` | one POST of `{"text": …}` |
| `PAGERDUTY_ENDPOINT` | — | not a sink: the PagerDuty service region, `https://` only, US default |

**A variable that is present and blank is not a configured sink.** `std::env::var` returns `Ok("")`
— not `Err(NotPresent)` — for a Kubernetes `env:` entry with an empty `value:`, and a `secretKeyRef`
to a key that exists and is blank projects the same thing.

**Stdout is one line per configured sink**, in the fixed order `pagerduty`, `webhook`, `slack`, as
the final lines the process writes:

```
notify-result=pagerduty:ok
notify-result=webhook:failed
notify-result=slack:ok
```

A reader scans a bounded tail and matches **by key name**, never by position — the rule erratum E4
draws for `refusal-reason=` and `offset-report-key=`, and for the same reason: a pod log is stdout
and stderr merged in nondeterministic order.

**A delivery with no configured sink prints exactly one line and exits 1:**

```
notify-result=none:unconfigured
```

`none` is not a sink and cannot collide with one, and `unconfigured` is deliberately not `failed`:
a sink that refused and an alert with nowhere to go are different findings that an operator fixes
in different places. **Do not read a bare exit 0 as delivered.** Before this line existed, "nothing
was configured" and "every sink accepted" were the same machine-readable answer — empty stdout,
exit 0 — so a `secretKeyRef` that had been rotated, renamed or left blank made a caller record a
delivery that reached nobody. A delivery Job that delivered nothing did not deliver; the
controller's retries exhaust and `NotificationsDelivered=False` is the correct record of it.

**The exit codes.**

| code | meaning |
|---|---|
| **0** | every configured sink accepted, and at least one was configured |
| **1** | at least one configured sink did not accept (`notify-result=…:failed` says which), **or** no sink was configured at all (`notify-result=none:unconfigured`) |
| **3** | the event document is missing, unreadable, larger than a ConfigMap can hold, of another `format_version` major, or malformed — **nothing was posted** |

**2 and 4 are never returned by this subcommand**, and that is a contract rather than an accident.
Global Constraint 11 reserves 2 for "a drill result that is not a pass — a scorecard IS written and
signed" and 4 for "signing or lock-proof failed"; `notify deliver` signs nothing and writes no
artifact, so either code would make a delivery failure indistinguishable from a drill result to
every reader of the exit contract, including `weirkeeper::conditions::reason_for_exit`.

**1 and 3 are distinguishable without parsing prose.** A refusal posted nothing, so it prints no
`notify-result=` line at all; a delivery failure prints one per configured sink, and a delivery with
nowhere to go prints `none:unconfigured`. A controller reads the exit code *and* the lines, which is
what D3 §3.4 specifies.

**A failed notification never rewrites a backup result.** This subcommand writes no Kubernetes
object of any kind — it reads a file, posts to sinks, and exits. Delivery failures reach a `Backup`
only through what the protection controller chooses to write on `protectionpolicies/status`.

**Sink credentials never reach stdout, stderr, a `tracing` field or a `Debug` output.** The routing
key travels in the PagerDuty request body and is never printed; every sink URL that reaches a
display surface is reduced to `scheme://host` by `redact_url` first, on the success arm and the
failure arm alike. All three streams are asserted together, over the shipped process with a routing
key really set, because a pod log has no stream selector and a `tracing` field is a surface a log
aggregator reads.

**The event's `summary` is the only free prose, and a claim in it is edited, not obeyed and not
fatal.** If the controller's `summary` contains a phrase claiming exhaustive verification, the
phrase is replaced with `[claim removed]`, the edit is reported on a named log line, and the alert
**is still delivered** — Logweir verifies a sample, and dropping the page would punish the responder
for the controller's wording. Identifiers are never scanned: `exhaustive` is a legal DNS-1123 label,
so a `ProtectionPolicy` named `exhaustive-backups` delivers normally. The one hard refusal is a
`verification_scope` outside `sampled`/`degraded`/`none`, which reads an enumerated field this
product serializes and cannot be tripped by a naming choice.

**Timeouts are the drill path's**: 5 s to connect and 10 s overall per POST, so three sinks cost at
most 30 s — inside the delivery Job's `activeDeadlineSeconds: 120`.

### `logweir check run`'s exit codes, its frames and the one key it may write (D2 §4.2)

`logweir check run --plan <path> --check-contract-version 1` is the **one** check runner
(decision D2 §4.2, seam ruling S1). Six plan kinds — `topicInventory`, `operationReadiness`,
`restorePreflight`, `destinationAccess`, `evidenceFetch`, `catalogSync` — share one argv surface,
one frame format and one closed error vocabulary, because a second runner would be a second place
the contract could drift from what the controller parses. There is deliberately no
`logweir topics discover`, no `logweir catalog controller-sync` and no per-kind subcommand.

**`timeoutSeconds` has two ceilings, and that is the one bound that is not uniform.** Five of the
kinds are bounded probes and may ask for at most **600** seconds; a `catalogSync` is a paged walk
of an adopter's own bucket, whose cost is set by how many recovery points they hold, and may ask
for at most **1800**. The `RecoveryCatalog` controller asks for 900. A single 600-second ceiling
refused every sync plan at startup step 4, before a credential was read, and
`the_controllers_sync_plan_is_one_the_runner_accepts` is the guard that now holds the two sides
together.

**`catalogSync` writes nothing at all** — not even the create-only readiness marker a
`destinationAccess` may write. It reads the durable catalog through the destination's `archiveRead`
grant, reports a signature verdict and a signer key id **and never a trust decision**, and relays
the result as the `details` body `docs/kubernetes.md` §7d specifies. A walk that could not start
relays **no body**, because a body that parses is a body the controller publishes in place of the
view it already has.

**The invocation.** The controller writes it (`weirkeeper::check::job::runner_argv` and
`runner_job_spec`); nothing else may:

```
argv: check run --plan /check/check-plan.json --check-contract-version 1
env:  LOGWEIR_CHECK_CONTRACT_VERSION=1
      LOGWEIR_CHECK_PLAN_SHA256=sha256:<64 lowercase hex>
      LOGWEIR_CHECK_SUBJECT_UID=<uid>
      RUST_LOG=warn        TMPDIR=/work
```

**No network before the plan verifies.** The runner parses argv, reads the plan bytes once,
compares their SHA-256 against `$LOGWEIR_CHECK_PLAN_SHA256`, parses strictly with
`deny_unknown_fields`, and requires the plan's `subjectUid` to equal `$LOGWEIR_CHECK_SUBJECT_UID`
— all before a client is built. A plan `ConfigMap` swapped under a running Job therefore cannot be
executed against the wrong object, and a plan a newer controller wrote with a field this runner
does not understand is refused rather than partly honoured.

**An `evidenceFetch` object may name only `evidence.payload` or `evidence.sidecar`, and no two
objects may name the same one.** A stream is one ordered run of part frames whose digest the end
frame declares once, so an object on `result` or `details` — the two streams the runner writes
itself — would make the relay unreadable with no way to say why. Both shapes are a named refusal
at exit 3 rather than a silent `ResultUnreadable`.

**Stdout is the machine contract and carries frames only.** `logweir-check-topic=` lines,
`logweir-check-part=` lines, and one final `logweir-check-end=` line, each at most 4,096 bytes
including its newline. The end line declares, per stream, how many parts were printed and their
SHA-256, plus the topic-line count and digest; a reader that cannot verify all of them reports
`ResultUnreadable` and never publishes the log content. **Stderr carries JSON tracing at `warn`**
and nothing a controller parses — a pod log has no stream selector, so nothing on stderr is
machine-readable (erratum E4).

**The exit codes.**

| code | meaning |
|---|---|
| **0** | an end line was printed, **whatever the per-check states**. A `notReady` check is a RESULT, not a runner failure, and the controller reads the relayed codes rather than the exit status. |
| **1** | an operational failure before a result existed: no end line, so the relay does not verify and the controller reports `ResultUnreadable`. |
| **3** | a contract refusal (the four startup steps above). `refusal-reason=CheckContractMismatch` is printed on stdout and **no frame is printed at all**. |

**2 and 4 are never returned by this subcommand**, and that is a contract rather than an accident.
Global Constraint 11 reserves 2 for "a drill result that is not a pass — a scorecard IS written and
signed" and 4 for "signing or lock-proof failed"; a check signs nothing and writes no artifact, so
either code would make a check indistinguishable from a drill result to every reader of the exit
contract, including `weirkeeper::conditions::reason_for_exit`. **Do not read a bare exit 0 as
"everything passed"** — read the relayed result.

**An old runner image does not fail this way.** A build without the `check` subcommand rejects it
as an unknown subcommand and exits 1 with a clap usage error, which the controller maps to
`RunnerContractUnsupported` (D2 §4.3). That is why the version handshake is checked in argv **and**
in the environment: a rollout that upgraded one and not the other is refused rather than guessed at.

**What a check may do to the world.**

* It **never invokes the engine binary.** Nothing under `crates/logweir/src/check/` names an engine
  invocation, so `scripts/check-no-oso.sh` passes unchanged.
* It **never writes**, except the optional create-only readiness marker
  `logweir/readiness/<destinationUid>.json`, put with `PutMode::Create` through
  `Store::put_create_only` under Global Constraint 6's `logweir/` root. **"Already exists" counts as
  write-authorised**: S3 and MinIO authorise a `PUT` before they evaluate the `If-None-Match`
  precondition, so a 412 proves the grant. It is reported as `MarkerAlreadyPresent` rather than
  `MarkerWritten`, because "I wrote it" and "it was already there" are different facts.
* It **never creates, alters or deletes a topic.** The restore preflight's collision answer is
  targeted metadata plus a `CreateTopics` with `validate_only = true`; the execution path's probe
  topic has no counterpart here.
* It **never prints a credential.** Every message, remedy, fact, scope, bounded detail sample and
  `details` line passes `logweir_core::check_contract::redact` and a 512-character cap, and no
  broker or object-store error string is ever interpolated into a frame — the code carries the
  classification and the message names the operation and the object. A value that is a KEY PATH
  (an archive object key, a topic name) has every *shape* rule applied over the whole string and
  the *long-run* rule applied per `/` segment, because `/`, `-`, `=` and the digits are all in the
  base64 alphabet and an ordinary archive key is one 70-character run: redacting it whole protects
  nothing and deletes the only fact the line carried. **Exactly one field is written verbatim** —
  an `evidenceFetch` result's `key`, which is the plan's own key echoed back so the controller can
  tell three answers apart.
* It **never dials TLS without SASL.** `auth.mode: plaintext` with `tls: true` is a shape the
  saved-connection contract does not support, and it is **refused rather than dialled in the
  clear** — the same refusal `logweir cluster-probe` and the controller already make (PLAT-07.1).
  A check's broker configuration carries no private copy: `security.protocol`, `sasl.*`, the
  hostname pin and the trust anchor all come from the one implementation the drill reader shares.
* **Every network call is time-bounded** by the plan's own `timeoutSeconds`: per-call timeouts on
  the broker side, `request_timeout` plus a retry cap on the object-store side.

**An empty inventory and a failed one are different answers.** A `topicInventory` that ran against
a cluster showing no topic declares `topicLines: {"count": 0, …}`; one whose broker never answered
declares **no** `topicLines` block at all and carries a `connection.authenticated` row with the
classified code. Reading the first as the second — or either as "the cluster is empty" — is the
PLAT-09.1 defect the distinction exists to prevent: a successful Kafka listing never proves full
visibility, because the broker silently omits topics the principal cannot `DESCRIBE`.

### `logweir-retention`'s exit codes, its key lines, and what its record does and does not prove (D3 §6.5)

`logweir-retention run --plan <path> --retention-contract-version 1 [--dry-run]` is the **one**
supported enforcer, and it is a **separate binary** from `logweir` on purpose. D-SEAMS **S1** says
there is one check runner; this is its third recorded exception, and the reason is structural
rather than convenient: *deletion linkage must not be reachable from the everyday binary*. A
`logweir retention run` subcommand would link `crates/logweir-reaper` — the one crate in the
workspace that names an object-store delete — into the same executable an operator uses to take a
backup, verify a receipt or print a scorecard, and `scripts/check-no-archive-write.sh` check 3
would have nothing left to prove.

**The invocation.** The controller writes it
(`weirkeeper::controllers::retention_policy::build_job`); nothing else may:

```
command: logweir-retention
argv:    run --plan /retention/plan.json --retention-contract-version 1
env:     LOGWEIR_RETENTION_PLAN_SHA256=sha256:<64 lowercase hex>
         LOGWEIR_RETENTION_POLICY_UID=<uid>
         LOGWEIR_RETENTION_POLICY_GENERATION=<int>
         LOGWEIR_RETENTION_SCOPE_PREFIX=<spec.scope.prefix>
         LOGWEIR_RETENTION_RUN_ID=<run id>
         LOGWEIR_RETENTION_APPROVER=<audit id | subject | unattended>
         LOGWEIR_RETENTION_MAX_DELETIONS / _MAX_OBJECTS
         LOGWEIR_RETENTION_LOCATION=<the destination's DestinationLocation, as JSON>
         AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY        ← the DELETE-capable grant
         LOGWEIR_EVIDENCE_AWS_ACCESS_KEY_ID / …_SECRET_ACCESS_KEY  ← the evidenceWrite grant
```

**The contract version is checked before the plan is read.** A newer controller handing this
binary a plan shape it does not implement is refused by name rather than partially obeyed — the
same handshake execution contract v2 uses, and for the same reason.

**The scope arrives twice and both copies must agree.** `LOGWEIR_RETENTION_SCOPE_PREFIX` is what
the Job was told; the plan document carries its own `scope_prefix`. A worker that took the scope
from the plan alone would accept a plan that widened its own scope, so the two are compared and a
disagreement is a refusal.

**The exit codes.**

| code | meaning |
|---|---|
| **0** | every planned point completed, or `--dry-run` finished having deleted nothing |
| **1** | at least one point did not complete — `Orphaned` (manifest gone, a segment refused) or `Kept` (the manifest itself refused), with its closed code |
| **3** | the plan was REFUSED **before anything ran**: a digest that is not the approved one, an unknown field, another media type, another policy or generation, a widened scope, a key outside `<scope>/<backupId>/`, a key under `logweir/`, a plan over a ceiling, or no record sink. **Zero objects deleted.** |

**2 and 4 are never returned**, and that is a contract rather than an accident. Global Constraint
11 reserves 2 for "a drill result that is not a pass — a scorecard IS written and signed" and 4 for
"signing or lock-proof failed"; this binary produces no drill result and signs nothing, so either
code would make a deletion failure indistinguishable from a drill outcome to every reader of the
exit contract, `weirkeeper::conditions::reason_for_exit` included.

**Stdout is key lines, read by name and never by position**, the same rule `notify-result=` and
`refusal-reason=` follow:

```
retention-plan=sha256:… points=3 objects=19
retention-point=lwp1-… state=Deleted objects=7
retention-point=lwp1-… state=Orphaned objects=3 code=AccessDenied
retention-record=logweir/retention/<policyUid>/<runId>.json sha256=sha256:…
retention-result=deleted=2 failed=1 objects=10
```

**A dry run asks for no credential at all.** The preview is the validation plus the plan echo, and
requiring a delete-capable Secret to produce one would make a preview need the authority it exists
to avoid.

**Bounded retry, by code.** Three attempts per key — **two waits, because three attempts have two
gaps** — of 1 s and 4 s, and **only** for a 5xx or a timeout. D3 §6.5 writes the backoff as
"1 s/4 s/16 s", which reads as three numbers; a third wait would come after the last attempt, i.e.
sixteen seconds of holding a Job open to learn nothing. `AccessDenied`, `Locked` and `PreconditionFailed` are answered once — a second
attempt at a policy decision is three seconds of nothing — and `NotFound` is success, because a key
an interrupted run already removed is a key this run wanted removed. That last rule is what makes
completion idempotent: a `Orphaned` point's leftover segment keys are exactly what the next plan
names.

**Execution order per point**: the intent tombstone, then the **manifest**, then the segments, then
the completion tombstone. Manifest first, so a run interrupted halfway leaves a set the catalog
reports `Missing` rather than a plausible-looking `Partial` one.

#### The retention record is create-only and **unsigned** in this build

D3 §6.5 calls for the enforcement record to be signed with the runner signing key. **It is not
signed here**, and the reason is recorded rather than glossed: signing would make
`logweir-retention` link `crates/logweir-evidence`, whose reaching set
`scripts/check-one-signer.sh` holds to exactly `{logweir, e2e}`. Widening that allowlist is a
security decision about the signer, taken in `scripts/check-one-signer.sh` and
`crates/logweir/tests/one_signer_gate.rs`, and it is not one a deletion feature gets to make on its
way past.

What the record and the tombstones therefore **do** prove: they are written with
`PutMode::Create` under `logweir/`, by the `evidenceWrite` grant, to a prefix the run's own
delete-capable credential cannot reach or remove; they name the policy identity and generation, the
approved plan digest, the approver reference, the rules as applied, every deleted point with its
object count and every failure with its closed code. They are therefore tamper-evident against the
retention principal, and against anyone who can only delete.

What they **do not** prove: anything against a principal holding `s3:PutObject` under `logweir/`.
Such a principal cannot replace the record — `PutMode::Create` refuses a second put — but it can
**pre-empt** it: the run id is deterministic and derivable from the status, so a plausible document
written at `logweir/retention/<uid>/<runId>.json` before the run makes the real put fail, which the
worker reports on stderr and does not treat as fatal. The run still exits on its real outcome and
the per-point tombstones are the surviving trail, but the top-level record is then an attacker's
document. That is exactly the gap a signature closes.

Closing it is either a reviewed widening of the one-signer allowlist for this binary, or a signing
step performed by something that already holds the key; **neither is implemented, and no surface may
describe the record as signed until one is.** That sentence is enforced rather than advisory:
`scripts/check-withdrawn-claim.sh` carries eight paraphrases of it in its `PHRASES` list, in the
same defect class as the original withdrawn signing claim, and fails the build on any shipped
surface that restates it — the CRD's `doc` string and its ten generated copies included.

**What W14 must assert instead of a signature:** that the record exists at
`logweir/retention/<uid>/<runId>.json`; that its bytes digest to
`status.lastEnforcement.recordSha256`, which the controller now publishes; that a second put at the
same key is refused; that each deleted point's intent tombstone exists and predates its deletion;
and — the one that actually matters — that the delete-capable principal can neither `PutObject` nor
`DeleteObject` under `logweir/`, probed directly.

### A phase-5 / phase-6 `restore.yaml` divergence is exit 1, not exit 3

Logweir renders `restore.yaml` twice: once for `kafka-backup validate-restore` at phase 5 and once
for `kafka-backup restore` at phase 6. Since v0.1.x Logweir hashes the exact bytes it writes at
phase 5 and refuses at phase 6 if the re-rendered document does not match, naming both digests.

The refusal is **exit 1** — an operational error, no artifact written. It is deliberately not
exit 3: the exit-code contract reserves `3` for a plan refused by a guard *before anything runs*,
and by phase 6 the admission guard has passed, the plan has been rendered and the engine's own
`validate-restore` has already executed. The ADR that would normally record this decision is gated
on an open question and is deferred; this section is the record.

### Upgrading past `signedAt`: a run that finished on an older controller is re-read once, not re-judged

`status.evidence.verification.signedAt` and `.trust` arrived with the trust
lifecycle. A `Backup` or `Restore` that finished before them carries neither,
and the new rule compares a key's validity window against a signing time — so
on those objects there is nothing on the status to compare.

**Measured, not predicted.** On the 2026-09-18 lab upgrade, five objects
written on 2026-09-14 (three `Backup`s, two `Restore`s) moved from `Valid` to
`Untrusted` with the reason *"the document carries no signing-time field"*,
while their phase, exit code and `status.reason` did not move at all. No
archive had been read and no signature re-checked. Nothing was wrong with those
receipts; their *status shape* predated the field the rule needs, and the
refusal reported that as a fact about the documents.

**The ruling.** A status that predates `signedAt` and a document that claims no
signing time are different facts and get different answers.

* A stored block with **no `signedAt` and no `trust`** was written by an older
  controller. On a policy event the controller performs **one** bounded `get`
  of the run's own receipt or scorecard, through the same evidence path that
  produced the original verdict, checks the bytes against the `sha256` the run
  recorded, and takes `signedAt` from the document exactly as a fresh run does.
  The signature is not re-checked: it was checked over these same bytes when
  the run finished, and the digest is what says they are the same bytes.
* A stored block with **no `signedAt` but a `trust` object** was written by a
  controller that has both fields and wrote no signing time, which it does only
  when the document carries none. That stays `Untrusted` /
  `SignedOutsideValidity`, and the fail-closed rule is unchanged.

Until the read succeeds, the object keeps the verdict it already had with
`trust.basis: Unverified`. That basis is **never green**: the badge is the
literal word `unverified` and the `Verified` condition is `False` with reason
`VerificationNotAttempted`. An unreachable archive leaves the object there and
the next policy event tries once more — one attempt per event, no retry loop.
A destination whose `evidenceRead` grant only a pod may hold is never repaired
by the controller, and the `detail` says so.

**What does not wait for the read.** An unlisted signer, a key-usage mismatch
and a `KeyCompromise` revocation still change a pre-`signedAt` object's verdict
immediately. None of those rows consults the signing time, so none of them is
delayed by an archive that will not answer.

**Rollback** is unaffected: both fields are additive, an older controller
ignores them and reports the `result` it finds, and a repaired object reads as
`Valid` there too. The operator-facing detail is in `docs/kubernetes.md`
§15.2c.

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

**The recorded result.**
`e2e/tests/pitr_boundary.rs::pitr_boundary_includes_the_record_whose_timestamp_equals_point_in_time`
is run by `just pitr` with the compose stack live. The 2026-09-10 measurement
used engine **0.21.0**, digest
`sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317`,
and Apache Kafka **3.7.1** (KRaft).

At the fixed recovery point `T = 1_760_000_000_000`, each of three partitions
contains records at `T − 1 ms`, `T` and `T + 1 ms`. The restore returns
**six of the nine records**: the first two from each partition. The boundary
record retains its timestamp and partition; its `x-original-offset` header
identifies its source offset. No `T + 1 ms` record is returned.

The standing test uses `sample.records_per_partition: 25`, matching the
examples. With this value the recorded 2026-09-11 rerun passed in 38 s, with
both readers accepting the signed result. This covers the repaired
reconciliation bug where a segment straddling the recovery point used to
inflate the minimum number of records the window must contain.

The current lower bound counts only segments **wholly inside** the window;
straddling segments contribute only to the upper bound. All three fixture
segments straddle `T`, so the count bound is **[0, 9]** and six restored
records fit it. Each partition reports `claimed=0`, one verified segment and
two verified records. The payload set proves the inclusive boundary; the
manifest's segment-level counts establish a bound, not exact equality.

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

  **One document is a deliberate exception and it is named here rather than
  left to be discovered: the protection event**
  (`application/vnd.logweir.protection-event+json;version=1.0.0`,
  [formats/protection-event.md](formats/protection-event.md)). It **refuses**
  unknown fields instead of ignoring them. The policy above is about SIGNED,
  ARCHIVAL documents read years later by something that was not there when
  they were written; the protection event is neither — it is a control message
  passed between the protection controller and a delivery Job whose image the
  same chart pins, and the thing a lenient reader would silently drop is an
  alert detail an on-call responder then never learns. **The cost is real and
  is stated:** a minor bump that adds a field is *not* backward-compatible for
  an older runner, which exits 3 and posts nothing, so the controller and the
  runner image upgrade together. The chart already pins both, so this binds a
  hand-edited deployment rather than a supported upgrade.

- **CLI flags and exit codes are stable within a major.** A given Logweir
  major version does not remove or repurpose a flag, nor change the meaning
  of an exit code, without a major bump. New flags and new exit codes may be
  added in a minor.

- **Supported engine pin.** The full-drill pin is 0.21.0 and the digest in
  `third_party/kafka-backup-binary.digest`; `doctor` checks that exact version.
  Older archive-manifest fixtures exercise parsing compatibility, not full
  runtime support. The old two-minor support table conflicted with the stated
  full-drill floor and is superseded by the [support matrix](support-matrix.md).
  A Strimzi installation using its historical `v0.19.1` default requires an
  engine upgrade before it meets that floor.

- **Explicit non-contracts.** The Rust crates in this workspace
  (`logweir-core`, `logweir-engine-oso`, `logweir-evidence`, `logweir-kafka`,
  `logweir`) are **not** a stable API before 1.0. Only the signed document
  formats (scorecard, put-receipt, teardown attestation) and the `logweir`
  CLI's flags and exit codes are covered by the stability promises above;
  internal crate APIs may change in any release before 1.0.

## Kubernetes: what the control plane does not stop, and what it was built against

**O0, default (a): none of this stops a cluster-admin, and that is accepted and
stated rather than implied.** Logweir's Kubernetes surface narrows *who can
change what*, and it does so with real mechanisms — each Restore owns an
immutable approval-bundle ConfigMap, the immutable Job template pins the
SHA-256 of every mounted public input (including the allowlist), and the runner
checks those bytes before constructing a data-plane client;
`--approver-key-ids` pins which approver key ids a run accepts and
refuses anything outside the set with exit 3 before phase 0 dials; and
`subject_kind` is inside the signed bytes so an approval cannot be retargeted at
another kind of object. **None of that is a claim about a cluster-admin.**
Anyone holding `create pods` in the runner's namespace can mount the signing
Secret and sign whatever they like with no Logweir crate involved
(`scripts/check-one-signer.sh`'s header states the same thing about the
narrowed link-time gate), and anyone who can create pods or replace the Job
template can replace the execution contract wholesale. The boundary this product draws is between *namespace
users* and *the operator's own trust roster*, not between an adopter and their
own cluster administrator. A document that said otherwise would be a guarantee
the code does not deliver, which is the defect class these pages exist to
remove.

**The Kubernetes client pair, and the toolchain it was resolved against.**
`crates/weirkeeper` is built against **`kube 0.99.0`** (`default-features =
false`, features `client`, `runtime`, `derive`, `rustls-tls`) and
**`k8s-openapi 0.24.0`** (feature `v1_29`), resolved and pinned under Rust
**`1.89.0`** (`rust-toolchain.toml`). `kube 0.99`'s own `rust-version` is
`1.81.0`, comfortably below the pin, and `k8s-openapi 0.24` is the release that
exposes the `v1_29` feature Global Constraint 25's minimum Kubernetes needs —
so the pair was taken as resolved, with no `cargo update --precise` anywhere.
The versions are declared in `crates/weirkeeper/Cargo.toml` and in the
workspace root's pin-list comment; this page is where the *reason* lives, so a
future bump has something to disagree with.

## Deliberately not in tag 1

The original deferred scope is retained below with current status. Most items
remain deferred; the Helm chart is now delivered and is marked accordingly.
The separate **Never** list records product boundaries, not scheduled work.

### Later, named — original scope with current status

| # | Item | Reason | Citation |
|---|---|---|---|
| 1 | **MSK IAM auth** | The `TokenProvider` seam exists and is empty; nothing mints an IAM token. | `crates/logweir-kafka/src/token.rs:1-9` |
| 2 | **Strimzi as a source** | That population's default engine is `v0.19.1`, **below the `0.21.0` floor**. Supporting it would mean supporting an engine that lacks levers Logweir needs, which is why it is reported `unsupported (lever-absent)` and never as a fault. | spec §13; `docs/support-matrix.md` |
| 3 | **The in-browser WASM verifier** | Tag 1's UI ships no build step and no bundler, so there is nothing to compile a verifier into; verification is the CLI and `docs/verify_scorecard.py`. | spec §8 |
| 4 | **`OsoCliEngine::validation_run`** | The trait method is not overridden, so the engine's own validation run is never executed and `engine_subreport` is `null` in every document tag 1 produces. The subcommand is on the allowlist as a ceiling, not as a description. | spec §13; `docs/platform/find-engine.md` |
| 5 | **Retention deletion** | Retention **reports** and never deletes. Deleting would need a two-handle store model — the archive handle is `read_only_from_url` — and an amendment to the constraint that says Logweir writes only under its own prefix with create-only semantics. ADR 0008 **Amendment H** has now taken that amendment, so the claim is version-scoped: it holds wherever `RetentionPolicy.mode != Enforce`. **In this build it holds unqualified** — `RetentionPolicy` ships as a shape whose `mode` defaults to `Report`, no retention worker is linked, `logweir-store` is delete-free, and no Logweir component holds any object-store delete capability. | spec §5, §13 |
| 6 | **Byte-faithful production restores** (`strip_offset_headers: true` for `mode: newTopic`) | Gated on phase 7 gaining a **second reconciliation key**: today the injected header is the only key phase 7 has, so stripping it removes the only thing that makes a per-record claim checkable. | spec §6.1, §13 |
| 7 | **Multi-tenancy beyond namespace RBAC** | The isolation tag 1 offers is the API server's own: namespaces and RBAC. There is no tenant object, no per-tenant quota and no cross-namespace policy. | spec §13 |
| 8 | **Delegated rule-based schedule approval** | Every approval in tag 1 is a signed document over exact bytes. A rule that approves on a schedule's behalf is a different trust model and gets its own design. | `design-operator.md:663-670` |
| 9 | **A Helm chart — delivered** | The self-contained chart now ships alongside kustomize. `just chart-check` verifies copied assets and rendered manifests; `just helm-demo` exercises a cluster installation. This item is no longer deferred. | [Chart guide](../charts/logweir/README.md) |
| 10 | **KMS / PKCS#11 signing** | `sign_detached` takes a concrete `&SigningKey` with **no trait seam**, so an external signer is a refactor and not a configuration option. | spec §13 |
| 11 | **Key generation and rotation** | Tag 1 mints nothing and rotates nothing: the operator creates both keypairs with `openssl` and puts the public halves in the `TrustRoster`. `docs/keys.md` is the rotation story, not a rotation feature. | spec §13 |
| 12 | **A PVC for the runner pod** | The runner streams and writes to an emptyDir; a large restore is bounded by that, and a persistent volume would be a new lifecycle to own. | spec §13 |
| 13 | **Subprocess timeout, cancellation and SIGTERM handling** | The engine subprocess runs to completion. A Job deleted mid-run leaves the child to the kubelet, and nothing in tag 1 propagates a cancel. | spec §13 |
| 14 | **A configurable Kafka client timeout** | It is a **20 s constant**, not a setting. | `crates/logweir-kafka/src/rdkafka_reader.rs:16` |
| 15 | **The kind + Calico NetworkPolicy probe** | The shipped NetworkPolicy is labelled `[UNVERIFIED]` in the manifest itself: docker-desktop runs no CNI that enforces NetworkPolicy, so a deny is never observed there and the probe that would make the claim real stays backlogged. | spec §9, §13 |
| 16 | **Stage-2 Tasks 10 and 17-24** | Carried forward as a block, in the backlog they were written in. | `docs/platform/04-stage2-backlog.md` |

### Never — four entries

These are not scheduled. They are refused.

| # | Entry | Why it is a never, not a later |
|---|---|---|
| 1 | **Restore-in-place into a live topic** | The product's whole safety argument is that a restore writes into topics that did not exist. Writing into a live topic removes the property that makes an unattended restore defensible at all. |
| 2 | **Confluent Schema Registry / Apicurio / RBAC-MDS / CSFLE** | Each is a separate product surface with its own trust model. Logweir moves records and reconciles bytes; it does not resolve schemas, evaluate a registry's RBAC, or hold field-level encryption keys. |
| 3 | **MSK ZK-to-KRaft migration, and the word "migration"** | Logweir is not a migration tool and the word is avoided on every surface, because a document that says "migration" is a document somebody will act on as though it were one. |
| 4 | **Multi-cluster or fleet views, and the words** | One controller per cluster. There is no fleet object, no cross-cluster list and no aggregated view, and the vocabulary is kept out of the UI and the docs for the same reason as the previous row. |

Historical specification references in this table refer to the planning corpus.
The linked repository guides describe the implementation that ships here.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
