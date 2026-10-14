# Stability

What this project promises to keep stable, what it does not, and the limits it
states rather than leaves to be discovered. Each signed format's fields and
versions are in [formats/](formats/); exit codes and the runner's output lines
are in [reference.md](reference.md).

A fresh clone must run `just engine` before `cargo test --workspace`; CI does
this in the `build` job.

## The v0.1.0 tag is the compatibility boundary

Read this before adding a field to the scorecard.

`format_version` was `1.0.0` throughout the build. Up to and including the
v0.1.0 tag, adding a field to `schemas/logweir-drill-scorecard-1.0.0.json` was
an internal edit: no document had been published, no reader existed, and the
schema could be regenerated freely. **The tag ends that.** From v0.1.0 onward,
every scorecard in an adopter's evidence bucket is a document some reader may
already parse, so:

- **Adding an optional field is a MINOR bump** — `1.1.0` — with a new schema
  file beside the old one. It is not a free edit and it is not "still 1.0.0".
- **Changing a field's type, its meaning, or an identity rule
  (`validate_invariants`) is a MAJOR bump** — `2.0.0` — and needs the owner's
  recorded decision, a row in the tracker's
  [Owner decisions](to-do/product-expansion.md#owner-decisions) table, while the
  project has one maintainer ([MAINTAINERS.md](../MAINTAINERS.md)). The owner's decision
  [OD-7](to-do/product-expansion.md#owner-decisions), taken on 2026-10-05,
  rules two cases MINOR:
  - **A `validate_invariants` arm that reads ONLY a new optional block is
    MINOR.** An arm that changes how an existing field is judged stays MAJOR.
  - **FX-4's entry `"<target topic>: configuration not assessed (<why>)"` in
    the existing `topic_parity.unexpected_divergence` is MINOR** (OD-7's
    follow-up ruling, the same day), because it can only weaken an older
    reader's verdict.

  The first MAJOR the owner has decided is [OD-9](to-do/product-expansion.md#owner-decisions)
  (a), 2026-10-09: a partition-subset restore's scorecard is format **2.0.0**
  ([the format](formats/drill-scorecard.md#format-200-a-partition-subset-prod-111b-od-9-a)).

  On 2026-10-07 the owner added a third, general case (FX-7's review V3 and
  FX-3):
  - **A new cause for an existing value, or new content in an existing field,
    is MINOR when it can only move a reader's verdict to the safer side**: not
    restorable, not trusted, not intended. It is never MINOR when it can make
    any verdict stronger. FX-7's superseded pin is this case: `availability:
    Conflict`, exit 3, and exit 1 / `Unreadable` for a failed read of the
    pinned version, reached only through the new optional
    `archive.manifest_version_id`. So is FX-3's move of the new-topic
    deviations from `intentionally_deviated` to `unexpected_divergence`. So is
    FX-8's refusal `PointInTimeByProducerTime`, a new cause for exit 3
    ([the format](formats/drill-scorecard.md#sourcetime_basis-format-130)). So is
    PROD-08.1's sampled-lane order check, a new cause for `fail-integrity`, and
    its phase-0 refusals of a plan that asks for two verifications at once, new
    causes for exit 3
    ([the format](formats/drill-scorecard.md#integrityverification-format-140)).
    So is FX-16's refusal `PointBindingSetMismatch`, a new cause for exit 3: a
    point-bound plan that restored another set under the point's receipt is
    now refused before any target is created
    ([below](#a-bound-points-receipt-is-verified-and-restores-its-own-set-d3-55-fx-16)). So are
    FX-21's `topic_parity` entries for a replication factor or partition
    count the archive does not record: not assessed where a writer before
    FX-21 signed a match
    ([the format](formats/drill-scorecard.md#topic_parity-and-what-its-silence-means)).

  Nothing else is ruled: any other change to an existing field's content is
  still a MAJOR bump.
- **Removing or renaming a field is a MAJOR bump**, including a rename that
  merely fixes a spelling. `integrity.restoredPrincipalCouldConsume` is
  camelCase on the wire *permanently* for exactly this reason: it was frozen
  before SP3 fills it in.

The reason this is written down rather than left to judgement: the first change
after a release is the one most likely to be treated as if the release had not
happened. It has. **Do not let the first post-release addition inherit a
pre-release ruling by accident.** The receipt, the catalog point and the
approval documents take the same rule.

## The signed formats

Each format page has a **Versions** table: what each version adds, which
documents carry it, what an older reader does with it and what a rollback
writes.

| Document | Versions | Defined in |
|---|---|---|
| Drill scorecard | 1.0.0–1.12.0, 2.0.0–2.4.0 | [formats/drill-scorecard.md](formats/drill-scorecard.md#versions) |
| Backup receipt | 1.0.0–1.8.0 | [formats/backup-receipt.md](formats/backup-receipt.md#versions) |
| Consumer positions document | 1.0.0 | [formats/backup-receipt.md](formats/backup-receipt.md#two-documents-the-receipts-summary-and-the-positions-document) |
| Catalog point record | 1.0.0–1.7.0 | [formats/catalog-point.md](formats/catalog-point.md#versions) |
| Positions mapping, switchover plan, switchover record | 1.0.0 | [formats/switchover-record.md](formats/switchover-record.md#versions) |
| Put receipt, teardown attestation | 1.0.0 | [verify-a-scorecard.md](verify-a-scorecard.md#the-storage-receipt-a-second-signed-document) |
| Approval document v1, authorization document v2 (2.0.0–2.2.0), standing rehearsal authorization (1.0.0, 1.1.0) | as listed | [formats/approval-documents.md](formats/approval-documents.md) |

### Format policy

`format_version` in the signed scorecard, the put receipt, the backup receipt
and the catalog point is semver, and so is the standing authorization's
`formatVersion`. A **minor** bump adds optional fields only — existing readers
keep working by ignoring what they don't recognise. A reader **ignores unknown
fields** on a document whose major matches what it supports, and **refuses** a
document whose major is higher than it supports, rather than guessing at a
shape it has never seen. Two documents refuse unknown fields instead: the
authorization document v2, whose older readers refuse a newer minor's field
(the safer verdict, [the format](formats/approval-documents.md#the-authorization-document-v2)),
and the protection event, which is no longer written (release notes, item 74).

### Where the per-version sections went

This page carried one section per format version until v1. Each is now a row
of its format's Versions table:

| Section before v1 | Now |
|---|---|
| <a id="the-first-post-tag-addition-format-110-fx-4"></a>The first post-tag addition: format 1.1.0 (FX-4) | [scorecard](formats/drill-scorecard.md#versions), [receipt](formats/backup-receipt.md#versions) and [catalog point](formats/catalog-point.md#versions) 1.1.0 |
| <a id="format-120-fx-3-what-a-newtopic-restore-did-not-reconstruct"></a>Format 1.2.0 (FX-3) | [the scorecard's `not_reconstructed`](formats/drill-scorecard.md#topic_parity-in-a-newtopic-restore-not-reconstructed-120) |
| <a id="scorecard-format-130-sourcetime_basis-fx-8"></a>Scorecard format 1.3.0 (FX-8) | [`source.time_basis`](formats/drill-scorecard.md#sourcetime_basis-format-130) |
| <a id="scorecard-format-140-integrityverification-prod-081"></a>Scorecard format 1.4.0 (PROD-08.1) | [`integrity.verification`](formats/drill-scorecard.md#integrityverification-format-140) |
| <a id="receipt-and-catalog-point-format-130-topic_configuration-prod-051"></a>Receipt and catalog point 1.3.0 (PROD-05.1) | [`topic_configuration`](formats/backup-receipt.md#topic_configuration--the-topic-configuration-model-format-130) |
| <a id="three-new-auth-modes-receipt-and-catalog-point-140-scorecard-150-prod-013"></a>Three new auth modes (PROD-01.3) | the receipt's [1.4.0](formats/backup-receipt.md#versions) and the scorecard's [1.5.0](formats/drill-scorecard.md#versions) rows; [credential binding](kubernetes.md#credential-binding) |
| <a id="scorecard-format-160-sampleunsampled_topics-and-a-stricter-sampled-check-fx-23"></a>Scorecard format 1.6.0 (FX-23) | [`sample.unsampled_topics`](formats/drill-scorecard.md#sampleunsampled_topics-format-160) |
| <a id="a-source-replication-factor-the-archive-does-not-record-is-not-assessed-fx-21"></a>A source replication factor the archive does not record (FX-21) | [`topic_parity`](formats/drill-scorecard.md#topic_parity-and-what-its-silence-means) |
| <a id="scorecard-format-170-sourceselection-a-restore-from-a-stated-window-start-prod-111"></a>Scorecard format 1.7.0 (PROD-11.1) | [`source.selection`](formats/drill-scorecard.md#sourceselection-format-170-and-200) |
| <a id="scorecard-format-200-a-partition-subset-restore-prod-111b-the-first-major"></a>Scorecard format 2.0.0 (PROD-11.1b) | [format 2.0.0](formats/drill-scorecard.md#format-200-a-partition-subset-prod-111b-od-9-a) |
| <a id="receipt-and-catalog-point-format-150-schema_dependency-prod-030"></a>Receipt and catalog point 1.5.0 (PROD-03.0) | [`schema_dependency`](formats/backup-receipt.md#schema_dependency--does-a-restore-need-a-schema-registry-format-150) |
| <a id="receipt-and-catalog-point-format-160-the-topics-id-before-and-after-the-engine-prod-014a"></a>Receipt and catalog point 1.6.0 (PROD-01.4a) | [`generations`](formats/backup-receipt.md#generations--the-topics-id-before-and-after-the-engine-format-160) |
| <a id="receipt-and-catalog-point-format-170-consumer_positions-prod-041"></a>Receipt and catalog point 1.7.0 (PROD-04.1) | [`consumer_positions`](formats/backup-receipt.md#consumer_positions--consumer-position-evidence-format-170) |
| <a id="receipt-format-180-source_watermarks-and-the-empty-capture-prod-021"></a>Receipt format 1.8.0 (PROD-02.1) | [`source_watermarks`](formats/backup-receipt.md#source_watermarks-and-the-empty-capture-format-180) |
| <a id="scorecard-format-180-targetoriginal_name-a-restore-under-the-original-topic-names-prod-151"></a>Scorecard format 1.8.0 (PROD-15.1) | [`target.original_name`](formats/drill-scorecard.md#targetoriginal_name-format-180), [the approval subject](formats/approval-documents.md#the-per-run-approval-document-v1), [restoring under the original names](kubernetes.md#restoring-under-the-original-topic-names) |
| <a id="scorecard-190-and-210-authorization-document-220-two-person-approval-in-the-console-prod-162"></a>Scorecard 1.9.0 and 2.1.0, authorization document 2.2.0 (PROD-16.2) | [`approval.console`](formats/drill-scorecard.md#approvalconsole-format-190-and-210), [the authorization document](formats/approval-documents.md#the-authorization-document-v2) |
| <a id="scorecard-1110-and-230-targettopic_configuration-prod-052"></a>Scorecard 1.11.0 and 2.3.0 (PROD-05.2) | [`target.topic_configuration`](formats/drill-scorecard.md#targettopic_configuration-format-1110-and-230), [the plan field](formats/drill-spec.md#targettopic_configuration-prod-052) |
| <a id="the-standing-rehearsal-authorization-is-signed-and-the-runner-checks-the-signature"></a>The standing rehearsal authorization | [formats/approval-documents.md](formats/approval-documents.md#the-standing-rehearsal-authorization-format-100-and-110) |
| <a id="logweir-notify-delivers-exit-codes-and-its-notify-result-lines-plat-142"></a>`logweir notify deliver` (PLAT-14.2) | withdrawn in v1 (release-notes item 74): clap refuses the subcommand |
| <a id="refusal-detail-carries-a-guard-refusals-reason-code-and-sentence-fx-34"></a>`refusal-detail=` (FX-34), and the other runner output contracts (I8, the progress channel, `teardown-failed=`, `teardown-key=`, `logweir check run`, `logweir-retention`) | [reference.md](reference.md#runner-output-lines); the mixed-version rules [below](#mixed-versions) |

## The product API's OpenAPI document is pre-release, and says so

`schemas/logweir-api-v1.openapi.json` is the one checked-in schema the rule
above does **not** cover. `info.version` is `1.0.0-alpha.3`. Its consumers are
the console image and its typed client (PLAT-18.1); the binary is
`publish = false`, and no client outside this repository is supported, so the
console and the API ship and upgrade together. The owner decided on 2026-10-07
not to freeze it before the first release candidate: the `-alpha.N` suffix
moves once per release candidate, whose release notes list the changes; the
freeze at `1.0.0`, after which the MINOR/MAJOR rule applies verbatim, belongs to
PROD-14.2. Until then a field change is a pre-release bump that needs no
maintainer approval, but never a silent edit: `just schema-check` and
`crates/logweir-api/tests/contract.rs::the_checked_in_openapi_document_is_what_the_types_generate`
compare the checked-in bytes with the generator (regenerate with `just schema`).
Adding an optional field is MINOR and removing or retyping one MAJOR, so prefer
leaving a field out (it is why `Operation` carries no `jobName`).

Six changes since `1.0.0-alpha.2` (`v0.2.0-rc.1`) are breaking. The document
is still pre-release, so they move only the pre-release number, from
`-alpha.2` to `-alpha.3`, and not the major:
- **breaking:** `GET .../rehearsal-schedules[/{name}]` and the `rehearsals`
  capability are removed (release-notes item 73);
- **breaking:** `POST .../destinations:from-legacy` and the problem codes
  `legacy_location_unknown` and `legacy_location_mismatch` are removed (item 75);
- **breaking:** a schedule's `status.retentionReport` is removed (item 78);
- **breaking:** a protection policy's `notifications` and an alert's `delivery`
  and `notifiedTransition` are removed (item 74);
- **breaking:** a connection create that sets `auth.credentialRef` is refused
  (item 33);
- **breaking:** a destination create that sets a grant's `secret.existing` is
  refused (item 38).

[release-notes.md](release-notes.md) item 86 lists every change, one line each.

## The CLI, and what is not a contract

- **CLI flags and exit codes are stable within a major.** A given Logweir major
  version does not remove or repurpose a flag, nor change the meaning of an
  exit code, without a major bump. New flags and new exit codes may be added in
  a minor. The exit codes are in [reference.md](reference.md#exit-codes).
- **Explicit non-contracts.** The Rust crates in this workspace
  (`logweir-core`, `logweir-engine-oso`, `logweir-evidence`, `logweir-kafka`,
  `logweir`) are **not** a stable API before 1.0. Only the signed document
  formats and the `logweir` CLI's flags and exit codes are covered by the
  promises above; internal crate APIs may change in any release before 1.0.

## Mixed versions

A controller and its runner image are one rollout: `helm upgrade` of the
packaged chart moves both (`controllerImage` and `runnerImage` are rendered
into one Deployment). These are the rules for the times they differ.

### The runner image must be at least as new as the controller

**FX-43 makes this the contract, and names the pair when it is broken.** The
controller passes `--runner-line-token` to every runner Job it creates
(`Restore`, `Backup`, the `KafkaCluster` probe, the check Jobs and the
`RetentionPolicy` Job), and reads only the lines the runner seals with that
token ([sealed lines](reference.md#sealed-lines-fx-43)). A runner
that does not know the flag (every runner image published before this change,
the released `v0.2.0` included) stops while parsing its arguments, before any
work: exit 1, stdout empty, stderr opening with `error: unexpected argument
'--runner-line-token' found`. The object says so: `exitReason:
RunnerOlderThanController` on a `Restore` or `Backup` (a schedule does not
retry it), reason `RunnerOlderThanController` and `reachable` cleared on a
`KafkaCluster`, `CheckContractMismatch` naming it on a check, and
`lastEnforcement.refusalCode: RunnerOlderThanController` on a retention run
(nothing deleted). The same name is given to any run whose runner exited 0 to 4
and printed no sealed `runner-exit=` line. Upgrade the runner image to the
controller's version; to roll back, move both together or the controller
first. What it looks like and what to do are in
[the guide](kubernetes.md#the-runner-image-must-be-at-least-as-new-as-the-controller).

The other directions degrade to what they did before, never to trusting more:

- *A newer runner under an older controller.* An FX-34 controller passes
  `--line-token`, and this runner then prints exactly what an FX-34 runner
  printed. A controller older still passes no token and reads the plain
  lines, which this runner prints after the sealed ones, in their old order
  and place.
- *A Job an older controller created, read by this one* (an upgrade with runs
  in flight): with `--line-token`, only `refusal-detail=` is read by its
  token, as FX-34 did; with no token, every reader keeps its own rules. Such
  a run is never called `RunnerOlderThanController`.
- *An older controller reading a Job this one created* (a rollback with runs
  in flight): it finds no flag it knows, reads the Job as untokened, and
  reads the plain lines. A check's plain end frame follows its sealed twin,
  which an older decoder ignores as a non-frame line.
- A code the controller does not know is not shown, while the state on
  `refusal-reason=` and the exit code are recorded as always.

### Interface I8 is amended: exit 2 prints the signed failure's keys too

**Amended 2026-09-23 (D3 §2.5, PLAT-14.3).** The evidence key lines are printed
by every run that signed and put a scorecard — exit 0 and exit 2 — and by no
other ([the lines](reference.md#logweir-restore-run--drill-run)). **The exit code
stays authoritative for success:** at exit 2 the controller records and
verifies the signed FAILURE and copies its `outcome`, the `Restore` stays
`phase: Failed`, `Verified` is never `True` over a non-zero `exitCode`
(`ExitCodeNotZero`), and the API's `result.status` is `notPass`.

- *A new runner under an older controller* (`7487682`): nothing becomes a pass,
  but that controller writes `Restore.status.completion` for an exit-2 run
  whose signed failure verifies `Valid`, and the console shows cutover guidance
  over a restore that did not reconcile. **Roll the controller out before the
  runner**, and roll the runner back before the controller.
- *An older runner under a newer controller* prints no keys at exit 2: one
  `Failed` condition, no evidence, and no `EvidenceKeysUnreadable`.

### Execution contract v2: what v1 still buys

`logweir_core::execution_contract::VERSION` is `"2"` (decision D3 §8,
Amendment I), so an old binary refuses `--execution-contract-version 2` before
dispatch rather than run a plan whose checks it does not implement. **v1 is
still accepted, for already-created legacy Restores only**: a v1 invocation
carrying v2 material — a `source.point` block, a standing rehearsal
authorization, an approval-policy snapshot or a confirmation-issuer public key
— is **refused by name, exit 3**. Let the in-flight legacy Restore finish (or
delete it) and create the new one. **Every v2 addition is additive and
absent-tolerant**, each all-or-nothing as a block:

| Addition | Absent means |
|---|---|
| `source.point` (the plan) | the set comes from `source.backup`, and no binding is checked |
| `LOGWEIR_EXECUTION_AUTHORIZATION_KIND` | `approval` — the per-run `Approval` |
| `LOGWEIR_EXECUTION_AUTHORIZATION_SHA256`, `…_AUTHORIZATION_SIDECAR_SHA256`, `…_AUTHORIZATION_KEYS_SHA256`, `…_REHEARSAL_SCHEDULE_UID` | no standing authorization. Standing authorizations are withdrawn (PROD-17.1): a complete contract naming the `standing` kind is refused by name, exit 3 |
| `LOGWEIR_EXECUTION_POLICY_SNAPSHOT_SHA256` | the synthesized `legacy-governed-v1` policy |
| `LOGWEIR_EXECUTION_CONFIRMATION_KEY_SHA256` | the legacy single-key bundle |
| `LOGWEIR_EXECUTION_EVIDENCE_KEYS_SHA256` | no evidence keyring, which the controller renders exactly when the plan carries `source.point`; a point-bound plan without it is refused `PointUntrusted` |

The optional bundle members (`--policy-snapshot`, `--confirmation-key`,
`--evidence-keys`) are digest-pinned in **both** directions, and the first two
are refused on a standalone invocation. `--evidence-keys` only ever NARROWS, so
a standalone run of a point-bound plan (the disaster path) needs it.

### A bound point's receipt is verified, and restores its own set (D3 §5.5, FX-16)

Before any data moves, the runner verifies a bound point's receipt sidecar
against the mounted evidence keyring and judges the key for `EvidenceSigning`
at the receipt's `finished_at`; and it refuses, exit 3
`PointBindingSetMismatch`, a plan whose `source.backup` is not the receipt's
`backup_id` (**`latestCompleted` beside `source.point` included**), whose
`source.storage` would have the engine read another manifest, or whose
described set is not the one it verified
([the plan field](formats/drill-spec.md#sourcepoint-execution-contract-v2)).
FX-16's refusal is a new cause for exit 3 only to the safer side, MINOR under
OD-7's third case; no format changes.

- **Upgrade the controller and the runner image together.** A runner at this
  version handed a point-bound plan by an older controller refuses it
  `PointUntrusted`, and an older runner does not know `--evidence-keys` (exit
  1). A point-bound `Restore` whose approval bundle an older controller made
  before its Job existed is refused `ApprovalBundleConflict`: delete it and
  create it again.
- **A hand-written point-bound plan** that says `backup: latestCompleted`, or
  whose `source.storage` prefix is not the one the set was written under, is
  refused after the upgrade: name the point's set, which needs a new approval.
  A copy of the archive that keeps the original keys restores as before.
- **Rollback.** An older runner restores whatever `source.backup` names; no
  archive, catalog or evidence object changes in either direction.

### Saved destinations, the execution claim and an existing set

- **A `Backup` frozen against a saved destination is REFUSED by an older
  controller, not run**: its grammar is `deny_unknown_fields`, and its
  `storage_url_for` refuses the sentinel `archive.url`
  (`logweir-destination://<name>`), so the run ends `ArchiveUrlUnreadable`
  before any POST and nothing is written to the wrong place. Let
  destination-backed runs reach a terminal phase before a downgrade.
- **One `backup_id` is one engine run (RECEIPT-DUP), and a set that already
  exists is never written again (FX-7)**
  ([the execution claim](formats/backup-receipt.md#the-execution-claim-one-engine-run-per-backup_id)).
  A standalone `backup run` that reused a fixed `backup_id` must use a fresh id
  per run (`--backup-id-override`). Let a Backup Job whose older runner is
  still RUNNING finish before upgrading. An older runner ignores the claims
  and the set check, so let in-flight `Backup`s finish before rolling the
  runner back: re-run over a set, it leaves this build's point `Conflict` on a
  versioned bucket and is seen by nothing on an unversioned one.

### The Amendment G status additions are additive, and absent means *not observed*

`Backup.status.{progress,capture}`, `Restore.status.{progress,completion,teardown}`
and `*.status.evidence.verification.{signedAt,trust}` are optional blocks on
kinds that already existed. An object written before them is read exactly as
the controller that created it read it. **An absent field means *not
observed*, never a default that flatters the object:**

| Absent | What is reported |
|---|---|
| `status.progress` | the phase alone; the API never reports `queued` or `preparing` it did not see |
| `status.capture` | freshness falls back to the terminal condition's time, and `availabilityBasis` says so |
| `status.evidence.verification.trust` | `basis: None`, and the badge uses the pre-existing rule |
| a catalog point's `partitions` | the size bound is not enforced, recorded as `sizeBasis: unknown` |
| a record's `execution` | provenance unknown |

### Safe clones (PROD-11.1a): an older runner refuses, an older controller ignores

`Restore.status.clone` and the finalizer `logweir.dev/clone-cleanup` are new;
every other object reads as before. A runner from before PROD-11.1a refuses a
clone plan at parse (`target.teardown` was a string) and creates nothing; keep
the runner image at least as new as the controller. A controller from before it
ignores `status.clone` and the finalizer: after a rollback, a clone is never
cleaned up and deleting its `Restore` waits until the finalizer is removed by
hand ([the steps](kubernetes.md#safe-clones-a-scratch-restore-kept-for-a-declared-time)).
Every scorecard of a run that created topics is now 1.10.0 (2.2.0 for a
subset), carrying `target.creation`; older readers accept it and ignore the
block.

### A connection test asks more, and the probe asks every advertised broker (FX-45, FX-46)

A `SourceConnection` check plan now lists `capabilityChecks`; a runner from
before FX-46 refuses it (`CheckContractMismatch`, naming the field), so keep
the runner at least as new as the controller. A newer runner under an older
controller gets no list and emits no capability row. `cluster-probe` prints a
third line, `brokers-answered=<answered>/<listed>`, and answers
`reachable=false` when a broker the cluster lists does not answer on its
advertised address; an older controller reads the first two lines by key and
records `ProbeReportedUnreachable`, this one records
`AdvertisedBrokersUnreachable`. A cluster whose advertised addresses this
namespace cannot reach therefore turns `reachable: false` after the upgrade:
a `Restore` against it is refused at admission (terminal,
`ClusterNotReachable`); backups are not gated on `reachable`. A Test
connection in flight across the upgrade (a check Job the older controller
created) reports none of the three capability rows this controller expects,
so that one `Preflight` reads `unknown`; run the test again.
`connection.topicsReadable` may now be blocking `notReady`
(`TopicAuthorizationFailed`), a code and a gating every controller reads.
`backup run` and `drill run` refuse, by the row's name, what the broker
reports the principal may not READ and what the endpoint was seen not to
serve, before the engine; not observed is never a refusal. No signed document
changes.

### Upgrading past `signedAt`: a run that finished on an older controller is re-read once, not re-judged

A `Backup` or `Restore` that finished before `status.evidence.verification.signedAt`
and `.trust` existed carries neither. **Only `trust.basis: Current` or
`Historical` means a signing time was compared with the key's validity
window**; such a block without `signedAt` stays `Untrusted` /
`SignedOutsideValidity`. Anything else — no `trust` block, no `basis`,
`basis: None`, `basis: Unverified`, or a spelling a later build invents —
compared nothing, so the controller reads the run's own document **once**,
through the original evidence path, checks its bytes against the recorded
digest and takes `signedAt` from it; until that succeeds the verdict is
`NotAttempted`, never green. An unlisted signer, a key-usage mismatch and a
`KeyCompromise` revocation still change such an object's verdict at once. The
allow-list is what stays correct under a build nobody has written yet. Both
fields are additive, so a rollback is unaffected
([when trust changes](kubernetes.md#when-trust-changes)).

## Known limitations of v0.1

- **A drill posts no notification.** Its webhook, Slack and PagerDuty post is
  withdrawn (release notes, item 74), and a plan that names a sink is refused
  ([the plan field](formats/drill-spec.md#notifications)). Alert on the
  runner's metrics textfile or the `Restore`'s conditions instead.
- **Backups and drills are separate commands.** `logweir backup run` captures
  the source cluster and writes a signed backup receipt; `logweir restore run`
  and `logweir drill run` consume an existing archive. The historical
  `--from-cluster` flag is not implemented. A drill does not contact the source
  cluster: its scorecard has `source.captured_by_logweir: false`, null
  `measured.rpo_source_relative_seconds`, and
  `rpo_source_relative_unmeasured_reason: "source cluster never contacted"`; a
  separately created backup receipt does not turn a drill's archive-relative
  RPO into source-relative RPO.
- **The YAML parser is archived.** The drill spec and both rendered engine
  documents are YAML, parsed by `serde_yaml` 0.9, which resolves to
  `0.9.34+deprecated` — an archived crate (RUSTSEC-2024-0370), over the
  archived `unsafe-libyaml` (RUSTSEC-2024-0449); `kube-client` uses the same
  pair. It is kept for v0.2 too: the drill spec sits inside signed plan bytes,
  so a replacement parser must first be shown to parse every plan the same
  way, and that is v1.1's work. `deny.toml` carries the matching `ignore`
  entries and points here.
- **No musl release target.** `rdkafka` compiles `librdkafka` from C, which
  does not cross-compile to musl without much more work
  ([ADR 0004](architecture.md#adr-0004-kafka-client)); `release.yml` does not
  attempt one, and no release ships a musl binary.
- **Release execution remains an evidence requirement.** The
  [release checklist](tag1-checklist.md) records which release obligations are
  blocked and [gates.md](gates.md) records dated workflow results; a local test
  of workflow YAML does not prove that a published artifact was pulled and
  executed. The runner release image is built once, checked with
  `scripts/check-image.sh`, and those same bytes are pushed only after the
  check succeeds (`crates/logweir/tests/workflow_lint.rs`).
- **The sample window in `examples/drill.yaml` is illustrative and will not
  match your archive.** A window that overlaps no segment is REFUSED with exit
  1 rather than reported as a pass over nothing.
- **The exit-code contract is nearly invisible under Kubernetes** unless the
  Job is shaped with `restartPolicy: Never` and `backoffLimit: 0`; otherwise
  exit 2 renders as a generic `Error`, indistinguishable from exit 1
  ([the guide](kubernetes.md#read-a-runs-exit-code)).
- **A drill that verified correctly and could not clean up still exits `0`.**
  Phase 9 deletes the scratch topics *after* phase 8 signed and uploaded the
  result. A topic the broker would not delete is named on that run — the metric
  `logweir_drill_teardown_topics_failed{cluster}` (`0` included, so `> 0` is a
  valid alert), a `WARN` naming the missing grant, the summary line, and
  `teardown-failed=` (copied to `Restore.status.teardown.failed`) — and the
  signed teardown attestation (`logweir/drills/{run_id}.teardown.json`) is the
  durable record; one that could not be persisted is a `WARN` only, and its
  absence from the bucket is the signal. A run that fails after its creation
  step (exit 1, 3 or 4) tears down what that step created by the same rule,
  never an original name (FX-44). **Whether a leftover topic should change the
  exit code is an open question** (stage-2 ruling R-C);
  `teardown_failure_does_not_change_the_exit_code` pins today's answer.
- **Fixtures document more than the current writer emits.** Some scorecard
  fixtures populate `engine_subreport`, which the writer always sets to null
  ([fixture notes](../e2e/fixtures/README.md)).
- **S3 credentials come from `object_store`'s own chain, not the AWS SDK's.**
  `Store::from_url` builds the S3 client with `object_store` 0.14's
  `AmazonS3Builder::from_env()` (static keys, then web identity / IRSA, ECS,
  EKS Pod Identity, IMDS). `~/.aws/credentials` profiles, `AWS_PROFILE` and SSO
  are unsupported.
- **MSRV is 1.89**, raised from 1.82 by `object_store` 0.14's feature set
  (edition 2024 needs rustc 1.85; `crc-fast`, the `icu_*` crates and
  `idna_adapter` raise the floor to 1.89).
- **Create-only evidence puts.** MinIO, SeaweedFS 4.48, RustFS 1.0.0 and
  versitygw v1.8.0 are the S3-compatible backends with a row in this
  repository; RustFS and versitygw are opt-in compose profiles that
  `compat-weekly.yml` runs weekly (PROD-01.2a)
  ([archive backends](support-matrix.md#archive-backends)). **Real AWS S3 is
  NOT exercised**: the AWS leg is
  `[UNVERIFIED — needs a real AWS S3 bucket and a credential source]` and is
  confirmed by the first adopter run, not by this plan. A backend that answers
  `NotSupported` to `PutMode::Create` takes the HEAD-then-PUT fallback in
  `Store::put_create_only`, a genuinely weaker guarantee (on AWS S3,
  `object_store` 0.14 uses the conditional `If-None-Match` put).
- **The signed scorecard's `evidence` block cannot establish upload facts**: it
  is signed before upload, so the four fields are zeroed
  ([the format](formats/drill-scorecard.md#evidence--makes-no-claim-about-the-upload)),
  and the readback is the separately signed put receipt; a missing one means no
  storage evidence was published. `object_store` 0.14 models no Object Lock
  readback, so the immutable fields stay unproven even on an Object Lock
  bucket.
- **Images.** Every image is built for `linux/amd64` and `linux/arm64` on
  native runners ([install.md](install.md#verify-the-images));
  `scripts/check-image-weirkeeper.sh --no-exec` inspects a foreign architecture
  without executing it, and `Dockerfile.weirkeeper` refuses cross-compilation
  because its `aws-lc-sys` build reads host headers. Local rebuilds change image
  digests; the chart uses `:latest` for the Logweir repositories by the owner's
  2026-09-12 decision.

### `sample.anchor` accepts only `head` in v0.1; `tail` and `random` are refused

A plan naming `tail` or `random` is REFUSED at phase 0 with **exit 3**, nothing
written, and the message names the limitation. Phase 7 reconciles the restored
topic's FIRST `sample.records_per_partition` records, which match the
archive's sampled records only for `head`; `tail` is unsound for the property
the drill measures, and `random` is unsound and unbounded (measured: `random`
scored `pass_rate_measured: 0.08`, `fail-integrity`, against a byte-for-byte
correct restore). Refusing is not substituting `head`: a signed scorecard must
not record an anchor the drill never applied. Set `sample.anchor: head` or omit
it; to rotate coverage, rotate the sampled WINDOW between drills. An
unrecognised spelling does not parse and exits **1**.

### A compacted target topic is reported as a mismatch, not as `partial`

Phase 7 does not tell "compaction removed this record" from "the restore or the
archive lost it", so a compacted target reads `integrity.result: fail` with the
offsets logged. That is the safe direction, but a drill against a compacted
scratch topic means nothing in v0.1: restore into a topic with
`cleanup.policy=delete`, which is what phase 6 creates by default.

### A `newTopic` restore reconstructs the source's topic settings only when asked

<a id="a-newtopic-restore-does-not-reconstruct-the-sources-topic-settings"></a>

Without `target.topic_configuration` in the plan, a restore creates every
target topic itself, in both modes, as it always did: the manifest's partition
count, the plan's `default_replication_factor` (1 when it names none),
`retention.ms=-1`, `message.timestamp.type=CreateTime`, and the target broker's
default for everything else, `cleanup.policy` included. For such a `newTopic`
restore the source's compaction, retention and replication factor are **not
reconstructed**; since scorecard format 1.2.0 the signed scorecard names each
in `topic_parity.not_reconstructed`, and in a `newTopic` document signed before
1.2.0 the `intentionally_deviated` label means the same
([the format](formats/drill-scorecard.md#topic_parity-in-a-newtopic-restore-not-reconstructed-120)).

**With `target.topic_configuration`** (PROD-05.2, a plan bound to a point whose
receipt is 1.3.0 or later) the topics are created from the point's model: the
source's partition count, a replication factor bounded by the target's brokers
with its reason, and the source's portable overrides, beside Logweir's own
settings that keep every restored record until the verdict. With
`transition: afterVerification`, phase 9 then applies the source's compaction,
retention and timestamp settings, after a passing verdict only and only to a
topic still holding what it was created with; a `retention.ms` shorter than
the restored records' age deletes them at the next retention check, which the
restore preflight states. Inherited values, reassignment throttles, provider
keys and secrets are never applied, and a topic a declarative owner manages is
never altered: its desired state is exported for the owner. The scorecard
carries `target.topic_configuration` (1.11.0, 2.3.0) and the phase-9
attestation what was applied
([the format](formats/drill-scorecard.md#targettopic_configuration-format-1110-and-230)).
`topic_parity` still compares what phase 7 saw, before the transition. A
scratch drill is unaffected unless its plan carries the block.

### `engine_subreport` is always null in v0.1

Logweir never runs the engine's `validation run` subcommand (phase 7's unused
call was removed in v1, release-notes item 74), so the field is always `null`.
Read null as "no engine sub-report retained", not as a successful upstream
validation.
`e2e/tests/full_drill.rs::the_engine_subreport_is_absent_until_oso_cli_engine_overrides_validation_run`
fails when that changes.

### A crashed restore is not resumable in v0.1

The rendered `restore.checkpoint_state` path is **pod-local and is never
uploaded**, so a restore that dies mid-run re-runs from phase 0. Since engine
build 3 the engine also saves it inside a topic every
`checkpoint_interval_secs`; it is still per run and never read.

### An archive holding transaction markers is refused unless the plan allows it

The pinned engine captures with `READ_UNCOMMITTED` and restores every archived
record non-transactionally, so a restored transactional topic holds aborted
records, records of transactions open at backup time, and one record per commit
or abort marker, all visible to every consumer
([decision record §2.1](to-do/decisions/PROD-01.1-record-semantics.md#21-transactions)).
Logweir does not claim transactional recovery. Since PROD-01.1a a restore, before
it creates anything, counts the records in its window's segments and every
later segment that are shaped like a marker (key `00 00 00 00` or
`00 00 00 01`, a 6-byte value starting `00 00`, no header but
`x-original-offset` and `x-original-timestamp`), and refuses, exit 3,
`refusal-reason=TransactionMarkersInArchive`, naming topics, partitions and the
count. To restore such an archive as it is, state
`restore.allow_transaction_markers: true` (inside `plan_hash`); the check is
then skipped and `sample.coverage_note` says so. A segment in the window the
check cannot decode refuses the run; one after the window that cannot be read
does not, and the note counts it. The shape is the only signal: a record of
your own with that shape is counted too, and a transaction whose marker the
archive lacks, or that sits in an unread segment, is not detected
(PROD-00.3a).

### Out-of-order timestamps inside a segment

The archive describes each segment by its first and last record timestamps,
not its minimum and maximum, and the engine selects the segments a restore
reads by those two values. Since PROD-01.1b a restore reads every segment of
the partitions it restores once before it creates anything (the pass the
transaction-marker check makes, which now runs under
`restore.allow_transaction_markers: true` too), and:

- the window starts at the **oldest archived record**, so no record older than
  every segment's first is dropped, and the count bounds use each segment's
  oldest and newest record;
- a window that selects a record held, out of timestamp order, in a segment
  the engine skips (its first record is after the window's end, or its last
  is before a stated start) is **refused**, exit 3,
  `refusal-reason=PointInTimeSelectionIncomplete`, naming the segment and the
  point or start that reads it. There is no override: choose that point or
  start;
- a segment the restore reads and the runner cannot decode refuses the run, as
  before, and now under the marker override too; one it does not read that the
  store does not hold, or that the runner cannot decode, is counted in
  `sample.coverage_note`, never refused (any other store error stops the run,
  exit 1).

The three rows of `e2e/tests/record_semantics.rs`
([decision record §2.2](to-do/decisions/PROD-01.1-record-semantics.md#22-non-monotonic-and-equal-createtime-within-one-segment))
measure it: the record below the old floor is restored and the drill passes, a
correct point-in-time restore passes, and the skipped record is refused before
any write. **Upgrade:** a `Restore` or a `RehearsalSchedule` slot over such an
archive that used to run, and could sign a pass with a record left out, is now
refused; an archive in timestamp order restores exactly as before. The restore
preview reads the manifest only: its `archive.coverage` row says the run may
refuse so, and cannot say whether it will. Rolling the
runner back restores the old selection. The engine itself still selects by
first and last; reading minimum and maximum from the manifest is PROD-00.3b.

### `LogAppendTime` sources are restored with the producers' timestamps

For a topic on `message.timestamp.type=LogAppendTime`, the archive holds the
timestamp each producer set, and the restored topic reports it as `CreateTime`
(measured by
`e2e/tests/record_semantics.rs::log_append_time_source_versus_restored_output`;
[decision record §2.3](to-do/decisions/PROD-01.1-record-semantics.md#23-a-logappendtime-source)).
A full restore runs and its drill passes; a point-in-time restore is refused
unless the plan accepts it (next).

### A point-in-time over a `LogAppendTime` source is refused unless the plan selects by producer time

Since FX-8, a restore whose plan selects records **by time** over a source topic
whose timestamp type is **recorded** as `LogAppendTime` is refused, exit 3,
`refusal-reason=PointInTimeByProducerTime`, before any target topic is created
(phase 0 has run, so on a `LogAppendTime` target its probe topic has been
created and deleted, [the exit-3 exception](reference.md#exit-codes)). A plan
that states `restore.time_basis: producerTime` runs, and its scorecard lists the
topic under `source.time_basis.producer_time`: a restore **by the producers'
clocks**, not by the broker's append time, which needs an archive that keeps it
(PROD-00.3c). Where nothing records the type, the selection runs and the topic
is listed under `source.time_basis.not_recorded`, never read as `CreateTime`
([the plan field](formats/drill-spec.md#restoretime_basis-fx-8)).

### A repeated header key keeps one copy

The pinned engine holds a record's headers as a map, so a header key repeated
in one record is archived and restored once — at the first copy's position,
with the last copy's value — and the drill does not detect it. A record that
already carried `x-original-offset` keeps only the backup's own, and a drill
that samples such a record fails
(`e2e/tests/record_semantics.rs::keys_nulls_tombstones_and_duplicate_headers`;
[decision record §2.4](to-do/decisions/PROD-01.1-record-semantics.md#24-keys-nulls-tombstones-and-headers)).

### A broker outage during a restore can leave a partial target with duplicates

The engine resends a produce whose acknowledgement it did not receive, and does
not produce idempotently, so a lost acknowledgement duplicates that batch. An
outage long enough for the broker to re-register can end the restore early,
exit 1 with no scorecard, leaving the partial target; a stall past Logweir's
20-second client timeout can fail a restore whose target is complete
(`e2e/tests/record_semantics.rs::a_lost_produce_acknowledgement_during_restore`;
[decision record §5.1](to-do/decisions/PROD-01.1-record-semantics.md#51-a-lost-produce-acknowledgement--run)).
A duplicated target that reaches phase 7 fails: complete coverage counts each
repeated `x-original-offset`, and the sampled check fails a duplicate in the
head it reads.

### SASL: two clients, two trust stores, and one password variable

Logweir dials a cluster with **two clients**, its own librdkafka client and the
engine's, and their trust stores are separate (Global Constraint 29): a
private-CA adopter configures one file, `KafkaCluster.spec.auth.tlsCa` (or
`LOGWEIR_SOURCE_TLS_CA_FILE` / `LOGWEIR_TARGET_TLS_CA_FILE` standalone), and
the runner hands it to both ([install.md](install.md#two-clients-two-trust-stores)).
**The password is never in a spec, a plan, a rendered document, a receipt or a
plan hash**, and there is **no CLI flag** for it: it is projected as
`LOGWEIR_SOURCE_PASSWORD` or `LOGWEIR_TARGET_PASSWORD`, and a value the engine
document cannot carry is refused, exit 3, `CredentialNotRenderable`
([saved connections](kubernetes.md#saved-connections)). Local SCRAM is covered
end to end by `e2e/tests/scram.rs` (`just e2e`).
**MSK-specific behavior remains [UNVERIFIED — needs an MSK cluster].**
MSK IAM / OAUTHBEARER is unimplemented. SCRAM-SHA-512 over TLS has a
repeatable local row since PROD-01.2a (the `auth` profile's `SCRAM512TLS`
listener, run by CI on every pull request); MSK itself is still unrun
([the compatibility contract](support-matrix.md#the-compatibility-contract)).

### The unit suite dials nothing; the e2e suite dials

The default `cargo test --workspace` suite must not contact a broker, bucket,
cluster or webhook (`just time-unit-suite`: 120 s for the suite, **15 s for any
individual test**); `just e2e` runs the rows that need the compose stack
([the gate reference](gates.md)). Run mutation tests in an isolated worktree
with `just mutant "<cargo args>"`, and clear them with `just mutant-clean`. The runner builder cross-compiles natively:
the former emulated Cargo layer took **33x** the native cross-compile
(2026-09-09, arm64), a measurement and not a release budget.

## Measured scale limits (PLAT-20.2)

These are the sizes the product bounds itself to. The bounds are contracts;
the timings measured at them are one machine's and are **not** budgets.

| Surface | Bound | Beyond it |
|---|---|---|
| Topic discovery | `maxTopics` 20,000 by default, 50,000 installation ceiling (`hardMaxTopics`); chunks of 2,500 names / 768 KiB, at most 64 | the inventory is `truncated: true` with `MaxTopics` or `RelayLimit` ([topic discovery](kubernetes.md#topic-discovery)) |
| The topics of one backup (FX-33) | **1,000 topics**, named or resolved, at a budget of **5,000 bytes a topic** in the receipt: a receipt of at most **5,131,072 bytes** and a catalog record of at most **6,131,072 bytes**. A dynamic selection is also at most 256 KiB of names | refused before the engine runs, by name (`BackupSelectionTooLarge`; a dynamic selection's `Backup` is `SelectionTooLarge`); nothing is left out to fit. Split the selection across schedules ([the budget](kubernetes.md#one-backup-names-at-most-1000-topics)) |
| Catalog view (history from an archive) | the newest `spec.sync.viewLimit` points, 100–5,000, in at most 8 page `ConfigMap`s | counted and histogrammed over the whole archive, `truncated: true`; the rest is `logweir catalog list` against the archive ([the catalog](kubernetes.md#the-recovery-catalog)) |
| Catalog point page (`…/points`) | 200 rows; the verdict join reads at most 2,000 `Backup`s (4 × 500) | `backupVerdictsIncomplete: Truncated`, and the console offers no restore from that page |
| API list page | 200 objects | follow `nextCursor` |
| Console lists (history, backups, runs) | 25 pages × 200 = 5,000 rows per kind | the page refuses and says how many it read, rather than showing a prefix as the whole; prune history or use `kubectl` |
| Manual runs holding a runner slot (P10) | 4 manual `Backup`s and 2 manual `Restore`s per namespace (`runs.*`); scheduled runs not counted | the run waits `Queued` / `ConcurrencyLimited` with nothing created and starts in creation order ([manual runs](kubernetes.md#concurrency-and-manual-runs)) |
| Manual runs started by one person (P10) | 10 "Back up now" and 5 restores per namespace per minute (`api.console.rateLimits.*`), per console process | `429 rate_limited` with `Retry-After`; nothing is created ([api.md](api.md) *Rate limits*) |

**What was measured.** Offline and in process (2026-09-23, release builds on
an Apple M4), with harnesses that re-run:

```bash
cargo test --release -p logweir-api  --test scale -- --ignored --nocapture --test-threads=1
cargo test --release -p weirkeeper   --test scale -- --ignored --nocapture --test-threads=1
cargo test --release -p logweir-kafka --test scale -- --ignored --nocapture --test-threads=1
MEASURE_ROWS=5000 MEASURE_TOPICS=5000 node scripts/plat18-2-measure.mjs
```

At the largest sizes the product pages, nothing was near a budget: discovery
steps took at most 18 ms for 50,000 topics against a 300-second Job deadline,
API pages stayed in tens of milliseconds, and the console rendered 5,000 rows
in under 30 ms. A catalog point page re-reads its view from the first page
(the cursor is a row offset), bounded at eight pages a request. Live on
docker-desktop (2026-09-24, 258 real points), a `Full` catalog sync took 58.5 s;
on that build a hundred accepted "Back up now" requests became a hundred runner
pods and turned the node `NotReady`, which release-notes item 15 (P10) bounded.
[UNVERIFIED — a 1,000-point archive was not reached live; 258 points were measured, and the offline rows above carry the 5,000-row bounds.]

**The regression checks are on work, not time.** `crates/logweir-api/tests/scale.rs`
asserts the bounds in the default suite — a point page reads at most 8 page
`ConfigMap`s and 4 `Backup` pages, a topic search at most 8 chunks, and the
console's 5,000-row walk one bounded `LIST` per page — and each was shown to
fail when its bound is removed.

## Recorded rulings that have no ADR yet

### The restore window's end is inclusive; the backup receipt's `covered.to_ms` is exclusive

A `Restore`'s window is closed: the engine's filter is `timestamp >= start &&
timestamp <= end`, so a record whose timestamp equals `restore.point_in_time`
**is** restored when its segment is read. The receipt's `covered.to_ms` is the
newest segment's end **plus one millisecond**, the first instant the archive
does *not* cover. So copying `covered.to_ms` into a `point_in_time` is refused
by the readiness check (`PointInTimeAfterCoverage`, `oldest < point_in_time <=
newest`), and so is a `point_in_time` equal to `covered.from_ms`
(`PointInTimeBeforeCoverage`); the console offers `[from_ms + 1 ms, to_ms - 1
ms]` and defaults to its end. Both floors are the minimum segment start over
the topics a document names, and a stated start (`"<start>/<end>"`, PROD-11.1)
must be at or after it ([out-of-order timestamps](#out-of-order-timestamps-inside-a-segment)).

### Exit 3 has one documented exception: phase 0's `LogAppendTime` override probe

Exit 3 means "refused by a guard, before anything runs", and there is exactly
one write before the approval is verified: on a target broker whose effective
`log.message.timestamp.type` is `LogAppendTime`, and only then, phase 0 creates
the first mapped target topic with `message.timestamp.type=CreateTime`, reads
the value back, and deletes it again on both branches, because no read answers
whether such a broker honours the override. The probe stays inside
`target.topic_mapping_prefix` (`TopicDeleter` refuses every other name), runs
only after phase 0 found no mapped target topic, and is deleted before any
verdict. A target whose broker configuration phase 0 may not read is exit 1
before the probe (FX-4), and a probe or target topic the cluster does not serve
within the bound is exit 1 naming it (FX-18).

### A phase-5 / phase-6 `restore.yaml` divergence is exit 1, not exit 3

Logweir hashes the exact `restore.yaml` bytes it writes at phase 5 (for
`kafka-backup validate-restore`) and refuses at phase 6 if the re-rendered
document does not match, naming both digests. It is **exit 1**, an operational
error with no artifact: exit 3 is reserved for a refusal before anything runs,
and by phase 6 the engine's own `validate-restore` has executed.

### An empty configuration answer is a refused read, never "no overrides" (PROD-04.0 T13, FX-4)

rust-rdkafka 0.36.2 never reads librdkafka's per-resource DescribeConfigs error
(`src/admin.rs:1121-1159`): a topic or broker the principal may not
DescribeConfigs comes back as a SUCCESS with ZERO entries, which Kafka never
answers for an authorised resource (`ConfigHelper.scala:54-98` at 4.3.1). So
`logweir-kafka` turns an empty answer into an error, NotAuthorized or unknown
topic (`e2e/tests/config_coverage.rs::a_denied_describe_configs_is_a_refusal_at_every_reader`):
phase 0 exits 1 when the restore identity may not read the target's broker
configuration (grant DescribeConfigs on the Cluster resource; Describe does not
imply it), and phase 2 and the probe readback exit 1 on an existing topic's;
`target.timestampBound` reads `unknown` (`BrokerConfigsNotReadable`); phase 7
names the topic `targetReadDenied` in `not_assessed`; and a backup never fails
for it — the topic's coverage reads `captureDenied`
([the format](formats/backup-receipt.md#config_coverage--topic-configuration-capture-coverage-format-110)).
For a visible topic `captureDenied` is an inference from the principal's
metadata (owner choice AP-OC1 would make it an observation); it never makes the
answer `captured`.

### An endpoint's capability is read from the endpoint, and a value it did not report is not recorded (PROD-01.2)

A Kafka-compatible endpoint is not always Apache Kafka (measured on Redpanda
v26.2.4,
`e2e/tests/compat_contract.rs::redpanda_backs_up_and_refuses_a_restore_before_it_starts`).
**When the endpoint cannot answer, the receipt, the catalog and the status say
"not recorded" with the reason, and never record an empty answer as a fact**
([the sweep](to-do/decisions/PROD-01.2-compatibility-contract.md#6-capture-paths-swept-unsupported-metadata-never-appears-captured)).
So `Restore.status.topicPreflight.timestampType` is absent when the broker did
not report its timestamp type (target topics are still created with
`CreateTime` pinned); `target.timestampBound` reads `unknown`,
`TimestampBoundNotReported`, when the broker reports neither bound key; four
capability rows (`connection.engineProtocol`, `target.engineProtocol`,
`connection.topicConfigsReadable`, `connection.groupTypes`) say what the
endpoint cannot do and are never `ready` for a broker that did not answer
([the rows](kubernetes.md#what-the-rows-check),
[capability checks](support-matrix.md#capability-checks)); and an object
store's answer is read from its HTTP status and error `<Code>`, never from a
word in its text (versitygw's `404 XAdminUserNotFound` is a refused credential,
`InvalidCredentials`). A marker topic the restore identity may not Describe is
refused as before (exit 3), and the message says it does not exist or is not
describable and names the grant; `connection.authenticated` says when a
cluster's advertised brokers cannot be reached (`logweir cluster-probe` read
only the cluster id then; since FX-46 it asks every advertised broker). An older runner refuses a plan that lists the new rows,
so the runner image moves with the controller. No signed document changes.

### `RetentionPolicy` in `Enforce` is where the deletion boundary moves, and it is an installation decision

In every other mode Logweir writes only under its own `logweir/` prefix,
create-only, and holds no object-store delete capability anywhere. `Enforce`
gives the controller no delete verb, and the deleting crate is not linked into
it: it turns the controller's authority to CREATE A JOB into a deletion
capability **in the namespace that holds the retention credential**, the same
residual class as the signing oracle. The hard boundary is that credential's own
scope — the policy's prefix, never `logweir/`. Arming it takes four things held
by different places: `logweir-retention-admin` setting `mode: Enforce`, the
`logweir-retention` ServiceAccount (chart value `retention.enabled`, no
Kubernetes verb), the prefix-scoped credential, and an administrator's approval
of the current `planSha256` ([enforcing](kubernetes.md#enforcing)).
`logweir-retention` is a **separate binary** so that `crates/logweir-reaper`,
the one crate that names an object-store delete, is never linked into the
everyday binary (`scripts/check-no-archive-write.sh` check 3).

#### The retention record is create-only and **unsigned** in this build

D3 §6.5 calls for the enforcement record to be signed. **It is not signed
here**: signing would make `logweir-retention` link `crates/logweir-evidence`,
whose reaching set `scripts/check-one-signer.sh` holds to exactly
`{logweir, e2e}`, and widening that is a decision about the signer, not one a
deletion feature makes on its way past. The record and the per-point
tombstones are written with `PutMode::Create` under `logweir/`, by the
`evidenceWrite` grant the delete-capable credential cannot reach, so they are
tamper-evident against the retention principal; they prove nothing against a
principal holding `s3:PutObject` under `logweir/`, which could pre-empt the
record at its deterministic key. **No surface may describe the record as signed
until a signing path exists**, and `scripts/check-withdrawn-claim.sh` fails the
build on a shipped surface that does.

### Guard G-PITR: upstream's six point-in-time tests contain zero assertions, so Logweir proves the boundary itself

`kafka-backup` 0.21.0 declares six point-in-time tests —
`crates/kafka-backup-core/tests/integration_suite/pitr_accuracy.rs:25,40,54,66,78,86` —
each ignored, with a body that is one `println!`: the file
**contains zero assertions**, at v0.23.3 too. The source-level filter is inclusive,
`r.timestamp >= s && r.timestamp <= e`
(`crates/kafka-backup-core/src/restore/helpers.rs:74-82`), so Logweir's guard
confirms a stated expectation.
`e2e/tests/pitr_boundary.rs::pitr_boundary_includes_the_record_whose_timestamp_equals_point_in_time`
(`just pitr`, compose stack live) holds records at `T − 1 ms`, `T` and
`T + 1 ms` in each of three partitions, `T = 1_760_000_000_000`; the restore
returns **six of the nine records**, the first two from each partition, with
the boundary record's timestamp, partition and `x-original-offset` intact.
Measured on 2026-09-10 with engine **0.21.0** (digest
`sha256:8ff5be71f92a118cde64c082a86d188a4187d8f8f64311458081b8727e99c317`) and
Apache Kafka 3.7.1, and on 2026-10-08 with the 0.23.3 pin (digest
`sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db`):
the same six of nine, accepted by both readers. All three fixture segments
straddle `T`, so the count bound is **[0, 9]** in total and **[1, 3]** per
partition since FX-23: the payload set proves the boundary, and the counts
only bound it.

### A broker on `LogAppendTime` honours a per-topic `CreateTime` override (Task 8, residual 3)

Apache Kafka 3.7.1 (KRaft) whose effective `log.message.timestamp.type` is
**`LogAppendTime`**
**honours a per-topic `message.timestamp.type=CreateTime` override** — measured by execution, with the broker setting applied as a
dynamic config and reverted in the same step. So phase 0's probe can succeed
there (`TargetTopicConfigRefused`'s deterministic arm is
`crates/logweir/tests/topic_preflight.rs::a_logappendtime_broker_that_refuses_the_override_is_a_guard_refusal`),
and G-PITR's fixture keeps its producers' timestamps. MSK stays
`[UNVERIFIED — needs an MSK cluster]`.

## Engine compatibility and support policy

- **Floors (spec §7.2).** `kafka-backup` **0.16.0** is the floor for the
  unknown-config-key warning `OsoCliEngine` parses (`Ignoring unknown config
  key ...`); below it, a dropped rendered key fails silently. **0.21.0** is the
  floor for the full drill, the version every vendored struct and CLI behaviour
  was verified against. An archive written before 0.21 carries no segment
  sha256, so a drill over it reports `integrity.result: partial`, never `pass`.
- **Runtime image floor.** The engine binary links **glibc >= 2.36** and needs
  a CA bundle: `debian:bookworm-slim` with `ca-certificates` and `libssl3`,
  never a musl or distroless base.
- **Supported engine pin.** The images ship **Logweir's build of OSO 0.23.3**,
  `kafka-backup 0.23.3+logweir.3` (PROD-00.2, OD-3; build 2 adds FX-21's patch
  0002, build 3 PROD-00.3g's patch 0003: the restore checkpoint is saved
  inside a topic when its interval is due, and hashed without the run's
  paths): OSO's 2026-10-07 release source (tag commit `afb160e7`) plus
  Logweir's patch folder, for linux/amd64 and linux/arm64
  (`third_party/kafka-backup-build.env`). `doctor` accepts exactly that
  version, as a whole token, and names OSO's own 0.23.3 binary (image digest
  `sha256:cc7d5a8aefa422dadc602d6349624c4563b38478ee6893de5240b98f16a732db`,
  `third_party/kafka-backup-binary.digest`) as the one-release rollback;
  `engine.version` and `engine.digest` in signed documents name whichever ran.
  PROD-00.3f evaluated 0.23.3: the segment format, the three subcommands Logweir
  runs and every key it renders are unchanged
  ([decision record](to-do/decisions/PROD-00-engine-route.md) §12). Moving the
  pin is a recorded decision (OD-3), never a routine bump:
  `crates/logweir/tests/engine_pin.rs` and `engine_build.rs` fail until every
  place that names the build names the new one.
- **Two engine behaviours since 0.22.0 are refused, not inherited:** an
  `http://` storage endpoint beside `allow_http: false` (exit 3, C15), and
  `VirtualHosted` addressing with a custom endpoint, which the engine forces to
  path-style (`AddressingUnsupportedByEngine`, C16).
- **What OSO's operators run.** `strimzi-backup-operator` has defaulted to
  engine v0.22.0 since its v0.3.0 (2026-09-07); its v0.2.22–v0.2.25 default to
  v0.19.1. `kafka-backup-operator` 1.4.0 to 1.4.2 link `kafka-backup-core`
  0.23.0 to 0.23.3; 1.3.0 links 0.19.2. The [support matrix](support-matrix.md)
  says which archives each can feed.

## Kubernetes: what the control plane does not stop, and what it was built against

**O0, default (a): none of this stops a cluster-admin, and that is accepted and
stated rather than implied.** Logweir's Kubernetes surface narrows *who can
change what*, with real mechanisms — each Restore owns an immutable
approval-bundle ConfigMap, the immutable Job template pins the SHA-256 of every
mounted public input, and the runner checks those bytes before constructing a
data-plane client; `--approver-key-ids` pins which approver key ids a run
accepts, refusing anything else with exit 3 before phase 0 dials; and
`subject_kind` is inside the signed bytes so an approval cannot be retargeted
at another kind of object. **None of that is a claim about a cluster-admin.**
Anyone holding `create pods` in the runner's namespace can mount the signing
Secret and sign whatever they like with no Logweir crate involved
(`scripts/check-one-signer.sh`'s header says the same), and anyone who can
create pods or replace the Job template can replace the execution contract
wholesale. The boundary this product draws is between *namespace users* and
*the operator's own trust roster*, not between an adopter and their own cluster
administrator.

**The Kubernetes client pair, and the toolchain it was resolved against.**
`crates/weirkeeper` is built against **`kube 0.99.0`** (`default-features =
false`, features `client`, `runtime`, `derive`, `rustls-tls`) and
**`k8s-openapi 0.24.0`** (feature `v1_29`), resolved and pinned under Rust
**`1.89.0`** (`rust-toolchain.toml`). `kube 0.99`'s own `rust-version` is
`1.81.0`, below the pin, and `k8s-openapi 0.24` is the release that exposes the
`v1_29` feature Global Constraint 25's minimum Kubernetes needs, so the pair
was taken as resolved, with no `cargo update --precise`. The versions are
declared in `crates/weirkeeper/Cargo.toml` and the workspace root's pin-list
comment; this page is where the *reason* lives.

## Deliberately not in tag 1

The original deferred scope is retained below with current status. Most items
remain deferred; the Helm chart and retention deletion are now delivered, and
key generation and rotation are partly delivered, each marked accordingly.
The separate **Never** list records product boundaries, not scheduled work.

### Later, named — original scope with current status

| # | Item | Reason | Citation |
|---|---|---|---|
| 1 | **MSK IAM auth** | The `TokenProvider` seam exists and is empty; nothing mints an IAM token. | `crates/logweir-kafka/src/token.rs:1-9` |
| 2 | **Strimzi as a source** | `strimzi-backup-operator` has defaulted to engine v0.22.0 since its v0.3.0 (2026-09-07), through v0.4.0 (2026-10-06); its v0.2.22–v0.2.25 default to `v0.19.1`, **below the `0.21.0` floor**, which the matrix reports `unsupported (lever-absent)` and never as a fault. The drill always restores with Logweir's pinned engine (0.23.3 since PROD-00.3f), so what matters is the archive. The CLI drill reads 0.21, 0.22 and 0.23 archives in full, and reads older ones as `partial` (no segment sha256). A non-empty consumer-group snapshot fails the drill until FX-1. No operator-written archive carries a Logweir receipt, so none can enter the catalog. | spec §13; `docs/support-matrix.md`; [PROD-00.1](to-do/decisions/PROD-00-engine-route.md) §7 |
| 3 | **The in-browser WASM verifier** | Tag 1's UI ships no build step and no bundler, so there is nothing to compile a verifier into; verification is the CLI and `docs/verify_scorecard.py`. | spec §8 |
| 4 | **`OsoCliEngine::validation_run`** | Not implemented: the engine's own validation run is never executed (v1 removed phase 7's unused call, release-notes item 74), and `engine_subreport` is `null` in every document tag 1 produces. The subcommand is on the allowlist as a ceiling, not as a description. | spec §13; `docs/platform/find-engine.md` |
| 5 | **Retention deletion — delivered, opt-in** | ADR 0008 **Amendment H** took the amendment deletion needed, and the worker exists: a `RetentionPolicy` in `mode: Enforce` runs the separately linked `logweir-retention` binary under its own delete-capable credential, only against an administrator-approved plan digest, and writes create-only (unsigned) tombstones and a record under `logweir/retention/`. `mode: Report` is the default and deletes nothing; a schedule's `spec.retention` is withdrawn and ignored; the controller, `logweir-store`, `logweir` and `logweir-api` link no delete path (`scripts/check-no-archive-write.sh` check 3). Versioned and Object Lock buckets are refused (`VersionedBucket`). | [kubernetes.md](kubernetes.md#retention); [release-notes.md](release-notes.md) |
| 6 | **Byte-faithful production restores** (`strip_offset_headers: true` for `mode: newTopic`) | Gated on phase 7 gaining a **second reconciliation key**: today the injected header is the only key phase 7 has, so stripping it removes the only thing that makes a per-record claim checkable. | spec §6.1, §13 |
| 7 | **Multi-tenancy beyond namespace RBAC** | The isolation tag 1 offers is the API server's own: namespaces and RBAC. There is no tenant object, no per-tenant quota and no cross-namespace policy. | spec §13 |
| 8 | **Delegated rule-based schedule approval** | Every approval in tag 1 is a signed document over exact bytes. A rule that approves on a schedule's behalf is a different trust model and gets its own design. | `design-operator.md:663-670` |
| 9 | **A Helm chart — delivered** | The chart is the install. `just chart-check` verifies rendered manifests; `just helm-demo` exercises a cluster installation. This item is no longer deferred. | [Chart guide](../charts/logweir/README.md) |
| 10 | **KMS / PKCS#11 signing** | `sign_detached` takes a concrete `&SigningKey` with **no trait seam**, so an external signer is a refactor and not a configuration option. | spec §13 |
| 11 | **Key generation and rotation — partly delivered** | The chart's bootstrap Job now generates and retains the installation signing key (PLAT-02.1), and a `TrustPolicy` carries a key lifecycle so a rotation is an overlap rather than a replacement (PLAT-19.1). Still not shipped: an in-place runner cutover to a new signing key (`identity.activeSigningSecretName` and `logweir identity rotate` do not exist), and approver keys remain operator-generated. | [keys.md](keys.md), *The supported procedure* |
| 12 | **A PVC for the runner pod** | The runner streams and writes to an emptyDir; a large restore is bounded by that, and a persistent volume would be a new lifecycle to own. | spec §13 |
| 13 | **Subprocess timeout, cancellation and SIGTERM handling** | The engine subprocess runs to completion. A Job deleted mid-run leaves the child to the kubelet, and nothing in tag 1 propagates a cancel. | spec §13 |
| 14 | **A configurable Kafka client timeout** | It is a **20 s constant**, not a setting. | `crates/logweir-kafka/src/rdkafka_reader.rs:16` |
| 15 | **The kind + Calico NetworkPolicy probe** | The shipped NetworkPolicy is labelled `[UNVERIFIED]` in the manifest itself: docker-desktop runs no CNI that enforces NetworkPolicy, so a deny is never observed there and the probe that would make the claim real stays backlogged. | spec §9, §13 |
| 16 | **Stage-2 Tasks 10 and 17-24** | Carried forward as a block, in the backlog they were written in. | `docs/platform/04-stage2-backlog.md` |

### Never — four entries

These are not scheduled. They are refused.

| # | Entry | Why it is a never, not a later |
|---|---|---|
| 1 | **Restore-in-place into a live topic** | The product's whole safety argument is that a restore writes into topics that did not exist. Writing into a live topic removes the property that makes an unattended restore defensible at all. **Narrowed by the owner's decision [OD-2](to-do/product-expansion.md#owner-decisions) (2026-10-05) to exactly that: a LIVE topic.** A restore under the source's ORIGINAL topic name into a topic that does NOT exist is not this entry: it is PROD-15.1's one original-name path, behind its own approval subject (`originalName`), and it still only writes a topic it created itself, exclusively, after proving the name absent, the target another cluster or auto-creation disabled, and no declarative owner unless the owner path is chosen ([kubernetes.md](kubernetes.md#restoring-under-the-original-topic-names), "Restoring under the original topic names (PROD-15.1)"). A name that exists, in any mode, and an identity mapping in a scratch drill stay refused. |
| 2 | **Confluent Schema Registry / Apicurio / RBAC-MDS / CSFLE** | Each is a separate product surface with its own trust model. Logweir moves records and reconciles bytes; it does not resolve schemas, evaluate a registry's RBAC, or hold field-level encryption keys. |
| 3 | **MSK ZK-to-KRaft migration, and the word "migration"** | Logweir is not a migration tool and the word is avoided on every surface, because a document that says "migration" is a document somebody will act on as though it were one. |
| 4 | **Multi-cluster or fleet views, and the words** | One controller per cluster. There is no fleet object, no cross-cluster list and no aggregated view, and the vocabulary is kept out of the UI and the docs for the same reason as the previous row. |

Historical specification references in this table refer to the planning corpus.
The linked repository guides describe the implementation that ships here.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
