# Release notes

One entry per release candidate, newest first. Each entry says what an operator
must know before running it: the **supported behaviour** that changed, the
**operator actions** an upgrade requires, the **verification scope** behind
each claim, who holds **retention authority**, and **migration and rollback**.
[The release checklist](tag1-checklist.md) row 9 points here, and its *What to
record* list is filled in this file's candidate record, not in a pull request.

A claim with no evidence behind it yet carries an `[UNVERIFIED]` mark and the
sentence that would close it (`scripts/check-unverified-labels.sh` refuses a
mark without one). The supported path these notes assume is
[quickstart.md, *The supported path*](quickstart.md); the measured limits are in
[stability.md, *Measured scale limits*](stability.md#measured-scale-limits-plat-202).

---

## Unreleased — `main` after `v0.1.5`

The last tag is `v0.1.5` (`9cc78a3`). This entry covers `main` through
`306cebf` (2026-09-23): the platform tracker's shipped tasks, the operator
actions collected for PLAT-20.2, and the upgrade from the last published image.
The shipped task list, commits, tested environments and results are in
[release-handoff.md](release-handoff.md).

### Candidate record

Fill every row for the exact candidate before the tag; a row left as `—` is an
unrecorded fact, not a pass. A previous run does not validate new bytes.

| What | Value |
|---|---|
| Candidate commit | — |
| Version tag | — |
| CI run (`ci.yml`) for that commit | — |
| Release run (`release.yml`) and release drill | — |
| Runner image digest (`linux/amd64`) | — |
| Controller image digest (manifest list; amd64 and arm64) | — |
| Console image digest (`logweir-console`) | — |
| UI image digest (`logweir-ui`) | — |
| `ui/` bundle, file by file | the output of the command below |
| Kubernetes exercises run on this candidate (context, auth mode, storage, limits) | — |
| Checks deliberately deferred, each with its reason | — |

**The `ui/` bundle is listed by digest** because the page runs with the
viewer's authority (README, *Threat model*): the bytes a browser executed must
be comparable with the bytes that were released. List exactly the files the
images ship — the selection `scripts/check-image-api.sh` check 1 pins at
twenty-six:

```bash
find ui -type f ! -name '*.md' ! -path 'ui/tests/*' | LC_ALL=C sort | xargs shasum -a 256
```

### Supported behaviour in this release

- **One supported path** for a new installation: the Helm chart with the
  managed installation identity, saved connections and destinations, schedules,
  the product console (`logweir-api`, `localAdmin` or `shared`), restore of a
  chosen point with an approval, verification, and a disaster restore from a
  connected archive with no `Backup` objects at all
  ([quickstart.md](quickstart.md)). The static page behind `kubectl proxy`
  remains a supported legacy view; the destination, discovery, readiness,
  catalog and schedule-policy flows are console-only.
- **A versioned PoC install profile**, [deploy/poc/](../deploy/poc/README.md):
  Traefik, cert-manager with a local CA, Dex and the shared console, all by
  Helm from the published OCI chart and its `sha-` images, with no post-install
  patch: the six chart gaps it first had to stand in for (G1–G6) are chart
  values and behaviours now — the issuer CA bundle, host aliases, the
  connection objects' namespace, the published chart, controller probes and the
  ingress controller trusted by its Service. Installed, signed into, upgraded from
  `v0.1.5` and from `sha-f49849d…`, and rolled back on docker-desktop on
  2026-09-24 ([deploy/poc/](../deploy/poc/README.md), *What the first live round
  showed*); the fixes that round needed are in the profile.
- **The chart is published** as `oci://registry-1.docker.io/vladyslavhaina/logweir-chart`,
  beside the images and versioned with them ([install.md](install.md), *(c) The
  Helm chart*). Published: `0.1.0-sha-86a554e6…` pulls anonymously (digest
  `sha256:90b4d41b…`) and names its own commit's four images.
  **On first publication, `vladyslavhaina/logweir-chart` must be Public in Docker Hub**, or `main` CI's
  chart step fails closed (its anonymous pull-back is refused) until the repository is made Public and
  the job is re-run.
- **Fourteen kinds** on `logweir.dev/v1alpha1`, all additive over `v0.1.5`
  ([install.md](install.md), *Upgrade CRDs before upgrading the controller*).
- **Trust has a lifecycle.** A `TrustPolicy` governs a namespace's keys with
  retirement and revocation; `TrustRoster/default` is the deprecated fallback
  and still works unmigrated ([keys.md](keys.md)).
- **Approval policy is per namespace.** Unbound namespaces keep
  `legacy-governed-v1` (an out-of-band signed approval); a namespace may be bound
  to `Ordinary` (the console's own confirmation) or `Governed` (the console's
  confirmation plus an independent approver) ([kubernetes.md](kubernetes.md)
  §8, *Approval policy*).
- **Retention can delete, and only when an administrator says so** — see
  *Retention authority* below.

### Required operator actions, in upgrade order

Do these before rolling the controller, in this order. Each is also listed
under its item below.

1. **Back up the installation identity** (`logweir-signing-key` and
   `logweir-signing-trust`) and export the trust material
   (`logweir trust export`) — [install.md](install.md), *Back up and recover
   the installation identity*.
2. **Grant every retention delete credential `s3:GetObject` on
   `<bucket>/<prefix>/*`** (item 1).
3. **Move every `RetentionPolicy` on a versioned or Object Lock bucket to
   `mode: Report` or `mode: ExternalLifecycle`** (item 2).
4. **Review alert rules that match `NoExitCode`** (item 4).
5. **Bring a shared-console values file in line** (`controller.watchNamespaces`
   and the proxy CIDR bound) before `helm upgrade`, or the render stops
   (item 6).
6. **Apply the CRDs** (`kubectl apply --server-side --force-conflicts`), **wait
   for all fourteen to be established**, then roll the
   controller **and the runner image together** (item 5), with no Backup in
   flight (item 11) and no Restore running (item 12), then the console,
   then any approval-policy binding (item 7). On the Helm path these are one
   release and successive `helm upgrade`s: first the image values (controller,
   runner and console move together), then the `approvalPolicy.*` values.

### The fifteen operator-facing changes

Each item names what changed, what to do, what the claim rests on (its
verification scope), and how to roll it back. Every one of them was collected
for PLAT-20.2 from a merged change; the defect names are the platform
tracker's.

#### 1. Retention needs `s3:GetObject` — required action

**Changed.** The retention worker HEADs every key before deleting it, to refuse
a versioned bucket where a delete by key would only write a delete marker
(OBJECT-LOCK-DELETE-MARKER). **Do:** grant the retention delete credential
`s3:GetObject` on `<bucket>/<prefix>/*` **before** upgrading. Without it the
enforcer deletes nothing and records every point `Kept` with
`VersionProbeRefused`. A policy degraded for that reason re-probes 24 h after
its last run, or at once on any spec edit. **Scope:** unit and controller tests
on `main` `b57753b`; the grant is documented in
[install.md](install.md) §3.11 and [kubernetes.md](kubernetes.md) §7a. [UNVERIFIED — the grant set with s3:GetObject is re-measured by U6/retention-enforcer at lab-refresh-9.]
**Rollback:** the extra grant is harmless to an older worker.

#### 2. `Enforce` refuses versioned and Object Lock buckets

**Changed.** `Enforce` on a versioned or Object Lock bucket now deletes nothing
and degrades with `VersionedBucket`. **Records written by earlier builds on such
buckets that say `Deleted` are false**: the data is still there as noncurrent
versions behind delete markers. **Do:** use an unversioned bucket or
`mode: ExternalLifecycle` (a provider rule such as
`NoncurrentVersionExpiration`). Do not change a bucket's versioning while a
retention run is in flight, and do not enforce on a bucket whose versioning was
ever enabled and later suspended — there a deletion removes only the newest
copy while being recorded `Deleted` (accepted residual
RET-VERSIONED-SUSPENDED-REWRITE; `Deleted` means the current object at each key
was removed). **Scope:** found live by harness-rows-11 on the lab MinIO; fixed
on `main` `b57753b` with two review rounds. [UNVERIFIED — the flipped object-lock row runs at lab-refresh-9 and must show nothing deleted.]
**Rollback:** set such policies to `mode: Report` **before** rolling back; the
older worker records delete markers as deletions.

#### 3. Re-run receipts over one backup set are protected together

**Changed.** Two receipts can name one backup set (a runner Job re-created from
its frozen inputs signs a second receipt). The older one is now protected as
`SharedSegment` until every receipt naming the set is due, and then the set is
removed by one plan line naming the others in `co_point_ids`
(SHARED-SET-RETENTION). **Do:** re-approve any plan digest approved over such a
plan — it changed. Plans with no shared set are byte-identical and their
approvals hold. A shared set with more receipts than `maxDeletionsPerRun` is
never selected; raise the ceiling to let it go. **Scope:** found live by
harness-rows-11 (it was data loss); fixed on `main` `b57753b`, Tier-A review
with two fix rounds. [UNVERIFIED — the shared-set row runs at lab-refresh-9 against the rebuilt lab.]
**Rollback:** set every `RetentionPolicy` to `mode: Report` **before** rolling
back: an older controller plans without this protection again and could plan
a set a retained receipt still names (a deletion would still need a fresh
`approvedPlanSha256`). An older worker refuses a plan carrying `co_point_ids`
(exit 3) and deletes nothing.

#### 4. Runs that end without an exit code are named

**Changed.** A run whose pod never started may now end `Failed/VolumeMountFailed`,
`Failed/PodUnschedulable` or `Failed/RunnerImageUnavailable` instead of
`NoExitCode`, when the matching diagnostic was still being observed as the Job
ended (WARNING-DIAGNOSTICS-NOEXITCODE; [kubernetes.md](kubernetes.md) §10,
*`RunnerReady`, and the four states a run can reach with no exit code*).
`exitCode` stays absent. **Do:** review alert rules that match `NoExitCode`.
**Scope:** controller tests on `main` `b57753b` and `c892650` (a crash-looping
runner's start is read from `lastState`). [UNVERIFIED — the operation-states rows run at lab-refresh-9.]
**Rollback:** an older controller writes `NoExitCode` again; nothing stored
needs converting.

#### 5. Disaster restore upgrades the controller and runner together (PLAT-15.2)

**Changed.** A restore bound to a catalog point verifies the point's receipt
signature in the runner before any data moves, against an evidence keyring the
controller renders into the approval bundle. **Do:** upgrade the controller and
runner images **together**. A standalone `logweir restore run` of a point-bound
plan now needs `--evidence-keys`. A point-bound `Restore` whose bundle an older
controller created ends `ApprovalBundleConflict`: delete it and create it again.
**Scope:** PLAT-15.2 is on `main` since `ac00819` with its own worker journey;
its Done record waits for lab-refresh-9 ([kubernetes.md](kubernetes.md)
§7d.1). [UNVERIFIED — the disaster-restore journey on a rebuilt lab is owed by lab-refresh-9.]
**Rollback:** an older runner refuses `--evidence-keys` and exits 1 before any
work; a runner at this version refuses a point-bound plan from an older
controller (`PointUntrusted`). Archives and catalog records are untouched.

#### 6. Shared console values must name the controller's namespaces (PLAT-17.2)

**Changed.** A values file with `api.console.mode: shared` stops rendering until
`controller.watchNamespaces` is set, excludes the release namespace, and lists
every `roles.bindings` namespace; `ui.enabled` must be off beside it; and
`trustedProxyCidrs` wider than `/16` (IPv4) or `/48` (IPv6) is refused when
`requireTrustedProxy` is on. The refused `helm upgrade` changes nothing in the
cluster. **Do:** follow [install.md](install.md) §5e's migration list, then
upgrade once. **Scope:** chart lint and render tests; PLAT-17.2 is on `main`
since `ac00819`. Shared mode ran behind a TLS ingress (Traefik, cert-manager) and
an OIDC provider (Dex) on docker-desktop on 2026-09-24 — the PoC profile: sign-in
per role, the role matrix, CSRF, forged headers, unauthenticated API and stream
requests, the trusted-entry `421` and the console journey held. Dex with static
users is the provider that ran; a corporate IdP bound by group has not.
**Rollback:** reinstall the previous chart version with the previous values;
the scoping objects go and the cluster-wide binding returns.

#### 7. Approval policy: Ordinary and Governed (PLAT-19.2)

**Changed.** `Ordinary` confirmation requires `allowOrdinaryConfirmation: true`
plus an explicit namespace binding, and is refused by the `localAdmin` console;
`Governed` requires a change ticket. The policy reference a document signs is
`{name, digest}`, so any edit to a policy is a different policy (D0 amendment).
**Do:** keys first (a `ConsoleConfirmation` key and, for `Governed`, each
approver's `GovernedApproval` key with `principal.id`), then the binding
([install.md](install.md) §5f). A `Restore` submitted during a policy rollout
may need resubmitting once the rollout completes. **Scope:** PLAT-19.2 is on
`main` since `ac00819`; controller, runner and API tests; Done record waits for
lab-refresh-9. [UNVERIFIED — the Ordinary and Governed journeys on a rebuilt lab are owed by lab-refresh-9.]
**Rollback:** unbinding returns the namespace to `legacy-governed-v1`;
not-yet-admitted v2 approvals are then refused, never admitted as v1. An older
controller refuses every v2 document as `PayloadTypeMismatch`.

#### 8. Restore completion is written only from a valid scorecard

**Changed.** `Restore.status.completion` appears only once the scorecard's
verification is `Valid` (RESTORE-COMPLETION-UNWRITTEN). Its `recordsRestored`
is the count read back from the target **in the sampled window** —
`sample.records_restored`, which the console labels *records verified in the
sampled window* — never the total the restore wrote. **Do:** nothing; read
absence as "not yet verified", never as zero. **Scope:** `main` `fa3384e`.
Seen live on 2026-09-24 (PoC install): restores of pre-upgrade points after both
upgrade rehearsals and the console journey's restore wrote `completion` from a `Valid`
scorecard (150 of 150 sampled records matching), and a restore whose scorecard the
controller could not read wrote none.
**Rollback:** an older controller writes no `completion` at all.

#### 9. A schedule never fires a slot due before it was created

**Changed.** A slot whose due time is before the schedule's
`metadata.creationTimestamp` never fires, for `BackupSchedule` and
`RehearsalSchedule` alike (D1 §4.7 row 5a; SCHEDULE-FIRES-SLOT-BEFORE-CREATION).
Use *Run first backup now* for an immediate first run. **Do:** nothing;
deleting and recreating a schedule (a GitOps prune and re-apply) resets the
bound. **Scope:** `main` `fa3384e`, both kinds. [UNVERIFIED — the pre-creation slot row is owed by lab-refresh-9.]
**Rollback:** an older controller may fire that slot once.

#### 10. The API's `trust.state` never calls a revoked-key observation green

**Changed.** `trust.state` for `result: Valid` beside
`trust.basis: RecordedBeforeRevocation`, `None`, an absent basis inside a
`trust` block, or a basis word the server does not know is now `untrusted`,
where it read `verified`; `Valid` beside `Unverified` (nothing compared yet) is
`notAttempted` (TRUST-STATE-RBR-VERIFIED). `Current` stays `verified` and
`Historical` stays `verifiedHistorical`. **Do:** a client that must also read older servers
checks `trust.basis` as well ([api.md](api.md)). **Scope:** `main` `178cc1c`,
Tier-A review; the console reads the same word in both modes. **Rollback:** an
older API server says `verified` again for that pairing.

#### 11. One backup id is one engine run; the evidence store must enforce conditional create

**Changed.** A Backup Job lost and re-created from its frozen inputs used to run
the engine again under the same execution id, rewrite the manifest the first
run's signed receipt attests, and leave that receipt describing an archive that
is no longer there (RECEIPT-DUP). `logweir backup run` now claims its execution
with a create-only `logweir/backups/<backupId>/execution.claim.json` before the
engine starts. A second run of one execution exits 1 with `status.exitReason:
ExecutionAlreadyClaimed` and writes nothing; a schedule retries it under a new
execution id only when it has `spec.retry`. An evidence store that does not
enforce `If-None-Match: *` makes every backup exit 4 `ExecutionClaimUnproven`
before any data is written, and a destination with `writeProbe` on reports it
`notReady / ConditionalCreateUnsupported` first. No permission is added, and no
signed format changes. **Do:** confirm the evidence store honours conditional
create — turn on `writeProbe: CreateOnlyMarker` for one run of the destination
check, and never set `AWS_CONDITIONAL_PUT=disabled` — see the store table in
[support-matrix.md](support-matrix.md). A standalone `logweir backup run` that
reused a fixed `backup_id` must pass a fresh `--backup-id-override` per run.
**Let in-flight Backups finish before upgrading:** an execution whose first run
was made by the older runner has no claim, so if its Job is lost and re-created
after the upgrade the new runner runs the engine again and can still invalidate
that first receipt — the one window the claim cannot close.
**Scope:** in-process rows, a private MinIO `RELEASE.2025-09-07T16-13-09Z`
container, and four planted mutants plus the review's two; the live case-e row
is owed. [UNVERIFIED — the case-e re-creation row runs at lab-refresh-10.]
**Rollback:** an older runner ignores the claims and returns to re-running the
engine over a re-created Job; the claims stay in the bucket, harmless, and are
honoured again after a re-upgrade.

#### 12. A failed restore's signed scorecard: roll the controller out before the runner

**Changed.** A restore runner at this version names its signed failure at exit 2
(the scorecard of a run whose data did not reconcile is published and verified
`Valid`), and this controller writes `Restore.status.completion` — the console's
completion panel with its cutover guidance — only for a run that PASSED
(`exitCode 0`, `outcome: pass`, a green verdict). An **older controller** gates
completion on the verdict alone, so it would write a completion panel over a
restore that FAILED. **Do:** roll the controller out before (or with) the
runner, never the runner first, and roll the runner back before the controller;
let running `Restore`s finish before either. On the Helm path both move in one
`helm upgrade`, which is safe once nothing is running. **Scope:**
`claude/rehearsal-fix` (`6b2704d`, Tier-A review), [stability.md](stability.md),
*Mixed versions*. **Rollback:** runner first, then controller; nothing stored
needs converting.

#### 13. Readiness rows are answered by the principal they name; an older runner says "upgrade"

**Changed.** A destination readiness check (`DestinationAccess`, and the
destination rows of a Backup/Restore check) now answers `evidenceWritable` with
the `evidenceWrite` grant and `evidenceReadable` with the `evidenceRead` grant
when they differ from the checked one — the row's sentence is about the right
principal (PREFLIGHT-EVIDENCEWRITABLE-WRONG-PRINCIPAL) — and a `DestinationAccess`
check on a `writeProbe: CreateOnlyMarker` destination now writes its one
create-only marker under `logweir/readiness/` (DESTINATIONACCESS-IGNORES-WRITEPROBE).
**Do:** upgrade the runner image **with** the controller: an older runner handed
such a plan refuses it at startup — the `Preflight` ends `Failed` /
`CheckContractMismatch` with a message saying the runner is older than the
controller — and nothing is written as the wrong principal. Set
`writeProbe: Disabled` on a destination that must never receive the marker.
**Scope:** `claude/readiness-principal` (merged `540e3ea`), closed live at
lab-refresh-10 (rows RP-L1…L15); [kubernetes.md](kubernetes.md) §21,
*The evidence-write grant in a check plan (mixed versions)*. **Rollback:** an
older controller renders the old plans again, which a newer runner still
accepts (the old wrong-principal answer returns until you re-upgrade).
#### 14. A recovery point with no saved destination is checked, verified and completed

**Changed.** Three behaviours for a point written without a `BackupDestination`
— every point `v0.1.5` wrote — found by the PoC upgrade round (P3, P5, P6):
**readiness** — a restore readiness check over such a point
(`legacySourceArchive`) reads the archive with the restore Job's own principal
(the Backup's Secret, keys `access-key-id` / `secret-access-key`) at the
approved plan's location, instead of ending `Failed/ArchiveUrlUnreadable` and
leaving the wizard's Create disabled; no `secretRef`, a non-`s3://` archive or
an unreachable plan location is `notReady` and named. **Evidence** — the
console writes such a restore's evidence to the archive's own bucket (it wrote
`logweir-evidence`), and the controller reads an inline-archive run's evidence
only in the bucket of its archive handle (`LOGWEIR_ARCHIVE_URL`): a run whose
evidence is elsewhere is `NotAttempted` naming its own evidence bucket (the
handle is named by role; its URL is only in the controller log) and is never
read in the wrong one, and an inline-archive scorecard the handle read nothing
for is `NotAttempted` naming the key — it used to publish no verification and
no completion at all. A rehearsal over such a point records
`VerificationNotAttempted` at once instead of `EvidenceVerdictNotReached` after
five minutes; destination-backed runs are unchanged. The readiness verdict is
bound to the archive Secret: editing it after a green check refuses the Create,
and a check of an existing `Restore` must name that Restore's own archive and
Secret.
**Catalog** — a `Full` or `Index` sync reads catalog records only, so a
pre-catalog point is not in a connected catalog until its record is backfilled
([kubernetes.md](kubernetes.md) §7d, §15.1a, §21.8). **Do:** if your legacy
schedules write to a bucket other than `LOGWEIR_ARCHIVE_URL`'s (chart
`archive.url`; with the bundled MinIO `s3://kafka-backups/<release>`), their
runs now read `NotAttempted` rather than a misleading store error — verify them
with the printed commands or move them to a `BackupDestination`; write a
hand-written legacy restore plan's `evidence:` to the handle's bucket; run
`logweir catalog sync` once per archive to list `v0.1.5` points in a catalog
(D3 designs `Full` as a read-only receipt walk that would make this unnecessary;
this build's `Full` reads records only, a gap tracked for PLAT-15.1);
re-run any readiness check made before the upgrade. **Scope:** in-process rows
over the preflight, restore and backup controllers and the console suite.
[UNVERIFIED — the v0.1.5 point's readiness, verdict and completion are re-proved live by the PoC re-proof round after the upgrade.]
**Rollback:** an older controller answers a legacy restore readiness check
`Failed/ArchiveUrlUnreadable` again and reads a legacy restore's evidence in the
handle's bucket whatever the plan names; a plan the new console rendered still
verifies under it when the archive's bucket is the handle's.

#### 15. Manual runs may queue; "Back up now" is rate limited (P10)

**Changed.** Nothing used to bound manual runs: on the PoC install one
operator's hundred accepted `POST …/backups` (all `201` within 2.4 s) became a
hundred simultaneous runner pods, the node hit its 110-pod limit and went
`NotReady`, and MinIO answered `503 SlowDown`. Now:

- **The controller bounds manual runs per namespace.** At most
  `runs.maxManualBackupsActivePerNamespace` (default `4`) manual `Backup`s and
  `runs.maxManualRestoresActivePerNamespace` (default `2`) admitted manual
  `Restore`s hold a runner slot at once in one namespace. The rest wait with
  `phase: Queued`, `Admitted=False / ConcurrencyLimited` and
  `status.queue.limit`, **with nothing created** — no plan, no Job, no
  execution claim — and start in creation order as slots free (within one
  requeue, 15 s). The console shows "Queued (limit N active)". Scheduled,
  catch-up and retry `Backup`s and a `RehearsalSchedule`'s `Restore`s are
  neither counted nor queued; `concurrencyPolicy` is unchanged. A queued
  `Restore` re-checks its approval when it leaves the queue, so an approval
  policy with a maximum age can expire it while it waits
  (`AuthorizationExpired`); re-confirm and create a new one.
- **The console limits how fast one person can start runs:** `10`
  "Back up now" and `5` manual restores per person, per namespace, per minute
  (`api.console.rateLimits.*`), then `429 rate_limited` with `Retry-After`. A
  malformed request does not spend the window; a replayed idempotency key does.

**Do:** nothing is required. An automation that starts more than the limits
above must pace itself, or read `429` and `Retry-After`; one that expects a
manual run to be `Running` right after `201` must also accept `Queued`. Raise
`runs.*` for a namespace whose nodes can carry more runner pods at once.
**Scope:** pure and route-table rows (`crates/weirkeeper/tests/manual_run_pool.rs`,
`restore_controller.rs`, `crates/logweir-api/tests/manual_run_limits.rs`) and
four planted mutants, each killed. [UNVERIFIED — the live row (twenty manual runs at once on the PoC install, at most four runner pods) runs at the next PoC re-proof.]
**Rollback:** an older controller has no pool; it reads `Queued` as an active
phase and runs every queued Backup at once, exactly as before. **Roll the
controller back together with the chart** (`helm rollback`): an older
controller refuses a `weirkeeper-policy` ConfigMap that carries the new `runs`
block and fails closed (no attestations, no evidence allowlist), and an older
console refuses a configuration file that carries `rateLimits` and does not
start. An older console reading a queued run shows it as `unknown`
(`UnrecognizedPhase`) until the console is upgraded too.

### Verification scope: what "verified" means in this release

- **A green badge** means the signed document's signature verified under a key
  the namespace's trust accepts **and** the run's own success field
  (`exitCode == 0` for a Backup, `outcome == pass` and a zero `exitCode` for a Restore; a failed
  restore's signed scorecard is published and verified, and is never green and never gets the
  completion panel — upgrade the controller before the runner). `Historical`
  (signed before the key was retired) is a pass; `RecordedBeforeRevocation` never
  is ([kubernetes.md](kubernetes.md) §15.2–15.2a).
- **Record checks are samples.** `verificationScope` is `sampled`, `degraded` or
  `none`, never `complete`; no level in this version compares every record
  (`recordsSampled`, `recordsSampledMatching` and the window beside them are the
  exact claim).
- **The independent verifier** (`docs/verify_scorecard.py`) checks the same
  signed documents with no Logweir code ([verify-a-scorecard.md](verify-a-scorecard.md)).
- **Tested environments** are named in [release-handoff.md](release-handoff.md):
  docker-desktop Kubernetes with a SCRAM (and private-CA TLS) Kafka and MinIO,
  and the GitHub Actions Compose suite. Nothing here was run against AWS S3,
  MSK, EKS or a NetworkPolicy-enforcing CNI.

### Retention authority

- **A schedule's `spec.retention` only reports.** It prints the commands an
  operator would run and deletes nothing.
- **A `RetentionPolicy` is created in `mode: Report`** and deletes nothing.
  Only `logweir-retention-admin` — namespaced, and bound to somebody who does not
  hold `logweir-operator` — may move it to `Enforce`.
- **`Enforce` deletes only when all four gates hold**: the mode, an
  administrator's `approvedPlanSha256` equal to the current plan digest and
  younger than `planMaxAgeSeconds`, a lease written before a cluster-wide
  `Restore` check, and a worker that re-validates every key against the
  policy's prefix. The worker runs under its own delete-capable credential,
  never under `logweir/`, and writes create-only tombstones and a record
  (unsigned in this build) under `logweir/retention/`
  ([kubernetes.md](kubernetes.md) §7f).
- **`ExternalLifecycle` is a declaration**: the bucket's rule deletes, the bucket
  wins, and Logweir verifies nothing about it.
- **A `Restore` created after a run's cluster-wide check and before its first
  delete is not held by the lease** in this build; suspend enforcement
  (`mode: Report`) around a large restore.

### Migration and rollback

**Upgrade order:** identity backup → retention grants and modes (items 1–2) →
CRDs (all fourteen established) → controller **and** runner image together →
console image → approval-policy binding. Every CRD change is additive; nothing
is converted and no stored object is rewritten
([install.md](install.md), *Upgrade CRDs before upgrading the controller*).

**Before a rollback**, in this order:

1. Set every `RetentionPolicy` to `mode: Report`, wait for `status.lease` to
   clear and any enforcement Job to finish (item 2; [kubernetes.md](kubernetes.md)
   §7f, *Upgrade and rollback*).
2. Delete `Preflight` objects with `operation: SourceConnection` — an older
   controller cannot decode them and stops reconciling every `Preflight`
   ([kubernetes.md](kubernetes.md) §21.0).
3. If rolling back past Amendment G, delete every `Approval` whose
   `spec.subjectRef.kind` is `RehearsalSchedule` first ([kubernetes.md](kubernetes.md)
   §12, *The one widening that is NOT rollback-safe*).
4. Let destination-backed and `v2`-frozen `Backup`s finish; an older controller
   refuses them terminally rather than running them ([kubernetes.md](kubernetes.md)
   §10, *Backups created under the previous execution contract*).
5. Let point-bound `Restore`s that have no Job yet reach one, or recreate them
   after the rollback: an older controller re-renders their approval bundle
   without the evidence keyring and ends them `ApprovalBundleConflict` (item 5;
   fail-closed, nothing is restored).
6. Unbind approval policies, or expect not-yet-admitted v2 approvals to be
   refused (item 7).
7. Roll the controller and runner back together (the runner not after the
   controller: item 12), with no `Restore` running, and leave the CRDs in place.
8. **Rolling back to a chart that did not render an object this one adopted
   deletes it.** `helm rollback` to `v0.1.5` removes the runner ServiceAccount
   and the `logweir-s3` Secret the upgrade adopted in each runner namespace
   (they were hand-provisioned at `v0.1.5`); re-create them before the next run,
   or every run fails `serviceaccount "logweir-runner" not found` (found by the
   PoC install's upgrade rehearsal R1, 2026-09-24).

**How this upgrade is rehearsed.** From `v0.1.5` (the last version tag: 6 →
14 CRDs, the managed identity adopting a hand-provisioned signer, the console
arriving) and from `sha-f49849d…` (the last build before `ac00819`, which
crosses all ten items above); [release-handoff.md](release-handoff.md) names
the images, the state each rehearsal sets up first, and which items it
exercises. An upgrade from `sha-7b0277b…` crosses items 1–4 only.

**The chart and the images move together.** This chart's controller probes run
`weirkeeper --probe`, and its console configuration can carry
`oidc.caBundleFile` and `trustedProxyService`: a controller image older than the
chart fails its liveness probe (and is restarted), and an older console refuses
the configuration (exit 2, `unknown field`). The published chart names its own
commit's images, and `helm rollback` restores the previous chart and images
together; pin all four images to one build whenever you override them.

Archives, evidence and catalog records are untouched in both directions; old
signed archives keep verifying as long as their public keys stay in the trust
policy or roster ([keys.md](keys.md)).

### Limitations and open items

- **The live half of PLAT-20.2 ran on 2026-09-24** with the PoC profile on
  docker-desktop: a clean install, upgrades from `v0.1.5` and from
  `sha-f49849d…` (and a rollback to each) that kept installation identities,
  schedules and archive readability, a restore of a pre-upgrade point after
  each, and 1,000+ points in one archive. [UNVERIFIED — R2's pre-upgrade retention, mount-failure and point-bound-restore states were not set up.]
- **The product API's OpenAPI document is still `1.0.0-alpha.1`**, although the
  console image and the chart now consume it; ship and upgrade the console and
  the API together until the owner freezes it ([stability.md](stability.md)).
- **Scale limits** are measured offline and recorded in
  [stability.md](stability.md#measured-scale-limits-plat-202); the console
  refuses a list longer than 5,000 rows rather than showing a prefix.
- **No in-place runner signing-key cutover** ([keys.md](keys.md), step 2 of
  *The supported procedure*).
- **Restore admission does not hold on a retention lease** (above).
- The standing limitations of [stability.md](stability.md), *Known limitations*,
  and the `[UNVERIFIED]` marks it carries (AWS S3 create-only puts, MSK, the
  NetworkPolicy, the 1.30+ admission policy) are unchanged.

---

Documentation is licensed [CC-BY-4.0](LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
