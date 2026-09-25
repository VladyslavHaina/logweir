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
`815249cb` (2026-09-25): the platform tracker's shipped tasks, the operator
actions collected for PLAT-20.2 and after it, and the upgrade from the last
published image. No tag is cut at `815249cb`, so the candidate record below
stays empty. The shipped task list, the five publications the PoC ran, the
tested environments and the results are in
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
  ingress controller trusted by its Service. Installed at `86a554e6`, signed
  into, upgraded from `v0.1.5` and from `sha-f49849d…`, and rolled back on
  docker-desktop on 2026-09-24 ([deploy/poc/](../deploy/poc/README.md), *What
  the first live round showed*). The running install was then upgraded in place
  four times by the profile's *Upgrade to a newer publication*, the last to
  `815249cb` on 2026-09-25 ([release-handoff.md](release-handoff.md)). The
  fixes those rounds needed are in the profile.
- **The chart is published** as `oci://registry-1.docker.io/vladyslavhaina/logweir-chart`,
  beside the images and versioned with them ([install.md](install.md), *(c) The
  Helm chart*). The first publication the PoC ran, `0.1.0-sha-86a554e6…`
  (digest `sha256:90b4d41b…`), and the last, `0.1.0-sha-815249cb…` (digest
  `sha256:820622f2…`, CI run 36152835598), pull anonymously and name their own
  commit's four images.
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

**Pre-upgrade check: which compromise revocations become installation-wide**
(item 16). This build applies every `KeyCompromise` revocation a `TrustPolicy`
records to every namespace, not only the ones that policy governs, and holds that
policy on deletion while anything else lists the key. Before deploying it, list
the records:

```bash
kubectl --context <ctx> get trustpolicies -o json \
  | jq -r '.items[] | .metadata.name as $p | .spec.keys[]
      | select(.state == "Revoked" and .revocationReason == "KeyCompromise")
      | "\($p)\t\(.keyId)"'
```

Each line is a policy and a key id that will read as compromised in EVERY
namespace after the upgrade. No output means nothing changes. For each line,
confirm that the key really is compromised for the whole installation. A key
revoked this way only to test a namespace's trust (a shared installation signer
revoked in a test policy, for example) turns every document it signed, in every
namespace, `Untrusted`. It also keeps the policy from being deleted while
`TrustRoster/default` or another policy still lists the key. Compare with the
roster's key ids:

```bash
kubectl --context <ctx> get trustroster default -o json \
  | jq -r '.spec.signingKeys[].keyId, .spec.approverKeys[].keyId'
```

A key that must not be compromised everywhere cannot be un-revoked (G3, and G9
in this build). Either delete that policy before the upgrade, or re-issue the key.
Do not deploy until every listed record is one you mean installation-wide.

### The twenty operator-facing changes

Each item names what changed, what to do, what the claim rests on (its
verification scope), and how to roll it back. Every one of them was collected
for PLAT-20.2 from a merged change; the defect names are the platform
tracker's. Items 17–20 were found by the PoC rounds and landed after its first
publication (`86a554e6`); each was proven on the running install by the
in-place upgrade that carried it.

#### 1. Retention needs `s3:GetObject` — required action

**Changed.** The retention worker HEADs every key before deleting it, to refuse
a versioned bucket where a delete by key would only write a delete marker
(OBJECT-LOCK-DELETE-MARKER). **Do:** grant the retention delete credential
`s3:GetObject` on `<bucket>/<prefix>/*` **before** upgrading. Without it the
enforcer deletes nothing and records every point `Kept` with
`VersionProbeRefused`. A policy degraded for that reason re-probes 24 h after
its last run, or at once on any spec edit. **Scope:** unit and controller tests
on `main` `b57753b`; the grant is documented in
[install.md](install.md) §3.11 and [kubernetes.md](kubernetes.md) §7a. Live on
lab-refresh-9 (`306cebf`, 2026-09-23): the enforcer's grant set measured 9/9
(U6), and `VersionProbeRefused` without `s3:GetObject` (PLAT-16.2's completion
record).
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
on `main` `b57753b` with two review rounds. Live on lab-refresh-9 (`306cebf`):
on a versioned or Object Lock bucket `Enforce` deleted nothing, named every
point `VersionedBucket` and wrote 0 delete markers, including a
plain-then-versioned arm (PLAT-16.2's completion record).
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
with two fix rounds. Live on lab-refresh-9 (`306cebf`): two receipts over one
set were both kept `SharedSegment`, with no plan line (PLAT-16.2's completion
record).
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
runner's start is read from `lastState`). Live on lab-refresh-9 (`306cebf`,
2026-09-23): two mount failures ended `VolumeMountFailed` and an unschedulable
pod `PodUnschedulable` (PLAT-14.1's completion record).
**Rollback:** an older controller writes `NoExitCode` again; nothing stored
needs converting.

#### 5. Disaster restore upgrades the controller and runner together (PLAT-15.2)

**Changed.** A restore bound to a catalog point verifies the point's receipt
signature in the runner before any data moves, against an evidence keyring the
controller renders into the approval bundle. **Do:** upgrade the controller and
runner images **together**. A standalone `logweir restore run` of a point-bound
plan now needs `--evidence-keys`. A point-bound `Restore` whose bundle an older
controller created ends `ApprovalBundleConflict`: delete it and create it again.
**Scope:** PLAT-15.2 is on `main` since `ac00819` ([kubernetes.md](kubernetes.md)
§7d.1), Done 2026-09-23 on lab-refresh-9 (`306cebf`): a restore after the loss
of every custom resource (0 CRs, 100 records restored), and an untrusted signer
refused by the Preflight (`CatalogPointSignerUntrusted`) and by the runner
(exit 3, `PointUntrusted`). On the PoC (2026-09-24, `86a554e6`) a
catalog-verified point restored `Valid` 150/150 through the console.
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
`main` since `ac00819`; controller, runner and API tests. Done 2026-09-23 on
lab-refresh-9 (`306cebf`): Ordinary admitted with the frozen policy and the
console key, Governed needing both signatures, self-approval refused `403`, an
unbound namespace keeping `legacy-governed-v1`. On the PoC: a Governed restore
approved by a second person (2026-09-24), and Ordinary restores in every round.
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
bound. **Scope:** `main` `fa3384e`, both kinds. Live on lab-refresh-9
(`306cebf`, SCHEDULE-FIRES-SLOT-BEFORE-CREATION), and on the PoC at
`86a554e6` (2026-09-25): a schedule created at 00:01:48Z did not fire its 00:00
slot. `v0.1.5` and `sha-f49849d…` each fired such a slot in the rehearsals.
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
container, and four planted mutants plus the review's two. Live on
lab-refresh-10 (2026-09-24): PLAT-06.1's case e (a lost Job re-created), both
arms, and the receipt-dup rows 2–5 (RECEIPT-DUP). The upgrade window above
stays open (RECEIPT-DUP-UPGRADE-WINDOW).
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
*Mixed versions*. Live on lab-refresh-10 (2026-09-24): the failed Restore got no
`status.completion`, and the passing one's panel was present
(CONSOLE-COMPLETION-ON-FAILED-RESTORE). **Rollback:** runner first, then
controller; nothing stored needs converting.

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
**Live** (PoC upgrade round, 2026-09-25, `claude/poc-upgrade-1`): on the PoC upgraded to `sha-02dc44b6…`, a point of a `v0.1.5`-shaped inline-archive schedule (`s3://kafka-backups/poc`, Secret `logweir-s3`) passed its readiness check with the restore Job's own principal, restored `Valid` with a 150/150 completion, refused a changed archive Secret, and a plan naming `logweir-evidence` read `NotAttempted` with the handle named by role only. The controller's handle needs its read credential (`logweir-evidence-ro`, [install.md](install.md) §3): without it every inline-archive run reads `NotAttempted` and is not offered as a recovery point until the controller reads it again ([kubernetes.md](kubernetes.md) §15.1b: at +1, +5 and +15 minutes, and once per controller process). [UNVERIFIED — a point written by the v0.1.5 runner itself was not available on the upgraded install to run this path.]
**Live** (`claude/poc-upgrade-2`, the upgrade to `sha-b748fd5f…`): the three inline-archive points that round 1 left `NotAttempted` verified `Valid`, with their covered window, 3 s after the new controller started, and one of them restored `Valid` with a 150/150 completion through the console.
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
  `Restore`s hold a runner slot at once in one namespace (one installation
  value each, applied in every namespace). The rest wait with
  `phase: Queued`, `Admitted=False / ConcurrencyLimited` and
  `status.queue.limit`, **with nothing created** — no plan, no Job, no
  execution claim — and start in arrival order as slots free (within one
  requeue, 15 s). Each admission is reserved in the controller and recorded on
  the run (`Admitted=True`) before anything is created, so runs released
  together — restores whose approvals verify at once, runs held on a
  destination, everything after a controller restart — never pass the
  ceiling. The console shows "Queued (limit N active)". Scheduled, catch-up
  and retry `Backup`s and a `RehearsalSchedule`'s `Restore`s are neither
  counted nor queued; `concurrencyPolicy` is unchanged.
- **A queued restore keeps its approval's deadline.** The queue does not
  extend an approval's maximum age: the deadline is on the object
  (`status.queue.authorizationExpiresAt`, "approval expires T" in the
  console), and a restore still queued when it passes is refused
  `AuthorizationExpired` "while queued behind N"; confirm again and create a
  new one.
- **The console limits how fast one person can start runs:** `10`
  "Back up now" and `5` manual restores per person (`issuer#subject`), per
  namespace, per minute (`api.console.rateLimits.*`), then `429 rate_limited`
  with `Retry-After`. A malformed request does not spend the window; a
  replayed idempotency key does.
- **Known limit:** a subject allowed to create `Backup` objects directly can
  declare a scheduled kind for an existing schedule and escape both the pool
  and `concurrencyPolicy`; RBAC on `create backups` governs that path.

**Do:** nothing is required. An automation that starts more than the limits
above must pace itself, or read `429` and `Retry-After`; one that expects a
manual run to be `Running` right after `201` must also accept `Queued`. Raise
`runs.*` where every namespace's nodes can carry more runner pods at once.
**Scope:** pure and route-table rows (`crates/weirkeeper/tests/manual_run_pool.rs`,
`restore_controller.rs`, `restore_policy.rs`,
`crates/logweir-api/tests/manual_run_limits.rs`), chart rows, and twelve
planted mutants (eight first round, four in the review round, including the
reviewer's two survivors), each killed. **Live** (PoC upgrade round, 2026-09-25, `claude/poc-upgrade-1`): twenty manual runs from two people at once held at most four runner pods, the rest `Queued` with nothing created, through a controller restart, while a scheduled slot started at once; three approved restores at once ran two and queued one with its approval deadline shown. The per-person `429` came at the 21st "Back up now" and the 11th restore request, because the window is per console process and the PoC runs two replicas.
**Rollback:** in this order, roll the **console** and the **controller** back
**with the chart** (`helm rollback`). A default install carries neither new
block — the chart renders `runs` in `weirkeeper-policy` and `rateLimits` in the
console configuration **only** when a value differs from the defaults — so an
image-only rollback of a default install keeps working. With a non-default
value, an older controller refuses the whole `weirkeeper-policy` ConfigMap and
fails closed (no attestations, no evidence allowlist), and an older console
refuses its configuration file and does not start. An older controller has no
pool: it reads `Queued` as an active phase and starts every queued run at
once. An older console shows a queued run as `unknown` (`UnrecognizedPhase`).

#### 16. A compromise revocation outlives its `TrustPolicy`; deleting one may wait

**Changed.** On the PoC install (rehearsal R2) a `TrustPolicy` revoked the
installation signer for `KeyCompromise`, every backup it had signed turned
`Untrusted`, and deleting the policy sent the namespace back to
`legacy-roster-v1`, whose roster still listed the key: all six backups
re-verified `Valid` (TRUSTPOLICY-DELETE-DROPS-REVOCATION). A compromise is now
a fact about the key, not the policy:

- **Every namespace.** A key ANY `TrustPolicy` records as `Revoked` /
  `KeyCompromise` is revoked for compromise wherever it is listed — the
  recording policy's namespaces, a namespace re-bound away from it, one that
  falls to the roster, and one governed by another policy that still lists the
  key `Active`. Evidence verification and re-trust, the catalog view, the
  runner's evidence keyring, fresh and consumed approvals, `signer.rostered`,
  the API's `trust.state` and the keys view all read it; the refusal names the
  recording policy. A `Superseded` revocation is not carried.
- **Deleting a recording policy is held.** The controller places the finalizer
  `logweir.dev/compromise-revocation` on it and releases a `kubectl delete` only
  when another live policy records the same revocation, or nothing (no other
  policy, not `TrustRoster/default`) lists the key. Until then the policy stays
  `Terminating` and keeps governing; `CompromiseGuard=True/DeletionBlocked`
  names what still lists the key.
- **CEL rule G9:** a revoked key's `KeyCompromise` reason can no longer be
  edited to `Superseded` or removed.
- **The controller now holds `patch` on `trustpolicies`** (the object, beside
  `/status`) for that finalizer and nothing else; the body is pinned by a test.

**Do:** before this upgrade, run the *Pre-upgrade check* above (the
`kubectl get trustpolicies -o json | jq …` listing) and confirm every
`KeyCompromise` record it prints is one you mean installation-wide. After the
upgrade, replace a policy that records a compromise by applying a successor
under a new name first
([keys.md](keys.md), *Replacing a `TrustPolicy` safely*); the delete-and-re-create
repair for a mistaken `notBefore` still works unchanged for a policy that
records no compromise. Wait until a newly revoked policy lists the finalizer
before deleting it: a policy revoked and deleted before the controller saw it is
not guarded. **Scope:** `crates/weirkeeper/tests/trust_revocation_consumers.rs`
(one row per consumer; eight of ten fail on the previous commit),
`trust_revocation_durable.rs` (the guard table and the finalizer over a route
table enforcing seam S7), `crd_shape.rs` (G9), `approval_policy.rs`,
`crates/logweir-api/tests/trust_revocation_durable.rs`, `ui/tests/d3.spec.js`,
and planted mutants, each killed.
**Live** (PoC upgrade round, 2026-09-25, `claude/poc-upgrade-2`, on the PoC upgraded to `sha-b748fd5f…`, with a MINTED key listed only on two test policies): the pre-upgrade check printed nothing; recording the compromise on one policy placed the finalizer and `CompromiseGuard=CompromiseRecorded` within 2 s, and the other policy, still declaring the key `Active`, read it `Revoked`/`CompromiseInherited` in the same 2 s (the API's `effectiveState` and the console's keys page agreed); a `kubectl delete` of the recorder stayed `Terminating` with `DeletionBlocked` naming the other policy until a successor recorded the same revocation (released in 2 s); G9 refused `KeyCompromise`→`Superseded` on the API server and accepted `Superseded`→`KeyCompromise`; cleanup released every policy at once, and the PoC's own policy and keys were unchanged. Not shown live: a re-derived `Backup`/`Restore` verdict, which needs evidence the minted key signed in a namespace the controller watches.
**Rollback:** an older controller never removes the finalizer, so a deletion
made while it runs is held until this build returns; an older
`TrustPolicy`-aware controller applies a compromise only in the recording
policy's own namespaces, and `v0.1.5` reads only the roster and trusts every key
it lists. So before rolling back, re-create `TrustRoster/default` without every
compromise-revoked key and record each compromise on every policy that still
lists the key (rollback step 9 below). G9 stays with the CRDs, which a rollback
leaves in place.

#### 17. An `Approval` its Restore was admitted under is kept as a record (P9)

**Changed.** On the PoC install (2026-09-24, `86a554e6`), 900 s after an
Ordinary confirmation the controller rewrote a succeeded Restore's `Approval`
to `Verified=False/AuthorizationExpired`, dropped its recorded authorization,
and rewrote it again every pass: about 42 `approval refused` lines a second,
with the API server above 100% CPU. Expiry, the policy binding and the key
windows now bound only the time to admission. Once the Restore it names is
`Admitted=True` and that Restore's approval bundle names this `Approval`'s UID,
the controller adds `Consumed=True` (reason
`RestoreAdmitted`; its `lastTransitionTime` is the admission instant) and keeps
`Verified=True`, `status.authorization`, the key id and the approver as
recorded. A compromise revocation of the recorded key still withdraws the green
(`RecordedBeforeRevocation` or `KeyRevoked`); an `Approval` deleted and
re-created is never `Consumed`; a refusal message no longer names the current
time ([kubernetes.md](kubernetes.md) §8, *After admission the `Approval` is a
record, not a gate*). `Consumed` can land at the `Approval`'s next pass — at
the latest its expiry, 15 minutes on the PoC — backdated to the admission; the
API reads `verified: true` meanwhile. **Do:** nothing is required. Drop any
workaround that deleted `Approval`s after their restore. On the upgrade, an
`Approval` an earlier build withdrew this way is re-checked at the admission
instant and, when it verifies there, returns to `Verified=True` with
`Consumed`; with the approval-policy binding off (the PoC's upgrade step 2) it
reads `ApprovalPolicyMismatch` until the binding returns. **Scope:**
`crates/weirkeeper/tests/approval_policy.rs` and `approval_controller.rs`, five
planted mutants killed, a review round (`claude/poc-fixes-2`, merged
`7974b43a`). **Live** (`claude/poc-upgrade-1`, the upgrade to `02dc44b6`,
2026-09-25): an `Approval` the old controller had withdrawn was restored at
upgrade step 3 with `Consumed` at its admission instant; its resourceVersion
did not move for ten minutes, across a controller restart, with no refusal
line; three new Ordinary restores kept `Verified` and `Consumed` past their
expiry; a deleted and re-applied `Approval` was never `Consumed`. All eight
`Approval`s of the second round and both of the third ended `Verified` and
`Consumed`. **Rollback:** an older controller ignores `Consumed` and judges
consumed `Approval`s again; on `86a554e6`, where P9 was found, that is the
loop above.

#### 18. One catalog per destination: a second `RecoveryCatalog` over it is refused (P11)

**Changed.** Several `RecoveryCatalog`s whose `spec.destinationRef` names one
`BackupDestination` in one namespace used to be accepted, each with its own
sync Job: on the PoC at `02dc44b6`, five over `primary`, all `Ready=True`. The
one created first now catalogs the destination. Every later one reports
`Ready=False/DuplicateCatalog` and `Synced=False/DuplicateCatalog`, naming the
catalog that holds it; it runs no sync Job and withdraws its view
(`status.pages`, `status.viewExpiresAt`), so a `ProtectionPolicy` over it reads
`CatalogStale`. If the elder is deleted, the next one in creation order takes
over within a minute ([kubernetes.md](kubernetes.md) §7d, *One catalog per
destination per namespace*). **Do:** before upgrading, list the catalogs per
destination:

```bash
kubectl --context <ctx> get recoverycatalogs -A -o json \
  | jq -r '.items[] | select(.spec.destinationRef != null)
      | "\(.metadata.namespace)\t\(.spec.destinationRef.name)\t\(.metadata.creationTimestamp)\t\(.metadata.name)"' \
  | sort
```

Lines with the same namespace and destination are duplicates; the oldest
survives, not the best. If a newer duplicate has the settings you want (a
larger `viewLimit`, `mode: Full`), delete the older one first. A
`RetentionPolicy.catalogRef`, a `RehearsalSchedule`'s point `catalogRef` or a
`ProtectionPolicy.protects.catalogRef` that names a duplicate loses its input
and fails closed: point it at the survivor (the first two are immutable, so
re-create them). **Scope:** `crates/weirkeeper/tests/recovery_catalog_controller.rs`
and `catalog_controller.rs`, five planted mutants killed, a review round
(`claude/poc-fixes-3`, merged `56205b1`). **Live** (`claude/poc-upgrade-2`, the
upgrade to `b748fd5f`, 2026-09-25): a duplicate the old controller had synced
turned `DuplicateCatalog` on the new controller's first pass; duplicates made in
the console and with kubectl were refused, with no sync Job; a
`ProtectionPolicy` over one read `CatalogStale`; after the elder was deleted the
younger's sync Job started in 28 s and it was `Ready` in 42 s. **Rollback:** an
older controller syncs the duplicates again.

#### 19. A failed controller evidence read is read again (P12)

**Changed.** The controller reads a run's evidence itself for an inline-archive
run (through its archive handle) and for a destination whose `evidenceRead` is
`ControllerIdentity`. A failed read there used to be final: on the PoC three
inline-archive `Backup`s stayed `NotAttempted`, and so were never recovery
points, after `logweir-evidence-ro` was created and after two controller
restarts. Now a transient failure (a denial, a missing credential, a timeout)
is read again three more times, 1, 5 and 15 minutes apart, recorded in
`status.evidence.observation` with its `retryAfter`: at most four reads per run
per controller process, and at most four in flight at once. Each new controller
process reads every eligible unverified run once more. A store `NotFound` is
final, and a `Backup` is never written `Valid` without its `windowCovered`
([kubernetes.md](kubernetes.md) §15.1b). **Do:** a created or rotated
`logweir-evidence-ro` takes effect only when the controller restarts. A run
recorded absent through a misconfigured handle is not read again once the
handle is fixed; check it with its printed `logweir drill verify` command. On
the upgrade, the first controller process of this build reads once each
`NotAttempted` inline-archive run an older controller wrote. **Scope:**
`crates/weirkeeper/tests/backup_controller.rs`, `restore_controller.rs` and
`verification.rs`, eight planted mutants and the review's, each killed
(`claude/poc-fixes-3`, merged `56205b1`). **Live** (`claude/poc-upgrade-2`, the
upgrade to `b748fd5f`, 2026-09-25): the three stuck points turned `Valid` with
their window 3 s after the new controller started, and one restored `Valid`
150/150 through the console; a denied read turned `Valid` on its second attempt
once the denial was lifted; a read denied for 25 minutes was attempted three
more times, 1, 5 and 15 minutes apart, and then stopped; a controller restart
read it once more, and it turned `Valid`. **Rollback:** an older controller ignores the observation
and never reads a failed run again; a verdict this build reached stays.

#### 20. A readiness replay names its own expiry, and the console asks again (P14)

**Changed.** One idempotency key names one `Preflight` for ever, so a key built
from the question alone replayed that check after its validity had passed. On
the PoC at `b748fd5f`, the schedule form's *Check readiness* with unchanged
inputs answered `200 replayed` with an expired check, and no click could make a
fresh one. The API's replay, cancel and operation projections of a finished
check now carry `expired` in `staleReasons` once its `expiresAt` has passed,
beside `unverifiable`, with `staleBasis: ["expiry"]`: the same object, the same
UID, `200` ([api.md](api.md), *A readiness key replays only while its check
can still be the answer*). The console keeps one intent token per form in the
key and renews it once the check is spent (expired, inapplicable, `failed` or
`cancelled`) on the schedule form, the Schedules list's readiness panel and
*Discover topics*; a retry after a lost response, or a second click inside the
validity, still replays. **Do:** a client that builds its own readiness keys
sends a new key once a replay reads `expired`. Nothing else: the response shape
is unchanged, and `expired` was already in the closed vocabulary. **Scope:**
`crates/logweir-api/tests/preflights.rs`
(`a_replay_names_its_own_expiry_and_a_new_key_asks_afresh`), three API and three
console mutants killed, `ui/tests/check-intent.spec.js` (`claude/poc-fixes-4`,
merged `a54fb823`). **Live** (`claude/poc-upgrade-3`, on `a54fb823`,
2026-09-25): after expiry one click on the schedule form gave a `200` replay
reading `staleReasons [expired, unverifiable]` and `staleBasis [expiry]`, then a
`202` under a new key whose check applied; a click inside the window replayed
the same check; the list panel did the same in two dedicated runs; Cancel then
Check made a new check. *Discover topics* after its inventory went stale could
not be staged inside the PoC's 15-minute session. **Rollback:** `helm rollback`
moves the API and the console together, and the older pair replays a spent
check again; nothing stored changes.

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
  docker-desktop Kubernetes with a SCRAM (and private-CA TLS) Kafka and MinIO;
  the PoC profile on docker-desktop (Traefik, cert-manager, Dex, the shared
  console) from the published chart and images; and the GitHub Actions Compose
  suite. Nothing here was run against AWS S3, MSK, EKS, a corporate identity
  provider or a NetworkPolicy-enforcing CNI.

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
9. **Take every compromised key out of the roster first** (item 16). An older
   controller cannot carry a `KeyCompromise` revocation across policies, and
   `v0.1.5` cannot express one at all: re-create `TrustRoster/default` without
   every key any `TrustPolicy` revoked for `KeyCompromise` (a policy's
   `CompromiseGuard` message says "TrustRoster/default still lists …" while
   one is there), and record each such revocation on every policy that still
   lists the key (`CompromiseInherited`). Do not delete a policy to "go back to
   the roster" before that: it is held until nothing lists the key.

**How this upgrade is rehearsed.** From `v0.1.5` (the last version tag: 6 →
14 CRDs, the managed identity adopting a hand-provisioned signer, the console
arriving) and from `sha-f49849d…` (the last build before `ac00819`), each to
the first PoC publication `86a554e6`, which crosses items 1–13, and each rolled
back. The running install was then upgraded in place four times: to
`02dc44b6` (items 14, 15 and 17), to `b748fd5f` (16, 18 and 19), to
`a54fb823` (20) and to `815249cb` (no item: a console-only fix, P15, and no
CRD change). [release-handoff.md](release-handoff.md) names the chart and
image digests, the state each rehearsal set up first, and what each round
showed. An upgrade from `sha-7b0277b…` crosses items 1–4 and 11–20.

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

- **The live half of PLAT-20.2 ran on 2026-09-24 and 2026-09-25** with the PoC
  profile on docker-desktop, from published charts and images only: a clean
  install at `86a554e6`; upgrades to it from `v0.1.5` and from `sha-f49849d…`,
  each with a rollback, that kept installation identities, schedules and
  archive readability, with a restore of a pre-upgrade point after each; then
  four in-place upgrades of the running install, to `02dc44b6`, `b748fd5f`,
  `a54fb823` and `815249cb`, after which all 344 of its backup receipts still
  passed the independent verifier ([release-handoff.md](release-handoff.md)).
  [UNVERIFIED — R2's pre-upgrade retention, mount-failure and point-bound-restore states were not set up.]
- **The large catalog was measured live at 258 real points, not 1,000.** The
  host could not run more runner pods: the `amd64` runner runs under
  emulation there, and manual runs had no bound before item 15. The timings at
  258 points, and the offline rows at 1,000 and 5,000 rows, are in
  [stability.md](stability.md#measured-scale-limits-plat-202); the console
  refuses a list longer than 5,000 rows rather than showing a prefix.
  [UNVERIFIED — a 1,000-point archive was not reached live; 258 real points were measured on docker-desktop.]
- **P15, fixed in `815249cb` and proven live:** a readiness check slower than
  the console's old follow (40 s on the Schedules page, 30 s for *Test
  connection*, 60 s for *Test access*, 90 s at restore step 5; *Discover
  topics* had none) was left "not finished" for good. Every follower now reads
  its check until the longest time a check may take (12 minutes; a discovery
  7), backing off from 2 s to 10 s between reads, and says "did not finish" —
  "not cancelled", with *Run the check again*, which starts a new check — if
  that passes ([ui/README.md](../ui/README.md)). On the PoC
  (`claude/poc-upgrade-4`), checks of 100–170 s were read to their verdicts
  without a reload on the five readiness followers, *Discover topics* now
  settles on the page, and the product API's request log showed the cadence. The 12-minute "did not finish" state was left to the
  offline rows (`ui/tests/check-deadline.spec.js`).
- **At restore step 5 the first repaint of a running check scrolls the page
  to its top (P16, console, open).** On a 390 px screen the focused status
  line is then off screen until the verdict lands; the verdict itself is
  brought into view above the Back/Next bar. Scroll back to the status line,
  or wait for the verdict. Found by the fourth PoC round on `815249cb`.
- **The demo MinIO is a rebuilt mirror.** MinIO withdrew its public images
  (Docker Hub on 2026-09-11; `quay.io` refuses anonymous pulls since
  2026-09-24). The chart's demo MinIO, the e2e stack and the PoC run the same
  MinIO and `mc` releases rebuilt from the archived upstream source,
  `docker.io/vladyslavhaina/minio-mirror` and `mc-mirror` (AGPL-3.0). Replacing
  MinIO with a maintained, permissively licensed S3 server is an open task
  (REPLACE-MINIO), not started.
- **The product API's OpenAPI document is still `1.0.0-alpha.1`**, although the
  console image and the chart now consume it; ship and upgrade the console and
  the API together until the owner freezes it ([stability.md](stability.md)).
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
