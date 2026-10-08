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
`fdb48cd8` (2026-09-25): the platform tracker's shipped tasks, the operator
actions collected for PLAT-20.2 and after it, and the upgrade from the last
published image. Items 21 (FX-2), 22 (FX-5), 23 (FX-10) and 24 (FX-3), from
the product-expansion tracker's fix-now rows, and item 25 (PROD-00.3f, the
engine pin), land after `fdb48cd8`, and so do
FX-7's additions to item 11 (the execution-claim set check, receipt and
catalog format 1.2.0, the pin's read by version id) and FX-4's format 1.1.0,
which has no item of its own. No tag is cut at `fdb48cd8`, so the candidate
record below stays empty. The shipped task list, the six publications the PoC ran, the
tested environments and the results are in
[release-handoff.md](release-handoff.md).

### Candidate record

Fill every row for the exact candidate; a row left as `—` is an unrecorded
fact, not a pass. A previous run does not validate new bytes. A tag does not
rebuild the images — it gives the `sha-<commit>` publication `main` CI already
made the version tag, unchanged — so the release dry run's `release.json`
(workflow artifact `release-assets`) gives the publication commit and the four
image digests **before** the tag. Nothing else carries over: the tag run
packages the chart and builds the three CLI archives again. The chart package
is not byte-reproducible (`helm package` records each file's modification
time, which is its checkout time), and nothing shows the archives to be. The
chart and archive rows therefore come from the **tag run's** `release.json`,
the GitHub Release asset, after the tag, together with the run rows
([the release checklist](tag1-checklist.md), *Cutting a release candidate*).

| What | Value |
|---|---|
| Candidate commit (the tagged commit) | — |
| Version tag | — |
| Publication commit (`release.json` `.images.publication`: the `sha-<commit>` images and chart the tag promotes) | — |
| CI run (`ci.yml`) for the publication commit, its `publish` job green | — |
| Release dry run (`release.yml` dispatched on the candidate) | — |
| Release run (`release.yml` on the tag) and release drill | — |
| Runner image digest (`linux/amd64`) | — |
| Controller image digest (manifest list; amd64 and arm64) | — |
| Console image digest (`logweir-console`) | — |
| UI image digest (`logweir-ui`) | — |
| Chart (`logweir-chart` version and package sha256 from the tag run's `release.json` `.chart`; OCI digest from the release's notes, as an anonymous `helm pull` reports it) | — |
| CLI archives (three; the tag run's `release.json` `.archives`, each with its sha256 and run-time needs) | — |
| `ui/` bundle, file by file | the output of the command below, which the release asset `ui-files.sha256` also carries |
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
  five times by the profile's *Upgrade to a newer publication*, the last to
  `fdb48cd8` on 2026-09-25 ([release-handoff.md](release-handoff.md)). The
  fixes those rounds needed are in the profile.
- **The chart is published** as `oci://registry-1.docker.io/vladyslavhaina/logweir-chart`,
  beside the images and versioned with them ([install.md](install.md), *(c) The
  Helm chart*). The first publication the PoC ran, `0.1.0-sha-86a554e6…`
  (digest `sha256:90b4d41b…`), and the last, `0.1.0-sha-fdb48cd8…` (digest
  `sha256:36e65b2a…`, CI run 36183088296), pull anonymously and name their own
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

**Pre-upgrade check: which `Restore`s and `RehearsalSchedule`s carry a
`runnerResources` block** (item 21). This build applies the block to the runner
container, or refuses the object, where earlier builds ignored it. Run item 21's
inventory before the controller rolls; no output means the upgrade changes
nothing there.

### The twenty-five operator-facing changes

Each item names what changed, what to do, what the claim rests on (its
verification scope), and how to roll it back. Items 1–20 were collected for
PLAT-20.2 from merged changes; the defect names are the platform tracker's.
Items 17–20 were found by the PoC rounds and landed after its first
publication (`86a554e6`); each was proven on the running install by the
in-place upgrade that carried it. Items 21 and 22 are the product-expansion
tracker's fix-now rows FX-2 and FX-5 and are not proven live yet: the PoC
upgrade that carries each runs its rows. Item 23 is fix-now row FX-10, proven
offline; the PoC upgrade that carries it checks that the PoC's policy
document and its digest are unchanged (the PoC sets neither withdrawn key).
Item 24 is fix-now row FX-3, proven on a compose stack (it changes the
runner's signed scorecard, not the controller). Item 25 is PROD-00.3f, the
engine pin, proven on a compose stack; the PoC upgrade that carries it runs its
controller and runner rows.

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
`notReady / ConditionalCreateUnsupported` first. The claim adds no permission and
changes no signed format; FX-7 below adds receipt and catalog format `1.2.0`, the MINOR after FX-4's `1.1.0` ([stability.md](stability.md#the-first-post-tag-addition-format-110-fx-4)).
**Do:** confirm the evidence store honours conditional create — turn on `writeProbe: CreateOnlyMarker` for one run of the destination
check, and never set `AWS_CONDITIONAL_PUT=disabled` — see the store table in
[support-matrix.md](support-matrix.md). A standalone `logweir backup run` that
reused a fixed `backup_id` must pass a fresh `--backup-id-override` per run.
**The upgrade window is closed since FX-7:** an execution whose first run was
made by the older runner has no claim, so if its Job is lost and re-created
after the upgrade the new runner wins a claim — and then finds the older run's
manifest or segments under `<prefix>/<backup_id>/` and stops, exit 1
`ExecutionAlreadyClaimed`, before the engine. Only an older runner that is still RUNNING when its Job is
re-created, and has written nothing yet, escapes both checks; let such a Job
finish before upgrading. A read of the archive that fails while proving the set
new is exit 1 when it is transient (a transport error, a timeout, a 5xx), so a
schedule with `spec.retry` retries it under a new execution id, and exit 4
`ExecutionClaimUnproven` otherwise (a 403, a wrong bucket). On a versioned bucket a receipt also pins its
manifest's version (`archive.manifest_version_id`, receipt format `1.2.0`), so a
set written again in that bucket after the point was signed — by an older
runner after a rollback, say — is refused by a point-bound restore
(`PointBindingMismatch`) and reported `Conflict` by the catalog even when the
manifest bytes came out identical. That detection covers the points this build
signed; the older runner's own receipt over the rewritten set pins nothing and
stays selectable. A version id belongs to one bucket, so a byte-for-byte COPY
of the archive (`aws s3 sync`, `mc mirror`, an unversioned destination) is the
same point, checked by its manifest digest: the restore runs and logs
`PointPinUnchecked`, and the catalog entry's remedy says the pin could not be
checked in that bucket. **The pin is checked only where the bucket still holds
the pinned version and serves it by id:** a rewrite whose pinned version was
since expired or DELETED, a copy synced after the set was written again, or a
store that cannot read by version reads the same way, and there the digest
cannot see segments rewritten under an identical manifest. Object Lock
retention covering a point's lifetime keeps its pinned version; when the
signing bucket's catalog says `Conflict` and a copy's says `Available`, believe
the `Conflict`. The pin's read by id needs
`s3:GetObjectVersion` on the archive prefix
([backup-receipt.md](formats/backup-receipt.md#the-pinned-manifest-version-versioned-buckets)).
**Scope:** in-process rows, a private MinIO `RELEASE.2025-09-07T16-13-09Z`
container, and four planted mutants plus the review's two. Live on
lab-refresh-10 (2026-09-24): PLAT-06.1's case e (a lost Job re-created), both
arms, and the receipt-dup rows 2–5 (RECEIPT-DUP). FX-7 (2026-09-29): compose
slot 3, MinIO unversioned and SeaweedFS versioned buckets, a `v0.1.5` runner
for the older build's run; its fix round (2026-10-05): slot 2, the same matrix
plus byte-for-byte copies of a pinned point into both stores and a point-bound
`restore run` against each bucket.
**Rollback:** an older runner ignores the claims, the set check and the pin,
and returns to re-running the engine over a re-created Job; the claims stay in
the bucket, harmless, and are honoured again after a re-upgrade. **Before
rolling the runner back to a build without the execution claim, let in-flight
`Backup`s finish:** on an unversioned bucket a re-created Job's older runner
rewrites the set's segments under an unchanged manifest, and no check reports
it — the first, signed point keeps verifying. On a versioned bucket that
point is reported `Conflict`.

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

#### 21. A `Restore`'s `runnerResources` is applied, or the object is refused (FX-2)

**Changed.** `Restore.spec.runnerResources`, and a `RehearsalSchedule`'s
`spec.bounds.runnerResources` through the child `Restore` it creates, were
accepted, documented as what the runner pod asks for and is capped at, and
dropped: no runner container carried `resources`, so every runner pod was
`BestEffort`. The block now reaches the `runner` container exactly as written.
A block the controller will not apply is refused, never clamped: a quantity
outside the schema's grammar, memory that is not whole bytes, CPU finer than
`1m`, anything above 4 CPUs or `8Gi` (requests included), a zero limit, a memory
limit below `32Mi`, or a request above its limit. A `Restore` with such a block
ends `Failed` with reason `ExecutionSpecInvalid` before its approval is read or
anything is created, and the product API serves it `refused`. A
`RehearsalSchedule` skips every slot as `AuthorizationInvalid` and creates no
child ([kubernetes.md](kubernetes.md) §12, *The runner's requests and limits*).
An object without the block gets exactly the Job it got before; the console
never sets the field.

What changes on the upgrade, for objects that already carry a block:

- **A `Restore` with no Job yet** (held for its approval, queued, or created
  during the upgrade) is judged on its next pass. A valid block now caps its
  pod, so the pod can be rejected by a `ResourceQuota` or `LimitRange` it never
  met before (reported as `RunnerReady=False` with `PodCreationForbidden`, then
  ending `PodCreationForbidden`), or be OOM-killed at its memory limit. A block
  outside the bounds ends it `Failed`/`ExecutionSpecInvalid`, and the remedy is
  a new `Restore`, because `spec` is immutable.
- **A `Restore` whose Job already exists** keeps that Job, which carries no
  `resources`. A terminal `Restore` is untouched.
- **A `RehearsalSchedule`** caps every rehearsal from its next slot, or, when its
  block is outside the bounds, skips every slot as `AuthorizationInvalid` for
  good. The spec is sealed, so the fix is a new schedule and a new signed
  standing authorization.

**Do:** before the upgrade, list every object that carries a block:

```bash
kubectl --context <ctx> get restores,rehearsalschedules -A -o json \
  | jq -r '.items[] | select(.spec.runnerResources // .spec.bounds.runnerResources)
      | "\(.kind) \(.metadata.namespace)/\(.metadata.name)"'
```

No output means nothing changes. Check each listed block against §12's rules
and against its namespace's `ResourceQuota` and `LimitRange`. Replace an
out-of-bounds schedule (and its authorization) before the upgrade, or expect its
slots to be skipped. Expect an out-of-bounds `Restore` that has no Job yet to
end `ExecutionSpecInvalid`. **Scope:** `crates/weirkeeper/tests/runner_resources.rs`
(the rules, and the exact quantity arithmetic in every suffix),
`restore_controller.rs` and `rehearsal_controller.rs` (the container carries
the block exactly, one-sided blocks included; the refusal comes before the
approval is read, with no `POST`; a quota rejection is reported and failed
fast), `crates/logweir-api/tests/status_mapping.rs` (`refused`), and planted
mutants, each killed (FX-2, its review and its fix round). Not yet proven live:
the PoC upgrade that carries FX-2 runs its CRD, refusal and Job-bytes rows. An
admitted `Restore`'s real Job and the `RehearsalSchedule` side wait for
PROD-10.1, which exposes the control in the console.
**Rollback:** an older controller ignores the field again. Its Jobs carry no
`resources`, and a `Restore` this build refused stays `Failed`; an older
product API serves that refusal `failed` again. A
`RehearsalSchedule` this build skipped for its block fires again under the
older controller, uncapped: suspend it (`spec.suspend: true`, the one mutable
field) before rolling back if it must not run. Nothing has to be deleted.

#### 22. A console restore asks for a replication factor it can explain, and keeps its topic subset (FX-5)

**Changed.** The restore wizard wrote `replicationFactor: 1` into every plan
and showed it read-only, so every console restore created topics with
replication factor 1, on any cluster. Step 4 now has a **replication factor**
input. Its default is the target connection's broker count, at most 3, read
from that connection's newest successful topic discovery: a fresh one, or one
whose only stale reason is `expired`, while the controller keeps it
(`checks.discovery.retentionSeconds`, a day by default). With no such
discovery the default stays 1, and step 4 says why and links *Discover
topics* on the target. The source's own factor is not read: it is recorded
only in the archive manifest, and projecting it is PROD-05.1's. Step 4 and the
review step print the factor with where it came from, such as `2 (the target's
2 brokers; ...)` or `3 (set by you; the target has 2 brokers)`. They also say
it can differ from the source's: a topic the source kept at replication factor
1, restored at 3, takes three times the storage it took there. A factor above
a fresh discovery's count is refused before anything is sent, with
`ReplicationFactorExceedsBrokers`. A factor above an older count is not
refused; the readiness check's `target.topicCreate` row stays the check
against the target as it is. The topic-discovery DTO of the product API gains
an optional, additive `brokerCount` ([api.md](api.md), *Bounded, honest topic
inventory*; [ui/README.md](../ui/README.md), *The replication factor: a
default with its basis, an input, and a refusal before Create*).

Also fixed: **a resumed restore draft lost its topic subset.** The console's
draft store keeps strings and booleans only, and the wizard handed it the
topic subset, and a catalog point's typed topic list, as arrays, which it
dropped without a word. After leaving the wizard and coming back in the same
page, every topic of the point was selected again while the page said "your
unsubmitted edits ... are back", so a Restore created from a resumed draft may
have restored more topics than were chosen. Both lists are now kept.

What an operator sees after the console image is upgraded:

- **No discovery of the target in the last day** (the PoC's state): the factor
  is still 1, now with a warning beside the input and a link to run *Discover
  topics* on the target connection.
- **After a *Discover topics* of the target:** a restore into a multi-broker
  target asks for 2 or 3 replicas where it asked for 1, so it stores up to
  three times as much on the target as the same restore did before the
  upgrade, plus replication traffic.
- **A `Restore` created before the upgrade** keeps its plan bytes
  (`Restore.spec` is immutable) and so its factor of 1. A retry builds a new
  plan with the new default.

**Do:** before restoring into a target with little free disk, check the factor
on step 4 and set it yourself if the default is not what you want. Run the
readiness check (step 5) before Create: with no check, a factor the target's
brokers cannot hold fails the approved run when it creates the topics
(`exitCode 1`, `operational`, nothing restored; [quickstart.md](quickstart.md)
§7). For console Restores created before this build from a resumed draft,
compare the topics each one restored with the ones you meant: they are the
`source.topics` list of the plan
(`kubectl --context <ctx> -n <ns> get restore <name> -o jsonpath='{.spec.planBytes}'`).
**Scope:** `ui/tests/replication-factor.spec.js` (the default rule and its
4-broker boundary, the count's freshness rule, the refusal before Create, the
review row, the sentence about the source's factor in the page and in both
documents, the readiness warning, both mounts, and the draft class), with a
negative control for each behaviour, each killed. Also
`crates/logweir-api/tests/topic_discoveries.rs`
(`a_discovery_publishes_the_broker_count_its_result_recorded`, over the fixture
the console rows read), with four API mutants killed, and a Chromium journey
over the real console modules at 1440 and 390 px (FX-5, its review and its fix
round). Not yet proven live: the PoC upgrade that carries FX-5 runs its rows,
stopping before Create. **Rollback:** rolling the console image back restores
the fixed, read-only 1 and the draft that drops the subset, and an older
product API omits `brokerCount`. Nothing stored changes: a `Restore` created
with a factor above 1 keeps it.

#### 23. Two policy values that changed nothing are withdrawn (FX-10)

**Changed.** `checks.discovery.defaultMaxTopics` and
`checks.preflight.defaultTimeoutSeconds` were documented as the default a
request that names none gets. They never reached anything. Both CRDs default
the request field at admission (`maxTopics` 20 000, `timeoutSeconds` 120) and
the console writes both, so an operator who set either changed nothing.

- They are gone from `values.yaml` and the chart README.
- The controller's policy parser accepts both keys, or neither, and reads
  neither. It applies no range rule, only the type: any whole number from 0
  to 4 294 967 295 is accepted. A hand-written document that puts `null`, a
  negative, a fraction, a quoted number or a larger number there is refused
  whole, as before.
- The chart renders both at fixed values: 20 000 (or `hardMaxTopics`, if that
  is lower) and 120. A controller older than this one requires both keys, and
  this keeps the document readable to it.
- Nothing that runs changes: every install has always used the request's own
  values.

**Also changed: the retention worker refuses a cap it cannot read.**
`logweir-retention` now refuses a run whose `LOGWEIR_RETENTION_MAX_DELETIONS`
or `LOGWEIR_RETENTION_MAX_OBJECTS` is absent, or is not a whole number of at
least 1. It exits 3 and deletes nothing. Before, it silently used 50 and
20 000. Every controller that creates an enforcement Job sets both from
`spec.enforcement`, so a supported controller and runner never hit the
refusal.

**Do:**

- Remove either key from your values file.
- A `helm upgrade` that still carries one succeeds, because the schema still
  accepts both. It renders exactly what it would render without them, and its
  notes print `WITHDRAWN VALUES ARE SET AND IGNORED`.
- An upgrade with `--reuse-values` carries an older chart's defaults forward
  and prints the same warning. Upgrade once with `--reset-then-reuse-values`.
- To bound a discovery, name `maxTopics` on it; `checks.discovery.hardMaxTopics`
  still caps it. To give a slow cluster longer, name `timeoutSeconds` (30–600)
  on the `Preflight`.

**Scope:**

- `crates/weirkeeper/tests/chart_policy.rs`;
- `crates/logweir/tests/chart_lint.rs`;
- `scripts/check-chart.sh`, its withdrawn-values arm;
- `scripts/check-chart-values.sh`, one render per chart value;
- `crates/logweir-retention/tests/worker.rs`, the caps below the old
  defaults (7 and 1234), above them (75 and 30 000, through execution), and
  the refusals;
- `crates/weirkeeper/tests/retention_policy_controller.rs`, raised ceilings
  (55 and 30 000) reaching the plan and the Job;
- the mutants in the FX-10 report.

**What an operator sees after the upgrade.** An install that never set either
key keeps a byte-identical policy document, so its policy digest does not move
and no retained `Preflight` reads `policyChanged`. An install that had set
either key to a value other than the one the chart now renders (20 000, or
`hardMaxTopics` if lower, and 120) gets a changed document once: the chart
renders the fixed value in place of its own. Its policy digest changes, and
every retained `Preflight` whose `ready` verdict has not expired yet reads
`unknown`, its message naming `policyChanged`. Run the check again. Nothing
else changes, because the controller never read either value.

**Rollback:** `helm rollback` restores the previous chart's values and
document. Rolling back only the controller image is also safe, because the
document still carries both keys at values an older controller accepts.

#### 24. A `newTopic` restore's scorecard names the source settings it did not reconstruct (FX-3)

**Changed.** A restore creates its target topics at the plan's replication
factor, with `retention.ms=-1` and the target broker's `cleanup.policy`. The
signed scorecard labelled those deviations from the source
`intentionally_deviated` in every mode — right for a scratch drill, wrong for a
`newTopic` restore, whose scorecard therefore signed lost compaction and
replication factor 1 as intended. Scorecard format `1.2.0` names them in the new
`topic_parity.not_reconstructed` and also in `unexpected_divergence`, never as
intended, and `logweir drill verify`, `docs/verify_scorecard.py` 1.17.0 and
`logweir drill show` say so in words; for a `newTopic` scorecard signed before
1.2.0 they say its intended entries were not reconstructed. A scratch drill's
scorecard is unchanged apart from `format_version` and `not_reconstructed: []`.
No exit code or `outcome` of a scorecard this build signs changes: `topic_parity`
decides neither. A 1.2.0 scorecard whose lists contradict `not_reconstructed`,
such as a `newTopic` one that labels these settings intended beside
`not_reconstructed: []`, is refused by both readers (`drill verify` exit 4, the
script exit 1), and phase 8 never signs one.
**Do:** nothing on the upgrade. After a `newTopic` restore, read
`not_reconstructed` and apply the source's settings once the restore is
verified ([stability.md](stability.md#a-newtopic-restore-does-not-reconstruct-the-sources-topic-settings));
Logweir does not apply them yet (PROD-05). Re-read any `newTopic` scorecard
signed by an earlier build with the current verifier. Automation that parses
`intentionally_deviated` or `unexpected_divergence` should expect these entries
in the second list for `newTopic` runs; how the format change is classified is
in [stability.md](stability.md#format-120-fx-3-what-a-newtopic-restore-did-not-reconstruct).
**Scope:** unit rows for each of the four settings in both modes and through
the whole phase sequence (`crates/logweir/tests/verify_phase.rs`,
`orchestrator.rs`), the five arms in both readers with the invariant corpus
and the verifier-parity gate, and a live row on compose
(`e2e/tests/new_topic_parity.rs`: a compacted, replication-factor-3 source on
the `cluster3` profile restored as `newTopic` and as a drill, the broker's own
configuration as the oracle, and a pre-FX-3 binary's restore of the same point
for contrast). Readers built before FX-3 accept the 1.2.0 scorecards: `logweir`
and script 1.15.0 at main `b8b9263f`, FX-7's script 1.16.0, and `v0.1.5`
(measured).
**Rollback:** an older runner writes 1.1.0 scorecards with the old labels again.
The 1.2.0 scorecards already written stay valid under older and newer readers.

#### 25. The engine is `kafka-backup` 0.23.3; an `http://` archive endpoint needs `allow_http: true` (PROD-00.3f)

**Changed.** The runner image carries `kafka-backup` **0.23.3** (image digest
`sha256:cc7d5a8a…`, upstream commit `afb160e7`), OSO's newest release on
2026-10-07, in place of 0.21.0. The segment format, the three engine commands
Logweir runs and every key it renders are unchanged, and archives written by
either engine read and restore with the other. `logweir doctor` accepts exactly
0.23.3, as a whole token: 0.21.0 is now a version mismatch, and so is a suffixed
`0.23.3+build`. Receipts and scorecards name the engine that ran, so new ones
say 0.23.3. Two engine behaviours since 0.22.0 are refused instead of
inherited. A storage location with a plain `http://` endpoint and
`allow_http: false` is refused at phase 0 with exit 3 (`refusal-reason=GuardRefused`),
by `drill run`, `restore run` and `backup run` alike, and no engine document
is rendered with it: the engine now derives plaintext from the scheme and
would dial the archive in the clear. `VirtualHosted` addressing with a custom
endpoint stays refused (`AddressingUnsupportedByEngine` /
`addressing_unsupported_by_engine`); its message no longer names an engine
version. The full-drill floor stays 0.21.0.
**Do:** nothing with the chart: the controller stamps the new version into
every runner Job, so roll the controller **and** runner image together (the
existing upgrade order). A standalone CLI install replaces its engine binary
with 0.23.3 (the digest in `third_party/kafka-backup-binary.digest`) and its
`LOGWEIR_ENGINE_VERSION` / `LOGWEIR_ENGINE_DIGEST` with the new pair
([quickstart.md](quickstart.md), step 4; `examples/cronjob-drill.yaml`). A spec
that names an `http://` endpoint must say `allow_http: true`; a saved
destination already cannot combine the two (rule R3).
**Scope:** source evaluation of every change from 0.21.0 to 0.23.3
([decision record](to-do/decisions/PROD-00-engine-route.md) §12: no capability gap
it lists is fixed, and nothing Logweir reads or renders changed shape). On a
compose stack (slot 4, Kafka 3.7.1, the engine under `linux/amd64` emulation)
CI's e2e command passed 177 tests, PROD-01.1's record-semantics contract
asserted on 0.23.3 included; the demo drill passed with both readers VALID; an
archive 0.21.0 wrote drilled with 0.23.3, and an archive 0.23.3 wrote drilled
with 0.21.0, both `pass`. On Kafka 4.3.1 the record-semantics, G-PITR, FX-1,
FX-7 and full-drill rows passed with the pin as well (34 tests). Unit rows refuse the `http://` combination in each
of the three engine documents and at phase 0 for drill and backup specs, with
mutants on those guards and on `doctor`'s pin, and `e2e/tests/engine_pin.rs`
(default test set) holds every place that names the pin to one version. The
weekly `engine-matrix` rows for 0.23.3 have not run on GitHub yet, and the
controller's `LOGWEIR_ENGINE_VERSION` reaches a live runner Job only at the
next PoC upgrade.
**Rollback:** an older runner and controller run 0.21.0 again; `doctor` from
that build refuses 0.23.3. Archives and receipts written by 0.23.3 stay
readable and verifiable by older builds: the manifest and segment bytes are the
shapes 0.21.0 reads.

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
the `runnerResources` inventory (item 21) → CRDs (all fourteen established) →
controller **and** runner image together → console image → approval-policy
binding. Every CRD change is additive; nothing
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
   §10, *Backups created under the previous execution contract*). Rolling the
   runner back to a build without the execution claim, let EVERY in-flight
   `Backup` finish first (item 11): on an unversioned bucket a re-created Job's
   older runner rewrites the set's segments under an unchanged manifest, and no
   check reports it.
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
10. **Suspend every `RehearsalSchedule` this build skips for its
    `runnerResources`** (item 21) that must not run uncapped: an older
    controller drops the block and fires its next slot. A `Restore` this build
    refused `ExecutionSpecInvalid` stays `Failed`.

**How this upgrade is rehearsed.** From `v0.1.5` (the last version tag: 6 →
14 CRDs, the managed identity adopting a hand-provisioned signer, the console
arriving) and from `sha-f49849d…` (the last build before `ac00819`), each to
the first PoC publication `86a554e6`, which crosses items 1–13, and each rolled
back. The running install was then upgraded in place five times: to
`02dc44b6` (items 14, 15 and 17), to `b748fd5f` (16, 18 and 19), to
`a54fb823` (20), to `815249cb` (no item: a console-only fix, P15, and no
CRD change) and to `fdb48cd8` (no item: console-only fixes, P16 and O2, and
no CRD change). [release-handoff.md](release-handoff.md) names the chart and
image digests, the state each rehearsal set up first, and what each round
showed. An upgrade from `sha-7b0277b…` crosses items 1–4 and 11–20. An upgrade
from `fdb48cd8` crosses items 21, 22, 23, 24 and 25, and item 11's FX-7 additions:
grant `s3:GetObjectVersion` before the upgrade, or a pinned point whose current
version differs fails closed at the binding, and let in-flight Backups finish
before rolling the runner back.

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
  five in-place upgrades of the running install, to `02dc44b6`, `b748fd5f`,
  `a54fb823`, `815249cb` and `fdb48cd8`, after which all 356 of its backup
  receipts still passed the independent verifier
  ([release-handoff.md](release-handoff.md)).
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
- **P16, fixed in `fdb48cd8` and proven live:** at restore step 5 the first
  repaint of a running check scrolled the page to its top, so on a 390 px
  screen the focused status line was off screen until the verdict landed.
  A repaint of the view on screen now keeps the reader's place (scroll and
  focus), the status is kept above the Back/Next bar, a reader who scrolled
  away is left where they are, and Next, Back and a `&step=` link open a step
  at its heading ([ui/README.md](../ui/README.md)). On the PoC
  (`claude/poc-upgrade-5`, 390×844) the page and the focused status held
  still through every repaint of a running check, where the same rows on
  `815249cb` had seen it jump to the top; *Test connection* and *Discover
  topics* kept the page, and *Test access* and the schedule form kept focus.
  The same publication prints `logweir catalog list` on the Catalog as code,
  not between backticks (O2).
- **A followed check stops at the ingress's own `503` (P17, console, open).**
  A follow tries past five failed reads of the API, which covers a restart,
  but when no console pod is ready the ingress answers `503` with a plain-text
  body, which the page reads as a contract failure and stops at once: the
  check shows "could not be read again" with *Run the check again*, though it
  goes on to finish in the cluster. Run the check again, or reload once the
  console answers. Found by the fifth PoC round on `fdb48cd8`, when a loaded
  host failed both console pods' readiness probes.
- **The demo MinIO is a rebuilt mirror.** MinIO withdrew its public images
  (Docker Hub on 2026-09-11; `quay.io` refuses anonymous pulls since
  2026-09-24). The chart's demo MinIO, the e2e stack and the PoC run the same
  MinIO and `mc` releases rebuilt from the archived upstream source,
  `docker.io/vladyslavhaina/minio-mirror` and `mc-mirror` (AGPL-3.0). Replacing
  MinIO with a maintained, permissively licensed S3 server is an open task
  (REPLACE-MINIO), not started.
- **A console restore's replication factor does not start from the
  source's** (item 22). The source's factor is recorded only in the archive
  manifest; the default is the target's broker count, at most 3, until
  PROD-05.1 projects the source's factor to the console.
- **The `v0.1.1`–`v0.1.5` image tags on Docker Hub are leftovers of failed
  runs, not releases.** Those tag runs pushed version-tagged images before
  they failed — `logweir:v0.1.1`–`v0.1.5`, `weirkeeper:v0.1.2`–`v0.1.5` and
  `logweir-ui:v0.1.3`–`v0.1.5`; no console image and no chart — and none of
  them published a chart, a GitHub Release or a release drill. Every run
  failed in the CLI build matrix; `v0.1.1`'s image job also failed its own
  repository-digest check after pushing, and the pull-back jobs of `v0.1.2`
  and `v0.1.3` failed with "cannot overwrite digest". Do not install or pin
  them. The owner decided on 2026-10-07 to delete them all once v0.2.0 ships. The repaired pipeline
  never builds an image under a version tag: it tags main CI's `sha-<commit>`
  images.
- **The CLI archives' run-time needs.** The Linux archives are built in the
  runner image's builder base (`rust:1.89-bookworm`), so they need what the
  runner image installs: a glibc at least as new as the version measured on
  each binary — 2.34 on the Linux arm64 archive built locally on 2026-10-05;
  each release's notes give its own — which is never above the Debian 12
  glibc (2.36) they are built against, so Debian 12's glibc or newer always
  suffices; and `libssl.so.3`, `libcrypto.so.3`, `libsasl2.so.2` and
  `libz.so.1` (Debian and Ubuntu: `libssl3`, `libsasl2-2`, `zlib1g`). A
  distribution whose SASL library has another soname
  (`libsasl2.so.3` on RHEL and Fedora) builds from the checkout. The macOS
  archive needs Homebrew's `openssl@3`. `logweir --version` prints the
  workspace version (`0.1.0`), not the tag; `release.json` ties each archive
  to its tag and commit.
- **The product API's OpenAPI document is `1.0.0-alpha.2`, still a
  pre-release**, although the console image and the chart consume it; ship and
  upgrade the console and the API together until PROD-14.2 freezes it at
  `1.0.0` ([stability.md](stability.md)). Since `1.0.0-alpha.1` (2026-09-16,
  never published) it gained 38 operations and removed none: destinations
  (list, create, read, usage, test, update access, adopt from legacy), catalogs
  with their points and signers, topic discoveries, preflights, the operation
  event stream, schedule updates, manual backups, the restore approval
  submission, read-only protection, rehearsal, retention and trust policies,
  the namespace's approval policy, cadence previews, and the shared console's
  sign-in routes (`/auth/login`, `/auth/callback`, session logout). Its
  component schemas grew from 66 to 257; none was removed.
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
