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
  an ingress controller, cert-manager with a local CA, Dex and the shared console, all
  by Helm and the published `sha-` images, with the chart gaps it had to stand
  in for listed there (G1–G6). [UNVERIFIED — the profile has been rendered, not yet installed on docker-desktop.]
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
6. **Apply the CRDs, wait for all fourteen to be established**, then roll the
   controller **and the runner image together** (item 5), then the console,
   then any approval-policy binding (item 7). On the Helm path these are one
   release and successive `helm upgrade`s: first the image values (controller,
   runner and console move together), then the `approvalPolicy.*` values.

### The ten operator-facing changes

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
since `ac00819` and its Done record waits for lab-refresh-9. The browser journey
against a real identity provider and TLS ingress is not run on docker-desktop
([api.md](api.md), *What ships today*). [UNVERIFIED — shared mode behind a real OIDC provider and TLS ingress has not been run.]
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
[UNVERIFIED — the completion panel on a real finished restore is owed by lab-refresh-9.]
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
7. Roll the controller and runner back together, and leave the CRDs in place.

**How this upgrade is rehearsed.** From `v0.1.5` (the last version tag: 6 →
14 CRDs, the managed identity adopting a hand-provisioned signer, the console
arriving) and from `sha-f49849d…` (the last build before `ac00819`, which
crosses all ten items above); [release-handoff.md](release-handoff.md) names
the images, the state each rehearsal sets up first, and which items it
exercises. An upgrade from `sha-7b0277b…` crosses items 1–4 only.

Archives, evidence and catalog records are untouched in both directions; old
signed archives keep verifying as long as their public keys stay in the trust
policy or roster ([keys.md](keys.md)).

### Limitations and open items

- **The live half of PLAT-20.2 is not yet run**: a clean install on
  docker-desktop following [quickstart.md](quickstart.md), and upgrades from
  `v0.1.5` and from `sha-f49849d…` that keep installation identities, schedules
  and archive readability. [UNVERIFIED — owed by PLAT-20.2's live round after lab-refresh-9 releases the cluster.]
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
