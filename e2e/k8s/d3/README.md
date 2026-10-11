# D3 live acceptance on docker-desktop

`d3_live.py` is the harness for decision D3 §15's catalog, retention and trust
scenarios — the live half of PLAT-15.1, PLAT-16.1 and PLAT-16.2, with evidence
toward PLAT-14.2 and PLAT-19.1. It is a *live* harness: it creates real
`Backup`s against a real Kafka, writes real objects into a real MinIO, and
reads the verdicts the controller wrote. A fixture-only run of it proves
nothing and it has no fixture mode.

## What it needs

- A docker-desktop cluster running the chart's controller. Every command it
  issues carries `--context docker-desktop`; it names no other context.
- The shared `logweir-scram-local` lab: it copies that namespace's
  `source-scram`, `logweir-s3`, `logweir-signing-key` and `minio-root` Secrets
  — and, for `old-archive`, `target-scram`, the scratch broker's own SCRAM
  credential — into its own namespace and dials that namespace's Kafka and
  MinIO by service DNS. **Outside its own namespace it writes exactly two
  things**: the cluster-scoped `TrustPolicy` the trust phases need (deleted
  only after an owner-label check), and, in `old-archive`, topics on the shared
  `kafka-target` broker — `old-archive`'s restored topic under the prefix
  `<owner>-<stamp>-hist-` — which the phase deletes again after an
  ownership check (see below). `old-archive`'s row requires the Restore to
  finish `Succeeded`/`pass` with a Valid scorecard and the restored topic
  holding exactly the records the Backup archived; `Running` is not a restore. Its plan's RPO objective is the age of the subject Backup's own covered window plus one hour, because the shared lab's seed records are days old and a one-day constant made every such restore a signed `fail-objective` (lab-refresh-8). No phase of THIS harness changes the shared release; D2's `evf5`
  (lab-refresh-8's rows, below) scales its controller and restores it.
- `openssl` on the host, for the phases that mint a keypair (`old-archive`,
  `multiple-namespaces`).
- For `old-archive`: the lab roster's approver PRIVATE key, `approver.pem`,
  read from `$LOGWEIR_SCRAM_OUT`, else `$HOME/.logweir-lab/scram-e2e` (the lab's
  durable home since 2026-09-22), else the `/tmp/logweir-scram-e2e` symlink to
  it. It must be the private half of `TrustRoster/default.spec.approverKeys[0]`
  — after a lab rebuild that is the NEW approver key, and a private half left
  over from the previous lab is refused by the cluster.
- `busybox:latest` present on the node (`imagePullPolicy: Never`), and the
  MinIO client mirror `d3_live.py` pins by digest — present, or pullable
  (`imagePullPolicy: IfNotPresent`; upstream's `mc` images were withdrawn, see
  `third_party/minio-mirror/README.md`).
- **An image carrying `logweir-retention`**, for the five enforcement phases
  only. The product runner image now carries it, so the default is the
  `LOGWEIR_RUNNER_IMAGE` the shared controller itself names — see
  §"The enforcer's image". `--retention-image` overrides it to measure a build
  the lab is not running; a phase records **NOT-RUN** only when neither can be
  resolved.
- A python3 of version 3.12 or newer with no third-party packages (the shared
  venv `/tmp/logweir-roadmap-run/venv/bin/python3` on the lab host; the macOS
  `/usr/bin/python3` is 3.9 and fails at the first PEP 701 f-string).

## Running it

```bash
export LOGWEIR_D3_STAMP=$(date -u +%Y%m%dt%H%Mz)
export LOGWEIR_D3_OUT=/tmp/d3-live/$LOGWEIR_D3_STAMP
RET=logweir:scram-local            # optional; the default is the controller's
                                   # own LOGWEIR_RUNNER_IMAGE — see below

python3 e2e/k8s/d3/d3_live.py setup            # namespace, destinations, buckets
python3 e2e/k8s/d3/d3_live.py catalog          # PLAT-15.1: reconstruction after CR loss
python3 e2e/k8s/d3/d3_live.py catalog-cases    # duplicate identity, Missing, stale index, format
python3 e2e/k8s/d3/d3_live.py catalog-scale    # PLAT-15.1: 104 REAL points past `viewLimit`
python3 e2e/k8s/d3/d3_live.py catalog-access   # PLAT-15.1: partial access, corrupt, future major
python3 e2e/k8s/d3/d3_live.py retention        # PLAT-16.1: two destinations, honest reports
python3 e2e/k8s/d3/d3_live.py legal-hold       # PLAT-16.2: spec.holds[]
python3 e2e/k8s/d3/d3_live.py packaging        # proves the shipped image DOES carry the enforcer
python3 e2e/k8s/d3/d3_live.py preview                 --retention-image $RET
python3 e2e/k8s/d3/d3_live.py no-evidence-credential  --retention-image $RET
python3 e2e/k8s/d3/d3_live.py enforce                 --retention-image $RET
python3 e2e/k8s/d3/d3_live.py wrong-prefix            --retention-image $RET
python3 e2e/k8s/d3/d3_live.py denied-deletion         --retention-image $RET
python3 e2e/k8s/d3/d3_live.py trust            # CLUSTER LOCK (TrustPolicy is cluster-scoped)
python3 e2e/k8s/d3/d3_live.py signed-at-probe  # CLUSTER LOCK
python3 e2e/k8s/d3/d3_live.py notify           # PLAT-14.2: staleness, dedup, transport
python3 e2e/k8s/d3/d3_live.py protection-cases # PLAT-14.2: recovery, unavailable archive, scope
python3 e2e/k8s/d3/d3_live.py protection-verdicts  # PLAT-14.2: PointFactsUnread, a refused
                                               #   signature, D3 §3.3's resolve column
python3 e2e/k8s/d3/d3_live.py operation-states  # PLAT-14.1: mount failure (ConfigMap and
                                               #   Secret), unschedulable pod, engine crash
python3 e2e/k8s/d3/d3_live.py notify-transport  # PLAT-14.2: a webhook scaled to zero
python3 e2e/k8s/d3/d3_live.py object-lock       # PLAT-16.2: a lock-enabled bucket with a legal
                                               #   hold, and a plain bucket then versioned:
                                               #   the controller's own Enforce Job refuses both
python3 e2e/k8s/d3/d3_live.py shared-set        # PLAT-16.2: two receipts over one backup set
python3 e2e/k8s/d3/d3_live.py control          # the negative control — it MUST fail
python3 e2e/k8s/d3/d3_live.py report
python3 e2e/k8s/d3/d3_live.py cleanup          # CLUSTER LOCK (deletes the TrustPolicy)
```

`catalog-scale` and `catalog-access` each create their OWN bucket and destination (`-c`, `-d`)
rather than reusing dest-b: `retention` asserts dest-b holds exactly its six points, and a
corrupt manifest planted there would fail a phase that never asked for one. `catalog-scale`
runs 104 real Backups, which is the only honest way past `viewLimit`'s CRD floor of 100 —
nothing here edits that floor. `protection-cases` needs `catalog` (for the view it refreshes)
and `notify` (for the namespace's first alert), and both are declared in
`PHASE_PRECONDITIONS`.

`notify` and `protection-cases` each create the point their policy measures, as a manual run
OF a `BackupSchedule` (`spec.scheduleRef.{name,uid}` — D3 §3.2: "the `spec.scheduleRef.uid`
field is the authority and the label is the index"). Without it `identity::is_run_of_schedule`
excludes a manual `Backup` from a policy naming `scheduleRefs`, the policy has an EMPTY
candidate set, and `Unprotected` is an answer about nothing — which `status.health` cannot
distinguish from the real verdict, so all three rows carry `selector_matched_a_run`. They name
DIFFERENT schedules (`keeps-running`, `recovery-runs`) so neither selects the other's point.
`notify` also ages its point past the objective, because `catalog` deletes every dest-a CR to
prove reconstruction and the `Stale` arm had nothing to be stale about; that makes the phase
take about six minutes.

The rehearsal phases (D3 §15 L6, `refused-point`, `rehearsal-faults`, `rehearsal-deleted`,
`rehearsal-verdict-not-reached`, `rehearsal-deleted-active`) are withdrawn in v1 with
`RehearsalSchedule` (release-notes item 73).

**No row revokes a shared key for compromise** (TRUSTPOLICY-DELETE-DROPS-REVOCATION). A
controller with the compromise guard applies a `KeyCompromise` revocation in every
namespace and holds the recording policy's deletion while any other trust source lists the
key, so revoking the lab signer — which `trust`'s revocation row first did — would make
every lab backup `Untrusted` and leave this run's policy `Terminating` for ever
(`TrustRoster/default` lists that key). `trust-revocation-flips-a-terminal-badge` therefore
mints a signer, adds it `Active`, signs one run with it, and revokes it
(`compromise_row_keys`, which refuses any key `TrustRoster/default` or another run's
policy lists). The key is listed by this run's policy alone, so the policy is released
the moment it is deleted. `test_rows.py` pins the refusal offline.


The offline tests: `python3 -m pytest e2e/k8s/d3 e2e/k8s/d2`. `test_rows.py` drives every
row's predicate over planted-wrong fixtures.

`protection-verdicts` needs only `setup`: it creates its own three policies, its own two
catalogs, its own legacy destination and every Backup it measures, because its rows assert
exact alert ledgers and an incident another phase opened on the same policy would make
every count in them somebody else's. Three of its rows are written against the CORRECT —
fixed — behaviour of `PROTECTION-SECRETKEYS-UNPROTECTED`, so on a controller image that
predates the fix they FAIL, and that failure is the defect's live reproduction; each row's
detail says which of the two a reader is looking at, and `verdicts/controller.json` records
the image's `org.opencontainers.image.revision`. It writes no cluster-scoped object: the
refused signature is produced by SIGNING with a key the installation has never heard of
(this namespace's `logweir-signing-key`, restored afterwards) rather than by revoking a
known one, which would need a `TrustPolicy` and the cluster lock.

Who this run is, and where: `LOGWEIR_D3_OWNER` (default `d3w14`) is the
`logweir.dev/test-owner` label `cleanup` checks, the prefix of the buckets and of the shared
MinIO's minted user and policy — the shared MinIO has no namespaces, so two workers running
this harness at once must not share it. `LOGWEIR_D3_NS` names the namespace (default
`<owner>-<stamp>`).

`--retention-image <ref>` may also be given as `LOGWEIR_D3_RETENTION_IMAGE`.
Each phase appends to `$LOGWEIR_D3_OUT/state.json`, so they run one after
another in one namespace and `report` renders the whole table. The order above
matters in three places: `catalog` before `catalog-cases`, `retention` before
the enforcement phases (they plan against its points), and
`no-evidence-credential` before `enforce` (it asserts that nothing was deleted,
which is only meaningful while the plan's objects are still there). Run
`control` before `cleanup` so the negative control gets its live read.

## The cluster lock

`trust`, `signed-at-probe` and `cleanup` touch one
cluster-scoped
object (the `TrustPolicy`, which binds only this run's namespace). Take the
orchestration's lock around exactly those phases and release it immediately
afterwards. Every
other phase is namespaced and needs no lock, and **no phase changes the shared
release** — not its image, not its env, not its CRDs. `packaging` reads the
controller's `LOGWEIR_RUNNER_IMAGE`; it never writes it.

## lab-refresh-8's rows (D2 and D3)

The rows `claude/evidence-fetch`, `claude/verdict-precedence` and
`claude/fix-standing-verify` owe live proof for, and the D2/D3 rows whose
expectation those branches changed. Each is a named phase; each row judges a
pure predicate that has planted-wrong twins in `e2e/k8s/d2/test_evidence_fetch_rows.py`
(`python3 -m pytest e2e/k8s/d2 e2e/k8s/d3`). D3's refused-point rows are withdrawn with
the rehearsal phases.

**Images and binaries.** Everything is built from main AFTER both
`claude/evidence-fetch` and `claude/verdict-precedence` have landed, with
`--label org.opencontainers.image.revision=$(git rev-parse HEAD)` (WORKER-RULES):

- the **controller** (`weirkeeper`) the lab release runs — the evidence-fetch
  Job, the `Pending` verdict and the Backup-verdict join in retention live there;
- the **runner** (`logweir`, `--platform linux/amd64`) the controller names in
  `LOGWEIR_RUNNER_IMAGE` — the evidence-fetch Job is a `check run` of it, and
  it prints the receipt digest the fetch is anchored on;
- the **`logweir` CLI** as `LOGWEIR_BIN`, for D2's approvals.

D2 runs in its own namespaces (`d2_live.py setup` first; `D2W14_OUT`,
`D2W14_STAMP`); D3 runs after `setup` in `LOGWEIR_D3_NS` as above. Use
`LOGWEIR_PYTHON=/tmp/logweir-roadmap-run/venv/bin/python3` for both.

| row | phase | command | PASS requires |
|---|---|---|---|
| `S1.statusVerification` (swept) | `s1b` | `d2_live.py s1 s1b` | bk-a and bk-b (SecretKeys `evidenceRead`) read **`Valid`** within 240 s: `matchedKeyId`, `observation.jobRef.name` = `lwc-ev-<sha256(uid:attempt)[:20]>`, `mode: SecretKeys`, `presence: Complete`, no `retryAfter`, `windowCovered`, `records`, `capture`, `Verified=True`, runner `receiptSha256`, and no controller-identity location allowlisted. `NotAttempted` here is EVIDENCE-FETCH-JOB-UNBUILT again |
| `S1.notAttempted` | `s1c` | `d2_live.py s1c` | for dest-noread (no `evidenceRead`) AND dest-ci (`ControllerIdentity`, not allowlisted): `NotAttempted` with a detail, no `matchedKeyId`, no `observation`, never `Pending` on the watch, no `lwc-ev-` Job by `logweir.dev/check-owner-uid`, no `windowCovered` |
| `EVF-1` | `evf` | `d2_live.py evf` | dest-arg (`evidenceRead: ArchiveReadGrant`, `archiveRead` = its own read-only principal): the watch sees `Pending` naming attempt 1's Job, then `Valid` and nothing else; every `S1.statusVerification` clause holds |
| `EVF-2` | `evf` | (same run) | the Job: `logweir.dev/check-kind=evidenceFetch`; controlled by the Backup; `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` only, both from `a-archread` (never `a-writer`, never `logweir-signing-key`); no `LOGWEIR_EVIDENCE_AWS_*`, no `envFrom`; `automountServiceAccountToken: false`; `logweir-runner`; volumes exactly `check-plan` (= `<job>-plan`, immutable, controlled by the Backup) and `work`; the REAL pod controlled by the Job's UID; `ttlSecondsAfterFinished: 600` |
| `EVF-1.ttl` | `evf` | (same run) | the Job EVF-2 recorded (same uid, TTL 600, completed) is last seen within 30 s of `completionTime + 600` and gone by +120 s, while its owner Backup and a long-TTL Job in the namespace still exist |
| `EVF-4` | `evf4` | `d2_live.py evf4` | dest-evbroken's `evidenceRead` Secret lacks `secret-access-key`: attempt 1 `NotAttempted` naming `lwc-ev-…(uid:1)` and `CredentialSecretKeyMissing`, "attempt 2 starts at", `retryAfter` 45–90 s after `verifiedAt`; never `Valid`, no window; attempt 2's own Job `lwc-ev-…(uid:2)`, controlled by the Backup, and the status at attempt 2 |
| `EVF-5` | `evf5` | `d2_live.py evf5` — **CLUSTER LOCK** (scales the shared `weirkeeper` to 0 and back; the original replica count is restored in a `finally` and recorded in `objects/evf5/controller-restored.json`) | `Pending` seen, then the controller stopped; the Job finished with none running; after the restart exactly ONE `lwc-ev-` Job for the Backup (attempt 1's, created before the stop), `Valid` naming that Job's UID and written after the new pod started. `notRun` — never `pass` — when the fetch beat the stop |
| `EVF-6` | `evf6` | `d2_live.py evf6` (after `s1`, whose bk-a it restores) | a Restore whose evidence destination (dest-b) has a `SecretKeys` `evidenceRead`: `Pending` then `Valid`, `outcome` and `objectives` copied, `scorecardSha256`, `Verified=True` exactly when `outcome: pass`, the Job controlled by the Restore |
| `EVF-3` (console Restore) | — | `scripts/plat10-ui-e2e.mjs` (not this harness; its NotAttempted assumption is in "Class sweep owed" of `harness-rows-10.result.md`) | the PLAT-10 journey reaches the wizard for EVF-1's point |
| `protection-catalog-places-an-unverified-point`, `protection-unplaceable-point-is-unknown`, `protection-unknown-keeps-its-incident-open` (swept) | `protection-verdicts` | `d3_live.py protection-verdicts` | unchanged clauses, now on `dest-noread`/`dest-noread-stays` (bucket `-n`, no `evidenceRead`), the one shape still `NotAttempted` with no capture |

**Not built, and why.** verdict-precedence's fix round adds two more rows: a
`/points` page served `backupVerdictsIncomplete: Unavailable` after the API's
`list` on `backups` is removed, and a Backup whose verdict field is unreadable.
The loopback API uses the operator's kubeconfig, not the chart's ServiceAccount,
so the first needs a chart install with an edited Role; the second has no
product path at all (only a status patch writes a non-string verdict). Both are
covered by the branch's own crate tests and are listed as gaps rather than
built as fault injection.

## harness-rows-11's rows (PLAT-14.1, 14.2, 16.2)

The tests lab-refresh-8 §5 found with NO committed row. Each is a named phase whose
row judges a pure predicate with planted-wrong twins in `test_rows.py` (the mount
and object-lock twins plant the exact shape the lab published on `f49849d`). Every
fault is induced from this run's own namespace, bucket and MinIO users; nothing
about the shared release changes. The console-view clauses read
`GET /api/v1/namespaces/{ns}/operations/{kind}/{name}` from a host `logweir-api`
(`LOGWEIR_API_BIN`, else the newer of `target/{release,debug}/logweir-api`); with
no binary those clauses read `None` and the row FAILS rather than passing on the
object alone.

| row | phase | PASS requires | negative control |
|---|---|---|---|
| `ops-mount-failure-is-volume-mount-failed` | `operation-states` | a Backup over a `KafkaCluster` whose `tlsCa.configMapKeyRef` names an absent ConfigMap: while waiting `progress.stage=Preparing`, `RunnerReady=False/VolumeMountFailed`, a `VolumeMountFailed` diagnostic about the runner Pod naming the ConfigMap, the operation view `preparing`/`VolumeMountFailed`; fail-fast collapses the Job deadline; terminal `Failed` with the `Failed` condition reason `VolumeMountFailed`, no exit code, view `failed`/`VolumeMountFailed`/`error` (D3 §13: "the terminal reason is the diagnostic") | twin: `f49849d`'s `NoExitCode` after the recorded diagnostic, no deadline patch, an unnamed object, a run that only said `WaitingForPod` |
| `ops-mount-failure-secret-is-volume-mount-failed` | `operation-states` | the same, for the projected `signing` Secret (this namespace's `logweir-signing-key` removed for one run, restored after): diagnostic `SigningKeyMissing`, `RunnerReady`/terminal reason `VolumeMountFailed` | twin: the Secret's shape judged for the ConfigMap's code |
| `ops-unschedulable-pod-is-pod-unschedulable` | `operation-states` | a namespace `LimitRange` defaulting 512Gi (deleted once the pod carries it): the pod asked for it and was `Unschedulable`; `RunnerReady=False/PodUnschedulable` with a Pod diagnostic; view `preparing`/`PodUnschedulable`; the Job keeps its own `activeDeadlineSeconds` (480, longer than fail-fast's 300 + 60 grace, so "no fail-fast" can fail); terminal `PodUnschedulable`, no exit code, view `failed`/`PodUnschedulable` | twin: a fail-fast patch, a terminal `NoExitCode`, a pod without the request |
| `ops-engine-crash-is-failed-operational` | `operation-states` | a destination whose `archiveWrite` user is denied `s3:PutObject` on SEGMENT keys only: the runner started (`RunnerReady=True/RunnerStarted`), the pod log carries `kafka-backup backup exited` and an `AccessDenied` on a `/topics/` key (the engine had read records), `Failed`, `exitCode: 1`, `exitReason: operational`, no receipt; view `failed`/`error`/`exitCode 1`/`noEvidence`; the fixture control (same source and topic, full grant) Succeeded | twin: exit 3, a manifest-only refusal, a runner that never started, a failed control |
| `notify-transport-failure-is-recorded-and-rewrites-nothing` | `notify-transport` | a policy whose only webhook is a Service with no endpoints, over a member point aged past the objective: one `Staleness` alert, `delivery.state: Failed` after exactly 3 attempts, `lastError` naming `webhook:failed`, `NotificationsDelivered=False/DeliveryFailed`; three delivery Jobs of one transition, each exit 1 with `notify-result=webhook:failed` and `transport error`, created ≥ 60 s then ≥ 300 s apart; no fourth; the health the alert opened on unchanged; every Backup's `resourceVersion` unchanged (`every_backup_unchanged`, the same comparison `notify-delivery-never-rewrites-a-backup` makes); the insecure-sink hatch open (else the failure is the https-only refusal) | twin: a rewritten Backup, a shut hatch, a fourth attempt, a rushed retry, a delivered alert, an https refusal, a relabelled verdict |
| `preflight-check-plan-conflict-after-pending-is-recorded` | `reservation-writes` | (lab-refresh-11; reserve-commit class sweep) a DestinationAccess Preflight held `Queued` by suspended check-labelled Jobs while a foreign `<lwc-da-…>-plan` ConfigMap is placed; released: `Pending/PodNotStarted` then `Failed/CheckPlanConflict` within 2 s (one pass), terminal, no failed-reconcile line, no Job | twins: the unfixed build's error requeue, left Pending, a Job created |
| `backup-job-name-conflict-over-a-frozen-run-is-recorded` | `reservation-writes` | (lab-refresh-11) a ResourceQuota at the namespace's Job count lets the freeze pass record `status.execution` while the runner Job POST is refused; a suspended ownerless Job of the Backup's name is then placed: the Backup is `Failed/JobNameConflict` with `status.execution` kept, the stranger neither adopted nor run, no 409 in the controller log. The in-pass freeze→POST 409 stays kube-mock only | twins: adopted, execution lost, a 409 logged, the Backup ran |
| `retention-object-lock-provider-refusal-is-recorded` | `object-lock` | **FLIPPED at lab-refresh-9** (OBJECT-LOCK-DELETE-MARKER, `62ef1a1`/`fd17eb2`/`de14881`). A bucket made `--with-lock`, three points, a legal hold ON on every object of the oldest, a `mode: Enforce` policy (`keepLast 1`, `requireApprovedPlan: false`, a delete principal with `s3:GetObject`): the controller's own run finishes with exit 1, `lastEnforcement.deleted` EMPTY, `failed` naming BOTH the held and the control point `VersionedBucket`; no delete marker anywhere under either set and every object still its key's latest live version; the next evaluation does NOT protect the held point `LegalHold` (it stays a candidate); three runs degrade the policy (`EnforcementDegraded=True` naming `VersionedBucket` and the remedy "Enforce on an unversioned bucket"), `guarantees.ageExpiry: NotEnforced`, `legalHold: ProviderEnforcedUnverified`. The provider's own semantics are recorded beside it (`lock/00-provider.json`) | twin: `f49849d`'s delete-marker shape recorded `Deleted`; the pre-flip expectation (control deleted, held `Locked`, `LegalHold`); the held point alone; exit 0; a marker anywhere; never degraded; `ageExpiry` still `LogweirEnforced`; a bucket without lock |
| `retention-versioning-enabled-after-write-is-refused` | `object-lock` | ctl-batch-2's H1 shape: a PLAIN bucket, three points written into it (null version ids), THEN `mc version enable`, then the same policy: every clause of the row above, with the fixture clause "written unversioned, Enabled before the run" in place of the hold | the same twin (one rule) |
| `retention-shared-set-is-never-planned-under-a-retained-point` | `shared-set` | one Backup's Job run twice from its frozen inputs (two receipts, one backup set, `catalog-duplicate-identity`'s technique), a Report policy `keepLast 1`: no candidate shares its `backupId` with a retained point. `NOT-REACHED` unless both points are Available and Verified | twin: a candidate over a retained point's set, both planned together, one unusable |

`notify-transport` must run with no other phase creating, deleting or verifying Backups in
the namespace: its no-rewrite clause compares EVERY Backup's `resourceVersion` across
the window, so a concurrent phase's churn fails it for the harness's reason (measured:
run alongside `operation-states`, it failed on exactly the Backups that phase recreated).

`object-lock` releases
every legal hold it placed and removes every version and both buckets in its
`finally` (`lock/99-cleanup.json`, `ver/99-cleanup.json`); `cleanup`'s `rb --force` cannot remove a bucket
with held versions.

## The enforcer's image

`crates/weirkeeper/src/controllers/retention_policy.rs` renders every
enforcement Job's command as `logweir-retention` and takes that Job's image from
the controller's `LOGWEIR_RUNNER_IMAGE`.

**RET-NOIMAGE was that no image this repository built contained that binary** —
the product `Dockerfile` ran `cargo build … -p logweir` and copied
`/usr/local/bin/logweir` alone, so `mode: Enforce` produced a Job that died at
the kubelet with exit 127, `executable file not found`, which the `packaging`
phase proved live.

**That defect is closed.** `Dockerfile` builds `logweir-retention` beside
`logweir`, and `scripts/check-image.sh` check 7 refuses a runner image that does
not carry it. `packaging` now proves the inverse from the same probe: the
container starts and exits `3`, `logweir_retention::EXIT_REFUSED` — the
enforcer's own refusal when it is given no plan, which only a binary that exists
and resolves by bare name through `$PATH` can produce. The row is named
`retention-enforcer-ships-in-the-runner-image`; it was
`retention-enforcer-ships-in-no-image` and asserted `not found` here. So the
enforcement phases default to the controller's own `LOGWEIR_RUNNER_IMAGE` and
need no hand-built image.

`Dockerfile.retention` beside this file stays as the *test* recipe for an
enforcer image the lab is not running — a build under review, or one from
another commit — reached with `--retention-image`. It starts from the product
`Dockerfile`'s own `builder` stage rather than copying it, so there is no second
home for the cross-compile setup to drift in, and it changes neither the product
`Dockerfile` nor the chart. Nothing in `justfile` or CI references it:

```bash
docker build --platform linux/amd64 --target builder \
  -f Dockerfile -t logweir-builder:d3w14 .

docker build --platform linux/amd64 --load \
  -f e2e/k8s/d3/Dockerfile.retention \
  --build-arg BUILDER_IMAGE=logweir-builder:d3w14 \
  --build-arg RUNNER_IMAGE=logweir:scram-local \
  --label org.opencontainers.image.revision="$(git rev-parse HEAD)" \
  -t logweir:scram-local-d3w14 .
```

The resulting image is the shipped runner plus one binary, so the enforcement
rows are about the enforcer and not about a runtime that differs from the
product's. `--label …revision` is not decoration: a locally built image has no
other way to say which commit it came from, and `WORKER-RULES.md` requires it.

**The enforcement scenarios are operator-run by design**, not a swap of the
shared controller's image. `docs/kubernetes.md` §7f says the real per-candidate
object count comes from an operator running `logweir-retention run … --dry-run`;
this harness runs that, and the enforced pass beside it, as Jobs in its own
namespace against the plan it wrote. Those five phases record NOT-RUN only when
no image can be resolved at all — neither an override nor the controller's own
`LOGWEIR_RUNNER_IMAGE` — which is the honest verdict, and not a skip.

## Safety rules it enforces in code, not in prose

- Every object it creates carries `logweir.dev/test-owner=<owner>`; `cleanup`
  re-reads the namespace's label AND compares the UID it recorded at `setup`
  before deleting anything, and refuses a bucket whose name is not this run's.
  Every phase that deletes a cluster-scoped `TrustPolicy` checks that label
  first and refuses another run's policy of the same name
  (`delete_owned_trust_policy`).
- Retention enforcement only ever runs against the buckets this run created.
  The fixture's `kafka-backups` is written (the legacy-archive Backups the
  Backup-level evidence verdict needs live there) and never deleted from.
- The one credential the harness mints — the read-only MinIO user the
  denied-deletion case needs — is generated per run, registered in `MINTED`, and
  scrubbed out of every recorded argv, every captured stream and every artifact.
  `redact` runs over both before either is written.
- `report` re-reads every artifact and fails the run on anything
  credential-shaped **or on any minted value**, and `sweep_selftest` plants both
  shapes first and fails the run if the sweep misses either — so the clean sweep
  means something.
- `control` asserts something false about a **live** read of the catalog's
  pages, through the same digest-checked path the catalog rows use, and fails
  the run if it passes.
