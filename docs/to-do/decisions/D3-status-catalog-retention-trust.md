# D3 — Operation status, protection health, rehearsals, recovery catalog, retention enforcement and trust lifecycle

Decision record (spike outcome) for PLAT-14.1, PLAT-14.2, PLAT-14.3, PLAT-15.1, PLAT-15.2,
PLAT-16.1, PLAT-16.2 and PLAT-19.1, plus the ADR 0008 Amendment A kind decision they need.

- Date: 2026-09-15. Repository: `/Users/admin/Desktop/Repos/logweir`, main `4956785`.
- Read-only investigation. No repository, tracker, cluster or deployment change was made.
- Builds on the adopted PLAT-17 decision (`/tmp/plat17-contract-decision.md`, "P17" below):
  `logweir-api` with `/api/v1`, application-managed authorization, no Kubernetes proxy, no
  object-store or Kafka client in the API, controller as final gate, ordinary vs governed
  approval (PLAT-19.2), key-usage separation, normalized operation states.
- Designs around in-flight, unmerged work: PLAT-06.1 (`Backup.status.execution{id,inputsRef,
  inputsSha256}`, seen in `/tmp/logweir-roadmap-run/claude/plat06-codex-interrupted.patch`),
  PLAT-07.1 (shared connection resolver), PLAT-08.1's `BackupDestination` (named by decision D2,
  which also adds `TopicDiscovery` and `Preflight`), PLAT-17.1
  (`crates/logweir-api`), PLAT-01/02 closure (per-Restore immutable bundle, persistent identity).
- "MUST", "never" and exact names below are the contract handed to implementation workers.
  Every field listed is additive unless explicitly called a breaking change.

## 0. Decisions in one page

1. **State model (PLAT-14.1).** The controller writes a bounded, controller-derived
   `status.progress` block plus a `RunnerReady` condition on `Backup` and `Restore`; the API maps
   status only (never pods/logs) to P17's normalized `state`, with `result`, `evidence`
   verification and `verificationScope` as separate fields. Pod scheduling, image, credential and
   volume-mount failures become resource-scoped diagnostics read by the controller from pod status
   and core `events` (new RBAC `list events`), with a fail-fast path for non-transient waits
   before any container starts. Refresh resumes from the durable CR; SSE per P17, polling fallback
   in the legacy localhost UI, both cancelled on navigation. Completed-Job TTL is repaired on
   terminal objects that missed it.
2. **Protection health (PLAT-14.2).** New namespaced kind `ProtectionPolicy` holds the objective
   (`maxRecoveryPointAgeSeconds`, failure threshold, verified-evidence requirement) and
   notification routes. Freshness is measured from the signed receipt's capture start, not from
   the newest record timestamp. Notifications reuse the existing `EventSink`/PagerDuty dedup
   mechanism (`crates/logweir/src/drill/phase7_verify.rs:1589-2000`), moved to a shared module and
   executed by a short-lived `logweir notify deliver` Job; alert state and delivery results live
   only on `ProtectionPolicy.status`, so a failed notification can never rewrite a Backup result.
3. **Rehearsals (PLAT-14.3).** New namespaced kind `RehearsalSchedule` (spec sealed except
   `suspend`) creates ordinary `mode: scratch` `Restore` objects through the normal admission,
   bundle, runner and evidence path. Authorization is a *standing rehearsal authorization*
   (authorization document v2, subject `RehearsalSchedule` UID, signed scope digest, max 90 days)
   under the namespace's PLAT-19.2 policy; the controller and runner both prove each rendered plan
   is inside the signed scope. Cleanup is runner phase 9 over exact mapped names under a
   per-schedule prefix; a leftover topic blocks the next run instead of being deleted.
4. **Recovery catalog (PLAT-15.1/15.2).** Durable, append-only, create-only metadata in object
   storage under `logweir/catalog/v1/`, written by the backup runner (already the only
   `logweir/` writer and signer). Point identity is content-derived from the signed receipt
   (`lwp1-` + 32 hex of `sha256(receipt bytes)`). A new namespaced kind `RecoveryCatalog` runs
   bounded sync Jobs whose output is materialized as a bounded Kubernetes view (newest ≤ 5000
   points, counts, histogram) in immutable ConfigMaps owned by the sync Job and garbage-collected
   through Job TTL, so no controller `delete` is introduced. Availability and verification are
   separate axes. Disaster restore on a fresh installation connects a destination, imports points,
   shows signers as `UntrustedSigner` until an administrator adds the key to a `TrustPolicy`
   after an out-of-band fingerprint check, and restores with the point bound into plan bytes
   (execution contract v2) and re-verified by the runner.
5. **Retention (PLAT-16.1/16.2).** New namespaced kind `RetentionPolicy` per destination.
   Reporting moves from the global controller handle to the destination's catalog view.
   **The supported enforcement boundary is an isolated, optional Logweir retention worker**
   (`logweir-retention` binary, new `logweir-reaper` crate, separate ServiceAccount and
   delete-capable credential scoped to the archive prefix) with preview, admin-approved plan hash,
   controller-held lease against active restores, minimum usable points, shared-segment and hold
   protection, wrong-prefix rejection, bounded retry and signed deletion records. External bucket
   lifecycle is supported only as a *declared*, report-only mode whose unsupported guarantees are
   rejected, never claimed. The controller, `logweir-store`, `logweir` and `logweir-api` stay
   delete-free (amended G-RET gate).
6. **Trust lifecycle (PLAT-19.1).** New cluster-scoped kind `TrustPolicy` replaces the implicit
   `TrustRoster/default`: explicit namespace bindings, append-only key entries with usage
   (`EvidenceSigning` | `GovernedApproval` | `ConsoleConfirmation`), validity, and monotonic
   lifecycle (`Active → Retired → Revoked`) enforced by CEL transition rules. Retirement keeps old
   evidence valid (`trust.basis: Historical`); compromise revocation invalidates evidence without an
   independent pre-revocation observation. Missing policy synthesizes `legacy-roster-v1` from the
   existing roster, so upgrade changes nothing until an administrator applies a policy.
7. **Kinds (Amendment A).** Five new kinds — `TrustPolicy` (cluster), `ProtectionPolicy`,
   `RehearsalSchedule`, `RecoveryCatalog`, `RetentionPolicy` (namespaced) — recorded as ADR 0008
   **Amendment G**, with **Amendment H** (storage deletion boundary) and **Amendment I** (execution
   contract v2) beside it; D2 owns Amendment F (§8, seam S8). `TrustRoster` becomes
   deprecated-but-served; no kind is removed.
8. **Cross-decision seams.** §17 records compliance with `D-SEAMS.md` and the points where D1 and
   D2 refine this design (one check runner, one execution-inputs grammar, D2's evidence fetch and
   waiting-code table, D1's schedule membership and retry semantics).

## 1. Current behavior this decision starts from (grounded in main `4956785`)

Status and execution:

- `Backup` has phases `Running/Succeeded/Failed` only; a running Job gets one `JobCreated`
  condition and nothing about its pod (`crates/weirkeeper/src/controllers/backup.rs:1134`,
  `running_status_patch`; reconcile step 2 at `:1957`). The pod is inspected only after the Job
  finishes (`crash_terminal_state`, `:846`), so a pod stuck in `ContainerCreating` on a missing
  ConfigMap/Secret waits for `activeDeadlineSeconds` and ends as generic `NoExitCode`
  (`docs/kubernetes.md` §10 "The crashed Job").
- `Restore` adds `Pending` + `Admitted=False/ApprovalNotVerified` holds and a scalar
  `status.reason` (`crates/weirkeeper/src/crds/restore.rs:255-290`), same running blindness.
- Terminal objects are never re-read (`backup.rs` step 2b, before `find_pod`; `restore.rs:2997`).
  The TTL patch (`TTL_SECONDS_AFTER_FINISHED = 604_800`, `backup.rs:110`) is sent after the
  terminal status; if that patch fails, step 2b means it is never retried.
- Evidence verification happens once, in a second status patch, through the single global
  read-only handle and `TrustRoster/default.spec.signingKeys` (`verification.rs:315`
  `verify_evidence`, `:703` `verify_oracle`, `backup.rs:2262`, `restore.rs:3276`). Results are
  `Valid | Invalid | NotAttempted` (`crds/mod.rs:165-201`). Badges: Backup green = Valid ∧ exit 0,
  Restore green = Valid ∧ outcome pass (`ui/pages/backups.js:96`, `ui/pages/history.js:59`).
- Controller RBAC: no Secret verb, no `delete` anywhere, `pods` `list`, `pods/log` `get`,
  `configmaps` `create/get`, `backups` `create` (`config/rbac/role.yaml:96-183`, mirrored by
  `charts/logweir/templates/clusterrole.yaml`). No `events` access.
- The UI calls the Kubernetes API directly through `ui/api.js` (single request site) and reads
  once per mount; there is no polling or streaming (`ui/pages/history.js:227-254`,
  `ui/lifecycle.js`).

Protection, notifications, retention:

- `BackupSchedule.status` shows scheduling facts (`lastFireTime`, `nextFireTime`,
  `lastMissedSlot`, `Ready`) and nothing about whether a recoverable point exists
  (`crates/weirkeeper/src/crds/backup_schedule.rs:307-347`).
- The only notification mechanism is the drill runner's: `notify_body` (`phase7_verify.rs:1589`),
  bounded agent (`NOTIFY_TIMEOUT`, `:1632`), `EventSink` seam (`:1679`), https-only PagerDuty
  endpoint (`:1736`), `dedup_key`/`failure_dedup_key` (`:1786`, `:1824`), trigger/resolve and
  transport failures swallowed without changing the exit code. Credentials are plaintext in the
  drill spec (`crates/logweir-core/src/spec.rs:107`, decision O17 not funded,
  `docs/formats/drill-spec.md`). Backups have no notification path.
- Retention reports, never deletes. The schedule controller evaluates with the ONE global handle
  built from `LOGWEIR_ARCHIVE_URL` (`retention.rs:91`) but lists the *schedule's* URL prefix inside
  that handle (`backup_schedule.rs:1421-1435`, `bucket_and_prefix(&archive_url).1`). A schedule
  writing to another bucket therefore gets a report about the controller's bucket — the
  "another destination's catalog" defect PLAT-16.1 names.
- `Store` has no delete method; writes are create-only and only under `logweir/`
  (`crates/logweir-store/src/lib.rs:149`, `:680`); `list_keys` is unbounded and sorted in memory
  (`:382`); object-lock readback always returns `None` (`:421`). G-RET
  (`scripts/check-no-archive-write.sh`) forbids deletes in the control plane and the store crate.

Catalog-relevant facts:

- Evidence lives at the destination bucket root `logweir/`, independent of the archive prefix
  (`crates/logweir/src/backup/mod.rs:809` `evidence_location`). Receipts:
  `logweir/backups/<backup_id>/<run_id>.receipt.{json,sig}` (`backup/phase_run.rs:222`).
  Scorecards/teardown: `logweir/drills/<run_id>.*` (`drill/phase9_teardown.rs:295`).
- Scheduled `backup_id = <schedule uid>-<slot>` (`crates/weirkeeper/src/slot.rs:624`); manual
  Backups use the object UID (`backup.rs:308` `plan_backup_id`). Manifests are
  `<prefix>/<backup_id>/manifest.json`; windows come from manifest bodies
  (`lib.rs:474` `manifest_facts`).
- A Restore does not reference a `Backup` object: it carries `sourceArchive`, `backupSetRef`,
  `pointInTime` and opaque `planBytes` (`crds/restore.rs:208-231`), and the runner never contacts
  the source cluster (`docs/stability.md`, "A drill does not contact the source cluster"). The
  wizard, however, can only start from `Backup` CRs with `phase: Succeeded` and a `backupId`
  (`ui/pages/restore-wizard.js:217`, `:997`).

Trust:

- Approvals and evidence resolve exactly `TrustRoster/default` (`controllers/approval.rs:97`,
  `:538`); the roster is immutable (`crds/trust_roster.rs:70-96`), keys carry only
  `keyId/spkiPem/subject/notAfter`, expiry is `status.expiredKeyIds` recomputed every 300 s
  (`controllers/trust_roster.rs:98`, `:258`), and no status field says when that happened.
- The keys page renders `valid` for every key not listed in `expiredKeyIds`, even when the roster
  has no status at all (`ui/pages/keys.js:75`) — the "unknown shown as valid" defect.
- Identity bootstrap publishes `logweir-signing-trust` with exactly four keys and requires
  `trust-reference == logweir.dev/v1alpha1/TrustRoster/default#spec.signingKeys`
  (`crates/logweir/src/identity.rs:25-26`, `:414` `validate_public`). Changing that ConfigMap
  would make the retained hook refuse on upgrade or rollback; this decision leaves it untouched.
- Separation of approver and signer today is a label (`selfAttestedRisk`,
  `controllers/approval.rs:485`), not enforced usage.

## 2. PLAT-14.1 — User-facing operation state model

### 2.1 Layering (what decides what)

| Layer | Owns | Never does |
|---|---|---|
| `weirkeeper` Backup/Restore reconcilers | observed facts: phase, exit code, conditions, `status.progress`, bounded diagnostics derived from Job, Pod and core Events | invent exit codes; expose raw logs; read Secrets |
| `logweir-api` `status.rs` (P17 stage 1) | pure mapping CR status → `Operation` DTO; SSE of that DTO | read pods, logs, Events or object storage |
| UI | render DTO; watch/poll lifecycle | compute verdicts, read clocks for verdicts |

Readiness/configuration (PLAT-03.1) stays a separate `readiness` object; it never overwrites
`state`. Until PLAT-03.1 lands the DTO carries `readiness: {state: "unknown", basis: "notImplemented"}`.

### 2.2 Controller-written status (additive; `BackupStatus` and `RestoreStatus`)

```yaml
status:
  progress:                         # absent on objects reconciled only by an older controller
    stage: Admission | Queued | Preparing | Running | Verifying | Finished
    reason: CamelCase               # bounded vocabulary below; metav1 reason regex
    message: string                 # <= 1024 bytes, sanitized
    lastTransitionTime: date-time   # changes only when stage or reason changes
    lastObservedTime: date-time     # active stages only; rewritten at most once per 60 s (E11(d))
    runner:                         # facts about the one runner pod, all optional
      jobName: string
      podName: string
      podPhase: Pending|Running|Succeeded|Failed|Unknown
      scheduled: bool
      containerState: Waiting|Running|Terminated
      waitingReason: string         # kubelet reason verbatim, <= 64 bytes
      startedAt: date-time          # runner container start
    runnerPhase:                    # optional; from `progress-phase=` key lines (2.4)
      number: int                   # -1..9 (backup reports -1 with a step name)
      name: string                  # <= 32 bytes, from the runner's closed phase/step list
    diagnostics:                    # maxItems 8, newest lastSeen first
      - code: CamelCase             # same vocabulary as RunnerReady reasons
        severity: Warning | Error
        message: string             # <= 512 bytes, sanitized
        object: {kind: Pod|Job, name: string}
        firstSeen: date-time
        lastSeen: date-time
        count: int                  # capped at 1_000_000
  capture:                          # Backup only; copied verbatim from the verified receipt
    startedAt: date-time            # receipt.started_at
    finishedAt: date-time           # receipt.finished_at
  completion:                       # Restore only; copied by JSON pointer from the signed scorecard
    newTopics: [{name, partitions}] # from target_diff.would_create
    recordsExpected: int            # sample.records_expected (canary size)
    recordsRestored: int            # sample.records_restored
    recordsSampled: int             # integrity.records_sampled
    recordsSampledMatching: int
    integrityLevel: byte-fingerprint | consume-only | not-attempted
    sampleWindow: {start, end}
  teardown:                         # Restore only; from the verified teardown attestation (2.4)
    attestationKey: string
    deleted: [string]               # maxItems 256
    failed: [{topic, error}]        # maxItems 256, error <= 256 bytes
```

Conditions: one new type `RunnerReady`. `False` while the runner container cannot start, with
reason one of `WaitingForPod`, `PodUnschedulable`, `VolumeMountFailed`,
`CredentialReferenceMissing`, `RunnerImageUnavailable`, `PodCreationForbidden`; `True` with reason
`RunnerStarted` once `containerStatuses[name=runner].state.running|terminated` has been seen.
Every terminal builder carries `RunnerReady` and `Verified` forward (generalize
`verification::carry_verified`, `verification.rs:779`, to `carry_conditions(&[Verified,
RunnerReady])`). `Restore.status.reason` stays "the reason of the condition this patch writes
about current state": the `RunnerReady` reason while it is `False`, otherwise `JobCreated` or
the terminal reason (`every_status_write_sets_the_scalar_reason` is extended, not weakened).

New terminal states (added to `conditions::TERMINAL_STATES`, regex-tested):
`VolumeMountFailed`, `CredentialReferenceMissing`, `RunnerImageUnavailable`,
`PodCreationForbidden`. They replace `NoExitCode` only when the matching diagnostic was recorded
before the Job ended; otherwise the existing `crash_terminal_state` table is unchanged.
`exitCode` stays absent in all of them.

### 2.3 Diagnostic derivation (`weirkeeper::diagnostics`, one implementation for every Job-backed run)

**The classification table is D2's `waiting.rs`, not a second one** (seam S1's "one classification
table" rule). D2 §4.3 already defines the codes for check Jobs; this task generalizes that module
over Backup and Restore runner Jobs and adds nothing to the vocabulary except the severity and
transience columns below. The codes are therefore exactly: `CredentialSecretNotFound`,
`CredentialSecretKeyMissing`, `TrustBundleNotFound`, `RunnerImagePullFailed`,
`RunnerImageNotPresent`, `RunnerImageInvalid`, `PodUnschedulable`, `SigningKeyMissing`,
`VolumeMountFailed`, `RunnerServiceAccountMissing`, `PodCreateRejected`, `DisruptedMidCheck`
(rendered `DisruptedMidRun` on a Backup/Restore), plus `WaitingForPod` for "no pod and no event
yet". The parameters D2 carries in a code (`{secret}`, `{volume}`) travel in the diagnostic's
`object`/`message`, never in a `metav1` condition reason.

While the Job exists and has not finished, each reconcile (15 s) lists the pod by the existing
selectors (`backup.rs:251` `pod_selectors`), **verifies the pod's controller owner UID equals the
Job's UID before trusting anything it says** (seam S6, defect SEC-PODLOG) and, only if the pod is
not Running, lists core `v1/events` with
`fieldSelector=involvedObject.kind=Pod,involvedObject.name=<pod>` and once more for the Job,
`limit=20`.

| Class | Codes | Severity | Transient? |
|---|---|---|---|
| never starts without a change | `CredentialSecretNotFound`, `CredentialSecretKeyMissing`, `TrustBundleNotFound`, `SigningKeyMissing`, `RunnerImageNotPresent`, `RunnerImageInvalid`, `RunnerServiceAccountMissing`, `PodCreateRejected` | Error | no |
| may resolve on its own | `PodUnschedulable` (after 60 s), `RunnerImagePullFailed`, `VolumeMountFailed` (first 180 s), `WaitingForPod` (after 60 s) | Warning | yes |
| already fatal | `DisruptedMidRun` | Error | terminal path |

Sanitization (`diagnostics::sanitize`): truncate to 512 bytes on a char boundary, collapse
whitespace, remove URL userinfo, query strings and fragments, drop anything after a
`-----BEGIN`, keep object *names* (Secret/ConfigMap names are already references in the spec).
Dedup key = `(code, object.kind, object.name)`; `count` increments by the Event `count`/series
delta; the list is sorted by `lastSeen` and truncated to 8.

**Fail fast before any work.** When a non-transient code has been continuously observed for
`failFastSeconds` (installation policy ConfigMap, default 300, min 60) and the runner container has
never started (`startedAt` absent, `restartCount == 0`, no `lastState.terminated`), the controller
cancels through **D2's `cancel.rs`** — the same owned-Job, UID-verified
`spec.activeDeadlineSeconds` patch, with D2's documented `spec.suspend` fallback — rather than a
second cancellation path. The Job fails with `DeadlineExceeded`, the existing crashed-Job path runs, and the terminal
reason is the recorded diagnostic code. No data-plane process ever started, so no approval, plan
or archive state is consumed; a new attempt is a new Backup/Restore (no mutation of the old one).

### 2.4 Runner progress channel (optional, contract-versioned)

The controller already reads pod logs by key name from a bounded tail (`backup.rs:795`
`tail_lines`). Runners print `progress-phase=<n>:<name>` on stdout at each phase start
(`logweir backup run`: `-1:admit`, then named steps `engine`, `readback`, `sign`, `upload` printed as
`progress-phase=-1:<step>` because the backup path has no numbered phases after admission;
`logweir restore run`: existing phases `0..9` with their existing names). While `RunnerReady=True` and not finished, the
controller reads at most once per 30 s with `LogParams{tail_lines: 50, limit_bytes: 65536}` and
records the last seen phase. Absence is not an error (older runners print nothing; `runnerPhase`
stays absent). The restore runner additionally prints a conditional
`teardown-key=logweir/drills/<run_id>.teardown.json` line after phase 9 (same conditional rule as
`offset-report-key=`); the controller verifies the teardown attestation with
`PAYLOAD_TYPE_TEARDOWN` and copies names into `status.teardown`. Both are additions to the
interface I7/I8 key-line contract and bump `logweir_core::execution_contract::VERSION` to `"2"`
together with the §5 point binding (old runners reject the new version argument before dispatch).

### 2.5 Stage and normalized state mapping (pure, table-tested; `logweir-api/src/status.rs`)

| Controller facts | `progress.stage` | API `state` |
|---|---|---|
| Restore `phase: Pending`, `Admitted=False/ApprovalNotVerified` | `Admission` | `pending` (+ `awaitingApproval: true`) |
| no Job yet, no terminal status | `Admission` | `pending` |
| Backup `phase: Resolving` (D1's dynamic discovery Job) | `Preparing` (reason `DiscoveryRunning`) | `preparing` |
| Job exists, no pod, or pod unscheduled without a failure code | `Queued` | `queued` |
| pod scheduled, runner waiting (incl. diagnostics) | `Preparing` | `preparing` |
| runner running, `runnerPhase` not a verification phase | `Running` | `running` |
| runner running in restore phase 7, or backup step `readback`/`sign`/`upload` | `Verifying` | `verifying` |
| exit recorded, verification block not yet written (controller holds an evidence handle) | `Verifying` | `verifying` |
| `Succeeded`, exit 0 | `Finished` | `succeeded` |
| `Failed` with a controller refusal reason (`NameTooLong`, `ApprovalNotReceived`, `ApprovalSubjectMismatch`, `PlanHashMismatch`, `ClusterNotReachable`, `PlanConfigMapConflict`, `JobNameConflict`, `ApprovalBundleConflict`, `ReferentNotFound`, `ArchiveUrlUnreadable`, `CredentialNotRenderable`) or exit 3, or legacy phase `Refused` | `Finished` | `refused` |
| any other `Failed` (exit 1, 2, 4, crash states, new terminal states) | `Finished` | `failed` |
| active stage with `lastObservedTime` older than 300 s, unknown phase, or no status 120 s after creation | as written | `unknown` (`reason: StatusStale`) |
| (reserved; no cancel route in v1) | — | `cancelled` |

Objects without `status.progress` (older controller) map from phase/conditions alone:
`Running` → `running`, `Pending` → `pending`; `queued`/`preparing` are never inferred.

DTO fields that stay separate from `state`:

- `result.value`: `none | succeeded | notPass | failed | refused` (`notPass` = Restore exit 2 or
  outcome ≠ pass with a signed scorecard), plus verbatim `exitCode`, `exitReason`, `outcome`.
- `evidence.verification`: `pending | verified | verifiedHistorical | untrusted | invalid |
  notAttempted | notApplicable` (from §7.4 `result` + `trust.basis`; `notApplicable` for exits
  1, 3, 4 which write no artifact). A `succeeded` result with `notAttempted` is never rendered
  as verified success.
- `verificationScope`: `{level: sampled | degraded | none, recordsSampled, recordsSampledMatching,
  recordsExpected, statement}`. `byte-fingerprint` → `sampled`; `consume-only` → `degraded`;
  `not-attempted` or Backup receipts → `none` for record bytes (Backup receipts attest counts and
  window, not a restore). The value `complete` does not exist in v1 (PROD-08 owns it).
- `lastUpdate` = max(`progress.lastTransitionTime`, `progress.lastObservedTime`, latest condition
  transition); `reason`, `message`, `diagnostics[]` verbatim from status.

### 2.6 Delivery: refresh, SSE, polling, navigation stop

- `GET /api/v1/namespaces/{ns}/operations/{kind}/{name}` (kind `backup|restore`) returns the DTO
  with `uid` and `resourceVersion`. A refresh or a new tab resumes the same operation because the
  CR is the source of truth; a UID different from the route's `uid` query parameter renders
  "this name now refers to a different run" instead of silently switching.
- `GET .../operations/{kind}/{name}/events`: P17 SSE (event id = resourceVersion, `Last-Event-ID`
  resume, one `reset` snapshot on 410, heartbeat every 15 s, connection ≤ 300 s). Event types:
  `operation` (full DTO), `heartbeat`, `reset`, `end` (terminal and verification settled).
- UI module `ui/operation-watch.js` exporting `watchOperation(ns, kind, name, onUpdate,
  lifecycle)`. API mode: `EventSource` created inside `ui/api.js` (single request site), closed on
  `lifecycle.signal` abort, reconnect with backoff 1 s → 2 s → 5 s → 30 s max plus jitter, falls
  back to polling after 3 failed connects. Legacy localhost mode: `get()` polling every 5 s while
  active (30 s after 5 consecutive errors), stops on abort and on terminal+settled. Aborting a
  watch never cancels the operation (P17 rule). Route: `#/operations?ns=&kind=&name=&uid=`.

### 2.7 Completed Job cleanup

Keep "status first, TTL second". Add a repair: in step 2b (terminal status) when the Job still
exists, is finished, carries exactly this object's owner, and has no
`spec.ttlSecondsAfterFinished`, patch the TTL without reading the pod. TTL value from
`LOGWEIR_JOB_TTL_SECONDS` (default 604800, min 3600; tests may set 60 via the harness only).
After Job and pod GC the DTO is unchanged because it never depended on them.

## 3. PLAT-14.2 — Protection freshness and notifications

### 3.1 `ProtectionPolicy` (namespaced, `logweir.dev/v1alpha1`)

The objective lives here — not on `BackupSchedule` (D1 makes that spec editable, but an objective
spans several schedules, outlives any one of them, and D1 deliberately detaches run history from
schedule ownership) and not on the destination (protection is about a source/topic set, a
destination is about storage).
The spec is **mutable** (`observedGeneration` tracked): an objective is evaluation policy, never
an execution input, so editing it cannot change a recorded run. There is no CEL seal beyond field
validation.

```yaml
apiVersion: logweir.dev/v1alpha1
kind: ProtectionPolicy
metadata: {name: orders-prod, namespace: team-a}
spec:
  protects:
    sourceRef: {name: prod-kafka}          # KafkaCluster, same namespace
    topics: [orders, payments]             # optional; absent = topics of the newest matching point
    scheduleRefs: [{name: nightly}]        # maxItems 16; points produced by these schedules count
    destinationRef: {name: primary}        # PLAT-08.1; OR legacyArchive: {url}
    catalogRef: {name: primary}            # optional; RecoveryCatalog for availability (PLAT-15.1)
  objectives:
    maxRecoveryPointAgeSeconds: 93600      # required, >= 300, <= 31536000
    maxConsecutiveFailedRuns: 2            # default 1, <= 100
    requireVerifiedEvidence: true          # default true
    requireCatalogAvailability: true       # default true when catalogRef is set
    maxRehearsalAgeSeconds: 2678400        # optional (PLAT-14.3)
  notifications:
    routes:                                # maxItems 4
      - name: oncall
        pagerDuty: {routingKeySecretRef: {name: pd, key: routing-key}, endpoint: "https://events.eu.pagerduty.com/v2/enqueue"}
        webhook:   {urlSecretRef: {name: hooks, key: ops}}
        slack:     {webhookUrlSecretRef: {name: hooks, key: slack}}
    kinds: [BackupFailure, Staleness, ArchiveUnavailable, RehearsalFailure, RecoveryCompleted]
    sendResolved: true                     # default true
    renotifyAfterSeconds: 86400            # 0 disables re-notify while an alert stays open
  evaluationIntervalSeconds: 300           # 60..3600, default 300
```

No credential value is ever in the spec: only `secretKeyRef`-shaped references, projected into the
delivery Job (§3.4). This closes decision O17 for this path; the drill spec's inline
`notifications` block is unchanged and remains documented as plaintext.

Status:

```yaml
status:
  observedGeneration: int
  evaluatedAt: date-time                   # rewritten only on change or when older than interval/2
  health: Healthy | AtRisk | Stale | Unprotected | Unknown
  availabilityBasis: KubernetesStatus | Catalog | CatalogStale
  lastAvailablePoint:
    pointId: string                        # §5 identity when the catalog supplied it
    backupRef: {name}                      # when a Backup CR still exists
    recoveryPointAt: date-time             # capture.startedAt (see 3.2)
    newestRecordAt: date-time              # windowCovered.toMs - 1 ms, labelled separately
    ageSeconds: int                        # at evaluatedAt
    evidence: Valid | ValidHistorical | Untrusted | NotAttempted
    topics: [string]                       # maxItems 64, truncated with topicsTruncated: true
  lastAttempt: {backupRef, phase, reason, at}
  consecutiveFailedRuns: int
  missed: {lastMissedSlot: string, sinceLastFire: int}
  schedules: [{name, suspended, ready, nextFireTime, lastMissedSlot}]   # maxItems 16
  rehearsal: {lastSucceededAt, lastRestoreRef: {name}, lastFailedAt, lastReason}
  staleSince: date-time
  alerts:                                  # maxItems 16, the dedup ledger
    - key: string                          # `logweir-protection-<policyUID>-<kind>`
      kind: BackupFailure | Staleness | ArchiveUnavailable | RehearsalFailure | RecoveryCompleted
      state: Open | Resolved
      openedAt: date-time
      resolvedAt: date-time
      transition: int                      # increments on Open, on Resolve and on each re-notify
      notifiedTransition: int              # last transition a delivery Job was created for
      delivery: {state: Pending|Delivered|Failed|Suppressed, attempts: int, lastAttemptAt, jobRef: {name}, lastError: string}
  conditions: [Ready, Protected, NotificationsDelivered]
```

### 3.2 Freshness definition (the decision that matters)

`recoveryPointAt` is the **capture start** of the newest available point
(`BackupReceipt.started_at`, copied to `Backup.status.capture.startedAt` in §2.2 and carried in the
catalog record). It is not `windowCovered.toMs`: that is the newest *record* instant, so an idle
topic would look stale forever, and it is not `finishedAt`, which would under-report the gap for a
long-running capture. `newestRecordAt` is published beside it and labelled "newest archived
record" so the two are never conflated.

A point is **available** when: `phase: Succeeded` ∧ `exitCode == 0` ∧ (evidence
`Valid`/`ValidHistorical` if `requireVerifiedEvidence`) ∧ the point matches `protects`
(source, destination, and `topics ⊆ point topics`) ∧ — when `catalogRef` is set and
`requireCatalogAvailability` — the catalog entry is `Available` and the catalog view is not stale.
Anything else is not available; a catalog that cannot answer yields `availabilityBasis:
CatalogStale` and `health: Unknown`, never `Healthy`.

Health:

| Condition | health |
|---|---|
| available point with `ageSeconds <= maxRecoveryPointAgeSeconds`, failures below threshold, no suspended schedule, no missed slot since last fire | `Healthy` |
| available point inside the objective but failures ≥ threshold, a suspended/NotReady schedule, or a missed slot | `AtRisk` |
| newest available point older than the objective | `Stale` |
| no available point at all | `Unprotected` |
| evaluation impossible (catalog stale/unreadable, referenced source or destination missing, own status stale) | `Unknown` |

`Protected` condition mirrors health (`True` only for `Healthy`). Scheduling health stays on
`BackupSchedule.status.conditions[Ready]`; the UI renders the two side by side and never collapses
them ("an enabled schedule can have no recent recoverable backup").

Failure and miss accounting is bounded and uses D1's contracts, not the legacy ones: list at most
the newest 50 Backups selected by `logweir.dev/schedule-uid` (D1 §PLAT-05.2 membership; the
`spec.scheduleRef.uid` field is the authority and the label is the index), and read
`status.activeRuns`, `status.lastSlot.disposition` and `status.missedSlots` from each referenced
schedule. A retry chain (D1's `spec.trigger.kind=Retry`, `attempt`, `retryOf`) counts as **one**
failed slot — its final attempt — so `maxConsecutiveFailedRuns` is a count of slots, not of
attempts.

### 3.3 Alert vocabulary and deduplication

| kind | opens when | resolves when |
|---|---|---|
| `BackupFailure` | `consecutiveFailedRuns >= maxConsecutiveFailedRuns` | an available point newer than the first failure exists |
| `Staleness` | `health == Stale` | `health` back to `Healthy`/`AtRisk` |
| `ArchiveUnavailable` | newest otherwise-available point is `Missing`/`Unreadable`/`Untrusted` in the catalog | that point becomes `Available` + verified, or a newer one does |
| `RehearsalFailure` | last rehearsal failed, or last success older than `maxRehearsalAgeSeconds` | a rehearsal succeeds |
| `RecoveryCompleted` | a Restore matching this policy's points reaches a terminal state | informational; auto-resolved immediately (never pages) |

Dedup: one open alert per `(policy, kind)`; the key is stable (`logweir-protection-<policyUID>-<kind>`,
the same shape as `failure_dedup_key`, `phase7_verify.rs:1824`). PagerDuty gets `trigger` on Open
and `resolve` on Resolved under that key; webhooks/Slack get one POST per transition. A condition
that stays true produces no further transitions except the optional `renotifyAfterSeconds`
re-notify (`transition` + 1). `RecoveryCompleted` keys on the Restore UID and is webhook/Slack
only.

### 3.4 Delivery, and why it is a Job

The controller must not hold sink credentials (it has no Secret verb, `config/rbac/role.yaml`) and
must not gain HTTP egress. Delivery therefore reuses the existing, tested notification code path
in the runner image:

1. `crates/logweir/src/drill/phase7_verify.rs`'s notification half moves verbatim to
   `crates/logweir/src/notify.rs` (`EventSink`, `UreqSink`, `notify_agent`, `pagerduty_endpoint`,
   `enqueue_pagerduty`, `redact_url`, dedup helpers), re-exported from its old path so drill call
   sites and `crates/logweir/tests/notify.rs`'s structural guards keep working.
2. New subcommand `logweir notify deliver --event /event/event.json`. It reads the event document,
   posts one PagerDuty event (when `PAGERDUTY_ROUTING_KEY` is set), one webhook and one Slack POST,
   prints `notify-result=<sink>:<ok|failed>` per sink as final stdout lines, and exits 0 only when
   every configured sink accepted. Timeouts and redaction are the existing ones
   (`NOTIFY_CONNECT_TIMEOUT`/`NOTIFY_TIMEOUT`, `phase7_verify.rs:1631-1632`).
3. The controller creates an immutable ConfigMap `<policy>-ev-<sha8(alertKey|transition)>` (owner:
   the ProtectionPolicy, `blockOwnerDeletion: false`) with `event.json`, and a Job named
   `<policy>-n-<sha8(policyUID|alertKey|transition)>-<attempt>` with `serviceAccountName:
   logweir-notifier`, `automountServiceAccountToken: false`, `backoffLimit: 0`,
   `activeDeadlineSeconds: 120`, `ttlSecondsAfterFinished: 3600`, and `secretKeyRef` env for the
   configured routes. The deterministic name makes a duplicate reconcile a 409, exactly as
   scheduled Backups do (guard G-SLOT).
4. The controller reads the Job's exit code and the `notify-result=` lines through the existing
   bounded-tail-by-key-name reader, writes `alerts[].delivery`, and retries at most 3 times with
   60 s/300 s/900 s backoff (a new attempt suffix). Exhaustion sets
   `NotificationsDelivered=False/DeliveryFailed` and nothing else.

**Notification failure never rewrites a backup result**: the protection controller patches only
`protectionpolicies/status`; a structural test asserts `controllers/protection_policy.rs` names no
`Api<Backup>`/`Api<Restore>` status patch, and the live scenario compares Backup
`resourceVersion`/`managedFields` before and after a delivery failure.

Event document (`application/vnd.logweir.protection-event+json;version=1.0.0`, **unsigned** — a
notification is not evidence and is never rendered as one):

```json
{"format_version":"1.0.0","event_id":"<sha256 of policyUID|alertKey|transition>",
 "policy":{"namespace":"team-a","name":"orders-prod","uid":"..."},
 "alert":{"key":"logweir-protection-<uid>-Staleness","kind":"Staleness","action":"trigger","transition":3},
 "health":"Stale","summary":"orders-prod: newest available recovery point is 31h old (objective 26h)",
 "last_available_point":{"point_id":"lwp1-...","recovery_point_at":"...","age_seconds":111600,"evidence":"Valid"},
 "consecutive_failed_runs":2,"missed_slots":1,
 "verification_scope":"sampled",
 "details_route":"#/protection?ns=team-a&name=orders-prod",
 "generated_at":"..."}
```

`verification_scope` is always `sampled`, `degraded` or `none` — never `complete` — and no
notification body claims exhaustive verification.

### 3.5 Recovery completion surface (incident-facing)

For a terminal Restore the API returns `completion` from §2.2 plus fixed guidance sentences owned
by `ui/render.js` (never server-authored prose), keyed by `spec.target.mode`:

- `newTopic`: the created topic names and their partition counts; "Consumers are not moved.
  Logweir wrote nothing to the original topics. Consumer group offsets were not restored; the
  engine's offset report at `<evidence.offsetReportKey>` is informational only (Global Constraint
  35). Point applications at the new names and validate reads before retiring anything."
- `scratch`: "These topics are a rehearsal and are deleted by teardown; never point an application
  at them."

Counts are labelled exactly: `recordsSampledMatching of recordsSampled sampled records matched
byte-for-byte; recordsRestored records were restored in the sampled window; this is a sampled
check, not an exhaustive comparison`. When `integrityLevel` is `consume-only` the word is
`degraded`; when `not-attempted`, the panel says no record check ran.

## 4. PLAT-14.3 — Recurring recovery rehearsals

### 4.1 `RehearsalSchedule` (namespaced), spec sealed except `suspend`

Same seal shape as `BackupSchedule` (one object-level CEL rule enumerating every field but
`suspend`, `crds/backup_schedule.rs:65`), for the same reason: the signed standing authorization
binds a template digest, and a template that could be edited after approval would authorize work
nobody approved.

```yaml
apiVersion: logweir.dev/v1alpha1
kind: RehearsalSchedule
metadata: {name: weekly-orders, namespace: team-a}
spec:
  schedule: "0 3 * * 0"                     # same 5-field UTC cron grammar as BackupSchedule
  suspend: false                            # the only mutable field
  protectionPolicyRef: {name: orders-prod}  # optional; results surface in protection health
  point:
    scheduleRefs: [{name: nightly}]         # candidates from these schedules' Backups, and/or
    catalogRef: {name: primary}             # candidates from this catalog (PLAT-15)
    selection: NewestAvailable              # only value in v1 (CEL)
    minAgeSeconds: 0                        # <= 2592000
    topics: [orders]                        # maxItems 32; must be a subset of the chosen point
    requireVerifiedEvidence: true           # CEL: must be true in v1
  target:
    clusterRef: {name: kafka-target}        # KafkaCluster in this namespace
    topicPrefix: "rehearsal-"               # CEL: ^rehearsal-[a-z0-9-]*-$ after rendering (4.4)
    markerTopic: logweir.scratch
    replicationFactor: 1                    # 1..8
  bounds:
    concurrencyPolicy: Forbid               # CEL: Forbid only in v1
    deadlineSeconds: 3600                   # 300..21600
    startingDeadlineSeconds: 3600           # missed-slot horizon, same 1 h default as schedules
    recordsPerPartition: 25                 # 1..1000
    maxPartitions: 200                      # 1..2000; enforced when the catalog knows the count
    runnerResources:                        # projected to Restore.spec.runnerResources (§4.5)
      requests: {cpu: 200m, memory: 512Mi}
      limits: {memory: 2Gi}                 # CEL: memory limit <= 8Gi, cpu limit <= 4
  objectives: {rtoSeconds: 1800, passRate: 1.0}
  authorization:
    approvalPolicyRef: {name: governed-default}   # PLAT-19.2; absent = legacy governed
    standingApprovalRef: {name: weekly-orders-standing}   # the Approval carrying the signed scope
status:
  lastScheduledSlot, nextFireTime, activeRestoreRef, pendingRestoreRef,
  lastSucceeded: {restoreRef, at, pointId, evidence, rtoSeconds}
  lastFailed: {restoreRef, at, reason}
  lastSkipped: {slot, reason}               # NoQualifyingPoint|TargetUnavailable|AuthorizationInvalid|AuthorizationExpired|ConcurrencyBlocked|TargetBusy|LeftoverTopics|PointRetentionInProgress
  cleanup: {pendingTopics: [string], since: date-time}
  conditions: [Ready, Authorized, RehearsalHealthy]
```

### 4.2 Qualifying point selection (pure `weirkeeper::rehearsal::select_point`)

Candidates = Backups from `scheduleRefs` that are available by §3.2's rule ∪ catalog entries that
are `Available` + `Verified`. Filters, in order, each recording a skip reason when it empties the
set: `topics ⊆ point.topics`; `now - recoveryPointAt >= minAgeSeconds`; covered window present and
`to_ms > from_ms`; partition total `<= maxPartitions` when the catalog supplies counts (otherwise
`sizeBasis: unknown` is recorded and the deadline plus `recordsPerPartition` are the only bound);
point not inside a retention lease (§6.6). Order: `recoveryPointAt` desc, tie-break `pointId` asc;
take the first. Derived plan values: `point_in_time = to_ms - 1 ms` (the restore window is
closed — `docs/stability.md`, "the restore window's end is inclusive"), `sample.window_start =
from_ms`, `sample.window_end = to_ms - 1 ms`, `sample.anchor: head` (the only implemented anchor).

### 4.3 Authorization: one standing approval, checked twice

Per-slot human approval would defeat an unattended rehearsal, and a controller that could mint its
own authorization would be the bypass PLAT-19.2 exists to prevent. The decision is a **standing
rehearsal authorization**, an authorization document v2 (P17 shape) with:

```
subject {apiVersion: logweir.dev/v1alpha1, kind: RehearsalSchedule, namespace, name, uid}
scope   {templateDigest, targetClusterId, topicPrefix, topics[], maxPartitions,
         recordsPerPartition, deadlineSeconds, modes: ["scratch"]}
policy  {...}, requester {...}, issuedAt, expiresAt   # expiresAt - issuedAt <= 90 days
```

- `templateDigest` = `sha256` over the canonical JSON of `spec` minus `suspend` — the same digest
  the controller recomputes each slot. A spec edit is impossible (sealed) except `suspend`, so the
  digest cannot drift; a *new* schedule needs a new authorization.
- Governed policy (and legacy, policy absent): the document carries a `GovernedApproval` signature
  from a currently valid key, produced out of band with
  `logweir approve rehearsal --schedule <file> --key ... --expires ...`; under PLAT-19.2 the
  console confirmation signature is required as well and the approver principal must differ from
  the requester.
- Ordinary policy: the console confirmation issuer signature alone, per PLAT-19.2.
- Transport: the existing immutable `Approval` object, with `spec.subjectRef.kind:
  RehearsalSchedule` (new enum value) and `spec.planHash` = `templateDigest`. The Approval
  controller's checks 7 and 8 become "the recomputed subject digest" and "the signed subject kind",
  which is the same rule applied to a different referent; `ReferentHasNoPlanBytes` no longer
  applies to this kind because the digest is recomputed from the referent's own sealed spec.
- Each slot: the controller (a) recomputes the digest, (b) re-verifies the Approval is
  `Verified=True` with a subject UID equal to this schedule's UID, (c) checks `expiresAt` is in the
  future, (d) renders the plan and proves `plan ∈ scope` with the pure
  `logweir_core::rehearsal_scope::plan_within_scope(plan, scope)` (prefix, target cluster id,
  topics, mode scratch, partitions, records per partition, deadline), (e) creates the Restore with
  `spec.authorization {kind: Standing, approvalRef, rehearsalScheduleRef}` and a bundle carrying
  the authorization document, its signatures, the trusted public keys, the scope and the rendered
  plan. The runner revalidates (d) against the mounted bundle before any client is constructed
  (execution contract v2). A skip is recorded, never a silent no-op:
  `AuthorizationInvalid` / `AuthorizationExpired`.

`Restore.spec.approvalRef` becomes optional (was required) and exactly one of `approvalRef` or
`authorization` must be set (CEL). An older controller reading a standing-authorized Restore sees
an empty `approvalRef` and refuses terminally with `ApprovalNotReceived` — fail closed, which is
the required rollback behavior.

### 4.4 Isolation, bounds and cleanup

- **Isolated target**: the target cluster id must be in the bound `TrustPolicy`'s
  `allowedTargetClusterIds` (§7), must not equal the point's `source.cluster_id` (existing runner
  rail), the mapped topics must not already exist (existing phase 0 check), the marker topic must
  exist and be healthy (existing phase 0 check), and the target cluster id must equal the signed
  `scope.targetClusterId`. `spec.role` is not authority (it is free-form, `crds/kafka_cluster.rs:104`).
- **Rendered prefix**: `target.topicPrefix` is rendered as `<prefix><schedule-uid-first-8>-`, e.g.
  `rehearsal-3f2a91c7-`. It is unique per schedule object, so two schedules can never map to the
  same topic name, and it satisfies the runner's `with_scratch_prefix` deletion guard
  (`crates/logweir/src/drill/mod.rs:1101`).
- **Concurrency**: `Forbid` only, implemented with the PLAT-04.1 reservation protocol
  (`pendingRestoreRef` written under a resourceVersion-checked status patch, then the deterministic
  child, then `activeRestoreRef`). Additionally at most one active rehearsal per target cluster in
  the namespace: Restores carry `logweir.dev/rehearsal-target: <clusterRef>` and a second schedule
  skips with `TargetBusy`. Slots older than `startingDeadlineSeconds` are skipped and recorded.
- **Cleanup that never touches unrelated topics**: teardown is the existing phase 9, which deletes
  the exact mapped names (`phase9_teardown.rs:103`, `mapping.values()`) through a deleter that
  refuses any name outside the prefix. The controller deletes no topic, ever. Failures are read
  from the signed teardown attestation into `Restore.status.teardown` (§2.4) and mirrored to
  `RehearsalSchedule.status.cleanup.pendingTopics`. While `pendingTopics` is non-empty the next
  slot is **skipped** with `LeftoverTopics` — the run that would otherwise collide is not allowed to
  adopt or delete topics it did not create; an operator clears them with the documented command.
  `ProtectionPolicy` raises `RehearsalFailure`.
- **Evidence**: rehearsals write ordinary scorecards, sidecars, offset reports and teardown
  attestations under `logweir/drills/` and are never deleted; `status.lastSucceeded.evidence`
  records the verification verdict. Rehearsal Restores are owned by the RehearsalSchedule
  (ownerReference, `controller: true`, `blockOwnerDeletion: false`) — deleting the schedule
  collects its Restore CRs but never the signed evidence; PLAT-05.2's history-retention decision
  applies unchanged.

### 4.5 Small contract additions this needs

- `Restore.spec.runnerResources {requests{cpu,memory}, limits{cpu,memory}}` — optional, immutable,
  CEL-bounded; set by the rehearsal controller and (later) by the API. The Job shape stays a
  function of the object, not of an annotation (PLAT-06.1's rule), and stays identical across
  target modes (`scratch_mode_and_new_topic_mode_produce_the_same_job_shape` keeps holding because
  resources come from the spec, not the mode).
- `Approval.spec.subjectRef.kind` gains `RehearsalSchedule` (enum extension, additive).
- `Restore.spec.authorization {kind: Standing, approvalRef{name}, rehearsalScheduleRef{name}}`.

## 5. PLAT-15.1 / PLAT-15.2 — Durable recovery catalog and disaster restore

### 5.1 Point identity

`pointId = "lwp1-" + lowercase_hex(sha256(receipt_bytes))[0..32]` (128 bits), where `receipt_bytes`
are the exact stored bytes of the signed backup receipt. Consequences, all of them wanted:

- identity is content-derived, so the same archive copied to a second bucket yields the same
  point with two `locations[]` rather than a duplicate;
- it is computable by anyone holding the receipt, including a fresh installation that never saw
  the Backup CR;
- it cannot be forged into a different point's identity without breaking the receipt signature;
- the full `receipt_sha256` travels beside it, so the short id is a display/lookup key and the
  digest is the binding.

`backup_id` (`<schedule-uid>-<slot>` or the Backup UID, `slot.rs:624`, `backup.rs:308`) stays the
*archive set* identifier; a set with two runs (upstream append) yields two points sharing a
`backup_id`, which is exactly the distinction "recovery point" needs.

### 5.2 Object-storage layout (create-only, append-only, under `logweir/`)

All keys are under the destination's evidence root `logweir/` (`backup/mod.rs:809`), so
`Store::put_create_only`'s existing `LOGWEIR_ROOT` assertion (`logweir-store/src/lib.rs:680-696`)
covers them and no new write authority is introduced.

```
logweir/catalog/v1/points/<pointId>/record.json     # signed point record (payload type below)
logweir/catalog/v1/points/<pointId>/record.sig
logweir/catalog/v1/log/<yyyy>/<mm>/<dd>/<recoveryPointAtMs:013>-<pointId>.json   # tiny index entry
logweir/catalog/v1/tombstones/<pointId>/<runId>.intent.json|.done.json + .sig    # §6 deletions
logweir/catalog/v1/snapshots/<catalogUid>/<generation>/page-<nnnn>.json + .sig   # sync output
```

Why both a record and a time-sharded log: `Store::list_keys` is an unbounded, in-memory, sorted
list (`lib.rs:382`) and receipts are keyed by `backup_id`, which carries no ordering for manual
runs. Day shards make "newest first" and "only what changed since the cursor" a bounded listing.
A new `Store::list_page(prefix, start_after, max) -> (Vec<String>, Option<String>)` wraps
`object_store::list_with_offset` (present in 0.14) and is the only new store capability; it adds no
write and no delete.

Point record, `application/vnd.logweir.catalog-point+json;version=1.0.0`, signed with the runner
signing key (DSSE detached sidecar, same PAE and key id rules as every other document):

```json
{"format_version":"1.0.0","point_id":"lwp1-...","recorded_at":"...",
 "receipt":{"key":"logweir/backups/<backup_id>/<run_id>.receipt.json","sha256":"sha256:...","sidecar_key":"...","payload_type":"application/vnd.logweir.backup-receipt+json;version=1.0.0"},
 "backup_id":"...","run_id":"...",
 "archive":{"location_id":"s3://bucket/prefix","manifest_key":"...","manifest_sha256":"sha256:...","prefix":"..."},
 "covered":{"from_ms":0,"to_ms":0},
 "capture":{"started_at":"...","finished_at":"..."},
 "topics":[{"name":"orders","partitions":6,"records":1234}],
 "source":{"cluster_id":"...","bootstrap_servers":["..."],"auth_mode":"scramSha512"},
 "execution":{"kind":"Backup","namespace":"team-a","name":"...","uid":"...","execution_id":"...","inputs_sha256":"sha256:...","schedule":{"name":"nightly","uid":"...","slot":"20260915-030000"},"triggered_by":"schedule"},
 "signing":{"key_id":"<sha256 SPKI hex>","algorithm":"ecdsa-p256-sha256"},
 "installation":{"key_id":"<sha256 SPKI hex>"}}
```

Reading rules (mirrored by the Rust reader and `docs/verify_scorecard.py`'s
`--payload-type catalog-point` signature-only mode):

1. `format_version` major must be `1`; a higher major makes the entry `UnsupportedFormat`, never
   fatal for the sync.
2. Unknown fields are ignored inside major 1; absent optional fields mean **unknown**, never zero:
   absent `topics[].partitions` → partition count unknown (blocks `maxPartitions` enforcement, §4.2);
   absent `execution` → provenance unknown (an imported archive from another installation);
   absent `installation` → unknown installation.
3. Everything except the four receipt-derived facts (`backup_id`, `run_id`, `covered`, `capture`,
   plus `archive.manifest_*`) is **informational**: the receipt signature is the verification root.
   The sync recomputes those facts from the verified receipt and marks a record whose copies
   disagree as `RecordMismatch` (availability `Conflict`).
4. `logweir/` objects are never rewritten. A correction is a new record under a new point id; a
   removal is a tombstone.

Who writes records: the backup runner, in `phase_run::persist_receipt` after the receipt and
sidecar are stored, as a fifth create-only put plus the log entry, printing a conditional
`catalog-key=` line. A failed catalog write is a `warn` and does not change the exit code or the
receipt (the archive and its evidence already exist); the scanner backfills.

### 5.3 `RecoveryCatalog` (namespaced) and the Kubernetes view

```yaml
apiVersion: logweir.dev/v1alpha1
kind: RecoveryCatalog
metadata: {name: primary, namespace: team-a}
spec:
  destinationRef: {name: primary}          # immutable (CEL); or legacyArchive {url, secretRef}
  sync:
    intervalSeconds: 3600                  # 300..86400; 0 = manual only
    mode: Index | Full                     # Index = shards since cursor; Full = rescan receipts+manifests
    maxObjectsPerRun: 100000               # <= 1000000
    deepCheck: None | ManifestDigest | SegmentSample   # default ManifestDigest
    viewLimit: 2000                        # 100..5000 newest points materialized into Kubernetes
  syncRequest: "<opaque token>"            # the ONLY mutable spec field (CEL), set by the API command route
status:
  observedSyncRequest, generation, syncedAt, viewExpiresAt
  cursor: {indexShard: "2026/09/15", rescanStartAfter: "<key>", complete: bool}
  counts: {total, available, missing, unreadable, unverified, untrustedSigner, invalid, conflict, deleted, unsupportedFormat}
  truncated: bool                          # total > viewLimit
  histogram: [{day: "2026-09-15", points: 24}]        # maxItems 400
  signers: [{keyId, principalHint, points, trusted: bool}]   # maxItems 16
  pages: [{configMapName, index: int, count: int, firstPointId, lastPointId, sha256}]  # maxItems 8
  indexConfigMap: string                   # fence pointers: pointId ranges -> page index
  lastSyncJob: {name, startedAt, finishedAt, exitCode, refusalReason}
  conditions: [Ready, Synced, Stale, TrustAvailable]
```

Sync execution: a short-lived Job running **D2's one check runner** — `logweir check run --plan
/plan/check.json --check-contract-version 1` with the new plan kind `catalogSync` (seam S1: no
second runner subcommand, one argv allowlist surface, one error-code table; `logweir catalog list`
remains an operator CLI that reads with the operator's own credentials and creates no Job). The Job
carries the destination's **complete, explicit** `AWS_*` set from the resolved destination snapshot
and never controller-forwarded addressing (seam S5, defect SEC-ENVHTTP), a **read-only** credential
projected by `secretKeyRef`, no API token, the bound
`TrustPolicy`'s public keys mounted from an immutable controller-written ConfigMap
`<catalog>-trust-<policyGeneration>`, and the same `activeDeadlineSeconds`/TTL discipline as the
existing probe Job (`controllers/kafka_cluster.rs`). It emits D2's result frames on stdout, with the `catalogSync` result body carrying
`catalog-page=<i>/<n> count=<c> sha256=<hex>` followed by `catalog-entry=<compact json>` lines, then
`catalog-counts=<json>`, `catalog-cursor=<json>`, `catalog-signers=<json>`; failures use D2's closed
error-code vocabulary rather than new strings. The controller reads the
log with `LogParams{limit_bytes: 3_145_728}`, verifies each page digest over its own entry lines
(transport integrity, not authorization), and writes immutable page ConfigMaps
`<catalog>-g<generation>-p<index>` **owned by the sync Job** (`blockOwnerDeletion: false`).

Garbage collection without a `delete` verb: the sync Job carries
`ttlSecondsAfterFinished = max(3 × intervalSeconds, 86400)`; when the TTL controller removes the
Job, Kubernetes GC removes its pages. Each successful sync publishes a new generation and the
previous one ages out. If syncing stops for longer than the TTL the view disappears and the
catalog reports `Stale` then `ViewExpired` — honest, and the durable truth is still in object
storage. **This decision adds no delete permission to the controller** (D2's transient check
kinds keep their own UID-preconditioned GC).

Bounds: `viewLimit` ≤ 5000 entries (~350 B each ⇒ ≤ 2 ConfigMaps under the 1 MiB object limit);
`Index` sync lists only day shards at or after `cursor.indexShard` minus one day of overlap;
`Full` sync walks receipts with `list_page` in `maxObjectsPerRun` budget and persists
`rescanStartAfter` so the next Job continues (`cursor.complete: false` until the walk finishes).
Points beyond `viewLimit` are counted, histogrammed and reachable by `logweir catalog list` on an
operator workstation; the API advertises `catalogWindowQuery: false` until a later task implements
windowed queries — an absent capability, never a fake stub.

### 5.4 Availability and verification are separate axes

| `availability` | meaning | selectable |
|---|---|---|
| `Available` | receipt, sidecar and manifest readable; manifest digest equals the receipt's | yes |
| `Missing` | a definite `NotFound` for receipt or manifest | no |
| `Unreadable` | any other storage error (403, timeout, truncated) — "could not tell" | no |
| `Deleted` | a completed tombstone exists | no |
| `Conflict` | two records disagree for one identity, or record facts contradict the receipt | no |
| `UnsupportedFormat` | record major > 1 | no |
| `Partial` | `deepCheck: SegmentSample` found a manifest-listed segment missing | no |

| `verification` | meaning |
|---|---|
| `Verified` | receipt DSSE verifies under a key the bound `TrustPolicy` accepts for `EvidenceSigning` (§7.4), digests match |
| `VerifiedHistorical` | same, under a retired/expired key within its validity |
| `UntrustedSigner` | signature verifies under a key the policy does not list (fresh installation, another installation) |
| `Revoked` | key revoked for compromise and no independent pre-revocation observation |
| `Invalid` | signature does not verify, or digest mismatch |
| `NoEvidence` | a manifest with no receipt at all (a foreign/legacy upstream archive) |
| `NotAttempted` | could not fetch or no trust material available |

Ordinary restore selection requires `Available` ∧ (`Verified` | `VerifiedHistorical`).

**Merging one point seen in several locations (amended 2026-09-17 at W8's
integration, review `d3w8` F9).** One `backup_id` seen in two buckets is one
entry with two `locations[]`. The entry's **availability is the best of its
locations** — a point is recoverable if any location can serve it — and every
`locations[]` element carries its own availability, with the degraded ones named
in `remedy`; hiding a point that bucket A holds because bucket B lost its copy
would contradict §5.1. The entry's **verification is the worst of its
locations** — two copies that disagree under signature are a `Conflict`
(`RecordMismatch`), because the catalog cannot say which copy is the record —
and the signer key id follows the worse verdict. Both rules are order-independent.
An entry with no `locations[]` keeps its own single verdict. The view's lifetime is
`viewExpiresAt = finish + max(3 × intervalSeconds, 3600)` (§5.3's TTL, amended from a
one-day floor at the same integration so that at most twelve generations coexist at
the 300 s floor, which the CRD now enforces); a manual-only catalog (`intervalSeconds:
0`) therefore keeps a view for an hour after each sync.
Everything else is listed with its exact state and a remedy sentence; nothing is silently hidden
and nothing unverified is presented as verified evidence.

### 5.5 PLAT-15.2 — Connect existing archive, and disaster restore with no source and no CRs

1. **Connect**: an operator creates the destination (PLAT-08.1) with a read-only credential and a
   `RecoveryCatalog` with `sync.mode: Full`. API: `POST /api/v1/namespaces/{ns}/catalogs`
   `{destinationRef | legacyArchive, syncMode}` with an `Idempotency-Key`; repeating it returns the
   same object (P17 rule). Two catalogs for one destination in one namespace are refused
   (`Ready=False/DuplicateCatalog` on the newer object).
2. **Discover**: the sync Job enumerates `logweir/catalog/v1/` records; for archives written before
   this feature it falls back to walking `logweir/backups/*/*.receipt.json` and, for sets with no
   receipt at all, the archive's own manifests (`NoEvidence` entries). Repeated import is
   idempotent: identities are content-derived, so a second sync produces the same point ids.
3. **Establish trust explicitly**: points signed by an unknown key are `UntrustedSigner`, and
   `status.signers[]` lists `{keyId, points, trusted: false}`. The UI shows the key id (the SHA-256
   of the DER SPKI — the same number `openssl` prints, `docs/keys.md`) and the out-of-band
   fingerprint command, and offers no one-click trust. An administrator adds the key to the
   namespace's `TrustPolicy` with `state: Retired` (or `Active`) and usage `EvidenceSigning`,
   either by `kubectl apply` (the supported v1 path) or through the installation-admin route
   `POST /api/v1/trust-policies/{name}:add-key` which **requires** `confirmFingerprint` to equal
   the server-computed key id. A public key found beside an archive is displayed as a claim and is
   never trusted by proximity (`docs/keys.md`, "A public key arriving beside an archive is never
   trusted merely by proximity").
4. **Select and restore**: the wizard (PLAT-11.1) takes `pointRef {catalogRef, pointId}` instead of
   a Backup name. The plan carries the point binding:
   `source.storage` from the destination, `source.backup: <backup_id>`, `source.topics ⊆ point
   topics`, and a new block `source.point {point_id, receipt_key, receipt_sha256, manifest_sha256}`.
   `restore.point_in_time ≤ covered.to_ms - 1 ms` (inclusive boundary).
5. **Preflight** (PLAT-03.2) re-reads the receipt and manifest through a bounded check Job and
   re-evaluates trust, so a green preview cannot survive a point that became `Missing`.
6. **Execute**: the runner, before constructing any client, fetches the receipt and sidecar named
   in the plan, checks `sha256(receipt) == source.point.receipt_sha256`, verifies the signature
   against the mounted trust bundle for `EvidenceSigning`, and checks the manifest digest; a
   mismatch is exit 3 `PointBindingMismatch`. This is why execution contract v2 exists: an old
   runner rejects the new argument before dispatch rather than silently skipping the check.
   No Backup CR is read at any step, and the source cluster is never contacted (a restore already
   never contacts it, `docs/stability.md`).

Catalog compatibility rules: the layout is versioned in the key path (`catalog/v1/`) and in each
record's `format_version`; a future `v2` writes under `catalog/v2/` and dual-reads during a
documented window; readers refuse a higher major per entry, not per catalog; missing optional
fields are `unknown`, never defaults; nothing in the catalog is ever rewritten or deleted by the
catalog code (tombstones are additive, §6).

## 6. PLAT-16.1 / PLAT-16.2 — Retention: recommendation, and the one supported enforcer

### 6.1 The boundary decision

**Supported enforcement = an isolated, optional Logweir retention worker.** External bucket
lifecycle stays supported as an operator-owned mechanism that Logweir can *declare and report on*,
never claim guarantees for.

Why not external lifecycle as the enforcer: a provider lifecycle rule can express age/prefix
expiry and honours its own Object Lock and legal holds, and nothing else. It cannot know a
minimum number of usable recovery points, cannot see an in-flight restore, cannot reason about a
segment shared by two manifests, and cannot be reconciled against Logweir's catalog. PLAT-16.2's
acceptance ("protected/unknown points are retained and every deletion is attributable") and its
test list (active restore, last usable point, shared segment, wrong-prefix rejection, bounded
retry, partial failure) are unimplementable in that mode; claiming them would be exactly the
withdrawn-guarantee defect this corpus gates against. A Logweir pin cannot override a bucket
lifecycle rule, and this document says so on every surface.

### 6.2 `RetentionPolicy` (namespaced, one per destination)

```yaml
apiVersion: logweir.dev/v1alpha1
kind: RetentionPolicy
metadata: {name: primary, namespace: team-a}
spec:
  destinationRef: {name: primary}          # immutable (CEL)
  catalogRef: {name: primary}              # immutable; the evaluation input (PLAT-15.1)
  scope: {prefix: "kafka-backups/team-a"}  # immutable; must equal the destination's bucket prefix
  rules: {keepLast: 30, keepDays: 30, minUsablePoints: 3}      # minUsablePoints >= 1, default 3
  holds: [{pointId: "lwp1-...", reason: "legal-2026-11", until: "2027-01-01T00:00:00Z"}]   # mutable, maxItems 256
  mode: Report | Enforce | ExternalLifecycle                    # default Report
  externalLifecycle:                        # required iff mode = ExternalLifecycle; a DECLARATION
    provider: s3 | gcs | azure
    ruleId: "expire-archive-90d"
    expirationDays: 90
    prefix: "kafka-backups/team-a"
  enforcement:                              # required iff mode = Enforce
    credentialSecretRef: {name: retention-delete}   # delete-capable, mounted only into retention Jobs
    schedule: "17 4 * * *"                  # evaluation/enforcement cadence, UTC cron
    requireApprovedPlan: true               # default true
    approvedPlanSha256: "sha256:..."        # mutable; set by an administrator after reviewing the preview
    planMaxAgeSeconds: 3600                 # 300..86400
    maxDeletionsPerRun: 50                  # 1..500 (points)
    maxObjectsPerRun: 20000                 # 1..200000 (keys)
    deadlineSeconds: 1800
status:
  enforcement: RecommendationOnly | LogweirWorker | ExternalLifecycleDeclared
  guarantees: {ageExpiry, minUsablePoints, activeRestoreProtection, sharedSegments, legalHold}
                                            # each: LogweirEnforced | ProviderEnforcedUnverified | NotEnforced
  lastEvaluation:
    at, pointsEvaluated, kept: [pointId], candidates: [{pointId, reason, recoveryPointAt, objects, bytes}]
    protected: [{pointId, reason}]          # ActiveRestore|MinUsablePoints|LegalHold|SharedSegment|Unknown|Hold
    skipped: [{pointId|key, reason}]        # Unreadable|UnsupportedFormat|Conflict
    planRef: {name}, planSha256, planExpiresAt
  lastEnforcement:
    runId, startedAt, finishedAt, planSha256, deleted: [pointId], failed: [{pointId, code}]
    objectsDeleted, recordKey, recordSha256, exitCode
  lease: {runId, pointIds: [string], acquiredAt, expiresAt}     # maxItems 500
  consecutiveRunFailures: int
  conditions: [Ready, Evaluated, Enforced, ExternalLifecycleConflict, EnforcementDegraded]
```

### 6.3 PLAT-16.1 — recommendation reporting that names the right destination

Evaluation input is the destination's **catalog view**, not a bucket walk with the controller's one
global handle. This removes today's defect where a schedule writing to another bucket is reported
against `LOGWEIR_ARCHIVE_URL`'s bucket (`backup_schedule.rs:1421-1435`), and it **is** the
"per-destination archive inventory" D2 §3.10 defers to PLAT-16.1: the `catalogSync` check kind
produces the inventory once, and retention consumes the view rather than adding a second scan of
the same bucket. D2's interim guard (evaluate the legacy report only when the schedule's bucket
equals the global handle's bucket) stays in place for schedules with no destination.

Legacy `BackupSchedule.spec.retention` keeps working, with two honesty fixes:
`status.retentionReport.enforcement: "recommendationOnly"` is added, and when the schedule's
archive URL does not normalize to the controller's configured archive location the report is
replaced by `note: "the controller's archive handle points at a different destination; this
schedule's retention is not evaluated here — create a RetentionPolicy"` with empty set lists.
When a `RetentionPolicy` covers the same destination the schedule report adds
`supersededBy: {kind: RetentionPolicy, name}`. Policy conflicts (two RetentionPolicies for one
destination) put both in `Ready=False/Conflict` and neither evaluates or enforces.

A retention evaluation failure never blocks a backup: it is a different controller, a different
object and a different condition; the existing rule ("a report is not a backup") is preserved and
tested by leaving a scheduled Backup running through an unreadable-catalog evaluation.

### 6.4 Evaluation algorithm (pure `weirkeeper::retention_plan::evaluate`)

Input: catalog entries for the destination, `rules`, `holds`, the controller-supplied protection
set (active restores, leases), `now`. Output: `kept`, `candidates`, `protected`, `skipped` and a
canonical plan document.

1. `usable` = entries with `availability: Available` ∧ verification `Verified|VerifiedHistorical`.
   Everything else is `skipped` and is **never** a deletion candidate ("unknown is retained").
2. Sort `usable` by `capture.started_at` desc, tie-break `pointId`.
3. Keep: rank ≤ `keepLast`; age ≤ `keepDays`; always the newest `minUsablePoints` usable points,
   whatever the rules say (a policy that would leave fewer is reported, not obeyed).
4. Protect (retained with a reason, never deleted): `ActiveRestore` — any nonterminal Restore in
   the cluster whose normalized archive location and `backupSetRef`/point binding match; `Hold` —
   an entry in `spec.holds` still in force; `LegalHold` — a previous attempt was refused by the
   provider (§6.5); `SharedSegment` — any segment key of this point appears in another retained
   point's manifest (v1 never partially deletes a shared set); `Unknown` — anything the catalog
   could not establish.
5. Candidates = `usable` − kept − protected, truncated to `maxDeletionsPerRun`.
6. Plan document (`application/vnd.logweir.retention-plan+json;version=1.0.0`, written to an
   immutable ConfigMap `<policy>-plan-<generation>` owned by the policy): policy identity and
   generation, destination location id, rules, `now`, and for each candidate the point id, the
   receipt digest, the manifest key and the complete, explicit list of object keys to delete.
   `planSha256` is the sha256 of those exact bytes and is what an administrator approves.

### 6.5 Enforcement (the worker), and the boundaries that make it safe

- **Binary and linkage.** New crate `crates/logweir-reaper` is the only place in the workspace
  allowed to name an object-store delete; it exposes `ArchiveReaper::delete_exact(keys)` over a
  credential-scoped handle. A new binary target `logweir-retention` links it. `weirkeeper`,
  `logweir-store`, `logweir` and `logweir-api` must not link it; `scripts/check-no-archive-write.sh`
  is amended from "no delete anywhere in the control plane or the store crate" to that plus a
  dependency-graph walk (the shape `scripts/check-one-signer.sh` already uses) proving the reaper
  is reachable only from `logweir-retention`. Global Constraint 6 and ADR 0008 Amendment E gain the
  recorded amendment in §8.
- **Credential.** `enforcement.credentialSecretRef` is projected only into retention Jobs. The
  documented IAM scope is `s3:ListBucket` on the bucket with prefix condition, and
  `s3:GetObject`/`s3:DeleteObject` on `<prefix>/*` **excluding** `logweir/*`; evidence, receipts,
  scorecards, catalog records and tombstones are never deletable by this principal. The controller
  never reads it and never links deletion code.
- **Two-step approval.** Each scheduled evaluation writes a preview plan. With
  `requireApprovedPlan: true` (default) no Job is created until
  `spec.enforcement.approvedPlanSha256` equals `status.lastEvaluation.planSha256` and the plan is
  younger than `planMaxAgeSeconds`; approving is an installation/namespace-administrator action
  (`POST /api/v1/namespaces/{ns}/retention-policies/{name}:approve-plan`, audited, or a
  `kubectl patch` by a subject bound to `logweir-retention-admin`). With `requireApprovedPlan:
  false` the controller records condition `Enforced` with reason `UnattendedDeletionEnabled` so the
  choice is visible on the object.
- **Lease and active-restore race.** Before creating the Job the controller (a) writes
  `status.lease {runId, pointIds}` with a resourceVersion-checked patch, then (b) performs a
  **consistent** (non-cached) cluster-wide list of Restores and RehearsalSchedules and aborts if
  any nonterminal one references a leased point. Restore admission and rehearsal point selection
  hold with reason `PointRetentionInProgress` (30 s requeue) while a matching lease exists, so a
  restore that arrives after (a) cannot slip past (b). The lease expires with the Job deadline.
- **Execution order per point** (so a half-deleted set never looks usable): write the signed
  tombstone *intent*; delete the manifest; delete the segment objects; write the signed tombstone
  *completion* with counts and failures. Evidence under `logweir/` is never touched, so the audit
  trail of a deleted point survives the point.
- **Wrong-prefix rejection.** Before any delete, the worker validates every key in the approved
  plan: it must start with `<scope.prefix>/<backup_id>/`, must not start with `logweir/`, must
  belong to a candidate in the approved plan, and must have been listed from that point's own
  manifest or set directory. Any violation exits 3 `RetentionScopeViolation` with zero deletes.
- **Bounded retry.** Per key: 3 attempts, 1 s/4 s/16 s backoff, only on 5xx/timeouts.
  `AccessDenied`, `Locked`, `PreconditionFailed` are not retried: the point is recorded as
  `LegalHold`/`Denied`, kept, and excluded from the next plan until the reason clears. Per run:
  `maxDeletionsPerRun`, `maxObjectsPerRun`, `deadlineSeconds`. Three consecutive failed runs set
  `EnforcementDegraded` and stop scheduling until the spec changes.
- **Attributable deletion.** Every run writes
  `application/vnd.logweir.retention-record+json;version=1.0.0` (signed with the runner signing
  key) to `logweir/retention/<policyUid>/<runId>.json` + `.sig`: policy identity/generation,
  `planSha256`, approver reference (the API audit id or the patching subject recorded in an
  annotation), rules, every deleted point id with its object count, every failure with its code,
  start/finish, installation key id. `status.lastEnforcement.recordKey` points at it.
- **Partial failure** leaves the point `Deleted` only when its manifest delete succeeded; a run
  interrupted after the manifest and before the segments leaves an `Orphaned` tombstone state, and
  the next run's plan includes exactly those leftover segment keys (idempotent completion).

### 6.6 External lifecycle mode (declared, not enforced)

`mode: ExternalLifecycle` records the operator's declaration and sets
`status.guarantees` to `ageExpiry: ProviderEnforcedUnverified` (Logweir cannot read a lifecycle
configuration — `object_store` exposes no such API), `legalHold: ProviderEnforcedUnverified`,
and `minUsablePoints|activeRestoreProtection|sharedSegments: NotEnforced`. The controller
**rejects** the policy with `Ready=False/UnsupportedCombination` when `rules.minUsablePoints > 0`
is combined with this mode without the explicit acknowledgement field
`externalLifecycle.acknowledgedUnenforceable: true`, and raises
`ExternalLifecycleConflict` when the declared `expirationDays` would expire points the rules
consider kept. The UI labels the destination "deletion performed by your bucket lifecycle rule
`<ruleId>`; Logweir reports and cannot protect individual points here."

## 7. PLAT-19.1 — Explicit trust lifecycle

### 7.1 `TrustPolicy` (cluster-scoped), and how a namespace is bound

Cluster scope is kept for the reason the roster has it: "a roster whose name the subject supplies
is a roster the subject can choose" (`docs/kubernetes.md` §8). A namespace therefore never names
its own trust; the policy names the namespaces it governs.

```yaml
apiVersion: logweir.dev/v1alpha1
kind: TrustPolicy
metadata: {name: org-default}
spec:
  default: false                     # at most one policy cluster-wide may set true
  namespaces: [team-a, team-b]       # exact names, maxItems 256
  allowedTargetClusterIds: [...]     # replaces TrustRoster.spec.allowedClusterIds
  keys:                              # maxItems 64; APPEND-ONLY (CEL)
    - keyId: "<sha256 of DER SPKI, lowercase hex>"   # immutable
      spkiPem: "-----BEGIN PUBLIC KEY-----\n..."     # immutable, maxLength 4096
      algorithm: p256 | ed25519                      # immutable; controller checks it against the PEM
      usages: [EvidenceSigning]                      # immutable; see 7.3
      principal: {id: "install:9f2c...", display: "prod installation signer"}   # id immutable
      notBefore: "2026-01-01T00:00:00Z"              # immutable once set
      notAfter:  "2027-01-01T00:00:00Z"              # may only be shortened
      state: Active | Retired | Revoked              # monotonic
      retiredAt: date-time                           # required iff Retired or Revoked-after-retirement
      revokedAt: date-time                           # required iff Revoked, immutable once set
      revocationReason: KeyCompromise | Superseded | Unspecified
      revocationEffectiveFrom: date-time             # required iff Revoked, immutable once set
status:
  observedGeneration, evaluatedAt                    # heartbeat, debounced to >= 300 s
  loaded: bool
  keys: [{keyId, effectiveState, usableForNewSignatures, usableForVerification}]
        # effectiveState: Active|NotYetValid|Expired|Retired|Revoked|Unparseable
        # usableForVerification: Full|Historical|None
  boundNamespaces: [...], conflicts: [{namespace, policies: [...]}]
  conditions: [Loaded, Bound, ExpiringSoon]
```

Resolution per namespace: explicit `spec.namespaces` match → that policy; else the `default: true`
policy; else the synthesized `legacy-roster-v1` (§7.5). A namespace claimed by two policies is a
`conflicts` entry and resolves to **nothing** — every approval and verification in it is refused
with `TrustPolicyConflict` rather than silently picking one.

Mutability is deliberate and is the "authorized updates" half of PLAT-19.1: `update` on
`trustpolicies` is granted only by a new cluster role `logweir-trust-admin`; an operator or
approver has read only. The spec is not sealed, but an object-level CEL rule makes every change
monotonic:

- every existing `keyId` must still be present, with identical `spkiPem`, `algorithm`, `usages`,
  `principal.id` and `notBefore` (public material needed by old archives cannot be edited away);
- `notAfter` may only move earlier;
- `state` may move `Active → Retired`, `Active|Retired → Revoked`, and nowhere else;
- `revokedAt`/`revocationEffectiveFrom` are immutable once written;
- `keyId` values are unique.

The rule is an object-level rule for the same reason `SUSPEND_ONLY_RULE` is
(`crds/backup_schedule.rs:9-21`): per-field transition rules do not fire on absent → present, and
`optionalOldSelf` is 1.30+, above the 1.29 floor. The enumerated list form is used and must be
applied against a real 1.29+ API server before merge — the map/`filter` form does not compile
(recorded live failure, same module header).

Deleting the whole object is still possible for a cluster-admin (RBAC grants no `delete` to any
Logweir role, but cluster-admin is outside the threat boundary, `docs/stability.md` O0). The
documented backup is `logweir trust export --policy org-default > trustpolicy.yaml`, which writes
public material only.

### 7.2 What replaces the roster's other fields

`allowedClusterIds` → `allowedTargetClusterIds` (same semantics, same cluster scope, consumed by
the restore bundle's `allowed-clusters.json`, `controllers/restore.rs:765`). `signingKeys`/
`approverKeys` → key entries with usages. `status.expiredKeyIds` → `status.keys[].effectiveState`
plus `evaluatedAt`, so a consumer can tell "not evaluated" from "evaluated and valid".

### 7.3 Key usage separation (required by P17 for PLAT-19.2)

| usage | who holds it | what it may do |
|---|---|---|
| `EvidenceSigning` | runner signing key (the installation identity, `logweir-signing-key`) | verify receipts, scorecards, teardown attestations, catalog records, retention records |
| `GovernedApproval` | human approvers, on their own machines | sign approval and standing-authorization documents |
| `ConsoleConfirmation` (amended 2026-09-17: the landed CRD spells this usage `ConsoleConfirmation`; the decision's earlier name `ConfirmationIssuer` is retired because an immutable enum value cannot be renamed once an object exists) | `logweir-api`'s confirmation key (PLAT-19.2 ordinary mode) | attest the authenticated requester |

CEL forbids combining `EvidenceSigning` with either approval usage, and forbids combining
`GovernedApproval` with `ConsoleConfirmation`. This turns today's labelled
`selfAttestedRisk` (`controllers/approval.rs:485`) into an enforced separation for policy-backed
namespaces; the label remains for legacy-roster namespaces, where the two lists may overlap.
A `ConsoleConfirmation` key is never synthesized from the legacy roster, so an old controller
reached by rollback cannot mistake an ordinary confirmation for a governed approval (P17's
fail-closed rule).

### 7.4 Verification semantics: retirement vs revocation

Two distinct questions, one pure function (`logweir_core::trust::decide`, no I/O, no clock read —
`now` and the observation time are arguments):

- *May this key sign something new?* `Active` ∧ `notBefore ≤ now < notAfter` ∧ usage matches.
  Approval verification at admission is a "new use": a retired or expired approver key refuses a
  fresh approval exactly as `KeyIdExpired` does today (`controllers/approval.rs:442`).
- *Does this existing evidence still verify?*

| key state | verdict for stored evidence |
|---|---|
| `Active`, evidence signed inside validity | `Valid`, `trust.basis: Current` |
| `Expired`/`Retired`, evidence signed at or before `notAfter`/`retiredAt` | `Valid`, `trust.basis: Historical` |
| `Expired`/`Retired`, evidence claiming a later signing time | `Untrusted`, reason `SignedOutsideValidity` |
| `Revoked` with `Superseded`/`Unspecified` | treated as retirement at `revocationEffectiveFrom` |
| `Revoked` with `KeyCompromise`, independent observation before `revocationEffectiveFrom` | `Untrusted`, reason `RecordedBeforeRevocation` (rendered with the recorded instant, never green) |
| `Revoked` with `KeyCompromise`, no independent observation | `Untrusted`, reason `Revoked` |
| key absent from the policy | `Untrusted`, reason `UntrustedSigner` |
| usage mismatch | `Untrusted`, reason `KeyUsageMismatch` |

"Signing time" is the document's own latest pre-signature timestamp, read by
`logweir_core::trust::claimed_signing_time(payload_type, json)`: receipt `finished_at`, scorecard
`phases[last].at`, teardown `deleted_at`, approval `approved_at`, catalog record `recorded_at`.
For a compromised key that claim is attacker-controlled, so it is deliberately **not** used there;
the only accepted independent observation is a controller-written one — `Backup`/`Restore`
`status.evidence.verification.verifiedAt` recorded by an earlier reconcile. An imported archive
with no such history and a compromise-revoked signer fails closed.

Status shape (additive, no existing value changes meaning):

```yaml
evidence:
  verification:
    result: Valid | Invalid | NotAttempted | Untrusted     # `Untrusted` is new
    matchedKeyId, payloadType, verifiedAt, detail          # unchanged
    signedAt: date-time                                    # new: the claimed signing time used
    trust:                                                 # new
      basis: Current | Historical | RecordedBeforeRevocation | None
      keyState: Active | Retired | Expired | Revoked | Unknown
      policy: {name, uid, generation}
```

Old readers (the shipped UI's `validVerification`, `ui/pages/backups.js:71`) treat anything that is
not `Valid` as `unverified` — the new value therefore fails closed on every old surface. The green
badge rule becomes `Valid ∧ (basis Current|Historical) ∧ run success`; a `Historical` badge carries
"verified against retired key `<id>` (signed before retirement)".

**Re-evaluation without re-fetching.** Terminal objects are not re-read (§1), but a revocation must
change what the console says. The controller watches `TrustPolicy`; on a generation change it
re-runs `trust::decide` for objects in the bound namespaces using the *stored* `matchedKeyId`,
`signedAt` and `verifiedAt` and patches only `evidence.verification.{result,trust,detail}` — never
`phase`, `exitCode`, `outcome` or any other field, and never a storage read. The work is bounded
(objects are enqueued with rate limiting, and a no-op comparison writes nothing per E11(d)).

### 7.5 Upgrade from the default roster, and rollback

- A controller with this change resolves trust as in §7.1. With no `TrustPolicy` in the cluster it
  synthesizes `legacy-roster-v1` from `TrustRoster/default`: `approverKeys` → `GovernedApproval`,
  `signingKeys` → `EvidenceSigning`, `allowedClusterIds` → `allowedTargetClusterIds`, `notAfter`
  verbatim, `state: Active`, `principal.id: legacy:<keyId>`. Behavior is byte-for-byte today's,
  including "a partially loaded roster is not a roster" (any unparseable PEM refuses everything).
- Migration is explicit and reviewable:
  `kubectl --context <ctx> get trustroster default -o json | logweir trust migrate-roster --stdin
  --name org-default --default > trustpolicy.yaml`, reviewed, then `kubectl apply`. The roster is
  **not** deleted; once a matching policy exists, `TrustRoster/default` gets
  `Superseded=True/SupersededByTrustPolicy` and stops being consulted for bound namespaces.
- Rollback: the old controller reads only `TrustRoster/default`, which is still present and
  unchanged, so governed approvals and evidence verification keep working. Keys added to the
  TrustPolicy after migration are unknown to it, so the documented rollback procedure is: add any
  post-migration key to the roster as well before rolling back, or accept `NotAttempted` on
  evidence signed by it. Nothing deletes public material in either direction.
- Multiple namespaces: several policies may exist, each naming its namespaces; the chart's
  one-installation-identity singleton marker (`charts/logweir/templates/identity.yaml`) is
  unchanged — multiple *installations* per cluster remain out of scope; multiple *trust policies*
  are now supported.

### 7.6 Rotation, with old archives still verifiable

The supported procedure, documented in `docs/keys.md` (which today says rotation is operator-driven
and that the immutable roster cannot express an overlap window):

1. Add the new public key to the bound `TrustPolicy` as `Active`, usage `EvidenceSigning`
   (overlap begins; both keys valid).
2. Point the runner at the new private key: a new retained Secret and the controller value
   `identity.activeSigningSecretName` (new chart value backing a new controller env
   `LOGWEIR_SIGNING_KEY_SECRET`, replacing the compiled constant `SIGNING_KEY_SECRET`,
   `controllers/backup.rs:141`). `logweir identity bootstrap` is unchanged and still refuses to
   replace an established identity; `logweir identity rotate --confirm-current <keyId>` creates the
   new Secret and its own retained public ConfigMap without touching the old pair.
3. Wait for in-flight Jobs, then set the old key `state: Retired` with `retiredAt: now`.
4. Old archives keep verifying under §7.4's `Historical` rule; the old public material can never be
   edited out of the policy.

Readiness (PLAT-03.1 input, not an execution gate): the controller compares the key id published in
`logweir-signing-trust` with the bound policy and reports `SignerTrusted=False/SignerNotInPolicy`.
Backups are **not** blocked by it — protection availability outranks verification display — but
their evidence verifies as `Untrusted`, and `ProtectionPolicy` therefore does not count those
points as available when `requireVerifiedEvidence` is set.

### 7.7 Keys view: unknown is not valid

`ui/pages/keys.js` today prints `valid` for any key not listed in `status.expiredKeyIds`, even when
the roster carries no status at all (`:75`). The replacement renders, per key:
`state` from `status.keys[].effectiveState`, `usages`, `principal`, validity window, and an
`EVALUATION` column that reads **`unknown`** when any of: `status` is absent;
`status.observedGeneration != metadata.generation`; or `evaluatedAt` is older than 15 minutes.
Freshness is measured against a **server** clock — the API's own time in shared mode, the
Kubernetes API response `Date` header in the legacy localhost mode — never the browser's, keeping
the existing rule that a page renders no verdict from a clock the cluster never saw. `valid` and
`expired` are only rendered for a fresh evaluation.

## 8. Kind and ADR decisions (ADR 0008 Amendment A)

Amendment A fixes the kind list and requires a recorded architectural decision for a new kind.
D2 already claims **Amendment F** for `BackupDestination`, `TopicDiscovery` and `Preflight`
(seam S8), so this decision takes the next letters: **Amendment G — operational recovery kinds**
(the five below), **Amendment H — storage deletion boundary**, **Amendment I — execution contract
v2**. All three are recorded in `docs/architecture.md` beside the existing amendments, with
`crates/weirkeeper/src/crds/mod.rs::KINDS` and `the_kind_list_is_exactly_six` updated to the
resulting fourteen kinds (six shipped + D2's three + these five):

| Kind | Scope | Why it is a kind and not a field |
|---|---|---|
| `TrustPolicy` | Cluster | It replaces a cluster-scoped kind; a namespace tenant must not be able to name or edit the trust that authorizes it. Cannot live on a namespaced object at all. |
| `ProtectionPolicy` | Namespaced | The objective outlives any one schedule (schedules are immutable and are replaced by drain-and-retain, `docs/kubernetes.md` §9), spans several schedules, and is mutable policy — it cannot go on a sealed `BackupSchedule` spec. |
| `RehearsalSchedule` | Namespaced | It creates executions on a cron, owns concurrency/reservation state and a standing authorization, and needs its own RBAC. Folding it into `ProtectionPolicy` would put execution authority into an object operators edit routinely. |
| `RecoveryCatalog` | Namespaced | A destination is storage configuration (PLAT-08.1, likely immutable); a catalog has a mutable sync trigger, its own bounded view, its own Jobs and its own failure modes, and must exist for archives that predate this installation. |
| `RetentionPolicy` | Namespaced | Deletion authority must be authorizable separately from destination or schedule editing, and its approved-plan state is mutable. Putting it on a destination would make "who may configure storage" and "who may delete data" the same grant. |

Names encode no cluster, fleet, topic, connector or backup (Amendment B). No kind is removed:
`TrustRoster` stays served and reconciled, marked deprecated in the CRD description and in
`docs/kubernetes.md` §7/§8, and the two reserved names are untouched.

The same record carries two constraint amendments:

- **Amendment H — storage deletion boundary.** Global Constraint 6 ("Logweir writes only under
  `logweir/`, create-only") is extended with: *a separately linked, separately credentialed,
  optional retention worker may delete objects under an explicitly configured archive prefix,
  never under `logweir/`, only from an administrator-approved plan, and only with an attributable
  signed record.* `logweir-store` remains delete-free, the control plane remains delete-free, and
  G-RET becomes a linkage gate as well as a source-text gate (§6.5). Tag 1's statement "no Logweir
  component holds any delete capability against object storage" becomes version-scoped: it is true
  wherever `RetentionPolicy.mode != Enforce`, and `docs/stability.md`, `docs/kubernetes.md` §9 and
  §15.1 and the chart README must be updated together with the code that changes it.
- **Amendment I — execution contract v2.** `logweir_core::execution_contract::VERSION` moves to
  `"2"`, carrying the point binding (§5.5), the standing rehearsal authorization (§4.3) and the new
  key lines (§2.4). v1 stays accepted for already-created Restores under the documented transition
  in `docs/kubernetes.md` §12.

## 9. RBAC, chart and network changes

Controller (`config/rbac/role.yaml` and `charts/logweir/templates/clusterrole.yaml`, kept
rule-for-rule identical, every granted verb with a named caller —
`manifest_lint::every_granted_verb_has_a_caller`):

| Rule | Verbs | Caller |
|---|---|---|
| `logweir.dev`: `protectionpolicies, rehearsalschedules, recoverycatalogs, retentionpolicies, trustpolicies` | get, list, watch | the five new reconcilers, trust resolution |
| `logweir.dev`: the same five `/status` | patch | those reconcilers |
| `logweir.dev`: `restores` | **create** (new) | `controllers/rehearsal_schedule.rs` only |
| `""`: `events` | list (new) | `diagnostics::observe` for pod/Job failures (§2.3) |
| existing rules | unchanged | unchanged |

Still **no** verb on `secrets`, no `pods/exec`, no `pods/attach`, and no `update` on any spec
except D1's editable `backupschedules` path. On `delete`: this decision adds **none** — no delete
on ConfigMaps, Backups, Restores, Jobs or object storage; D2's transient check kinds keep their own
UID-preconditioned GC deletes, and the catalog view is collected through Job TTL precisely so that
list does not have to grow (§5.3). Every status write in the new controllers is a merge PATCH
carrying `metadata.resourceVersion` as a precondition (seam S7); no new path uses `update` on a
status subresource.

Human roles (`config/rbac/*_role.yaml`, `charts/logweir/templates/human-roles.yaml`):
viewer gains get/list/watch on the five new kinds; operator gains create/update on
`protectionpolicies`, create on `rehearsalschedules` and `recoverycatalogs` plus the two
mutable-field patches (`rehearsalschedules` `suspend`, `recoverycatalogs` `syncRequest`, both
narrowed by CEL, not by RBAC); approver gains nothing; two new roles —
`logweir-retention-admin` (namespaced: create/update `retentionpolicies`) and `logweir-trust-admin`
(cluster: create/update `trustpolicies`, no delete).

Job ServiceAccounts (chart, `automountServiceAccountToken: false` on all of them):
`logweir-runner` (existing) also runs catalog sync; `logweir-notifier` for delivery Jobs;
`logweir-retention` for enforcement Jobs. Chart NetworkPolicies, with the existing honest caveat
that Docker Desktop does not enforce them: catalog sync and retention egress to DNS plus
object-store ports only; notifier egress to DNS plus 443 only; neither reaches a broker.

New chart values (all default off): `protection.enabled`, `rehearsals.enabled`,
`catalog.enabled`, `retention.enabled` (gates the retention SA/role), `identity.activeSigningSecretName`,
`controller.failFastSeconds`, `controller.jobTtlSeconds`.

## 10. API surface (P17 conventions: problem+json, Idempotency-Key, cursors, capability flags)

```
GET  /api/v1/namespaces/{ns}/operations/{kind}/{name}            kind = backup|restore
GET  /api/v1/namespaces/{ns}/operations/{kind}/{name}/events     SSE (§2.6)
GET  /api/v1/namespaces/{ns}/protection-policies[/{name}]
POST /api/v1/namespaces/{ns}/protection-policies                 operator; Idempotency-Key
PUT  /api/v1/namespaces/{ns}/protection-policies/{name}          operator; expectedResourceVersion
POST /api/v1/namespaces/{ns}/protection-policies/{name}:test-notification   operator; 1/min
GET  /api/v1/namespaces/{ns}/rehearsal-schedules[/{name}]
POST /api/v1/namespaces/{ns}/rehearsal-schedules                 operator
POST /api/v1/namespaces/{ns}/rehearsal-schedules/{name}:set-suspension
GET  /api/v1/namespaces/{ns}/rehearsal-schedules/{name}/authorization-packet   approver/operator
GET  /api/v1/namespaces/{ns}/catalogs[/{name}]
POST /api/v1/namespaces/{ns}/catalogs                            operator ("connect existing archive")
POST /api/v1/namespaces/{ns}/catalogs/{name}:sync                operator; sets spec.syncRequest
GET  /api/v1/namespaces/{ns}/catalogs/{name}/points              limit<=200, signed cursor, filters
GET  /api/v1/namespaces/{ns}/catalogs/{name}/points/{pointId}
GET  /api/v1/namespaces/{ns}/catalogs/{name}/signers
GET  /api/v1/namespaces/{ns}/retention-policies[/{name}]
GET  /api/v1/namespaces/{ns}/retention-policies/{name}/preview   the plan page, admin/operator read
POST /api/v1/namespaces/{ns}/retention-policies/{name}:approve-plan   admin; {planSha256, expectedResourceVersion}
GET  /api/v1/trust-policies[/{name}]                             installation-admin read
```

Trust **writes** stay off the API in v1 (`capabilities.trustAdministration: false`); the supported
path is `kubectl apply` plus the `logweir trust` helpers, per P17's "cluster-scoped trust writes
use a separately reviewed admin path". `catalogWindowQuery: false` until windowed queries exist.

Cursor for `points`: signed, binds actor, namespace, catalog, generation, filters and offset;
a generation change or an expired view is `410 cursor_expired` with "restart the list", never a
silently different page. Page reads are bounded to 8 ConfigMaps per request; a point lookup uses
the fence-pointer index ConfigMap, so it is one `get`.

DTOs are separate from CRDs (P17), `deny_unknown_fields` on every mutation input, and no response
carries a Secret name's *value*, a credential, a raw Kubernetes error, a pod log, or approval bytes
outside the explicit approval-packet route.

## 11. UI touchpoints

| File | Change |
|---|---|
| `ui/api.js` | one added request site: SSE via `EventSource` (same origin) plus `watch`/`poll` helpers; no new writable plural; no token, no browser storage |
| `ui/operation-watch.js` (new) | reconnecting watch with backoff, terminal stop, `lifecycle.signal` disposal (`ui/lifecycle.js`) |
| `ui/pages/operation.js` (new) | the durable operation view: state, reason, last update, diagnostics list, result and evidence rendered separately, completion panel (§3.5) |
| `ui/pages/protection.js` (new) | protection health: last available point + age, objective, missed/failed runs, schedule health beside protection health, alert ledger and delivery state |
| `ui/pages/catalog.js` (new) | connect-archive form, point list with availability/verification columns, untrusted-signer panel with the fingerprint command and no one-click trust, "Restore this point" action carrying `pointId` |
| `ui/pages/keys.js` | TrustPolicy rendering, `unknown` evaluation column (§7.7), usages and lifecycle states, retirement/revocation explanation; still submits nothing |
| `ui/pages/backups.js`, `ui/pages/history.js` | badge rule gains the `Historical` qualifier and the `Untrusted` case; rows link to the operation view; `verificationScope` sentence beside every restore result |
| `ui/pages/schedules.js` | retention panel labels the enforcement mode and, in `Enforce`, shows the approved-plan state and the irreversibility sentence; `RETENTION_SENTENCE` is kept verbatim for `Report` and replaced by a mode-specific sentence elsewhere |
| `ui/render.js` | new fixed sentences (cutover guidance, sampled-verification statement, unknown-evaluation, enforcement modes); still no verdict computed here |

Existing gates keep applying: `scripts/check-ui-offline.sh` (no external resource, no credential,
no browser storage, relative specifiers only), `crates/logweir/tests/ui_lint.rs`,
`scripts/check-ui-behaviour.sh`, `just chart-check` for asset parity.

## 12. Compatibility, migration, rollback

Absent-field behavior (the rule everywhere): an absent field means **not observed**, never a
default that flatters the object. `progress` absent → the API maps from phase alone and never
reports `queued`/`preparing`; `capture` absent → freshness falls back to the Backup's terminal
condition time and `availabilityBasis` says so; `trust` absent → `basis: None` and the badge uses
the pre-existing rule; catalog `partitions` absent → size bound not enforced and recorded as
`sizeBasis: unknown`; `execution` absent in a record → provenance unknown.

Schema changes, all additive except one:

- New CRDs (five). Install before the controller; Helm installs CRDs on first release only, so the
  documented `kubectl apply -f config/crd/...` + `wait --for=condition=Established` procedure of
  `docs/kubernetes.md` §12 applies unchanged.
- `Backup.status.{progress,capture}`, `Restore.status.{progress,completion,teardown}`,
  `*.status.evidence.verification.{signedAt,trust}` — additive status fields.
- `Restore.spec.{runnerResources,authorization}` — additive optional spec fields. **Breaking-ish**:
  `Restore.spec.approvalRef` becomes optional with a CEL rule "exactly one of `approvalRef` or
  `authorization`". Existing objects are unaffected (they set `approvalRef`), and an older
  controller reading a standing-authorized Restore refuses it terminally with
  `ApprovalNotReceived` — fail closed by design.
- `Approval.spec.subjectRef.kind` gains `RehearsalSchedule` (enum extension). Apply the CRD before
  any such Approval is created; an older controller fails to deserialize the new variant and
  therefore verifies nothing — again fail closed.
- `BackupSchedule.status.retentionReport.{enforcement,supersededBy}` — additive.

Upgrade order (extends the P17 order): CRDs → controller → runner image (contract v2) → console
API → UI assets → *then*, per namespace and only explicitly: apply a `TrustPolicy`, create a
`RecoveryCatalog`, create a `ProtectionPolicy`, create a `RehearsalSchedule`, create a
`RetentionPolicy` in `Report`. `Enforce` is a separate, later, administrator decision with its own
credential.

Rollback:

1. Set every `RetentionPolicy` to `mode: Report` and wait for `status.lease` to clear and any
   enforcement Job to finish. **Do not roll back with a retention Job in flight**: the old
   controller does not know about leases and the old restore admission does not hold on them.
2. Suspend `RehearsalSchedule`s (`spec.suspend: true`) and let active rehearsal Restores finish;
   an older controller will not create new ones (it ignores the kind) and will refuse existing
   standing-authorized Restores terminally.
3. Roll the controller and runner back together (the contract-v2 handshake refuses mixed pairs by
   design, which is what prevents an old runner from skipping the new point check).
4. New CRDs and their objects stay installed and inert; catalog views expire with their Job TTLs;
   the durable catalog in object storage is untouched and readable by the CLI.
5. Trust: the roster is still authoritative for the old controller (§7.5). Never delete public key
   material as part of a rollback.

Data compatibility: no change to the scorecard, put-receipt, backup-receipt or teardown formats;
their `format_version` stays `1.0.0` and every checked-in signed fixture must still verify
byte-for-byte (existing parity and fixture gates cover it). New payload types
(`catalog-point`, `retention-plan`, `retention-record`) are new media types with their own schemas
under `schemas/`, their own `docs/formats/*.md`, and both-reader parity for the signature check
(`scripts/check-verifier-parity.sh` gains `catalog-point` and `retention-record` in
signature-only mode; invariant mirrors are explicitly out of scope and stated as such).

Documentation to update in the same change: `docs/kubernetes.md` (§7 kind table and deprecation,
§9 retention, §10/§12 status progress and diagnostics, new sections for catalog, rehearsals and
trust), `docs/keys.md` (rotation with overlap; the roster paragraph), `docs/install.md` (trust
policy migration, connect-existing-archive, retention credential), `docs/stability.md` (the
version-scoped deletion statement, new formats, unknown-is-not-valid), `docs/architecture.md`
(ADR 0009 with Amendments F and G), `charts/logweir/README.md`, `ui/README.md`.

## 13. Test matrix (mapped to the tracker's required tests)

Legend: **U** unit/pure (`cargo test -p <crate>`), **C** controller-double (route tables and fake
APIs in `crates/weirkeeper/tests/*`), **L** live docker-desktop (§15 scenario id), **B** browser
(Playwright, in the shape of `scripts/plat13-ui-e2e.mjs`).

### PLAT-14.1

| Tracker test | Where |
|---|---|
| Mount failure | U `diagnostics` maps a `FailedMount` event + `ContainerCreating` to `VolumeMountFailed`; C the reconcile writes the diagnostic, then fail-fast patches the Job deadline and the terminal reason is the diagnostic; L1 |
| Unschedulable pod | U `crash_terminal_state` unchanged + new grace rule; C `PodUnschedulable` diagnostic after 120 s, no fail-fast (transient); L2 |
| Engine crash | C exit 1/2 mapping to `failed` with `result.notPass` for exit 2 and evidence untouched; U state table |
| Verification downgrade | U mapping of `Valid`→`Untrusted` after a policy change; C the re-trust patch touches only the verification block (asserts phase/exitCode/conditions unchanged); L8 |
| Stream disconnect | U SSE resume from `Last-Event-ID`, `reset` on 410; B reconnect during a run |
| Refresh | B reload mid-run resumes the same UID and state; U DTO is a pure function of stored status |
| Completed Job cleanup | C TTL patched after the status patch, and repaired on a terminal object missing it; L4 |
| (regression) status write economy | C a steady object issues **zero** patches per reconcile (E11(d)); `lastObservedTime` writes at most once per 60 s |

### PLAT-14.2

| Tracker test | Where |
|---|---|
| Stale point | U health table across the five states; C evaluation from Backup + catalog fixtures; L5 |
| Repeated failure deduplication | U alert ledger transitions; C exactly one delivery Job per `(key, transition)`, 409 on replay; L5 |
| Recovery notification | U `Resolved` transition and `RecoveryCompleted` keying on Restore UID; C resolve event body; L5 |
| Unavailable archive | C catalog `Missing`/`Unreadable` for the newest point → older point selected, `ArchiveUnavailable` opened; U availability rule |
| Sampled-versus-complete labelling | U `verificationScope` mapping and the notification body's `verification_scope`; a mutant that emits `complete` fails; B the completion panel sentence |
| Notification transport failure | U sink error is swallowed and reported; C three bounded attempts then `NotificationsDelivered=False` with **no** patch to any Backup/Restore (managed-field assertion); L5 |

### PLAT-14.3

| Tracker test | Where |
|---|---|
| Missing recovery point | U `select_point` empties with `NoQualifyingPoint`; C slot skipped and recorded, no Restore created |
| Unavailable target | C `ClusterNotReachable`/marker missing → `TargetUnavailable` skip; L6 |
| Approval policy | U scope check accepts the rendered plan and refuses a prefix/topic/deadline outside scope; C expired/absent/foreign-subject standing approval → zero ConfigMap and zero Job calls; L6 |
| Overlapping drill | C `Forbid` reservation under two reconciles and a restart; per-target `TargetBusy`; L6 |
| Failed verification | C a rehearsal Restore with `outcome: fail-integrity` sets `RehearsalHealthy=False` and opens `RehearsalFailure` |
| Cleanup failure | U teardown attestation parsing into `status.teardown`; C leftover topics block the next slot with `LeftoverTopics`; L6 asserts the unrelated topic survives |
| Successful evidence retention | L6 scorecard, sidecar, offset report and teardown keys still fetchable after Job TTL |

### PLAT-15.1

| Tracker test | Where |
|---|---|
| Missing/corrupt manifest | U availability mapping `NotFound` → `Missing`, other errors → `Unreadable`, digest mismatch → `Invalid`; C counts on status |
| Duplicate identity | U same receipt at two locations → one point, two locations; two records disagreeing → `Conflict` on both |
| Large catalog | U page/fence-pointer math; L (harness) 50 000 synthetic points: sync within budget, `truncated: true`, counts exact, API page reads ≤ 8 ConfigMaps |
| Partial access | C a 403 on some prefixes yields `Unreadable` entries and `Synced=False/PartialScan`, never `Missing` |
| Stale index | U `Stale` after `2 × interval`; C `ViewExpired` once pages are GC'd |
| Schema-version compatibility | U major 2 record → `UnsupportedFormat` entry, sync still completes; unknown field inside major 1 ignored; absent optional field → `unknown` |

### PLAT-15.2

| Tracker test | Where |
|---|---|
| Source offline | L7 (source deleted before the restore) |
| CR loss | L7 (`kubectl get backups` empty in the recovering namespace) |
| Old signer | L7/L8 point verified `VerifiedHistorical` after the old key is added as `Retired` |
| Untrusted signer | U/C `UntrustedSigner` state and signer summary; B no one-click trust, fingerprint mismatch refused with `confirmFingerprint` |
| Storage denial | C sync Job exit 1 → `Synced=False/StorageDenied`, previous view retained until TTL |
| Incomplete point | U manifest present + receipt missing → `NoEvidence`, not selectable; `SegmentSample` missing segment → `Partial` |
| Repeated import | C a second sync of the same archive yields identical point ids and no duplicate entries |

### PLAT-16.1 / PLAT-16.2

| Tracker test | Where |
|---|---|
| Two destinations | C two policies/catalogs report only their own points; the legacy schedule report carries the mismatch note; L11 |
| Missing lifecycle permissions | C `ExternalLifecycleDeclared` guarantees table and `NotEnforced` labels; no claim of enforcement |
| Unreadable manifest | U skipped, never a candidate; existing `skipped` semantics preserved |
| Overlapping keep rules | U `keepLast` ∩ `keepDays` union with `OlderThanKeepDays` precedence (existing rule) and `minUsablePoints` override |
| Evaluation failure | C backup execution continues; `Evaluated=False` only |
| Continued scheduled backup | L9 a scheduled Backup completes during an enforcement run |
| Dry preview | U plan bytes and `planSha256` are a pure function; C no Job without an approved hash; L9 |
| Denied deletion | U `AccessDenied` is not retried; C `Denied` recorded, point kept |
| Active restore | U protection set; C lease + consistent re-list before Job creation; admission hold `PointRetentionInProgress`; L9 |
| Legal hold / lock | U provider refusal → `LegalHold`, excluded from the next plan; `spec.holds` honored |
| Last usable point | U `minUsablePoints` overrides both rules; a policy that would empty the archive deletes nothing |
| Shared segment | U a segment referenced by another retained manifest protects the point |
| Partial failure | U manifest deleted + segments failed → `Orphaned` tombstone, next plan completes exactly those keys |
| Policy change | C changing rules invalidates the approved hash (plan hash differs) |
| Wrong-prefix rejection | U every key validated before any delete; a key outside the scope exits 3 with zero deletes; L9 negative control |
| Bounded retry | U 3 attempts/backoff only for 5xx and timeouts; C `EnforcementDegraded` after 3 failed runs |
| Linkage | `scripts/check-no-archive-write.sh` + dependency walk: `weirkeeper`, `logweir-store`, `logweir`, `logweir-api` must not reach `logweir-reaper`; mutant: adding the dependency fails the gate |

### PLAT-19.1

| Tracker test | Where |
|---|---|
| Overlap | U two `Active` `EvidenceSigning` keys both verify; C a rotation window verifies old and new archives; L8 |
| Retirement | U `Historical` verdict; old archive still `Valid`; new signature refused |
| Revocation | U compromise vs superseded tables, independent-observation rule; C re-trust patch downgrades to `Untrusted` |
| Unknown/stale expiry | U the `unknown` rule (absent status, generation skew, stale `evaluatedAt`); B the keys view renders `unknown`, not `valid`; mutant: rendering `valid` for an absent status fails |
| Unauthorized update | L8 `kubectl auth can-i update trustpolicies` as the operator subject is `no`; CEL refuses removing or re-materializing a key and refuses `Revoked → Active` |
| Old archive | L7/L8 |
| Multiple namespaces | U resolution order and conflict refusal; C two policies, two namespaces, one conflict |
| Upgrade from the default roster | U synthesized `legacy-roster-v1` equals today's behavior including the partial-load refusal; L10 |

Cross-cutting regression suites to extend rather than duplicate (PLAT-20.1):
`crates/weirkeeper/tests/{backup_controller,restore_controller,schedule_controller,verification,
retention,crd_shape,linkage}.rs`, `crates/logweir/tests/{notify,backup_run,two_reader_parity,
ui_lint,chart_lint,manifest_lint,doc_lint,label_gate,no_network_in_unit_tests}.rs`, plus
`just crds`, `bash scripts/check-chart.sh --write`, `just chart-check`, `just schema-check` and
`just lint` for anything touching CRDs, chart assets, scripts or docs. Unit tests dial nothing
(`docs/stability.md`): every storage, broker and HTTP boundary in the new code is behind an
injected oracle/sink, as `ArchiveOracle`, `VerifyOracle` and `EventSink` already are.

## 14. Implementation plan: bounded, parallelizable worker tasks

Ownership is exclusive: a worker edits only its files and reports any out-of-ownership edit.
Shared files (`crds/mod.rs`, `conditions.rs`, `config/rbac/**`, chart templates, docs) have exactly
one owner per wave to keep concurrent worktrees mergeable.

### Wave 0 — shapes (must land first, one worker)

**W0 `d3-crds`.** Owns `crates/weirkeeper/src/crds/{mod.rs,trust_policy.rs,protection_policy.rs,
rehearsal_schedule.rs,recovery_catalog.rs,retention_policy.rs,backup.rs,restore.rs,approval.rs}`,
`crates/weirkeeper/examples/emit_crds.rs`, `config/crd/**`, the chart CRD copies,
`crates/weirkeeper/tests/crd_shape.rs`.
Delivers: the five new kinds with their CEL rules (append-only trust entries, sealed rehearsal spec
except `suspend`, catalog `syncRequest`-only mutability, retention immutables), the additive status
blocks of §2.2/§7.4, `Restore.spec.{authorization,runnerResources}`, the `Approval` enum extension,
`KINDS` and the kind-list test, `just crds` output and chart parity.
Acceptance: `just crds`, `just crds-check`, `just chart-check`, `just schema-check`, plus a live
`kubectl --context docker-desktop apply` of every new CRD and a CEL transition probe per rule
(the map-form CEL failure recorded in `crds/backup_schedule.rs` is the reason this is live-tested,
under the cluster lock).

### Wave 1 — parallel, no cluster dependency

- **W1 `d3-trust-core`** — `crates/logweir-core/src/trust.rs` (+ `rehearsal_scope.rs`),
  `crates/weirkeeper/src/trust.rs` (resolution, synthesis of `legacy-roster-v1`),
  `crates/weirkeeper/src/controllers/trust_policy.rs`, `crates/logweir/src/trust.rs`
  (`logweir trust export|migrate-roster`), tests `trust_policy_controller.rs`, `trust_core.rs`.
- **W2 `d3-status-progress`** — `crates/weirkeeper/src/diagnostics.rs`, `conditions.rs` additions,
  the progress/diagnostic/fail-fast/TTL-repair paths in
  `controllers/{backup,restore}.rs`, tests in `backup_controller.rs`/`restore_controller.rs`.
  *Sequenced after PLAT-06.1 and PLAT-01.2 merge* (same two files); rebase, never revert.
- **W3 `d3-catalog-writer`** — `crates/logweir-store/src/lib.rs` (`list_page` only),
  `crates/logweir/src/catalog/**` (record writer, `logweir catalog sync|list`),
  `crates/logweir/src/backup/phase_run.rs` (the fifth put and the `catalog-key=` line),
  `schemas/logweir-catalog-point-1.0.0.json`, `docs/formats/catalog-point.md`,
  `docs/verify_scorecard.py` (+ `docs/test_verify_scorecard.py`) signature-only support,
  `scripts/check-verifier-parity.sh`.
- **W4 `d3-notify`** — move the notification half to `crates/logweir/src/notify.rs` with
  re-exports, add `logweir notify deliver` and the protection-event body,
  `crates/logweir/tests/notify.rs`.
- **W5 `d3-runner-contract`** — `crates/logweir-core/src/execution_contract.rs` v2, the
  `progress-phase=`/`teardown-key=` lines and the point-binding check in
  `crates/logweir/src/{backup,drill}/**`, the standing-authorization scope check in the runner,
  tests `crates/logweir/tests/{restore_phase,backup_run,orchestrator}.rs`.
  *Coordinates with PLAT-01.2 and PLAT-19.2 workers; owns the version bump.*

### Wave 2 — controllers (parallel; each depends on W0 and its wave-1 crate)

- **W6 `d3-protection`** — `crates/weirkeeper/src/protection.rs` (pure evaluation),
  `controllers/protection_policy.rs`, tests `protection_controller.rs`. Depends on W4 (event
  document), W3 (catalog entries as an input type).
- **W7 `d3-rehearsal`** — `crates/weirkeeper/src/rehearsal.rs`,
  `controllers/rehearsal_schedule.rs`, tests `rehearsal_controller.rs`. Depends on W1 (trust),
  W5 (scope/contract), PLAT-04.1's reservation helpers, PLAT-19.2's authorization document.
- **W8 `d3-catalog-controller`** — `controllers/recovery_catalog.rs`, page/fence-pointer
  materialization, `crates/weirkeeper/src/catalog_view.rs`, tests `catalog_controller.rs`.
- **W9 `d3-retention`** — `crates/logweir-reaper/**`, `crates/logweir-retention/**` (binary),
  `crates/weirkeeper/src/retention_plan.rs`, `controllers/retention_policy.rs`, the amended
  `scripts/check-no-archive-write.sh`, tests `retention_policy_controller.rs`, `reaper.rs`.
  Depends on W8 (catalog view) and PLAT-08.1 (destination + credential shape).
- **W10 `d3-verification-trust`** — `crates/weirkeeper/src/verification.rs` (trust projection,
  re-trust pass, `carry_conditions`), `controllers/approval.rs` (trust resolution in place of
  `load_roster`), tests `verification.rs`, `approval_controller.rs`. Depends on W1.

### Wave 3 — surfaces

- **W11 `d3-api`** — `crates/logweir-api/src/status.rs` (normalization + SSE) and
  `src/routes/{operations,protection,rehearsals,catalogs,retention,trust}.rs`, contract fixtures.
  Depends on PLAT-17.1 stage 1 and on the status/CRD shapes.
- **W12 `d3-ui`** — `ui/api.js`, `ui/operation-watch.js`, `ui/pages/{operation,protection,catalog,
  keys,backups,history,schedules}.js`, `ui/render.js`, `ui/tests/**`. Depends on W11 (or its
  fixtures) and must keep the offline/behaviour/lint gates green.

### Wave 4 — integration

- **W13 `d3-rbac-chart-docs`** — `config/rbac/**`, `charts/logweir/templates/**`,
  `charts/logweir/values.yaml` + schema, `charts/logweir/README.md`, `docs/{kubernetes,keys,
  install,stability,architecture}.md`, `ui/README.md`. Single owner to avoid template conflicts.
- **W14 `d3-live-acceptance`** — `scripts/test-d3-live.py` (in the shape of
  `scripts/test-plat04-live.py`), the §15 scenarios, artifacts under
  `/tmp/logweir-roadmap-run/claude/artifacts/d3-live/`. Owns no product source.

Review: a code reviewer and a Rust reviewer on every wave; an independent security review is
mandatory for W1/W10 (trust), W9 (deletion) and W11 (API), per the tracker's execution rules.

## 15. Live docker-desktop acceptance scenarios

Rules for every scenario: `kubectl --context docker-desktop` / `helm --kube-context
docker-desktop` only; namespaces `lw-d3-<case>-<utcstamp>` labelled
`logweir.dev/test-owner=d3-live`; only self-created resources are deleted, after verifying the
label and UID; the shared `logweir-scram-local` release's Kafka/MinIO may be read from, and
changing its controller/CRDs requires `/tmp/logweir-roadmap-run/claude/k8s-lock.sh`. Docker Desktop
does not enforce NetworkPolicy; no scenario may claim a deny-path result from it.

| # | Scenario | Exact observable pass criteria |
|---|---|---|
| L1 | Mount/credential failure surfaced without pod access | A Backup whose archive `secretRef` names a missing Secret reaches, within 90 s: `status.progress.stage=Preparing`, `conditions[type=RunnerReady].status=False` with `reason=CredentialReferenceMissing`, `diagnostics[0].message` naming the Secret and containing no value; after `failFastSeconds` the Job's `activeDeadlineSeconds` is patched, the Backup is `phase=Failed`, `conditions[type=Failed].reason=CredentialReferenceMissing`, `status.exitCode` **absent**; `kubectl auth can-i get pods/log --as system:serviceaccount:<ns>:<console SA>` = `no` |
| L2 | Unschedulable pod | With a namespace `LimitRange` defaulting a 512Gi memory request, a Backup shows `RunnerReady=False/PodUnschedulable` within 180 s with the scheduler's own message; no fail-fast patch is issued (transient class); at the Job deadline the terminal reason is `PodUnschedulable`, `exitCode` absent |
| L3 | Reconnect and navigation stop | Playwright: open the operation route during a run, kill and restart the console pod → the page shows `reconnecting` then continues to `succeeded` with the same `uid`; navigating away produces zero further requests for that route in the network log; a browser reload resumes the same operation |
| L4 | Completed Job cleanup | With `controller.jobTtlSeconds=60`: terminal status is written first (status `resourceVersion` precedes the Job patch in the recorded API trace), the Job and its pod are gone within 120 s, and `GET /operations/backup/<name>` returns the identical DTO afterwards; an object made terminal with the TTL patch suppressed gets its TTL repaired on the next reconcile |
| L5 | Protection staleness, dedup, transport failure | `maxRecoveryPointAgeSeconds=600` over a schedule with a broken source credential: `health=Stale` within 2 intervals; an in-cluster echo sink records exactly **1** POST for the Open transition across ≥ 3 reconciles; fixing the credential yields `health=Healthy` and exactly **1** more POST (resolve); with the sink scaled to zero, `alerts[].delivery.state=Failed` after 3 attempts, `NotificationsDelivered=False`, and every Backup's `resourceVersion` and `managedFields` are unchanged by the protection controller |
| L6 | Rehearsal end to end | A `RehearsalSchedule` (10-minute cron) with a standing authorization signed by the test approver key produces a `Restore` labelled `logweir.dev/rehearsal-schedule`, `outcome=pass`, `evidence.verification.result=Valid`; the target cluster holds exactly the mapped topics `rehearsal-<uid8>-<topic>` during the run and none after teardown; a pre-created unrelated topic `rehearsal-not-ours` still exists afterwards; a second slot while the first is active records `lastSkipped.reason=ConcurrencyBlocked` and creates no Restore; with a mapped name pre-created, the next slot records `LeftoverTopics`/`GuardRefused` and the pre-created topic is untouched; scorecard, sidecar, offset report and teardown objects are fetchable after the Job TTL |
| L7 | Source offline + CR loss on a fresh installation | Namespace A (own Kafka + own signer key K_A) backs up a 200-record topic to MinIO; the source Kafka and the whole namespace A (all CRs) are deleted; in namespace B with a different signer, a destination + `RecoveryCatalog` are created: the point appears with `verification=UntrustedSigner`; after an administrator adds K_A's public half as `Retired/EvidenceSigning` to B's `TrustPolicy` (fingerprint typed, matching `openssl pkey -pubin -outform DER \| openssl dgst -sha256`), the point becomes `VerifiedHistorical` and selectable; the restore of that `pointId` into `kafka-target` completes `outcome=pass`, `Valid`, and an independent consumer reads exactly 200 records with matching keys; `kubectl get backups -n B` is empty and the source pods do not exist. A wrong fingerprint is refused with `fingerprint_mismatch` and no key is added |
| L8 | Rotation and revocation with old archives | Backup with K1 → add K2 `Active` → switch `identity.activeSigningSecretName` → backup with K2: both Backups `Valid`; retire K1: the K1 Backup shows `Valid` + `trust.basis=Historical` and its badge carries the retired-key qualifier while the K2 Backup stays `Current`; revoke K1 with `KeyCompromise` effective now: the K1 Backup becomes `result=Untrusted`, `trust.basis=RecordedBeforeRevocation`, its `phase`, `exitCode` and `conditions[Complete]` unchanged; `kubectl auth can-i update trustpolicies` as the operator subject is `no`; an admin attempt to remove K1's entry is refused by CEL with the rule's message; the keys view renders `unknown` for a policy whose `observedGeneration` lags |
| L9 | Retention enforcement, safely | Destination with 6 points, `keepLast=2`, `minUsablePoints=3`: preview lists exactly 3 candidates and `protected` lists the `minUsablePoints` overrides; a nonterminal Restore referencing the oldest candidate makes it `ActiveRestore`-protected and the Job is not created until that Restore is terminal; after `approve-plan` with the current `planSha256` the worker deletes exactly the planned objects — MinIO shows the manifests and segments gone, every `logweir/` object still present, a signed retention record at `logweir/retention/<uid>/<runId>.json` verifying under the installation key, and catalog entries `Deleted`; a scheduled Backup started during the run completes normally; a plan whose approved hash is stale is refused (`planSha256` mismatch) with zero deletes; a run with a credential scoped to a different prefix records `Denied`, does not retry beyond 3 attempts, and deletes nothing |
| L10 | Upgrade from the default roster | With only `TrustRoster/default`, approvals verify and evidence is `Valid` (`trustSource=legacy-roster-v1` in the condition message); after applying the migrated `TrustPolicy` with `default: true`, approvals still verify, the roster reports `Superseded=True`, and evidence keeps `Valid`; rolling the controller back to the previous image keeps approvals verifying |
| L11 | Two destinations, honest reports | Two MinIO prefixes with distinct point sets and one `RetentionPolicy` each: each report lists only its own `backupId`s; a legacy `BackupSchedule` whose archive URL is not the controller's `LOGWEIR_ARCHIVE_URL` shows the mismatch note and empty set lists instead of another bucket's catalog |

Evidence to record per scenario (WORKER-RULES §Report): context, namespace + UID, image IDs,
commands, raw outputs, the object YAML at each assertion point, cleanup proof and lock usage.

## 16. Risks, gaps and explicitly unsupported claims

- **Deletion is the irreversible boundary.** Enabling `Enforce` makes the controller's existing
  Job-create authority a deletion capability in the namespace that holds the retention credential
  (the same residual class as the signing oracle, `docs/kubernetes.md` §15.4). The hard boundary is
  the credential's own scope (prefix-only, no `logweir/`), and the design keeps every other Logweir
  component delete-free. This must be stated in the chart README and `docs/stability.md`, not
  implied.
- **Object lock cannot be read.** `object_store` 0.14 exposes no WORM readback
  (`logweir-store/src/lib.rs:421`), so "legal hold respected" means "a provider refusal is
  authoritative and recorded", not "Logweir knows the hold exists".
- **The catalog view is bounded.** Points beyond `viewLimit` are counted but not listed in the
  console; windowed queries are an advertised-absent capability, not a stub.
- **Notification transport is best effort** and deliberately never affects a signed result; a
  delivery that fails is visible only on `ProtectionPolicy.status` and in the Job's log.
- **Diagnostics are derived from Events**, which are best-effort and rotated; an absent event
  yields a weaker code (`WaitingForPod`), never an invented cause.
- **Rehearsals prove the supported restore journey, not the application.** Exhaustive verification
  and application assertions remain PROD-08/PROD-06.
- **Compromise revocation of an imported archive's signer fails closed** — there is no independent
  timestamp for evidence this installation never observed; a trusted timestamping service is
  future work.
- **One controller per cluster** still holds, so the L7 "fresh installation" is simulated with a
  separate namespace, a separate signer and deleted CRs; a true two-installation test needs a
  second cluster and is out of scope here.
- **CEL rules must be proven on a real 1.29+ API server** before merge; the recorded map-form
  failure in `crds/backup_schedule.rs` is why this is an acceptance item and not an assumption.

## 17. Alignment with D1, D2 and the orchestrator's seam rulings (binding)

`decisions/D-SEAMS.md` wins over any decision document. This section records how D3 complies and
which of its statements are refined by D1 (`D1-backup-scheduling.md`) and D2
(`D2-destinations-discovery-readiness.md`); the paragraphs above were corrected in place where the
difference was material.

| Seam / neighbour decision | What D3 does |
|---|---|
| **S1 — one check runner** | Catalog sync is a plan kind `catalogSync` of D2's `logweir check run --plan <file> --check-contract-version 1`, using D2's result frames and closed error codes (§5.3). The controller's sync stays that plan kind. Three deliberate exceptions, each with a reason: `logweir notify deliver` (egress with sink credentials, not a check, different failure semantics — it adopts the same startup order, key-line and redaction conventions); the separate `logweir-retention` binary (deletion linkage must not be reachable from the everyday binary, §6.5); and `logweir catalog sync|list` (amended 2026-09-16 at W3's integration: the operator's backfill and read-only listing over the same create-only `logweir/catalog/v1/` key family, run by a human with a public key, which is not a controller-created check Job and produces no result frames — the `catalogSync` plan kind remains what the `RecoveryCatalog` controller runs). |
| **S2 — discovery results are never execution inputs** | Nothing in D3 reads a `TopicDiscovery` result as an input. A rehearsal's topic set comes from the chosen point's frozen topic list; a catalog entry's topics come from the signed receipt. |
| **S3 — completeness vocabulary** | `unknown` / `limited` / `attestedComplete` stays the *topic-coverage* vocabulary. D3's `verificationScope` (`sampled` / `degraded` / `none`) is a different axis — how thoroughly records were reconciled — and no surface may merge the two or claim "all topics" from a verification scope. |
| **S4 — execution inputs grammar** | Backup-side additions (the catalog record inputs, the resolved destination) are blocks in PLAT-06.1's `execution-inputs.json` v2 inside the one immutable Backup-owned ConfigMap, never a second freeze. Restore-side additions (point binding, standing authorization, scope) are blocks in PLAT-01.2's per-Restore immutable bundle, under execution contract v2 (Amendment I). |
| **S5 — transport security is never derived** | Catalog sync, notification and retention Jobs are destination- or route-backed and carry their complete, explicit `AWS_*`/endpoint set from the resolved snapshot; none inherits controller environment addressing, and no UI control derives `allowHttp`. |
| **S6 — pod identity by owner UID** | §2.3's diagnostics and every new log/exit read verify the pod's controller owner UID against the intended Job's UID before trusting output. |
| **S7 — conditional merge PATCH** | Every status write added here (progress heartbeat, alert ledger, retention lease and plan state, catalog generation, trust evaluation) is a merge PATCH with `metadata.resourceVersion` as precondition; no `update` on any CR or status subresource. |
| **S8 — new kinds** | D2 owns Amendment F; D3's five kinds are Amendment G, with Amendments H (deletion boundary) and I (contract v2) recorded in the same place (§8). |
| **D1 — editable schedules, run identity, retries, history** | ProtectionPolicy consumes `spec.scheduleRef.uid` + `logweir.dev/schedule-uid`, `status.activeRuns`, `status.lastSlot.disposition` and `status.missedSlots`; a retry chain is one failed slot; Backup phase `Resolving` and condition `TopicsResolved` map to `preparing`/`DiscoveryRunning`; catalog records copy `status.execution.{id,inputsSha256}`. D1's W0 reservation fix (conditional status PATCH) is a prerequisite for the rehearsal reservation. |
| **D2 — destinations, checks, evidence fetch** | `BackupDestination` is the destination kind everywhere in D3 (the "placeholder" in §0 resolves to it). Evidence verification is D2's `evidenceFetch` Job plus `verification::verify_fetched`, so D3's evidence vocabulary is `Pending` / `Valid` / `Invalid` / `NotAttempted` **plus** the new `Untrusted`, and the `verifying` stage covers "an evidence fetch is running". D3's trust re-evaluation (§7.4) reuses those stored fields and performs no fetch. Destination-scoped credentials and the `StoreCache` allowlist are D2's; D3 adds no new controller credential path. |
| **P17 — API and approval seam** | All new routes follow P17's problem+json, idempotency, cursor and capability rules; trust writes stay off the API in v1; ordinary vs governed approval is PLAT-19.2's, extended only by the standing rehearsal authorization (§4.3), whose governed form still requires an independent approver principal. |

Two consequences worth stating plainly for implementers:

1. **Nothing in D3 is a second source of truth.** The catalog view, the protection health block and
   the retention preview are derived, bounded projections; the signed receipt, the signed scorecard
   and the CR status remain authoritative, and every restore re-verifies its point at execution.
2. **Every new capability is off by default.** `ProtectionPolicy`, `RehearsalSchedule`,
   `RecoveryCatalog` and `RetentionPolicy` do nothing until an operator creates one, and
   `RetentionPolicy` deletes nothing until an administrator both selects `Enforce` and approves a
   plan hash. An upgrade changes no existing behavior; a rollback leaves the objects inert.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
