# D1 — Backup scheduling policy, editable schedules, retained history, dynamic topic selection and manual runs

Decision record and implementation brief for **PLAT-04.2, PLAT-05.1, PLAT-05.2, PLAT-09.2 and PLAT-06.2**.

- Date: 2026-09-15. Repository: `/Users/admin/Desktop/Repos/logweir`, main `4956785` (clean).
- Kind: spike outcome (implementation decision + runnable acceptance criteria). Read-only investigation; no repository, tracker or cluster state was changed. One read-only RBAC query was run against `docker-desktop` (§0.3).
- Inputs: `docs/to-do/platform-improvements.md`, `docs/architecture.md` (ADR 0008 Amendment A), `docs/kubernetes.md`, `/tmp/plat17-contract-decision.md` (adopted API decision), `/tmp/logweir-roadmap-run/claude/WORKER-RULES.md`, the sources cited inline, and the in-flight contracts summarised in §0.2 (PLAT-06.1's interrupted patch was read only to align names; it is not treated as authoritative).
- Not decided here: PLAT-09.1's discovery kind/attestation mechanism, PLAT-03.1's readiness resource, PLAT-08 destinations, PLAT-15.1 catalog format, PLAT-16 retention enforcement. Each seam is named where it is consumed.

---

## 0. Decision summary, dependencies and a prerequisite defect

### 0.1 Decisions in one page

1. **Cadence (PLAT-04.2).** `spec.schedule` (five-field cron, existing grammar) stays the single source of truth. Presets are an API/UI catalogue that compiles to canonical cron strings; no preset field is stored. New optional `spec.timeZone` (IANA name, evaluated with a compiled-in tz database via `chrono-tz`); **absent means UTC and reproduces today's slots exactly**. Slot identity stays the **UTC instant** (`yyyymmdd-hhmmss`). DST rule: *every real instant whose local wall time matches fires once; a matching local time that does not exist fires once at the end of the gap (deduplicated)*. Consequently a fixed-time schedule inside a repeated hour fires at both occurrences.
2. **Deadlines, catch-up, retries (PLAT-04.2).** `spec.startingDeadlineSeconds` (absent = 3600, today's horizon), `spec.catchUpPolicy: None | Latest` (absent = `None`, today's behaviour), `spec.retry {maxRetries 0..3, delaySeconds}` (absent = no retries), `spec.activeDeadlineSeconds` (absent = 3600, today's constant). Only the **latest due slot** is ever eligible; at most one catch-up; retries only for the latest slot, only for retryable failures, only until the next slot is due. **Bounded(N) catch-up is rejected** (§4.10). Retry identity: `logweir-backup-<schedule>-<slot>-r<k>` / execution id `<scheduleUID>-<slot>-r<k>`. Every run records `spec.trigger.kind: Scheduled | CatchUp | Retry | Manual`.
3. **Previews (PLAT-04.2).** Computed only in Rust. Controller writes `status.nextRuns` (5 entries, UTC instant + rendered local time + DST adjustment marker). The API serves draft previews at `GET /api/v1/cadence-previews` (default 10, max 20). The browser never evaluates cron.
4. **Editable policy (PLAT-05.1).** Every `BackupSchedule.spec` field becomes mutable **except `sourceRef`**. Revision identity is `(schedule UID, metadata.generation)` plus `runPolicySha256` (canonical digest of the fields that determine what a run does). Every created Backup copies the policy and records `spec.scheduleRef {name, uid, generation, runPolicySha256}`; PLAT-06.1's immutable execution inputs then freeze the resolved settings. Atomic boundary: the resourceVersion-conditional status reservation, made for **every** schedule-created run, followed by creation from the same in-memory object; a Backup always runs exactly the generation it records.
5. **History (PLAT-05.2).** New runs carry **no ownerReference to the schedule**; membership is the immutable `spec.scheduleRef.uid` plus labels `logweir.dev/schedule-uid`. The controller migrates legacy **terminal** Backups by a resourceVersion-conditional metadata patch that removes only the `BackupSchedule` owner entry and adds the UID label and a marker annotation (new RBAC verb: `patch` on `backups`). No finalizers. Logweir still deletes nothing; explicit, documented cleanup rules; the scheduler's steady-state reads become O(active runs), not O(history).
6. **Selection (PLAT-09.2).** `spec.topics` keeps its meaning (named allowlist). Dynamic mode is `spec.allUserTopics {exclude{topics,prefixes}, incompleteDiscovery: Refuse | BackUpVisibleTopics}` **with `topics: []`** (wire-compatible with old controllers, which then fail safe with exit 3). Each dynamic Backup runs a fresh, Backup-owned discovery Job (`Resolving` phase), freezes the exact sorted names plus discovery summary/digest into the execution inputs, and records a coverage label. **`incompleteDiscovery` is required with no default**: completeness cannot normally be established (a successful Kafka listing is `unknown`), so the user must explicitly choose refusal or an explicitly labelled visible-topics-only run. Internal topics are always excluded; empty resolution is a terminal non-retryable failure; wildcards never reach the engine.
7. **Manual runs (PLAT-06.2).** One CR path: a `Backup` with `spec.trigger.kind: Manual`, `triggeredBy: manual`, deterministic name `logweir-manual-<26 base32>` from the idempotency scope. API: `POST /api/v1/namespaces/{ns}/backups` with either `{scheduleRef:{name, expectedGeneration?}}` (copies the current revision) or an ad-hoc body. Allowed while the schedule is suspended. Manual runs are **not** subject to, and never block, `concurrencyPolicy` (CronJob "run now" precedent). Preflight never gates the API or controller; the UI requires an explicit "Run anyway" on a `notReady` readiness result.
8. **No new kind.** All five tasks extend `BackupSchedule` and `Backup` (§9). PLAT-09.1's discovery/check kind, if introduced, needs its own Amendment A record; PLAT-09.2 does not depend on that kind, only on the discovery runner contract.

### 0.2 In-flight contracts this decision assumes

| Worker | Contract relied on | What this decision adds on top |
|---|---|---|
| PLAT-06.1 (`plat06`) | Backup runner argv derived only from typed spec; immutable Backup-owned ConfigMap `<backup>-plan` with canonical `execution-inputs.json` + `backup.yaml` + `allowed-clusters.json`, `immutable: true`; `status.execution {id, inputsRef, inputsSha256}` and `status.backupId` written at freeze; scheduled id `backup_id_for(scheduleUID, slot)`; manual id = Backup UID; `logweir.dev/runner-argv` never written or executed; `triggeredBy ∈ {manual, schedule}` | Inputs grammar `v2` (§3.3); identity derivation moves to one module that no longer depends on owner references (§3.1); topic validation branches on selection mode |
| PLAT-07.1 (`plat07`) | One connection resolver for `KafkaCluster` (bootstrap, auth, TLS CA refs, credential refs, env builder) | Discovery Jobs and runner Jobs use the same resolver; resolution snapshot compared at freeze (§7.2) |
| PLAT-17.1 (`plat17-api`) | `crates/logweir-api`, `/api/v1`, problem+json, Idempotency-Key → deterministic names, request-hash annotations, no delete routes | Routes in §4.4, §5.6, §8.2; one new top-level problem code `policy_changed` (409); new field-error codes `field_immutable`, `selection_invalid`, `schedule_invalid`, `timezone_unknown` under the existing `validation_failed` (422) |
| PLAT-09.1 (not started) | Discovery runner + visibility states `attestedComplete`, `limited`, `unknown`; admin-governed attestation | §7.3 states exactly what 09.2 consumes and a fallback runner contract if 09.1 has not fixed one |
| PLAT-03.1 (not started) | Readiness result `ready`, `notReady` or `unknown` per operation | UI-only gate (§8.4); API/controller never gate on it |

### 0.3 Prerequisite defect found during this spike (P0, independent of this decision)

`reconcile_schedule_with_archive` reserves a `Forbid` slot with `schedules_api.replace_status(...)` (`crates/weirkeeper/src/controllers/backup_schedule.rs:1359-1364`). kube-core 0.99 issues **PUT** for that call (`kube-core-0.99.0/src/request.rs:243-257`), which RBAC authorizes as `update` on `backupschedules/status`. The shipped controller role grants only `patch` on status subresources (`config/rbac/role.yaml:118-126`, `charts/logweir/templates/clusterrole.yaml:29-37`). Verified read-only on docker-desktop:

```
kubectl --context docker-desktop auth can-i update backupschedules.logweir.dev --subresource=status \
  -n logweir-scram-local --as=system:serviceaccount:logweir-scram-local:weirkeeper   # no
... patch ...                                                                          # yes
```

With the default `concurrencyPolicy: Forbid`, every due slot's reservation is rejected, the reconcile requeues, and **no scheduled Backup is created on a shipped install**. PLAT-04.1's live run used custom namespace-local Roles (`/tmp/logweir-roadmap-run/plat04-live.result.md`, "Isolation and runtime identity"), so it did not exercise the shipped ClusterRole, and `crates/logweir/tests/manifest_lint.rs:512` only checks that grants have callers. A separate task chip (`task_0ac11b77`) was raised: reserve with a merge PATCH carrying `metadata.resourceVersion` (reusing `status_patch_with_preconditions`, `backup_schedule.rs:745-766`) and add the reverse "every caller has a grant" lint. **This decision assumes that fix (worker W0) lands first**; every reservation below is a resourceVersion-conditional merge PATCH needing only `patch`.

---

## 1. Current behaviour this decision changes (grounding)

| Area | Today | Evidence |
|---|---|---|
| Cadence | Hand-written 5-field cron, always UTC; `@hourly/@daily/@weekly`; slots are UTC instants | `slot.rs:230-270`, `slot.rs:281-334`, CRD text `crds/backup_schedule.rs:274-281`, UI help "in UTC" `ui/pages/schedules.js:200` |
| Missed slots | Only the latest due slot is considered; older than 1 h is skipped and recorded in `lastMissedSlot`; a `Forbid`-blocked slot is admitted later if still inside the hour | `backup_schedule.rs:206`, `decide` `:445-478`, Forbid path `:1302-1411`, docs `docs/kubernetes.md:478-499` |
| Retries | None. Jobs are `backoffLimit: 0`; exit 1 is never retried by the Job | `job.rs:152-158`, `job.rs:590-613`, `docs/kubernetes.md:652-655` |
| Run deadline | Constant 3600 s for scheduled runs | `backup_schedule.rs:191` |
| Immutability | One object-level CEL rule seals every schedule field except `suspend`; every other kind `self == oldSelf` | `crds/backup_schedule.rs:65`, `crds/mod.rs:242-288`, test `tests/crd_shape.rs:764-851` |
| Run creation | Scheduler copies `sourceRef/topics/archive` into `Backup.spec`, sets labels `logweir.dev/schedule`, `logweir.dev/slot` and a controller ownerReference with `blockOwnerDeletion` | `backup_schedule.rs:601-653` |
| Membership | Complete controller ownerReference (API version, kind, name, UID) | `backup_schedule.rs:661-675` |
| History lifetime | Deleting the schedule lets GC delete every Backup, its plan ConfigMap and Job; docs prescribe drain-and-retain | `docs/kubernetes.md:422-441`, `backup.rs:500-512`, `job.rs:820-827` |
| Scheduler reads | Lists **all** Backups in the namespace every 30 s | `backup_schedule.rs:1130`, `:1199`, `REQUEUE_SECS` `:215` |
| Topic selection | Named list only; glob metacharacters refused by controller and runner; empty list refused by runner (exit 3) | `backup.rs:1878-1887`, `crates/logweir/src/backup/phase_minus1_admit.rs:78-105`, `logweir-core/src/spec.rs:673-684` |
| Discovery input | `ClusterReader::list_topics` → `TopicMeta{name, partitions, error}`; no `internal` flag | `crates/logweir-kafka/src/reader.rs:31-47`, `:217` |
| Archive identity | A second run into an existing `backup_id` does not accumulate and yields a partial archive (measured) | `slot.rs:5-14` |
| Scheduled identity | `plan_backup_id` = controller-owner UID + slot, else Backup UID | `backup.rs:308-321` |
| Manual runs | Require the scheduler-owned argv annotation (PLAT-06.1 removes this) | `backup.rs:910-915`, `:1456-1458` |
| UI writes | `create` on five plurals (includes `backups`) and `patchSuspend` only; the in-cluster UI ServiceAccount lacks `create backups` | `ui/api.js:44-50`, `:145-156`, `charts/logweir/templates/ui/ui.yaml:108-117` |
| Operator role | `create` on four kinds, `update` (not `patch`) on schedules | `config/rbac/operator_role.yaml:42-54` |
| Controller writes | Status patches, `create backups`, Job create/patch, ConfigMap create/get; no delete, no patch on custom objects | `config/rbac/role.yaml:96-182` |
| Receipt | `triggered_by` free text; `source.topics` is the exact named set | `logweir-core/src/backup_receipt.rs:92`, `:116-120` |

---

## 2. Normative vocabulary used below

- **Schedule-created run**: a Backup whose `spec.trigger.kind ∈ {Scheduled, CatchUp, Retry}` (or legacy: `triggeredBy: schedule` without `trigger`). Only these participate in `concurrencyPolicy`.
- **Latest due slot `S`**: `Cron::last_fire_at_or_before_in(tz, now)`; older slots are never admitted.
- **Attempt chain of S**: objects named `name(S, 0)`, `name(S, 1)`, … discovered by GET of deterministic names (never by listing).
- **Retryable failure**: §4.6.
- **Revision**: `(schedule.metadata.uid, schedule.metadata.generation)`. **Run policy digest**: `runPolicySha256` (§3.2).
- **Freeze**: PLAT-06.1's creation of the immutable `<backup>-plan` ConfigMap.

---

## 3. Cross-cutting contracts

### 3.1 Run identity and naming (one module: `crates/weirkeeper/src/identity.rs`)

| Run kind | `metadata.name` | Execution id / archive `backup_id` | Required spec fields |
|---|---|---|---|
| Scheduled (attempt 0) | `logweir-backup-<schedule>-<slot>` (unchanged, `slot.rs:604-613`) | `<scheduleUID>-<slot>` (unchanged, `slot.rs:624-626`) | `scheduleRef{name,uid}`, `slot`, `trigger{kind: Scheduled, attempt: 0, timeZone}` (`timeZone` informational: the zone the slot was computed in, so history rows keep their local time after an edit), `triggeredBy: schedule` |
| CatchUp (attempt 0) | same as Scheduled — it is slot S started late | same | `trigger.kind: CatchUp` |
| Retry k ∈ 1..3 | `logweir-backup-<schedule>-<slot>-r<k>` | `<scheduleUID>-<slot>-r<k>` | `trigger{kind: Retry, attempt: k, retryOf: {name: name(S, k-1)}}` |
| Manual | `logweir-manual-<26 lowercase base32 chars>` (API: of `sha256(scope)`; legacy UI: 130 random bits) | Backup UID (PLAT-06.1) | `trigger.kind: Manual`, `triggeredBy: manual`, no `slot`; optional `scheduleRef` |
| Legacy scheduled (pre-upgrade) | as Scheduled | `<ownerUID>-<slot>` from the controller ownerReference | none of the new fields |

Rules (all enforced by `identity::run_identity(&Backup) -> Result<RunIdentity, IdentityError>`, called by the Backup controller before any POST and by the scheduler when adopting a 409 winner):

1. A scheduled-kind identity is honoured **only if** `metadata.name == scheduled_backup_name(scheduleRef.name, slot, attempt)`. Otherwise terminal `ScheduledIdentityMismatch` (never silently re-labelled as manual). Name uniqueness per namespace plus UID uniqueness per cluster makes each scheduled execution id held by at most one object.
2. Before freeze, a scheduled-kind run requires a `BackupSchedule` named `scheduleRef.name` **in the same namespace** whose UID equals `scheduleRef.uid` (legacy: the owner UID). Otherwise terminal `ScheduleNotFound` ("deleting a schedule stops future work"). After freeze nothing is re-checked. Manual runs never require the schedule to exist.
3. `Retry` requires `attempt ≥ 1` and `retryOf.name == name(S, attempt-1)`; `Scheduled`/`CatchUp` require `attempt == 0`; `Manual` forbids `slot` and `attempt > 0`; a scheduled kind with neither `scheduleRef.uid` nor a legacy BackupSchedule controller owner UID has no identity. Violations → terminal `ScheduledIdentityMismatch` (checked before rule 2).
4. Absent `trigger` (legacy): `triggeredBy == "schedule"` ⇒ Scheduled/0; otherwise Manual.
5. If `scheduleRef.runPolicySha256` is present it must equal the digest recomputed from the Backup's own policy fields; otherwise terminal `RunPolicyDigestMismatch` (integrity against bugs; not a security boundary, see §8.7).
6. Name budget: attempt 0 keeps the 32-character schedule-name budget (`docs/kubernetes.md:386-410`). With retries enabled the budget is **29** (`-r<k>` adds three characters). Enforced at admission by CEL (§5.2 rule R3). Defensively (e.g. an older CRD without R3), the scheduler treats a retry policy whose retry names cannot fit as an invalid policy: `Ready=False reason=NameTooLong`, no admissions, never a silent run without retries.
7. Pure-function guard (keeps G-SLOT, `tests/schedule_controller.rs:649`): `decide()` still mints only the attempt-0 name from `(name, spec, now)`; a second pure function `decide_attempt(slot_decision, observed_attempts, retry_policy, now)` mints retry names from **observed deterministic objects**, never from a status field or a clock.

Membership (`identity::is_run_of_schedule(backup, name, uid)`), used everywhere `is_owned_by_schedule` is used today:
`spec.scheduleRef.name == name && spec.scheduleRef.uid == uid`, **or** legacy complete controller ownerReference (existing check), **or** migrated legacy marker annotation `logweir.dev/history-retained-from-owner == uid` (written only by the §6.2 migration) with the Backup's existing `spec.scheduleRef.name == name`.

Labels written on every new run (hints for selection, never authority): `logweir.dev/schedule=<name>`, `logweir.dev/schedule-uid=<uid>` (absent for ad-hoc manual runs), `logweir.dev/slot=<slot>` (scheduled kinds), `logweir.dev/trigger=scheduled|catch-up|retry|manual`, `logweir.dev/attempt=<k>`.

### 3.2 Run policy digest (`crates/weirkeeper/src/policy.rs`)

`runPolicySha256 = logweir_core::ids::sha256_prefixed(det_json(RunPolicyV1))` where

```json
{"formatVersion":"run-policy/v1",
 "sourceRef":{"name":"source"},
 "topics":["orders","payments"],              // sorted, deduplicated; [] in dynamic mode
 "allUserTopics":null | {"exclude":{"topics":[sorted unique],"prefixes":[sorted unique]},
                          "incompleteDiscovery":"Refuse|BackUpVisibleTopics"},
 "archive":{"url":"s3://kafka-backups/logweir","secretRef":{"name":"logweir-s3"} | null},
 "activeDeadlineSeconds":3600}
```

Cadence, time zone, deadlines, catch-up, retry, concurrency, retention and suspend are excluded: they decide *when*, not *what*. The same module exposes `validate_run_policy(&…) -> Vec<FieldError>` used by the scheduler (fail closed), the Backup controller (terminal refusal) and the API (422), so the three cannot disagree. `Backup.spec.topics` keeps the user's order verbatim; only the digest canonicalises.

### 3.3 Execution inputs grammar `v2` (extends PLAT-06.1's `execution-inputs.json`)

Written once at freeze, canonical JSON, inside the same immutable `<backup>-plan` ConfigMap. `v1` objects frozen by an earlier controller are still loaded and executed unchanged (S4).

**Amended 2026-09-16 at W3b's integration** (review `d1w3b`, both grammar deviations ratified). The landed grammar is `crates/weirkeeper/src/backup_execution.rs` `BackupExecutionInputs`, whose field declaration order IS the wire order (`logweir_core::det_json` emits struct fields in declaration order), and the sketch below is rewritten from it. The earlier sketch showed top-level `executionId`, `run`, `schedule`, `triggeredBy` and `deadlineSeconds` and a `selection.topics` list; none of those exist. `v2` is exactly `v1` plus five optional blocks — `trigger`, `scheduleRef`, `runPolicySha256`, `selection`, `destination` — each omitted (never `null`) when absent, so a `v1` document re-encodes to its own bytes. The run block is named `trigger` and the schedule block `scheduleRef` because each is a verbatim copy of `spec.trigger` / `spec.scheduleRef` (copied, never re-resolved), and because `v1` already spends `schedule` on `execution.schedule = {name, uid, slot}`. The topic list is the single top-level `topics` that `v1` defined — the exact list handed to `backup.yaml` — and `selection` carries its provenance and counts, never a second copy (one answer, one place; §7.2 R8 bounds the list at 256 KiB inside a 1 MiB ConfigMap). The block below is an annotated shape sketch in wire order (`| absent` alternatives, `//` comments, `N` placeholders), not literal JSON.

```json
{"version":"logweir.dev/backup-execution-inputs/v2",
 "execution":{"id":"3f0c…-20260915-020000-r1",                    // v1 — <scheduleUID>-<slot>[-r<k>], or the Backup's UID (§3.1)
              "trigger":"manual|schedule",                          // v1 — spec.triggeredBy, unchanged
              "backup":{"namespace":"…","name":"…","uid":"…"},
              "schedule":{"name":"nightly","uid":"3f0c…","slot":"20260915-020000"} | absent},
 "trigger":{"kind":"Scheduled|CatchUp|Retry|Manual","attempt":N,
            "retryOf":"logweir-backup-nightly-20260915-020000" | absent,
            "timeZone":"Europe/Berlin" | absent},                   // v2 — spec.trigger, copied
 "scheduleRef":{"name":"nightly","uid":"3f0c…","generation":7,
                "runPolicySha256":"sha256:…"} | absent,             // v2 — spec.scheduleRef, copied; absent for an ad-hoc manual run
 "runPolicySha256":"sha256:…",                                      // v2 — §3.2 digest of THIS run's own policy fields, recorded for every run
 "source":{ /* PLAT-06.1 ResolvedBackupSource: clusterRef, clusterUid, bootstrapServers, auth, passwordSecretRef, observedClusterId */ },   // v1
 "topics":["…"],                                                    // v1 — THE exact names handed to backup.yaml; spec order (SelectedTopics) or byte-sorted (AllUserTopics)
 "selection":{"mode":"SelectedTopics|AllUserTopics",
              "coverage":"NamedTopics|AllUserTopicsAttested|VisibleUserTopicsOnly",
              "resolvedTopicCount":N,"resolvedTopicBytes":N,
              "exclude":{"topics":[…],"prefixes":[…]} | absent,
              "incompleteDiscovery":"Refuse|BackUpVisibleTopics" | absent,
              "discovery":{"observedAt":"…","clusterId":"…","visibility":"unknown|limited|attestedComplete",
                           "basis":"metadata-list","resultSha256":"sha256:…","visibleTopicCount":N,
                           "internalExcluded":{"count":N,"names":[≤50]},"excludedByRule":{"count":N,"names":[≤200],"truncated":bool},
                           "limitedTopicCount":N,"discoveryJob":"lwd-<backup-uid>"} | absent},   // v2 — the provenance of `topics`
 "destination":{…} | absent,                                        // v2 — reserved; D2 W10 writes the resolved BackupDestination snapshot (D2 §3.7)
 "archive":{…},                                                     // v1
 "runner":{"args":[…],"deadlineSeconds":3600,"settings":{…}}}       // v1 — the argv, the deadline and the tunables
```

A stored `v1` plan is compared against the fresh resolution's `v1` view (the five blocks dropped, `as_version_v1`); a stored `v2` plan is compared whole, and each block refuses by name before the generic executable-equality refusal. PLAT-06.1's `validate_inputs_for_backup` must compare the top-level `topics` with `spec.topics` only in `SelectedTopics` mode; in `AllUserTopics` mode the frozen `topics` deliberately differs from `spec.topics`, and it checks `spec.topics == []`, `spec.allUserTopics` equals the frozen policy, and `selection.discovery` is present. `topics` never contains a glob metacharacter or `${` (Kafka-legal names `^[a-zA-Z0-9._-]{1,249}$` are re-validated before freeze) and is never empty; both rails are enforced at the freeze boundary (`resolve_inputs`) for every producer, including W5's runner-derived list. A `v1` plan carries none of the `v2` provenance by construction; a `v1` plan under a controller that writes `v2` is worth an operator's attention but is never refused (S4).

### 3.4 Condition and reason vocabulary (added to `crates/weirkeeper/src/conditions.rs`)

All CamelCase and added to the closed lists that `the_condition_reasons_are_valid_metav1_reasons` walks.

- **BackupSchedule `Ready` reasons** (existing kept: `Scheduled`, `Suspended`, `SlotMissed`, `ConcurrencyBlocked`, `NoDueSlot`, `UnparseableSchedule`, `NameTooLong`): new `CaughtUp`, `CatchUpBlocked`, `RetryScheduled`, `RetryPending`, `RetryBlocked`, `RetryExhausted`, `RunFailed`, `SlotNameUnavailable`, `ActiveRunLimit`, `UnknownTimeZone`, `InvalidTopicSelection`, `InvalidRunPolicy`, `CrdOutdated`. `Ready=False` only for `Suspended`, `UnparseableSchedule`, `NoDueSlot`, `NameTooLong`, `UnknownTimeZone`, `InvalidTopicSelection`, `InvalidRunPolicy`, `CrdOutdated`.
- **BackupSchedule `HistoryRetained`** (new condition type): `Retained` (True), `HistoryLarge` (True, warning), `LegacyOwnerReferencesRemain` (False), `ActiveLegacyRunsOwned` (False), `MigrationBlocked` (False).
- **Backup terminal states** (`Failed=True`, `exitReason: operational`, no `exitCode`): `ScheduledIdentityMismatch`, `ScheduleNotFound`, `RunPolicyDigestMismatch`, `InvalidTopicSelection`, `DiscoveryFailed`, `DiscoveryIncomplete`, `SelectionEmpty`, `SelectionTooLarge`, `SourceChangedDuringResolution`, `DiscoveryResultUnreadable`.
- **Backup `TopicsResolved`** (new condition type, dynamic mode only): `DiscoveryRunning` (False), `Resolved` (True), and the terminal reasons above where they apply.
- **Backup phase** adds `Resolving` (nonterminal; `backup_is_terminal`, `backup_schedule.rs:683-691`, already treats unknown phases as active).

Merge-patch warning for implementers: a JSON merge patch **replaces** `status.conditions`. The schedule status builder must always emit both `Ready` and `HistoryRetained` (the same class of defect `verification::carry_verified` fixes on Backups, `backup.rs:1289-1297`).

---

## 4. PLAT-04.2 — cadence, time zones, deadlines, catch-up and retries

### 4.1 `BackupSchedule.spec` additions (all optional; absent = today's behaviour)

| Field | Type / validation (OpenAPI unless noted) | Absent means | Mutable |
|---|---|---|---|
| `schedule` | string (existing, unchanged; no new pattern so no stored object can become invalid on the 1.29 floor) | — (required) | yes |
| `timeZone` | string, `maxLength: 64`, `pattern: ^[A-Za-z][A-Za-z0-9_+\-]*(/[A-Za-z0-9_+\-]+){0,2}$`; controller resolves against compiled tzdb | `UTC` | yes |
| `startingDeadlineSeconds` | int64, `minimum: 60`, `maximum: 604800` | 3600 | yes |
| `catchUpPolicy` | enum `None`, `Latest` | `None` | yes |
| `retry` | object; `maxRetries` int32 **required** `0..3`; `delaySeconds` int64 `60..21600` | no retries; delay 300 when `retry` present without it | yes |
| `activeDeadlineSeconds` | int64, `minimum: 60`, `maximum: 86400`; copied into `Backup.spec.deadlineSeconds` | 3600 (`SCHEDULED_DEADLINE_SECONDS`) | yes |

No OpenAPI `default:` is added for these fields (so `has()` keeps meaning "set by the user" and `kubectl get -o yaml` of old objects does not change); Rust `#[serde(default)]` supplies the documented value.

### 4.2 Presets (API/UI only; compile to canonical cron)

| Preset DTO | Parameters | Canonical `schedule` |
|---|---|---|
| `hourly` | `minute` 0–59 | `M * * * *` |
| `everyNHours` | `n ∈ {2,3,4,6,8,12}`, `minute` | `M */n * * *` |
| `daily` | `hour`, `minute` | `M H * * *` |
| `weekly` | `dayOfWeek` 0–6 (0 = Sunday), `hour`, `minute` | `M H * * D` |
| `monthly` | `dayOfMonth` 1–28, `hour`, `minute` | `M H D * *` |

`weirkeeper::cadence::presets` is the single catalogue; `compile_preset` and `match_preset(cron) -> Option<Preset>` (exact match of canonical integer fields; `@hourly`/`@daily`/`@weekly` map to `hourly{0}`/`daily{0,0}`/`weekly{0,0,0}`). A drift test emits `ui/tests/fixtures/cadence-presets.json` so the static UI's display mapping (string templates only, no evaluation) matches Rust. Everything else is "Advanced cron". `*/n` hours are local wall-clock hours divisible by `n`, not "n hours after creation"; the form says so.

### 4.3 Time-zone semantics

- Evaluation: cron fields match the **local** date and time in `timeZone`. Slot identity, `status.nextFireTime`, `Backup.spec.slot`, labels and execution ids stay the corresponding **UTC** instant, so names remain unique, monotonic and DNS-1123 (`slot.rs:543-545`).
- For each local calendar date, the matched local minutes are mapped with `chrono_tz::Tz::from_local_datetime`:
  - `Single(t)` → `t`.
  - `Ambiguous(first, second)` (repeated hour) → **both** instants, each its own slot.
  - `None` (gap) → the **transition instant**: the UTC instant of the first valid local minute after the requested time (search forward ≤ 48 h, which also covers whole skipped days such as `Pacific/Apia` 2011-12-30).
  - Identical instants on a date are deduplicated.
- Worked examples (all become unit fixtures):

| Zone | Expression | Local event | Slots (UTC) |
|---|---|---|---|
| `Europe/Berlin` | `30 2 * * *` | 2026-10-25 repeated 02:00–02:59 | `20261025-003000`, `20261025-013000` |
| `Europe/Berlin` | `30 2 * * *` | 2027-03-28 gap 02:00–02:59 | `20270328-010000` (gap end), then `20270329-003000` |
| `Europe/Berlin` | `*/15 * * * *` | 2027-03-28 gap | …`00:45`, `01:00`, `01:15`… — continuous 15-minute UTC cadence, no burst |
| `America/New_York` | `30 1 * * *` | 2026-11-01 repeated 01:00–01:59 | `20261101-053000`, `20261101-063000` |
| `America/New_York` | `30 2 * * *` | 2027-03-14 gap | `20270314-070000` |
| `Australia/Lord_Howe` | `15 2 * * *` | 30-minute DST gap | gap end at local 02:30 |
| `Asia/Kathmandu` | `0 9 * * *` | fixed +05:45 | `…-031500` |
| absent / `UTC` | any | none | byte-identical to today's `Cron::last_fire_at_or_before` / `next_fire_after` |

- Rationale: the rule never loses a real-time interval (interval schedules keep their UTC cadence through both transitions) and never skips a fixed-time day (gap matches are shifted, not dropped). The cost is at most one extra run for a fixed-time schedule whose local time lies in a repeated hour; the previews show both instants, so the outcome is predictable. `Forbid` still prevents overlap.
- Unknown zone: `Ready=False reason=UnknownTimeZone`, no admissions, running work unaffected. `Etc/GMT+5` POSIX sign inversion is documented and the preview shows the rendered offset.
- tz database: compiled into the binary (`chrono-tz = "0.10.4"`, MIT OR Apache-2.0; with default features its new runtime packages are `phf` and its `phf_shared`/`siphasher` closure beside the existing `chrono`, and its prebuilt zone sources mean the optional `chrono-tz-build` generator is not compiled). Rule updates require a controller/API release; `weirkeeper::cadence::TZDB_SOURCE = "chrono-tz 0.10.4"` is recorded in `status.policy` and pinned to `Cargo.lock` by a test. Rejected: system `/usr/share/zoneinfo` (controller, API and test images would carry different tzdata versions and compute different slots); a hand-written tz parser (tzdb rule complexity). This is a new dependency decision: `THIRD_PARTY_NOTICES.md` regeneration and `cargo deny check licenses advisories` are required in W1.

### 4.4 Next-run previews

- Controller: `status.nextRuns` — up to **5** entries for the current generation, empty when suspended or invalid:
  `{at: date-time (UTC), localTime: "2026-10-25T02:30:00+02:00", adjustment?: "NonexistentLocalTimeShifted" | "RepeatedLocalTimeFirst" | "RepeatedLocalTimeSecond"}`. Rewritten only when the generation changes or `nextRuns[0].at` has passed (no write churn; `status_unchanged` still applies).
- API (PLAT-17 console): `GET /api/v1/cadence-previews?schedule=<urlencoded>&timeZone=<tz>&count=<1..20, default 10>&after=<RFC3339, default server now>` → `200 {schedule, timeZone, tzdb, runs:[…same shape…]}`; `422 validation_failed` with field errors `schedule: schedule_invalid` / `timeZone: timezone_unknown`. Viewer role and above; no Kubernetes access; no idempotency key (safe GET).
- Browser: renders what either source returns and never computes cron. In legacy kubectl-proxy mode there is no draft preview; the saved schedule's `status.nextRuns` are shown after creation, and the form shows the preset's text ("Every day at 02:00, Europe/Berlin").

### 4.5 Scheduler algorithm (one reconcile at instant `now`; replaces `reconcile_schedule_with_archive` body)

```
0. Validate policy: cron parse, timeZone resolve, run policy (§3.2), selection (§7.1), retry name budget.
   If generation != status.observedGeneration: set status.policy{generation, runPolicySha256, timeZone,
   effectiveSince=now, evaluatedAt=now}, observedGeneration.
1. Refresh active set: GET every name in status.activeRuns ∪ {status.pendingRun.name}; drop 404/terminal.
   If status.history.inventoriedAt is absent or older than 60 min, or this process has not inventoried this
   schedule since start: run the inventory (§6.7) before any admission.
2. If status.pendingRun exists and its child does not exist:
     run policy (§3.2) invalid → clear reservation, lastSlot.disposition=Released reason=InvalidRunPolicy.
     else → create the child from the current object (records the current generation); go to 8.
     (Cadence or time-zone invalidity does not block this: the reserved slot is already a UTC instant.)
   (Suspension and deadlines do not cancel an accepted reservation — today's behaviour.)
3. suspend → no admissions; nextRuns=[]; Ready=False Suspended; go to 8.
4. policy invalid → no admissions; Ready=False <reason>; go to 8.
5. S = latest due slot (tz). None → NoDueSlot; go to 8.
   Account skipped slots in (status.missedSlots.lastEvaluatedSlot, S) (enumeration capped at 1000; reasons
   ControllerUnavailable | ConcurrencyBlocked | PastStartingDeadline | BeforeRevision | NameUnavailable — amended 2026-09-17 at W2's integration: `Superseded` is not emitted; a slot that waits and is then overtaken is accounted under the reason that blocked it, and the three added reasons are what the landed controller records; a `ControllerUnavailable` gap records ONE boundary entry in `recent`, only when the gap count moved, because `SkippedSlots` exposes no names; not while suspended). lastEvaluatedSlot advances
   to S only when S receives a final disposition (admitted, missed, name unavailable, done or exhausted), so a
   slot that waits and is then superseded is counted exactly once.
6. Observe attempt chain of S by GET name(S,0), name(S,1), … up to the hard maximum 3, stopping at the first 404
   (independent of the current maxRetries, so a lowered maxRetries still sees existing attempts); verify membership (§3.1) of each hit;
   a hit held by another schedule UID → SlotNameUnavailable (record skipped); go to 8.
   6a. any attempt Succeeded → AlreadyFired.
   6b. highest attempt nonterminal → in progress.
   6c. highest attempt k Failed, not retryable (§4.6) → RunFailed.
   6d. retryable, k >= maxRetries → RetryExhausted (covers a maxRetries lowered by an edit).
   6e. retryable, k < maxRetries, now < finishedAt(k) + delaySeconds → RetryPending (requeue at due).
   6f. retryable, admissible but blocked by concurrency (§4.7) → RetryBlocked.
   6g. otherwise → ADMIT Retry attempt k+1.
7. No attempt of S exists:
   S <= status.lastFireTime (history pruned) → AlreadyFired (never re-run a fired slot).
   name(S,0) too long → NameTooLong.
   now - S <= startingDeadlineSeconds → blocked ? ConcurrencyBlocked (lastMissedSlot=S) : ADMIT Scheduled.
   else catchUpPolicy None → SlotMissed (record).
   else S < status.policy.effectiveSince → SlotMissed (reason BeforeRevision).
   else blocked ? CatchUpBlocked : ADMIT CatchUp.
ADMIT: resourceVersion-conditional status merge PATCH setting pendingRun{name,slot,attempt,kind,generation}
       (+ mirrored pendingBackupRef); a 409 aborts the reconcile (requeue; a newer object is re-read).
       Then create the Backup from the same in-memory object (§5.4); a 409 on create adopts the winner only
       if is_run_of_schedule and run_identity match; otherwise SlotNameUnavailable / ForeignBackup error.
8. Final status: resourceVersion-conditional merge PATCH (existing `status_patch_with_preconditions`):
   Ready + HistoryRetained conditions, nextFireTime, nextRuns, lastSlot, missedSlots, activeRuns (+ mirrored
   activeBackupRef), pendingRun cleared when child observed, lastFireTime = S when attempt 0 of S was
   created/observed, retention report (unchanged), history block. Requeue min(30 s, next retry/slot due).
```

`finishedAt(k)` = `lastTransitionTime` of attempt k's `Failed=True` condition (written once by the terminal patch, `backup.rs:1197-1207`). Reservation is uniform for `Forbid` and `Allow` (today `Allow` creates without one, `backup_schedule.rs:1119-1195`); this makes `status.activeRuns` complete by construction and gives every run the same crash semantics.

### 4.6 Retryable failure classification (closed `match`, unknown ⇒ not retryable)

| Attempt's terminal record | Retryable |
|---|---|
| `exitCode` 1 (`Operational`) | yes |
| `exitCode` not in {0,1,2,3,4} (e.g. 137/143 after `activeDeadlineSeconds`, OOM) | yes |
| no exit code: `DisruptedMidDrill`, `PodUnschedulable`, `NoExitCode` | yes |
| `DiscoveryFailed` (§7) | yes |
| `exitCode` 2, 3 (any `refusal-reason`), 4 (`SigningOrLock`, `OrphanedScorecard`) | no |
| controller refusals before any POST: `NameTooLong`, `ReferentNotFound`, `CredentialNotRenderable`, `ArchiveUrlUnreadable`, `PlanConfigMapConflict`, `GuardRefused`, `ScheduledIdentityMismatch`, `ScheduleNotFound`, `RunPolicyDigestMismatch`, `InvalidTopicSelection`, `DiscoveryIncomplete`, `SelectionEmpty`, `SelectionTooLarge`, `SourceChangedDuringResolution`, `DiscoveryResultUnreadable` | no |
| legacy phase `Refused` | no |

Retries use the **current** generation at retry creation (a user who fixes a credential reference by editing gets the fix). Each retry is a new Backup with a new execution id: a failed attempt's partial archive is never appended to (`slot.rs:5-14`) and a terminal Backup is never mutated or re-executed (PLAT-06.2 migration note).

### 4.7 Policy truth table

Evaluation order is top to bottom; the first matching row decides. "Blocked" means: `Forbid` and at least one schedule-created run of this schedule UID is nonterminal or unknown; or `Allow` and 10 schedule-created runs are nonterminal (`ActiveRunLimit`). Manual runs never block and are never blocked.

| # | Observed state | Decision | Creates | `Ready` / `lastSlot.disposition` |
|---|---|---|---|---|
| 1 | Accepted reservation, child absent, run policy valid | Resume creation with current generation | reserved name | `Scheduled`/`CaughtUp`/`RetryScheduled` |
| 2 | Accepted reservation, child absent, run policy invalid | Release reservation | nothing | False `InvalidRunPolicy` / `Released` |
| 3 | `suspend: true` | No slot, catch-up or retry admission; `nextRuns` empty | nothing | False `Suspended` |
| 4 | Cron, tz, selection or run policy invalid | No admission; running work continues | nothing | False `UnparseableSchedule`/`UnknownTimeZone`/`InvalidTopicSelection`/`InvalidRunPolicy` |
| 5 | No due slot within walk bound | Idle | nothing | False `NoDueSlot` |
| 5a | Due slot S earlier than `metadata.creationTimestamp` (amended 2026-09-23) | Idle: S is neither fired nor counted as missed | nothing | True `Scheduled` (message names the skipped slot and the creation time) |
| 6 | Deterministic name of S held by another schedule UID | Skip S, record | nothing | True `SlotNameUnavailable` / `NameUnavailable` |
| 7 | Some attempt of S Succeeded | Done | nothing | True `Scheduled` / `Admitted` (steady `AlreadyFired`) |
| 8 | Highest attempt of S nonterminal | Wait | nothing | True `Scheduled` |
| 9 | Highest attempt k Failed, not retryable | Done; next slot | nothing | True `RunFailed` |
| 10 | Failed retryable, k ≥ `maxRetries` (incl. 0) | Done; next slot | nothing | True `RetryExhausted` (absent `retry` ⇒ `RunFailed`) |
| 11 | Failed retryable, k < `maxRetries`, delay not elapsed | Wait until `finishedAt + delaySeconds` | nothing | True `RetryPending` |
| 12 | Failed retryable, delay elapsed, blocked | Wait (S stays latest) | nothing | True `RetryBlocked` |
| 13 | Failed retryable, delay elapsed, not blocked | Reserve + create | `…-r<k+1>`, kind `Retry` | True `RetryScheduled` / `Retried` |
| 14 | No attempt, `S ≤ lastFireTime` | Treat as fired (pruned history) | nothing | True `Scheduled` |
| 15 | No attempt, name too long | Skip | nothing | False `NameTooLong` |
| 16 | No attempt, within `startingDeadlineSeconds`, blocked | Wait; record `lastMissedSlot=S` (today's audit field) | nothing | True `ConcurrencyBlocked` |
| 17 | No attempt, within deadline, not blocked | Reserve + create | `name(S,0)`, kind `Scheduled` | True `Scheduled` / `Admitted` |
| 18 | No attempt, past deadline, `catchUpPolicy: None` | Skip, count in `missedSlots` | nothing | True `SlotMissed` / `Missed` |
| 19 | No attempt, past deadline, `Latest`, `S < policy.effectiveSince` | Skip | nothing | True `SlotMissed` (reason `BeforeRevision`) |
| 20 | No attempt, past deadline, `Latest`, blocked | Wait while S is latest | nothing | True `CatchUpBlocked` |
| 21 | No attempt, past deadline, `Latest`, not blocked | Reserve + create | `name(S,0)`, kind `CatchUp` | True `CaughtUp` / `CaughtUp` |
| 22 | A newer slot becomes due while rows 11/12/16/20 wait | Older S is superseded, counted in `missedSlots` | per new S | per new S |

**Amended 2026-09-23 (SCHEDULE-FIRES-SLOT-BEFORE-CREATION, `claude/ctl-batch-1`).** The earlier text kept admitting, by row 17, a slot that came due before the schedule was created but was still inside `startingDeadlineSeconds`. Live, that made a schedule created at 00:53:11Z fire its 00:30:00Z slot. Row 5a now decides first: such a slot is idle, never fired and never counted in `missedSlots`, which is Kubernetes CronJob semantics (a slot exactly at the creation second still fires). The same rule applies to `RehearsalSchedule`. Rows 16–21 are unchanged for slots after creation. Users who want an immediate first run still use "Run first backup now" (§8). Migration: a schedule that an older controller already fired for a pre-creation slot keeps its `lastFireTime` until its next slot.

Downtime outcomes users can predict from the table: a controller down for a week with `None` runs nothing until the next slot and records the skipped count (capped at 1000); with `Latest` it runs exactly one `CatchUp` for the most recent slot. **Backlog bound**: per schedule at most one admission per reconcile, only for the latest due slot, at most `1 + maxRetries ≤ 4` runs per slot, at most one nonterminal schedule-created run under `Forbid` and at most 10 under `Allow`.

### 4.8 `BackupSchedule.status` additions

| Field | Shape | Bound |
|---|---|---|
| `observedGeneration` | int64 | — |
| `policy` | `{generation, runPolicySha256, timeZone (effective, "UTC" when absent), tzdb, effectiveSince, evaluatedAt}` | — |
| `nextRuns` | list of `{at, localTime, adjustment?}` | ≤ 5 |
| `lastSlot` | `{slot, dueAt, attempt, disposition, backupRef?, reason, decidedAt}`; `disposition` is one of `Admitted`, `CaughtUp`, `Retried`, `Missed`, `Blocked`, `NameUnavailable`, `Released`, `Failed`, `Exhausted` (amended 2026-09-17 at W2's integration: `Superseded` is not a disposition the landed controller emits; a reconcile that resumes an accepted reservation returns without considering the newer slot, which is decided on the next pass) | — |
| `missedSlots` | `{count, countCapped, lastEvaluatedSlot, recent:[{slot, reason, recordedAt}]}` | recent ≤ 10 |
| `pendingRun` | `{name, slot, attempt, kind, generation}`; `pendingBackupRef` stays mirrored | 1 |
| `activeRuns` | `[{name, kind, attempt}]`; `activeBackupRef` stays mirrored (first entry) | ≤ 10 |
| `history` | `{runCount, estimatedBytes, legacyOwnedRuns, inventoriedAt}` (§6) | — |
| `lastMissedSlot`, `lastFireTime`, `nextFireTime`, `retentionReport`, `conditions` | unchanged meaning | — |

### 4.9 Compatibility, upgrade and rollback (PLAT-04.2)

- Existing schedules need no rewrite: absent fields reproduce UTC evaluation, the one-hour deadline, skip-without-catch-up, no retries and the 3600 s run deadline. Unit property test: for 10 000 random `(expression, instant)` pairs the v2 decision with absent fields equals the legacy decision and name.
- Legacy `status.pendingBackupRef` without `pendingRun` is resumed through the existing `reserved_slot` parser, extended to accept `-r<k>`.
- CRD-before-controller guard (no new RBAC): the API server silently prunes fields an old CRD does not declare. The scheduler therefore checks its own write responses — the reservation response must contain `status.pendingRun`, and the created Backup must contain `spec.trigger` and `spec.scheduleRef.uid`. If either is missing it sets `Ready=False reason=CrdOutdated` (added to §3.4) and admits nothing further; a Backup already created without its identity fields has neither `scheduleRef.uid` nor an ownerReference and is refused `ScheduledIdentityMismatch` by the Backup controller before any POST, so nothing executes under an ambiguous identity.
- Upgrade order: W0 fix → CRD apply (additive) → controller rollout. A mixed old/new replica pair does not share `pendingRun`; suspend schedules or stop the old controller first for strict no-overlap, as `docs/kubernetes.md:466-476` already requires.
- Rollback (controller only; never downgrade the CRD): an older controller ignores `timeZone` (fires in UTC), retries, catch-up and deadlines; it clears `-r<k>` reservations as unparseable (safe: no retry is created). Before rollback, suspend or edit schedules that set `timeZone` ≠ UTC, `retry`, `catchUpPolicy` or `startingDeadlineSeconds`. `status.nextRuns`/`missedSlots` go stale; the UI marks them stale when `status.nextRuns[0].at` is in the past (amended 2026-09-17 at W2's integration: `status.policy.evaluatedAt` is the instant the status last MOVED, not a liveness probe — rewriting an instant on every 30 s pass would bump `resourceVersion` 2 880 times a day on a schedule that never changes — so a console must not compare it with the requeue interval; `status.activeRuns` absent means not yet computed, never empty).

### 4.10 Rejected alternatives

- **Bounded(N) catch-up.** `logweir backup run` captures what the broker retains *when it runs*; it is not a historical slot window (`crates/logweir/src/backup/phase_run.rs:61-198`). N catch-up runs executed back-to-back produce near-identical archives, multiply broker and storage load, and recreate the backlog this task must prevent, with no recovery-point benefit. Skipped slots are accounted in `status.missedSlots` instead. The enum stays extensible.
- **Stored preset fields** (two representations of one cadence, and a mismatch state). **Browser-side cron/tz evaluation** (a second implementation that can disagree with the controller). **Retrying inside the Job** (`backoffLimit > 0` loses the single exit code, `docs/kubernetes.md:652-655`).
- **Encoding the attempt in the slot's seconds digits** (keeps 32 characters but makes `reserved_slot` and humans read a false instant).

---

## 5. PLAT-05.1 — editable future policy with immutable run snapshots

### 5.1 Mutability matrix

| Field | After this task | Reason |
|---|---|---|
| `sourceRef` | **immutable** | Identity of the protected cluster; one schedule's history must not mix clusters. Create a new schedule for a different cluster. |
| `schedule`, `timeZone`, `startingDeadlineSeconds`, `catchUpPolicy`, `retry`, `concurrencyPolicy`, `suspend` | mutable | Future admissions only |
| `topics`, `allUserTopics`, `archive`, `activeDeadlineSeconds` | mutable | Future runs only; each run records its own copy |
| `destinationRef` | mutable | Amended 2026-09-17 at W2's integration (review `d1w2`, ratified): `archive.url` and `destinationRef` are two spellings of one location, so sealing one while the other moves is incoherent, and PLAT-05.1's own text says a changed destination must not require a new schedule; a run frozen before the edit keeps its snapshot (§3.3), so a mid-run edit is safe; the wrong-bucket case of RET-WRONGBUCKET is reachable only between runs. Supersedes D2 §3.6's W6b sealing. |
| `retention` | mutable | Reporting only; evaluates the **current** `archive.url` (PLAT-16.1 owns per-destination history) |

`metadata.name` is immutable by Kubernetes; `Backup.spec` remains fully sealed (`self == oldSelf`).

### 5.2 CEL rules (exact text; injected by `crds/mod.rs`)

```text
R1  placement: BackupSchedule .spec (transition rule; replaces SUSPEND_ONLY_RULE, crds/backup_schedule.rs:65)
    rule:      has(self.sourceRef) == has(oldSelf.sourceRef) && (!has(self.sourceRef) || self.sourceRef == oldSelf.sourceRef)
    message:   spec.sourceRef is immutable; create a new BackupSchedule to protect a different cluster

R2  placement: BackupSchedule .spec (validation rule)
    rule:      !has(self.allUserTopics) || size(self.topics) == 0
    message:   spec.allUserTopics requires spec.topics to be empty

R3  placement: BackupSchedule schema root (validation rule; the root may read self.metadata.name)
    rule:      !has(self.spec.retry) || self.spec.retry.maxRetries == 0 || size(self.metadata.name) <= 29
    message:   a BackupSchedule with retries must be named in 29 characters or fewer; retry Backups are named logweir-backup-<schedule>-<slot>-r<N>
```

All three reference only fields that no stored object has (R2, R3) or compare a required field to itself (R1), so **no existing object can fail them on the 1.29 floor without validation ratcheting**. Deliberately *not* in CEL: "exactly one of non-empty `topics` or `allUserTopics`" (a stored `topics: []` object would block even `suspend` updates on 1.29), cron validity and tz validity (not expressible). Those are controller fail-closed checks (§4.5 step 0) and API 422s. `crates/weirkeeper/tests/crd_shape.rs` must be rewritten: `every_spec_is_sealed_and_only_suspend_is_mutable` (`:764`) becomes a check of R1–R3 and of `self == oldSelf` on the other five kinds; its evaluator (`:853-1068`) gains `size()`, `<=` and integer literals; `an_absent_optional_field_cannot_be_added_on_update` (`:1082`) is replaced by a table proving which edits R1–R3 accept and refuse. Because the evaluator is not the API server, W8 applies the CRD to docker-desktop and exercises each rule (L-05.1-4).

### 5.3 Revision recording

- Schedule: `status.observedGeneration`, `status.policy {generation, runPolicySha256, effectiveSince}`.
- Every schedule-created or schedule-linked manual Backup: `spec.scheduleRef {name, uid, generation, runPolicySha256}` (new optional fields on the existing `scheduleRef`, whose schema is today `LocalRef{name}`, `crds/backup.rs:113-114`), plus the copied policy fields. The frozen `execution-inputs.json` repeats `schedule{…}` (§3.3), so the revision survives schedule deletion.
- `generation` increments on every spec change including `suspend`; the UI shows "revision g7 · policy sha256:ab12…" so a suspend flip visibly leaves the run policy digest unchanged.

### 5.4 Atomic boundary when an edit races a due slot

```
read schedule @RV_n (generation G_n)
decide S due and admissible
PATCH status {pendingRun{…generation:G_n}, metadata.resourceVersion: RV_n}
    ├─ 409 (an edit landed: RV_n+1, G_n+1) → abort; requeue; re-read; decide again under G_n+1
    └─ 200 (RV_r) → POST Backup built from the same in-memory object (policy of G_n, scheduleRef.generation=G_n)
                     crash before POST → restart reads G_m ≥ G_n → step 2: create with G_m, recording G_m
```

Invariant (tested): **a Backup's copied policy equals the schedule spec at the generation recorded in it.** An edit landing after the reservation affects the next admission, never a created run. Edits never touch a created Backup, its frozen inputs or its Job (PLAT-06.1). A pending retry or catch-up after an edit uses the new generation (§4.6).

### 5.5 Invalid edits

| Edit | Where refused | Effect |
|---|---|---|
| Change `sourceRef` | API server (R1), 422 | object unchanged |
| `allUserTopics` with non-empty `topics` | API server (R2) | unchanged |
| `retry.maxRetries > 0` on a name > 29 chars | API server (R3) | unchanged |
| Out-of-range deadlines/retry, bad enum, bad tz syntax, exclusion item not Kafka-legal | API server (OpenAPI) | unchanged |
| Unparseable cron, unknown tz, empty selection, glob in `topics`, unreadable `archive.url` | Controller (accepted by schema) | `Ready=False` with reason; no new admissions; pending reservation released; running Backups unaffected; fixing the spec resumes within one reconcile |
| Same via console | API 422 `validation_failed` before any write | nothing stored |

No last-known-good fallback: silently continuing an old policy after an edit would contradict what the user sees. PLAT-14.2 staleness alerts cover a schedule left invalid.

### 5.6 API and UI touchpoints

- `PUT /api/v1/namespaces/{ns}/schedules/{name}` (operator): body = editable policy DTO (`cadence`, `timeZone`, `topicSelection`, `archive`, `concurrencyPolicy`, `startingDeadlineSeconds`, `catchUpPolicy`, `retry`, `activeDeadlineSeconds`, `retention`, `suspend`) + required `expectedResourceVersion`. Implemented as `Api::replace` with that resourceVersion (Kubernetes verb `update`). `412 precondition_failed` on a changed version, **except** when a re-read shows the stored spec already equals the requested one (lost-response replay → `200`). `422 validation_failed` with field error `sourceRef: field_immutable` (and the §5.5 semantic checks as field errors, evaluated before any write). Response includes `generation` and `runPolicySha256`.
- `POST …/schedules/{name}:set-suspension` (PLAT-17) unchanged.
- UI: copy that claims immutability or UTC-only must change in the same release (`ui/pages/schedules.js:88-90`, `:192-193`, `:200`, `config/samples/backupschedule.yaml:6-13`, `docs/kubernetes.md:180`, CRD `doc` strings). The guided edit form is PLAT-10.1.

### 5.7 Conversion, upgrade and rollback (PLAT-05.1)

- Single stored version; no conversion webhook; no object rewrite. Existing Backups have no `scheduleRef.uid/generation`: the UI shows "revision not recorded (created before PLAT-05.1)".
- Upgrade order: CRD (R1–R3) first, then controller, then API/UI. An old controller running against the new CRD honours edits of `schedule/topics/archive` naturally (it copies at creation) but writes no revision, ignores new fields (§4.9) and copies `topics: []` for dynamic schedules, whose runs then fail safe at the runner (exit 3, `phase_minus1_admit.rs:98-105`).
- Rollback: roll back the controller only. Re-applying the old CRD would re-seal the spec and prune unknown fields from API responses; if a CRD downgrade is unavoidable, first remove `timeZone`, `retry`, `catchUpPolicy`, deadlines and `allUserTopics` from every schedule and record them for re-application.
- RBAC: add `patch` to `logweir-operator` on `backupschedules` (`kubectl edit`/`apply` send PATCH; `update` alone cannot, as `charts/logweir/templates/ui/ui.yaml:85-90` already notes). The in-cluster UI ServiceAccount's existing `patch` (`ui.yaml:115-117`) now reaches every mutable field; the page still sends only `spec.suspend`, and `docs/kubernetes.md` §16 must say that the CRD, not the page, now bounds that account.

---

## 6. PLAT-05.2 — decouple schedule deletion from retained history

### 6.1 Ownership model

- New Backups: **no** ownerReference to the schedule (`scheduled_backup`, `backup_schedule.rs:630-637`, drops it). Membership = `spec.scheduleRef.uid` (immutable) + labels (§3.1). Backups keep owning their plan ConfigMap and Jobs (`backup.rs:500-512`, `job.rs:820-827`).
- Deleting a schedule (any propagation policy) therefore stops future admissions and leaves every Backup, plan ConfigMap, Job (until its TTL) and archive object in place.
- Active runs at deletion: frozen runs continue to completion (their Jobs are owned by the Backup). Created-but-unfrozen scheduled runs are refused `ScheduleNotFound` (§3.1 rule 2). Manual runs are unaffected.
- No finalizer is added: a finalizer cannot stop foreground GC of `blockOwnerDeletion` dependents, and it would strand schedules after uninstall or rollback.

### 6.2 Migration of legacy ownerReferences (controller, idempotent, resumable)

For each schedule, while `status.history.legacyOwnedRuns > 0`:

1. Inventory (§6.7) finds Backups carrying an ownerReference with `apiVersion: logweir.dev/v1alpha1`, `kind: BackupSchedule`, `uid == schedule.uid`.
2. **Only terminal** Backups (`Succeeded`, `Failed`, `Refused`) are migrated. Nonterminal legacy runs keep their ownerReference until terminal, because PLAT-06.1's scheduled identity for an unfrozen legacy object still derives from it.
3. One JSON merge PATCH per Backup on the main resource:
   `{"metadata":{"resourceVersion":"<observed>","ownerReferences":[<all entries except the matching BackupSchedule entry, byte-identical>] | null,"labels":{"logweir.dev/schedule-uid":"<uid>"},"annotations":{"logweir.dev/history-retained-from-owner":"<uid>"}}}`
   Other ownerReferences, labels and annotations are preserved exactly (merge-patch key semantics; the ownerReferences array is rewritten from the observed object under the resourceVersion precondition).
4. `409` → re-read next reconcile. `422` (a legacy object that fails a newer schema) or `403` → record in `HistoryRetained=False reason=MigrationBlocked` naming up to 10 Backups; never retried faster than the inventory interval. *(Amended 2026-09-17 at W4's integration, review `d1w4` M-2: the blocked set has a third member — a legacy `Backup` that carries this schedule's controller ownerReference but whose `spec.scheduleRef` does not name this schedule cannot be detached without orphaning it (membership rule 3 requires that name and `Backup.spec` is CEL-sealed), so it is recorded, terminally, as `migrationBlocked[].reason: NoScheduleReference`, a `status.history` value and not a §3.4 condition reason, with a message naming the only two remedies: delete the schedule with `--cascade=orphan`, or delete the run. A capped namespace-wide inventory never unlocks the UID label selector and never claims `HistoryRetained=True`; the migration works inside the page loop under one read budget, so the migration window costs one paginated walk per pass, not a namespace re-list every requeue.)*
5. Condition: `HistoryRetained=True reason=Retained` when no matching owner entry remains; `False ActiveLegacyRunsOwned` while only nonterminal legacy runs remain; `False LegacyOwnerReferencesRemain` otherwise, with counts.

Interruption safety: progress is derived from observation, not a cursor. Each patch is atomic for its object (resourceVersion precondition); a crash between patches leaves every object either fully migrated or untouched; a restart re-inventories and continues. No Backup is ever deleted, and no spec is patched (CEL would refuse it anyway).

Migration window, stated in the upgrade notes: until `HistoryRetained=True`, deleting the schedule with the default propagation can still garbage-collect unmigrated history. Use `kubectl --context <ctx> delete backupschedule <name> --cascade=orphan`, which is always safe, before or after migration.

**Status across an upgrade (amended 2026-09-18, at the §13.1 fenced run of L-05.1-3).** The migration above is metadata-only and a terminal run's `spec`, `status.phase`, exit code, reason and `status.records` never change across an upgrade. Independently of the migration, D3's trust rule may add `signedAt` and `trust` to a terminal object's `status.evidence.verification` by ONE bounded, digest-checked re-read of its receipt (the TRUST-UPGRADE-SIGNEDAT repair), and may re-derive `trust`/`result` on a policy event; it never changes `result`, `matchedKeyId` or `verifiedAt` of a compared verdict without a read. L-05.1-3 asserts exactly that carve-out: nothing outside the migration fields and the verification block's additive trust fields moves.

### 6.3 Deletion semantics (tested)

| Action | Before this task | After (migrated or new runs) |
|---|---|---|
| `delete` (background, kubectl default) | Backups, plan ConfigMaps, Jobs GC'd | All retained; running Jobs finish; receipts verify |
| `delete --cascade=foreground` | Same, owner waits | All retained |
| `delete --cascade=orphan` | Retained (owner refs removed by GC) | Retained |
| Delete a Backup explicitly (operator) | Its ConfigMap and Job GC'd | Unchanged (§6.5 rules) |

### 6.4 Same-name recreation

A recreated `nightly` has a new UID, so it sees none of the old runs as its own: history queries use `logweir.dev/schedule-uid=<new uid>`, `concurrencyPolicy` ignores old active runs, execution ids differ (`<uid>-<slot>`), and archive prefixes cannot collide. If the recreation happens inside a slot or retry window whose deterministic name is still held by the old generation, the new schedule records `SlotNameUnavailable` for that slot and continues with the next. The UI lists "previous generation of `nightly` (uid 3f0c…, deleted)" from `logweir.dev/schedule=nightly` rows with another UID. API touchpoint for PLAT-10.2: `GET /api/v1/namespaces/{ns}/schedules/{name}/runs?limit=&cursor=&trigger=&includePreviousGenerations=` lists by the `schedule-uid` label with PLAT-17 cursors and, when asked, adds same-name rows from other UIDs flagged `previousGeneration: true`. The drain-and-replace procedure in `docs/kubernetes.md:422-441` is replaced by "edit the schedule (PLAT-05.1)"; recreation is only for changing `sourceRef`.

### 6.5 Explicit history cleanup rules (Logweir still deletes nothing)

1. No Logweir component deletes Backup CRs, ConfigMaps or archive objects; the controller role keeps no `delete` verb (`config/rbac/role.yaml:33-37`).
2. An operator may delete **terminal** Backup CRs with their own RBAC. Deleting one removes its plan ConfigMap (frozen inputs) and any remaining Job, and removes the run from Kubernetes-backed history and restore selection until PLAT-15.1 provides catalog-backed discovery. Archive data and signed receipts remain in object storage; record `status.backupId`, `status.evidence.receiptKey` and `sidecarKey` first.
3. Never delete a nonterminal Backup (its Job would be collected mid-run: `NoExitCode`, partial archive).
4. Keep, per schedule UID, at least the newest `Succeeded` run whose evidence verification is `Valid`, every run newer than `max(startingDeadlineSeconds, maxRetries × delaySeconds + activeDeadlineSeconds)`, and any run whose `backupId` appears in a nonterminal Restore plan.
5. Select with labels, then filter terminal runs locally, then delete by name (documented `kubectl --context` + `jq` recipe in `docs/kubernetes.md` §9).
6. The scheduler's "S ≤ lastFireTime ⇒ fired" guard (§4.7 row 14) prevents a pruned current-slot Backup from being re-run under the same execution id.

### 6.6 Resource-retention cost (to publish in docs)

Per retained run: Backup CR ≈ 4–6 KiB (spec, status with 3–4 conditions and evidence, managedFields); plan ConfigMap ≈ 2 KiB + 2 × Σ(topic name bytes + ~6) (names appear in `backup.yaml` and `execution-inputs.json`). Jobs and pods (≈ 11 KiB each run, two Jobs for dynamic runs) are bounded by the 7-day TTL (`backup.rs:110`). Examples:

| Schedule | Per run retained | Runs/year | etcd growth/year | TTL-window Jobs/pods |
|---|---|---|---|---|
| daily, 20 named topics | ≈ 8 KiB | 365 | ≈ 3 MiB | ≈ 0.08 MiB |
| hourly, 20 named topics | ≈ 8 KiB | 8 760 | ≈ 70 MiB | ≈ 1.8 MiB |
| every 15 min, dynamic, 1 000 topics × 30 bytes | ≈ 79 KiB | 35 040 | ≈ 2.6 GiB | ≈ 14 MiB |

The last row exceeds a default etcd quota within a year, so pruning (§6.5) is **required** for high-frequency dynamic schedules. The controller reports `status.history {runCount, estimatedBytes}` (estimate = Σ serialized Backup length + 2048 + 2 × topic bytes, using `spec.topics` or `status.selection.resolvedTopicBytes`) and sets `HistoryRetained=True reason=HistoryLarge` above 2 000 runs or 64 MiB per schedule. Before this task the same growth already accumulated while a schedule existed; the task removes only the destructive cleanup path.

### 6.7 Scheduler read cost with retained history

Listing every Backup every 30 s (`backup_schedule.rs:1199`) becomes unbounded once history is kept. Steady state after this task:

- Per reconcile: GET the ≤ 10 `activeRuns` names, the ≤ 1 `pendingRun` name and the ≤ 4 deterministic names of the latest slot (≤ 15 GETs). O(active), independent of history.
- Inventory (at process start per schedule, on generation change, and every 60 min): paginated list (`limit=500`) filtered by `logweir.dev/schedule-uid=<uid>`, feeding `history` and repairing `activeRuns`. Namespace-wide inventories needed by several schedules in one namespace within the same minute share one paginated list. While legacy owned runs may exist (until `HistoryRetained=True` has held for one inventory after start), the inventory uses a namespace-wide paginated list, because legacy objects carry no UID label.
- Correctness does not depend on list freshness: every schedule-created run is recorded in `pendingRun`/`activeRuns` by a resourceVersion-conditional status write before creation, so admission always sees the most recent admitted run.

### 6.8 RBAC (PLAT-05.2)

Controller ClusterRole adds `{apiGroups:["logweir.dev"], resources:["backups"], verbs:["patch"]}` with its caller named (`schedule_history.rs` migration patch) in `config/rbac/role.yaml`, `charts/logweir/templates/clusterrole.yaml` and `logweir.yaml`. `manifest_lint.rs::every_granted_verb_has_a_caller` and `chart_lint` agreement tests are updated. Security review note: `patch` on Backup metadata cannot change a sealed spec or a status; the caller writes only `ownerReferences`, one label and one annotation, always with a resourceVersion precondition.

### 6.9 Upgrade and rollback (PLAT-05.2)

- Upgrade: CRD → controller → wait for `HistoryRetained=True` on every schedule (`kubectl --context <ctx> get backupschedules -A -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name} {.status.conditions[?(@.type=="HistoryRetained")].reason}{"\n"}{end}'`) before deleting schedules without `--cascade=orphan`.
- Rollback: an older controller identifies children only by ownerReference, so it neither counts new runs for `Forbid` (possible overlap) nor sees their history, and it creates owned runs again (the GC hazard returns for those runs only). Migrated history stays retained. Before rollback, suspend schedules whose overlap matters; after roll-forward, the migration picks up any new owned runs.

### 6.10 PLAT-15.1 integration

PLAT-15.1 indexes recovery points from object storage by execution id (`status.backupId`, the receipt's `backup_id`). The schedule history view becomes `CR history (by schedule-uid label) ∪ catalog points attributed to that schedule UID`, deduplicated by execution id; CR pruning then no longer removes discoverability. Manual execution ids are Backup UIDs and do not encode the schedule, so attribution needs origin metadata. Recommendation for PLAT-15.1 (not done here): a receipt minor version `1.1.0` optional nested `origin {namespace, backupName, backupUid, scheduleRef{name,uid,generation}, trigger{kind,slot,attempt}}` and `selection {mode, coverage, visibility, discoverySha256}`, fed from execution inputs `v2` via a runner flag. These tasks change no signed format.

---

## 7. PLAT-09.2 — explicit and dynamic topic selection per run

### 7.1 CRD fields (shared type in new `crates/weirkeeper/src/crds/selection.rs`)

`BackupSchedule.spec.allUserTopics` and `Backup.spec.allUserTopics`, optional:

| Field | Type / validation | Notes |
|---|---|---|
| `exclude.topics` | list of string, `maxItems: 1000`, items `^[a-zA-Z0-9._-]{1,249}$` | exact names |
| `exclude.prefixes` | list of string, `maxItems: 32`, same pattern | literal prefixes, never patterns |
| `incompleteDiscovery` | enum `Refuse`, `BackUpVisibleTopics`; **required** | no default (§7.4) |

Modes: `topics` non-empty and no `allUserTopics` ⇒ **SelectedTopics** (today's named allowlist, unchanged); `topics: []` and `allUserTopics` ⇒ **AllUserTopics**. Anything else ⇒ schedule `Ready=False InvalidTopicSelection` / Backup terminal `InvalidTopicSelection` before any POST. API DTO: `topicSelection: {mode: "selectedTopics", topics} | {mode: "allUserTopics", exclude, incompleteDiscovery}`. Internal topics named explicitly in a SelectedTopics list remain allowed (preserves existing allowlists); the UI discourages them.

### 7.2 Backup controller algorithm for AllUserTopics (new `controllers/backup_selection.rs`, one call site in `backup.rs`)

Runs after the existing name and glob rails (`backup.rs:1857-1887`) and the identity checks (§3.1), before PLAT-06.1's freeze. Nothing below applies once `<backup>-plan` exists.

```
R1 Resolve source connection once with the PLAT-07.1 resolver → in-memory ResolvedConnection
   (clusterUid, bootstrapServers, auth, credential env, TLS refs) and its digest.
R2 Discovery Job `lwd-<backup-uid>` (40 chars; owner = this Backup; runner ServiceAccount; no token; no signing
   key mount; RunnerJobSpec without plan ConfigMap; activeDeadlineSeconds = min(300, spec.deadlineSeconds);
   podFailurePolicy/backoffLimit as job::build; label logweir.dev/purpose=topic-discovery):
     absent → POST; status phase Resolving, TopicsResolved=False DiscoveryRunning; requeue 15 s.
     running → requeue.
     finished → read exit code (container `runner`, by name) and the FULL pods/log (not the 8-line tail),
                refusing bodies > 2 MiB.
R3 exit != 0, no exit code, missing/duplicate summary, count or digest mismatch, non-Kafka-legal name
   → terminal DiscoveryFailed (retryable) or DiscoveryResultUnreadable (not retryable: malformed output).
R4 summary.clusterId must equal the source cluster observed so far (KafkaCluster status.clusterId when
   present); resolver digest recomputed now must equal R1's → else SourceChangedDuringResolution.
R5 Classify: internal = entry.internal || name starts with "__";
   limited = entries with TopicAuthorizationFailed; excludedByRule = exact or prefix match;
   resolved = visible − internal − limited − excludedByRule, byte-sorted, deduplicated.
R6 visibility = runner-reported unknown|limited, upgraded to attestedComplete only through PLAT-09.1's
   administrator attestation for (namespace, KafkaCluster uid, clusterId, principal).
     attestedComplete                       → coverage AllUserTopicsAttested
     unknown|limited, BackUpVisibleTopics   → coverage VisibleUserTopicsOnly
     unknown|limited, Refuse                → terminal DiscoveryIncomplete (not retryable)
R7 resolved empty → terminal SelectionEmpty (not retryable; the runner is never started).
R8 resolved count > 5 000 or name bytes > 256 KiB → terminal SelectionTooLarge.
R9 Freeze (PLAT-06.1 path) with inputs v2 selection block (§3.3); status.selection; TopicsResolved=True
   Resolved. Only after the status patch: PATCH the discovery Job with ttlSecondsAfterFinished (same ordering
   rule as runner Jobs).
```

No Kubernetes object other than the discovery Job and the plan ConfigMap is created; no new RBAC is needed (Job create/get/patch, pod list, `pods/log` get, ConfigMap create/get already granted). Discovery runs per run, so it is never stale; a retry is a new Backup and therefore a fresh discovery. The engine only ever receives `backup.yaml` with the frozen non-empty named list; the runner's own empty-list and glob refusals (`phase_minus1_admit.rs:78-105`) remain as defence in depth.

### 7.3 Discovery contract consumed

**Amended 2026-09-17 at W5's integration (D-SEAMS S1, one check runner).** The fallback command below (`logweir topics discover`) was never landed and is superseded: a dynamic `Backup` discovers through D2 §4.2's `logweir check run --plan <file>` with plan kind `topicInventory`, rendered by `check::plan::build` from the PLAT-07.1 resolver's connection (the CA projected as a mount) into a plan ConfigMap and a Job named `lwd-<backup-uid>`, both owned by the `Backup`; the result is read through `check::relay` off the pod proven by owner UID (S6) and parsed by the same code D2 W8's `TopicDiscovery` controller uses. The visibility states, the internal-topic rule and the count/digest verification below still bind; the runner's grammar is `crates/logweir/src/check/kinds/inventory.rs`. A `TopicDiscovery` object's result is never an execution input (S2): every dynamic run discovers afresh. Two clauses of §7.2 R2 change with it: the discovery Job DOES carry a plan ConfigMap (`lwd-<backup-uid>-plan`, owned, immutable, digest-annotated — the check runner reads its plan from a file), and the relay body is bounded by the check framework's relay budget rather than a separate 2 MiB figure. A discovery Job is counted into the connection's check ceiling by its `logweir.dev/purpose=topic-discovery` label, and `spec.deadlineSeconds` must leave at least 30 s of plan budget after the 90 s margin or the run is refused up front.

From PLAT-09.1: visibility states `attestedComplete | limited | unknown` (a successful listing alone is `unknown`), internal topics excluded by default, bounded output. If PLAT-09.1 has not fixed a runner output format when W5 starts, W5 uses and PLAT-09.1 adopts this one:

- Command: `logweir topics discover --bootstrap-server <s>… --auth-mode plaintext|scramSha512 [--username U] [--tls]`, password from `LOGWEIR_SOURCE_PASSWORD`, no archive or signing inputs, no Kubernetes API calls.
- stdout, one line per visible topic, byte-sorted: `discovery-topic={"name":"orders","partitions":6,"internal":false,"error":null}`.
- Final line: `discovery-summary={"formatVersion":"topic-discovery/v1","clusterId":"…","observedAt":"…","topicCount":N,"visibility":"unknown|limited","basis":"metadata-list","sha256":"sha256:<hex of det_json(array of topic entries)>"}`.
- Exit 0 on success; 1 on unreachable, timeout or authentication failure (no summary). The controller verifies `topicCount` and `sha256` over the lines it parsed, which also detects truncated or rotated logs.
- `logweir-kafka::TopicMeta` gains `internal: bool` from broker metadata; the `__` prefix rule is applied by the controller as well.

### 7.4 Completeness decision

Kafka silently omits topics a principal cannot describe, so without an administrator attestation no discovery can prove whole-cluster visibility. Defaulting to refusal would make dynamic mode unusable out of the box; defaulting to visible-only would silently weaken the "all user topics" promise. **Decision: the user chooses per policy, explicitly, and the choice is required.** Every run records its coverage label; the UI, API and status never render "all topics" unless the coverage is `AllUserTopicsAttested`. Labels: `NamedTopics` → "Named topics (N)"; `AllUserTopicsAttested` → "All user topics (attested complete)"; `VisibleUserTopicsOnly` → "Visible user topics only (N) — completeness not established". The signed receipt continues to attest the exact named set only (`backup_receipt.rs:116-120`); coverage is controller-recorded and labelled as such until PLAT-15.1 decides on a signed selection block.

### 7.5 Races, internal topics, limits

| Case | Behaviour |
|---|---|
| Topic created after discovery | Not in this run; included in the next dynamic run |
| Topic deleted after discovery, before the engine reads it | The frozen list still names it; the run records whatever the runner reports (0 records for that topic, or exit 1 → `Failed`, retryable → a retry rediscovers without it). Never a coverage claim for a different set |
| Topic deleted and recreated between runs | Separate runs; each freezes its own list |
| Internal topics (`__consumer_offsets`, `__transaction_state`, any `__*`) | Always excluded in dynamic mode, counted in `internalExcluded` |
| Principal lacks Describe on a topic | Omitted by Kafka; visibility stays `unknown`; `Refuse` fails, `BackUpVisibleTopics` runs labelled visible-only |
| Every user topic excluded or cluster empty | `SelectionEmpty`, no runner Job, not retried |
| All resolved topics empty of records | Existing runner behaviour: exit 1 "captured nothing" (`phase_run.rs:155-166`) → retryable |

### 7.6 `Backup.status.selection`

`{mode, coverage, visibility?, resolvedTopicCount, resolvedTopicBytes, internalExcludedCount?, excludedByRuleCount?, limitedTopicCount?, discoveryObservedAt?, discoverySha256?}`. Names are deliberately not copied into status (unbounded); they live in the immutable inputs and in the signed receipt. Restore subset selection (PLAT-11.2) reads them through a later API route over the plan ConfigMap.

### 7.7 Compatibility and rollback (PLAT-09.2)

- Existing named schedules and Backups are unchanged (`coverage: NamedTopics` recorded for new freezes only).
- Old controllers deserialize dynamic objects (because `topics` stays present) and fail safe: the old Backup controller renders `topics: []`, and the runner exits 3 without contacting the engine (`phase_minus1_admit.rs:98-105`). Old controllers never create discovery Jobs.
- Rollback: suspend dynamic schedules first; dynamic Backups created but not yet frozen must be allowed to fail or be deleted by an operator.

---

## 8. PLAT-06.2 — Back up now and Run first backup now

### 8.1 Canonical manual Backup (one CR path for kubectl, API and UI)

```yaml
apiVersion: logweir.dev/v1alpha1
kind: Backup
metadata:
  name: logweir-manual-<26 lowercase base32>
  labels: {logweir.dev/schedule: nightly, logweir.dev/schedule-uid: <uid>, logweir.dev/trigger: manual, logweir.dev/attempt: "0"}
  annotations: {logweir.dev/request-sha256: sha256:…}   # plus PLAT-17 audit/idempotency annotations in console mode
spec:
  sourceRef: {name: source}              # copied from schedule generation G
  topics: [orders, payments]             # or [] + allUserTopics
  archive: {url: s3://kafka-backups/logweir, secretRef: {name: logweir-s3}}
  deadlineSeconds: 3600                  # schedule.activeDeadlineSeconds or 3600
  triggeredBy: manual
  trigger: {kind: Manual, attempt: 0}
  scheduleRef: {name: nightly, uid: <uid>, generation: 7, runPolicySha256: sha256:…}   # omitted for ad-hoc runs
```

Shipped as `config/samples/backup-manual.yaml` (the CLI path is `kubectl --context <ctx> create -f`). PLAT-06.1 executes it with execution id = Backup UID.

### 8.2 API contract (PLAT-17 route owned by PLAT-06.2)

`POST /api/v1/namespaces/{ns}/backups`, operator role, `Idempotency-Key` required.

- Body A (from schedule): `{"scheduleRef":{"name":"nightly","expectedGeneration":7}, "readinessAcknowledgement"?:{"preflight":"…","state":"notReady|unknown"}}`. The API reads the schedule and copies sourceRef/selection/archive/deadline and `{uid, generation, runPolicySha256}`. Policy fields in the body are rejected (`422 validation_failed`).
- Body B (ad-hoc from a cluster): `{"sourceRef":{"name":"source"},"topicSelection":{…},"legacyArchive":{"url":"…","secretRef":{"name":"…"}},"deadlineSeconds"?:3600}` (`destinationRef` replaces `legacyArchive` after PLAT-08).
- Name: `logweir-manual-` + first 26 chars of lowercase, unpadded RFC 4648 base32 of `sha256` over the length-prefixed fields `(issuer, sub, namespace, "POST /api/v1/namespaces/{ns}/backups", key)`; annotations carry the request hash and key hash (never the key).
- Responses: `201` created; `200` replay (same key, same request hash) returning the same UID; `409 idempotency_conflict` (same key, different request hash); `409 policy_changed` (`expectedGeneration` ≠ current; body includes the current generation and `runPolicySha256`); `404 not_found` (schedule); `422 validation_failed` with field errors such as `topicSelection: selection_invalid` (the run policy fails `policy::validate_run_policy`, e.g. empty selection); `403 forbidden`. Body: `{backup:{name, uid, phase, trigger, scheduleRef}, schedule?:{name, uid, generation, suspended, activeRuns}}`.
- The request hash covers the body as sent (including `expectedGeneration`), so a lost-response replay after a later schedule edit still returns the originally created run.

### 8.3 Schedule state interactions

| Situation | Manual run |
|---|---|
| Schedule suspended | Allowed; future scheduled runs stay suspended; response and UI say so |
| Schedule `Ready=False` for cadence/tz reasons | Allowed (run policy valid) |
| Run policy invalid (empty selection, glob, bad archive URL) | `422`; legacy direct-CR path → controller terminal refusal |
| Scheduled run active (`Forbid` or `Allow`) | Allowed; not counted and not blocked; UI shows a non-blocking notice. (Amendment P10, §15: manual runs have their own per-namespace pool and may queue; scheduled runs are never in it.) |
| Schedule deleted after request, before freeze | Runs (manual runs do not require the schedule, §3.1 rule 2) |
| Schedule edited after request | Runs the copied generation |

### 8.4 Preflight seam (PLAT-03.1)

The API and controller never gate on readiness: execution-time guards stay authoritative and the direct CR path exists regardless. When the console advertises the readiness capability, the UI fetches the latest readiness for the same operation inputs. `ready` → submit. `notReady` → show each failed prerequisite and remedy and require a second explicit "Run anyway" (sent as `readinessAcknowledgement`, recorded as annotation `logweir.dev/readiness-ack`, not authoritative). `unknown`/capability absent → submit, with the label "Readiness not checked; the run reports its own prerequisites".

### 8.5 UI states (both modes)

| State | Trigger | UI |
|---|---|---|
| Idle | Schedule card, cluster page, or post-create panel ("Run first backup now" / "Later") | Button enabled when the user may create Backups |
| Confirming | Readiness `notReady` | Remedies + "Run anyway" |
| Submitting | Click | Button disabled; one idempotency intent `K` (console: key; legacy: minted name + request hash) held in memory for this intent |
| Accepted | `201`/`200` (legacy: `201`, or `409 AlreadyExists` whose stored `logweir.dev/request-sha256` equals the local hash) | Navigate to `#/backups/<ns>/<name>` with a banner: trigger, schedule revision `gN`, policy digest |
| Outcome unknown | Network error, timeout, `5xx` | "Check status" re-submits with the same `K`; no new intent is created automatically |
| Conflict | `409 policy_changed` | Show the new revision; confirmation starts a new intent |
| Rejected | `4xx` | Field/problem message; the draft is kept (PLAT-13.2) |
| After refresh | In-memory `K` lost | The card lists manual runs of this schedule created in the last 10 minutes ("started 40 s ago"); a new click is a deliberate new run |

Legacy direct-CR mode uses the existing `create(ns, "backups", body)` (`ui/api.js:128`, `backups` already writable, `:44-50`), so no new `api.js` export and `ui_lint.rs::the_suspend_toggle_is_the_only_update` (`:1035`) stays valid. The in-cluster UI ServiceAccount gains `create` on `backups` (`charts/logweir/templates/ui/ui.yaml:108-114`). Browser storage stays unused (`scripts/check-ui-offline.sh`).

Backup detail (`ui/pages/backups.js:147-184`) adds: trigger kind and attempt, `retryOf` link, schedule name + short UID + revision + policy digest (or "schedule deleted"), selection coverage label and counts.

### 8.6 RBAC (PLAT-06.2)

`logweir-operator` already has `create backups`. Console ServiceAccount (PLAT-17): `get/list` on `backupschedules`, `kafkaclusters`, `backups`; `create` on `backups`; `update` on `backupschedules` (§5.6). No `patch`, `delete`, Secret or Job permissions.

### 8.7 Residual risk accepted

A subject who can create Backups (operators) can pre-create a deterministic scheduled name with copied fields; the scheduler would adopt it. Name uniqueness still prevents two objects sharing an execution id (no partial-archive collision), and the operator could already run any policy manually in that namespace. The digest check (§3.1 rule 5) is integrity, not authentication. Documented, not mitigated further here.

---

## 9. Amendment A: no new kind

| Candidate | Decision | Why |
|---|---|---|
| `ScheduleRevision` / `ControllerRevision` snapshots | No | Each Backup copies the policy and records `(uid, generation, runPolicySha256)`; PLAT-06.1's immutable inputs are the snapshot. A revision object would add lifetime and GC questions (its owner is the schedule whose deletion must not affect history) without adding information a run lacks. |
| `BackupRun` / `BackupRequest` | No | `Backup` is the run; a deterministic name is the idempotent request. |
| `TopicDiscovery` check resource | Not for PLAT-09.2 | Per-run discovery is a Backup-owned Job; the shared code is a module and a runner contract. PLAT-09.1's interactive discovery (PLAT-17 `topic-discoveries` routes) may need a kind; that task must record its own Amendment A decision. |
| `HistoryPolicy` / pruning kind | No | Cleanup is an operator action under documented rules; enforcement belongs to PLAT-16.2. |

The kind list in `crates/weirkeeper/src/crds/mod.rs:66-73` and `tests/crd_shape.rs::the_kind_list_is_exactly_six` stays unchanged.

---

## 10. RBAC and documentation changes (all tasks)

| Principal | Change | Task |
|---|---|---|
| `weirkeeper` ClusterRole | none for reservations (W0 uses `patch`); `+patch backups` | W0 / PLAT-05.2 |
| `logweir-operator` | `+patch backupschedules` | PLAT-05.1 |
| In-cluster UI ServiceAccount | `+create backups` | PLAT-06.2 |
| `logweir-api` ServiceAccount | `create/get/list backups`, `get/list/update backupschedules`, `get/list kafkaclusters` | PLAT-06.2 / 05.1 via PLAT-17 |
| Runner ServiceAccount | none (discovery Job reuses it, no token) | PLAT-09.2 |

Documentation (each worker edits only its sections): `docs/kubernetes.md` §7 table and seal text (W2), §9 schedules — time zones, deadlines, catch-up, retries, truth table, editing, history retention, migration, cleanup, cost (W2, W4), §10 Backup — trigger, identity, `Resolving`, selection (W3b, W5), §13 RBAC (W4), §16 page writes (W7); `docs/install.md` upgrade order; `charts/logweir/README.md` RBAC table and UI account; `ui/README.md`; `docs/stability.md` supported scheduling semantics and limits; `config/samples/backupschedule.yaml` comments; new `config/samples/backup-manual.yaml`.

---

## 11. Implementation plan: bounded worker tasks

### 11.1 Tasks, ownership and dependencies

| Id | Scope (tracker) | Owned files | Depends on | Parallel with |
|---|---|---|---|---|
| **W0** | P0 reservation fix (§0.3) | `controllers/backup_schedule.rs` (reservation fn only), `tests/schedule_controller.rs` (routes), `crates/logweir/tests/manifest_lint.rs` (reverse lint), `docs/kubernetes.md` §9 (one paragraph) | — | W1 |
| **W1** | 04.2a cadence engine | `crates/weirkeeper/src/slot.rs`, new `src/cadence.rs`, `src/lib.rs` (module line), `Cargo.toml` (root + weirkeeper), `Cargo.lock`, `THIRD_PARTY_NOTICES.md`, `deny.toml` if needed, new `tests/cadence.rs`, new `ui/tests/fixtures/cadence-presets.json` | — | W0, in-flight work |
| **W3a** | Run contract types (04.2/05.1/05.2/09.2) | `crds/backup.rs`, new `crds/selection.rs`, new `src/identity.rs`, new `src/policy.rs`, `src/conditions.rs` (all §3.4 constants), new `tests/run_identity.rs`, `tests/crd_shape.rs` (Backup parts), Backup CRD copies (`config/crd/backups.yaml`, `charts/logweir/crds/backups.yaml`, `logweir.yaml`, `charts/logweir/rendered/*`) | PLAT-06.1 merged, W1 (slot names) | W6 contract drafting |
| **W3b** | Backup controller consumes identity/inputs v2 (05.1, 06.2 controller side) | `controllers/backup.rs`, `tests/backup_controller.rs`, `docs/kubernetes.md` §10 | W3a | W2, W4-dev, W6 |
| **W2** | 05.1 then 04.2b schedule controller (two commits/reviews, same owner, sequential) | `crds/backup_schedule.rs`, `crds/mod.rs`, `controllers/backup_schedule.rs`, `tests/schedule_controller.rs`, `tests/crd_shape.rs` (schedule parts, after W3a), schedule CRD copies + `logweir.yaml` + rendered charts, `config/samples/backupschedule.yaml`, `config/rbac/operator_role.yaml`, `charts/logweir/templates/human-roles.yaml`, `docs/kubernetes.md` §7 and §9 (policy parts) | W0, W1, W3a | W3b, W4-dev, W6 |
| **W4** | 05.2 history | new `controllers/schedule_history.rs` (migration, inventory, `HistoryRetained`), W2 leaves the call site `schedule_history::observe(...)` and the condition slot in its status builder, new `tests/schedule_history.rs`, `config/rbac/role.yaml`, `charts/logweir/templates/clusterrole.yaml`, `logweir.yaml` RBAC, `crates/logweir/tests/{manifest_lint,chart_lint}.rs`, `charts/logweir/README.md` RBAC, `docs/kubernetes.md` §9 history + §13 | W2 merged for integration (develop against the agreed signature earlier) | W3b, W5, W6 |
| **W5** | 09.2 dynamic selection | new `src/discovery.rs`, new `controllers/backup_selection.rs`, one call site in `controllers/backup.rs` (after W3b), new `tests/backup_selection.rs`, `crates/logweir-kafka` `TopicMeta.internal` only if PLAT-09.1 has not added it, `docs/kubernetes.md` §10 selection | W3b, PLAT-07.1 resolver, PLAT-09.1 runner contract (or §7.3 fallback agreed with 09.1) | W4, W6 |
| **W6** | API routes: previews (04.2), `PUT schedules` (05.1), `POST backups` (06.2) | `crates/logweir-api/src/routes/{cadence_previews,schedules,backups}.rs`, their DTOs/problem codes in `contract.rs`, OpenAPI fixtures, route tests | PLAT-17.1 stages 1 and 3 merged, W1, W3a | W2, W3b, W4, W5 |
| **W7** | UI: Back up now / Run first backup now (06.2), schedule copy + next runs + time zone (04.2/05.1), coverage labels (09.2) | `ui/pages/{schedules,backups,clusters}.js`, `ui/render.js`, `ui/tests/*.spec.js`, `crates/logweir/tests/ui_lint.rs`, `charts/logweir/templates/ui/ui.yaml` + rendered, `ui/README.md`, `docs/kubernetes.md` §16 | `ui-correct` merged, W3a fields, W2 status fields; release-coupled with W2 (copy) | W4, W5, W6 |
| **W8** | Live docker-desktop acceptance (§13) | new `scripts/live/d1/*.sh` (+ proxy/VAP fence reused from the PLAT-04.1 harness pattern), evidence under `/tmp/logweir-roadmap-run/claude/artifacts/d1-live/` | W2, W3b, W4 (core); W5, W6, W7 for their slices | — |

Every worker: code review + Rust review (security review additionally for W4 RBAC and W6 idempotency); `cargo fmt --all -- --check`, `cargo clippy --locked --workspace --all-targets -- -D warnings`, focused test targets, then `just crds-check`, `just chart-check`, `just lint` for touched areas.

### 11.2 Sequencing

```
now ─┬─ W0 (P0) ───────────────┐
     └─ W1 ───────────────┐    │
PLAT-06.1 merge ──────────┴─ W3a ─┬─ W3b ──────────┬─ W5 (needs PLAT-07.1 + 09.1 contract)
                                  ├─ W2 (05.1 → 04.2b) ─┬─ W4 integrate
                                  │                     └─ W7 (after ui-correct)
PLAT-17.1 stage 1/3 merge ────────┴─ W6
W2+W3b+W4 merged ─────────────────────────────────────── W8 core; W5/W6/W7 slices as they land
```

Serialization points: CRD regeneration order W3a → W2 (W5 adds no CRD fields because W3a defines every type); `tests/crd_shape.rs` W3a → W2; `controllers/backup.rs` PLAT-06.1 → W3b → W5 call site; `controllers/backup_schedule.rs` W0 → PLAT-06.1 (annotation removal) → W2 → W4 call site; `docs/kubernetes.md` merged section by section by the orchestrator.

---

## 12. Test matrix mapped to the tracker's required tests

"Unit" = pure functions; "Double" = route-table controller tests using `weirkeeper::testing` (panics on unexpected requests, `src/testing.rs:1-35`); "Live" ids refer to §13.

### PLAT-04.2 (required: timezone/DST boundaries, long downtime, invalid cron, retry exhaustion, duplicate reconciliation)

| Required test | Unit (W1/W2) | Double (W2) | Live |
|---|---|---|---|
| Timezone/DST boundaries | `tz_gap_fixed_time_fires_once_at_gap_end` (Berlin 2027-03-28, NY 2027-03-14, Lord_Howe 30-min), `tz_overlap_fires_at_both_occurrences` (Berlin 2026-10-25, NY 2026-11-01), `tz_interval_schedule_keeps_utc_cadence_through_both_transitions`, `apia_skipped_day_maps_all_matches_to_one_instant`, `absent_timezone_is_byte_identical_to_legacy_utc` (10 000-case property), `slot_names_stay_utc_dns1123_for_shifted_slots`, `previews_mark_shifted_and_repeated_instants` | `a_tz_schedule_creates_the_utc_slot_name_and_records_the_zone`, `next_runs_are_rewritten_only_when_generation_or_first_run_changes` | L-04-1 |
| Long downtime | `latest_due_slot_supersedes_older_slots`, `missed_slot_accounting_caps_at_one_thousand` | `downtime_with_catch_up_none_records_missed_and_creates_nothing`, `downtime_with_catch_up_latest_creates_exactly_one_catch_up`, `catch_up_never_runs_a_slot_before_the_observed_revision`, `accepted_reservation_resumes_after_downtime` (existing, extended) | L-04-2 |
| Invalid cron | existing `cron_parse_refuses_what_it_does_not_understand` (`tests/schedule_controller.rs:825`), `unknown_timezone_is_refused_by_name` | `an_invalid_cron_edit_stops_admission_and_releases_a_pending_reservation`, `an_unknown_timezone_is_ready_false_and_creates_nothing` | L-04-3 |
| Retry exhaustion | `retry_classification_is_a_closed_match_and_unknown_is_not_retryable`, `retry_name_and_execution_id_carry_the_attempt`, `retry_names_fit_the_29_character_budget` | `a_retryable_failure_creates_r1_after_the_delay`, `a_guard_refusal_is_never_retried`, `retries_stop_at_max_retries_and_report_exhausted`, `a_retry_is_superseded_when_the_next_slot_is_due`, `a_succeeded_attempt_is_never_retried`, `retry_is_blocked_by_an_active_forbid_run` | L-04-4 |
| Duplicate reconciliation | `decide_attempt_is_a_pure_function_of_observed_objects` (source guard update of `the_name_never_reads_a_reconcile_clock_or_a_status`, `:649`) | `two_replicas_create_one_retry_via_409_adoption`, existing `two_controller_replicas_cannot_admit_different_slots_from_the_same_resource_version` (`:2981`) with PATCH reservations, `a_409_winner_with_another_schedule_uid_is_slot_name_unavailable` | L-04-5 |
| Truth table | table-driven `truth_table_rows_1_to_22` over `decide` + `decide_attempt` | one double per admitting row (13, 17, 21) | L-04-6 (Allow cap) |

### PLAT-05.1 (required: edit during execution, concurrent edit/fire, conversion of existing resources, invalid edit, rollback)

| Required test | Unit | Double (W2/W3b) | Live |
|---|---|---|---|
| Edit during execution | `run_policy_digest_ignores_cadence_and_order` | `editing_topics_does_not_touch_a_running_backup_or_its_inputs`, `the_next_run_records_the_new_generation_and_digest` | L-05.1-1 |
| Concurrent edit/fire | — | `an_edit_between_read_and_reservation_gets_409_and_readmits_under_the_new_generation`, `a_crash_after_reservation_creates_with_the_current_generation_and_records_it` | L-05.1-2 (proxy-held interleaving) |
| Conversion of existing resources | `absent_fields_reproduce_legacy_decisions` | `a_pre_upgrade_schedule_and_its_backups_reconcile_without_rewrites`, `a_legacy_backup_without_schedule_ref_uid_is_a_member_through_its_owner` | L-05.1-3 |
| Invalid edit | `crd_rules_r1_r2_r3_accept_and_refuse_the_table` (crd_shape evaluator) | `semantic_invalid_edits_fail_closed_with_reasons` | L-05.1-4 (real API server for R1–R3) |
| Rollback | — | `an_old_style_status_without_pending_run_is_resumed`, `an_unparseable_retry_reservation_is_cleared_by_legacy_parser` (compat of `reserved_slot`) | L-05.1-5 |

### PLAT-05.2 (required: delete with active/completed runs, API garbage collection, migration interruption, same-name recreation, unrelated-resource preservation)

| Required test | Unit | Double (W4, W2) | Live |
|---|---|---|---|
| Delete with active/completed runs | — | `a_new_scheduled_backup_carries_no_schedule_owner_reference`, `an_unfrozen_scheduled_run_whose_schedule_is_gone_is_schedule_not_found`, `a_frozen_run_is_not_rechecked_after_schedule_deletion` | L-05.2-2 |
| API garbage collection | — | (live only; GC is the API server's) | L-05.2-2 (background, foreground, orphan) |
| Migration interruption | `migration_patch_removes_only_the_matching_owner_entry` | `migration_patches_carry_resource_version_and_resume_after_a_failed_patch`, `a_409_migration_patch_is_retried_next_inventory`, `nonterminal_legacy_runs_keep_their_owner_until_terminal`, `a_422_legacy_object_reports_migration_blocked` | L-05.2-1, L-05.2-3 |
| Same-name recreation | — | `a_recreated_schedule_does_not_count_old_uid_runs`, `a_held_deterministic_name_is_recorded_as_slot_name_unavailable` | L-05.2-4 |
| Unrelated-resource preservation | `foreign_owner_references_labels_and_annotations_are_preserved_byte_for_byte` | `migration_never_patches_backups_of_other_schedules_or_manual_runs`, `the_reconciler_patches_only_status_and_never_deletes` (existing `tests/schedule_controller.rs:2070`, extended to allow only the migration metadata patch) | L-05.2-5 |
| Scale | — | `steady_state_reconcile_makes_no_list_request`, `inventory_is_paginated_and_label_selected_after_migration` | L-05.2-6 (500-run history timing) |

### PLAT-09.2 (required: topic creation/deletion between runs, excluded/internal topic, empty resolution, ACL limitation, discovery/execution race, immutable snapshot)

| Required test | Unit (W5) | Double (W5) | Live |
|---|---|---|---|
| Topic creation/deletion between runs | `resolution_is_a_pure_function_of_discovery_and_policy` | `two_backups_freeze_two_discovery_results` | L-09-2 |
| Excluded/internal topic | `internal_flag_and_double_underscore_are_excluded`, `exact_and_prefix_exclusions_are_literal` | `excluded_and_internal_counts_reach_inputs_and_status` | L-09-1 |
| Empty resolution | `empty_resolution_is_selection_empty` | `selection_empty_creates_no_runner_job_and_is_not_retried` | L-09-4 |
| ACL limitation | `authorization_failed_entries_are_limited_not_resolved` | `refuse_policy_fails_discovery_incomplete_without_runner_job`, `visible_only_policy_records_the_coverage_label` | L-09-5 |
| Discovery/execution race | `summary_count_and_digest_mismatch_is_unreadable` | `source_change_between_discovery_and_freeze_is_refused`, `discovery_failure_is_retryable_and_the_retry_rediscovers` | L-09-3 |
| Immutable snapshot | `inputs_v2_selection_block_is_canonical_and_sorted` | `a_frozen_dynamic_backup_never_reruns_discovery`, `no_glob_or_empty_list_reaches_backup_yaml` | L-09-2 (digest unchanged) |
| Preserve named allowlists | — | existing Backup/schedule tests pass unchanged; `named_mode_records_named_topics_coverage` | L-09-6 |

### PLAT-06.2 (required: double click, lost HTTP response, refresh, paused schedule, failed preflight, successful scheduled-policy copy)

| Required test | Unit / API (W6) | Double / UI spec (W3b, W7) | Live |
|---|---|---|---|
| Double click | `same_key_same_body_returns_200_and_uid` | `submitting_state_disables_the_button` (`ui/tests/pages.spec.js`) | L-06-1 (browser) |
| Lost HTTP response | `replay_after_schedule_edit_returns_original_run`, `same_key_different_body_is_409` | `outcome_unknown_resubmits_same_intent`; legacy: `already_exists_with_matching_request_hash_is_success` | L-06-2 (proxy drops 201) |
| Refresh | — | `recent_manual_runs_are_listed_after_reload` | L-06-3 |
| Paused schedule | `run_from_suspended_schedule_is_allowed_and_reported` | `manual_run_does_not_unsuspend_or_admit_slots` (schedule double) | L-06-4 |
| Failed preflight | `api_never_gates_on_readiness_but_records_acknowledgement` | `not_ready_requires_run_anyway`, `unknown_readiness_is_labelled` | L-06-5 (if PLAT-03.1 present; otherwise recorded as unrun dependency) |
| Successful scheduled-policy copy | `copied_policy_digest_equals_schedule_generation_digest`, `expected_generation_mismatch_is_policy_changed` | `manual_run_identity_is_backup_uid_and_not_slot_based`, `manual_runs_neither_block_nor_are_blocked_by_forbid` | L-06-6 (kubectl, API, UI produce the same CR shape) |

---

## 13. Live docker-desktop acceptance (W8)

### 13.1 Environment rules (from WORKER-RULES)

- Every command uses `kubectl --context docker-desktop` / `helm --kube-context docker-desktop`. Namespaces `lw-d1-<scenario>-<utc>` labelled `logweir.dev/test-owner=d1-live`; deletion only after verifying that label and the recorded UID.
- The shared `logweir-scram-local` release is not modified. Its Kafka/MinIO are not written to: W8 deploys its own single-node KRaft Kafka (`apache/kafka:3.7.1`) and MinIO in the test namespace (rendered from the chart's `demo-kafka`/`minio` templates with `helm template --kube-context docker-desktop … --show-only`).
- The test controller is a source-matched image fenced to the test namespace with the PLAT-04.1 pattern: a namespace-rewriting API proxy plus a `ValidatingAdmissionPolicy` that blocks the shared controller's ServiceAccount from mutating objects labelled `logweir.dev/test-owner=d1-live` (negative control required). Applying CRDs takes the cluster lock (`/tmp/logweir-roadmap-run/claude/k8s-lock.sh acquire d1-live`); the original CRDs are saved with `kubectl --context docker-desktop get crd <name> -o yaml` and restored before release.
- NetworkPolicy is not enforced on docker-desktop; no deny-path claims.
- Evidence per scenario: commands, outputs, object names/UIDs, controller image digest, source hashes, cleanup proof.

### 13.2 Scenarios and exact pass criteria

Notation: `get BS` = `kubectl --context docker-desktop -n $NS get backupschedule`; `get B` = same for `backup`.

**L-04-1 Time zone evaluation.** Create `tz` with `timeZone: Asia/Kathmandu` and `schedule: "M H * * *"` where `H:M` is local time now + 3 minutes. Pass: `get BS tz -o jsonpath='{.status.nextRuns[0].at}'` equals the UTC instant (local − 05:45) and `{.status.nextRuns[0].localTime}` ends in `+05:45`; within 90 s after that instant, exactly one Backup exists with `spec.slot` equal to that UTC instant (`yyyymmdd-hhmmss`) and `spec.trigger.kind=Scheduled`. Negative control: the same schedule without `timeZone` does not fire at that instant.

**L-04-2 Long downtime.** `dt-none`: `*/2 * * * *`, `startingDeadlineSeconds: 60`, `catchUpPolicy: None`; `dt-latest`: same with `Latest`; both observed by the controller (non-empty `status.policy.effectiveSince`) before downtime. Scale the test controller to 0 for 7 minutes and restart it between 70 s and 110 s after a slot instant, so that no slot is inside its 60 s deadline at restart (the harness records both instants and fails the scenario otherwise). Pass within 60 s of restart: `dt-none` has **no** Backup whose slot is earlier than the restart and `.status.missedSlots.count ≥ 3`, and its next slot fires on time as `Scheduled`; `dt-latest` has **exactly one** new Backup with `spec.trigger.kind=CatchUp`, whose slot is the latest slot due before restart, and `.status.lastSlot.disposition=CaughtUp`; neither ever has more than one attempt-0 Backup per slot.

**L-04-3 Invalid cron.** `kubectl --context docker-desktop -n $NS patch backupschedule dt-none --type merge -p '{"spec":{"schedule":"61 * * * *"}}'` succeeds (schema cannot parse cron). Pass: `Ready=False`, reason `UnparseableSchedule`, zero new Backups over two cadence periods; reverting the patch returns `Ready=True` and the next slot fires.

**L-04-4 Retry and exhaustion.** `retry`: `*/3 * * * *`, `retry: {maxRetries: 2, delaySeconds: 60}`, archive on the namespace MinIO. Scale MinIO to 0 before the slot. Precondition asserted by the harness: the first attempt records `exitCode` 1 (if the environment yields another code the scenario is invalid, not passed). Pass: `logweir-backup-retry-<slot>` `Failed` with `exitCode` 1; `…-r1` created ≥ 60 s after the first attempt's `Failed` transition, with `spec.trigger.kind=Retry`, `attempt=1`, `retryOf.name` = attempt 0, and `status.backupId` ending `-r1`; MinIO still down → `…-r2` `Failed`; no `…-r3` ever; `.status.lastSlot.disposition=Exhausted`. Second slot with MinIO restored before `r1`: `r1` `Succeeded`, receipt verifies with `logweir drill verify --payload-type backup-receipt` and `docs/verify_scorecard.py`, and no `r2`. Negative control: a guard refusal (source KafkaCluster with `scramSha512` and no username) produces no `-r1`.

**L-04-5 Duplicate reconciliation.** Two fenced controller replicas for the `retry` schedule. Pass: for every slot and attempt exactly one Backup object exists (list by `logweir.dev/slot` shows attempts 0..k with no duplicates); controller logs show at least one `409` adoption.

**L-04-6 Allow cap and Forbid.** `allow` (`Allow`, `*/1 * * * *`, long runs via a large topic): pass when nonterminal schedule-created runs never exceed 10 and `Ready` reason `ActiveRunLimit` appears; `forbid` never has two nonterminal schedule-created runs. A manual run during an active scheduled run starts immediately (not blocked).

**L-05.1-1 Edit during execution.** While `ed`'s run is `Running`, patch `topics` from `[t1]` to `[t1,t2]` and change `archive.url` prefix. Pass: the running Backup's `spec.topics` stays `[t1]`; the plan ConfigMap `resourceVersion` and `execution-inputs.json` sha256 are unchanged; the next run has `spec.topics [t1,t2]`, the new archive URL, `spec.scheduleRef.generation` = the schedule's new `metadata.generation`, and a different `runPolicySha256`; the first run's receipt lists only `t1`.

**L-05.1-2 Concurrent edit/fire.** The proxy holds the controller's reservation PATCH for slot S; the harness applies an edit (new generation); the proxy releases. Pass: the held PATCH returns `409`; the Backup for S is created exactly once and records the **new** generation and digest. Second ordering (hold the Backup POST after a successful reservation, edit, release): the Backup records the **old** generation and its copied policy equals the old spec saved by the harness.

**L-05.1-3 Conversion.** Before upgrade, create schedule `legacy` and one completed Backup with the main@4956785 controller (W0 fix applied only if required for it to fire). Apply new CRDs and controller. Pass: no `metadata.generation` change on `legacy`; `.status.observedGeneration` equals `metadata.generation`; the next run fires in UTC at the same slot name the old controller would compute; the old Backup is untouched apart from §6.2 migration fields.

**L-05.1-4 Invalid edits on the real API server.** Pass: patching `sourceRef` fails with the R1 message; `allUserTopics` with non-empty `topics` fails with the R2 message; `retry.maxRetries: 1` on a 30-character schedule name fails with the R3 message and succeeds on a 29-character name; `timeZone: "Mars/Olympus"` is accepted by the schema and yields `Ready=False UnknownTimeZone` with no Backups for two periods.

**L-05.1-5 Rollback.** Following the documented procedure (suspend schedules using new fields), swap the fenced controller to the main@4956785 image with the new CRDs installed. Pass: no Backup is created for suspended schedules; existing Backups and ConfigMaps keep their `resourceVersion`; after rolling forward and resuming, schedules fire with their time zone and retries; any Backup created by the old controller carries an ownerReference and is migrated when terminal.

**L-05.2-1 Legacy migration.** With the old controller, create `hist` (`*/2`) and wait for two `Succeeded` runs; pre-add a foreign ownerReference (a ConfigMap `anchor` in `$NS`, `controller: false`) plus label `team=x` and annotation `note=y` to one of them. Upgrade. Pass: `HistoryRetained=True reason=Retained`; neither Backup has an ownerReference of kind `BackupSchedule`; the `anchor` ownerReference, `team=x` and `note=y` are byte-identical; label `logweir.dev/schedule-uid` equals `hist`'s UID; annotation `logweir.dev/history-retained-from-owner` equals it; proxy logs show every migration PATCH body carries `metadata.resourceVersion`.

**L-05.2-2 Deletion with active and completed runs.** Schedules `bg`, `fg`, `orph`, each with one `Succeeded` and one `Running` run. Delete `bg` (default), `fg` (`--cascade=foreground`), `orph` (`--cascade=orphan`). Pass: after 120 s all six Backups, their plan ConfigMaps and Jobs still exist (same UIDs); both running Backups reach `Succeeded` with receipts that verify; no new Backup with these schedule UIDs appears. Unfrozen case (declared harness injection): the proxy answers the Backup controller's plan ConfigMap POST for a fourth schedule's new run with `503` until that schedule is deleted, then forwards normally; pass when that run ends `Failed` with reason `ScheduleNotFound`, with no plan ConfigMap and no Job.

**L-05.2-3 Migration interruption.** Create 20 legacy terminal Backups for `many` with the old controller; upgrade; the proxy forwards 7 migration PATCHes, then the controller is scaled to 0. Scale to 1; the proxy answers the first migration PATCH after restart with `409 Conflict` (declared harness injection). Pass: all 20 end migrated (no BackupSchedule owner entry, UID label present), the set of Backup UIDs is unchanged, no Backup was patched after it was already migrated, the injected `409` object is migrated on a later inventory, and `HistoryRetained=True`.

**L-05.2-4 Same-name recreation.** Delete `hist`, recreate `hist` with the same spec. Pass: the new UID differs; `.status.history.runCount` counts only new runs; `kubectl --context docker-desktop -n $NS get backups -l logweir.dev/schedule-uid=<new-uid>` excludes old runs; an old run still `Running` does not appear in the new `activeRuns` and does not block admission; if recreated inside the old run's slot, `lastSlot.disposition=NameUnavailable` and no Backup is created for that slot.

**L-05.2-5 Unrelated resources.** Before the upgrade, record `resourceVersion` of a manual Backup, a Backup of another schedule and an unrelated ConfigMap. Pass: unchanged after migration and after all deletions.

**L-05.2-6 Read cost.** Seed 500 terminal runs for one schedule. Pass: with the proxy counting requests over 10 minutes of steady state, zero LIST requests on `backups` except inventories at the documented interval, and each reconcile issues at most 15 GETs on `backups`.

**L-09-1 Dynamic resolution.** In-namespace Kafka topics `t1`, `t2`, `skip-me`, `pfx-a`; consume with a group so `__consumer_offsets` exists. `dyn`: `topics: []`, `allUserTopics {exclude {topics: [skip-me], prefixes: [pfx-]}, incompleteDiscovery: BackUpVisibleTopics}`. Pass: the first run passes through `phase=Resolving` with Job `lwd-<backup-uid>`; `execution-inputs.json` has `selection.topics == ["t1","t2"]`, `coverage=VisibleUserTopicsOnly`, `discovery.visibility=unknown`, `internalExcluded.count ≥ 1`, `excludedByRule.count == 2`; `backup.yaml` lists exactly `t1,t2`; the receipt `source.topics == ["t1","t2"]`; `status.selection.resolvedTopicCount == 2`.

**L-09-2 New topic in next run; immutable snapshot.** Create `t3` after run 1 freezes. Pass: run 2's frozen topics are `["t1","t2","t3"]`; run 1's plan ConfigMap sha256 and `resourceVersion` are unchanged; run 1's receipt still lists two topics.

**L-09-3 Discovery/execution race.** The proxy holds the runner Job POST after the plan ConfigMap is created; delete `t2`; release. Pass: the run either `Succeeded` with receipt `records.t2 == 0`, or `Failed` exit 1 followed by a retry (with `retry` configured) whose fresh discovery excludes `t2`; in no case does any status, UI label or receipt claim records for a topic absent from that run's frozen list. Second case: change the source `KafkaCluster` between discovery and freeze (delete and recreate with different bootstrap) → `SourceChangedDuringResolution`, no runner Job.

**L-09-4 Empty resolution.** `empty`: exclude every user topic. Pass: `Failed`, reason `SelectionEmpty`; only the discovery Job exists; no runner Job; no `-r1` even with retries enabled.

**L-09-5 ACL limitation.** Kafka with `StandardAuthorizer`, SCRAM principal `limited` without Describe on `secret-t`. `acl-refuse` (`Refuse`) and `acl-visible` (`BackUpVisibleTopics`). Pass: `acl-refuse` run `Failed` `DiscoveryIncomplete`, no runner Job; `acl-visible` run `Succeeded` with coverage `VisibleUserTopicsOnly`, `secret-t` absent from frozen topics and receipt; the UI label reads "Visible user topics only".

**L-09-6 Named allowlist preserved.** `named`: `topics: [t1]`. Pass: no discovery Job; `selection.mode=SelectedTopics`, `coverage=NamedTopics`; behaviour identical to the pre-upgrade controller for the same spec.

**L-06-1 Double click (browser).** Playwright (`/opt/homebrew/lib/node_modules/playwright`) against the console (or the legacy proxy UI if the console is unavailable, recorded as such): click "Back up now" twice within 50 ms. Pass: exactly one new Backup with `logweir.dev/trigger=manual` for that schedule UID; exactly one `201` and no second create request (console) or a matching `409 AlreadyExists` treated as success (legacy).

**L-06-2 Lost response.** The proxy drops the `201` response body. Pass: the UI enters "Outcome unknown"; "Check status" re-sends the same key/name; the result is `200` (console) or `409` with a matching request hash (legacy); still exactly one Backup, same UID.

**L-06-3 Refresh.** Reload after acceptance. Pass: no request is re-sent automatically; the schedule card shows the recent manual run; a deliberate second click creates a second Backup with a different name and UID.

**L-06-4 Paused schedule.** Suspend `nightly`, then "Back up now". Pass: the manual Backup runs to `Succeeded`; `spec.suspend` stays `true`; no scheduled Backup appears for the next two slots.

**L-06-5 Failed preflight.** Requires PLAT-03.1; otherwise recorded as an unrun dependency. With a missing archive Secret, readiness `notReady` → the UI blocks until "Run anyway" → the Backup ends `Failed` with the pod's actual prerequisite error and the annotation `logweir.dev/readiness-ack` present.

**L-06-6 Policy copy and one CR path.** Create the same logical manual run three ways: `kubectl create -f config/samples/backup-manual.yaml` (fields filled from the schedule), API `POST backups {scheduleRef}`, UI button. Pass: all three specs are identical except `metadata.name` and annotations; each has `scheduleRef.uid/generation/runPolicySha256` equal to the schedule's `status.policy`; each run's `status.backupId` equals its own Backup UID; all three receipts verify.

---

## 14. Risks, open items and non-goals

1. **P0 prerequisite** (§0.3) blocks meaningful live acceptance of anything scheduled; W0 first.
2. **PLAT-06.1 shape drift**: if it lands with different field names, W3a adapts the adapters; invariants that must not change are §3.1 rules 1–7 and §3.3's "names are frozen before the Job, never empty".
3. **PLAT-09.1 absent**: W5 cannot start without an agreed runner contract; §7.3 is offered as that contract, and `attestedComplete` is unreachable until an administrator attestation exists, so `Refuse` policies will refuse.
4. **etcd growth** with retained history (§6.6) is real for high-frequency dynamic schedules; pruning is manual until PLAT-15.1/16.2.
5. **Fall-back double runs** for fixed-time schedules in repeated hours are intentional and previewed.
6. **Retention reports** count failed partial sets and retry sets as sets; PLAT-16.1 should classify by receipts.
7. **tz database updates** ship only with releases.
8. Not decided here: `TopicDiscovery` kind, readiness resource, destinations, catalog format, signed selection/origin receipt block, automatic pruning, `sourceRef` rebinding after a `KafkaCluster` is deleted and recreated under the same name (runs record `clusterUid` in frozen inputs; PLAT-07.2 should surface identity changes).

---

## 15. Amendment P10 (2026-09-24): manual runs are bounded per namespace

**Defect.** §8.3 keeps manual runs outside `concurrencyPolicy` ("not counted and
not blocked"), and nothing else bounded them. On the PoC install one operator's
hundred accepted `POST …/backups` (all `201` in 2.4–12.5 s) became a hundred
simultaneous runner pods: docker-desktop hit its 110-pod limit, the node went
`NotReady`, MinIO answered `503 SlowDown`
(`/tmp/logweir-roadmap-run/claude/poc-install.result.md`, 2026-09-24T16:08:53Z).

**Decision.** §8.3's row stands — a manual run neither occupies a `Forbid` slot
nor is blocked by one, and a scheduled run is never counted against or queued by
manual runs — and manual runs get a pool **of their own**, per namespace, in the
installation policy D2 §4.4 already owns (`runs` block beside `checks`):

| | Manual `Backup` | Manual `Restore` (no `spec.authorization`) |
|---|---|---|
| ceiling | `runs.maxManualBackupsActivePerNamespace`, default 4 | `runs.maxManualRestoresActivePerNamespace`, default 2 |
| gate position | after §3.1's refusals and the destination hold, before discovery, the freeze and the Job | after the admission (approval, target, destinations), before the trust read, plan, bundle and Job |
| queued state | `phase: Queued`, `Admitted=False/ConcurrencyLimited`, `status.queue.limit` | the same, plus the scalar `reason` |
| release | one status write (`queue: null`, `Admitted=True`), then creation | the same |

- **FIFO, and bounded under a burst.** A candidate is admitted when the manual
  runs holding a slot (recorded `status.execution`/`jobRef`, `Running`,
  `Resolving`, or any unknown phase) plus the OLDER manual runs still waiting
  (no phase, `Queued`) are below the ceiling, ordered by `creationTimestamp`
  then name. `Pending` (a destination or approval hold) neither holds a slot
  nor a place in line. The count reads the Backup/Restore watch the controller
  already runs, and admits nothing until it has synced.
- **The frozen-inputs contract (§3.3) and the execution claim are unchanged.**
  A queued run has no plan, no `status.execution`, no Job and so no claim; it
  freezes on the admitting pass. A frozen run is never re-queued.
- **API (D0's `rate_limited`).** `POST …/backups` and `POST …/restores` are
  limited per `(actor, namespace, route)` like discoveries and preflights:
  10 and 5 per minute by default (`rateLimits.*`), `429` with `Retry-After`.
  §8.2's response table gains that row; the rest of §8.2 is unchanged.
- **Console (§8.5).** A queued run renders "Queued (limit N active)" from
  `status.queue.limit`; an Accepted banner no longer implies a running Job.
- **Rollback.** An older controller has no pool and runs a `Queued` object as
  active; it refuses a policy document that carries `runs` (fail closed), so the
  controller is rolled back with the chart.

Implementation: `crates/weirkeeper/src/run_pool.rs`, the two gates in
`controllers/{backup,restore}.rs`, `routes::run_create_rate` in `logweir-api`.
Rows: `crates/weirkeeper/tests/manual_run_pool.rs`,
`crates/weirkeeper/tests/restore_controller.rs` (P10 section),
`crates/logweir-api/tests/manual_run_limits.rs`.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
