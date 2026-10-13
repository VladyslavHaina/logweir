# PROD-04.2 — Cutover: the shared design for v2 theme A

- Rows: **PROD-04.2** (translate positions, reviewed cutover), **PROD-05.2** with **PROD-05.1a** (target topic configuration; Strimzi owners from the controller), **PROD-11.1a** (safe clones), in the [product-expansion tracker](../product-expansion.md). Owner decisions: OD-15 (built together, tested once), OD-16 (no console work in theme A), OD-11 (smallest thing that meets the row), OD-7 (versioning), OD-8 and OD-10 (approval modes), OD-12 (keep consumer positions), OD-14 (Amendment D).
- Date: 2026-10-13. Base: main `e53ce68e`. Branch `claude/v2a-design`. A design record: it ships no product code. Three workers build from it in parallel: **W1** = 04.2, **W2** = 05.2 + 05.1a, **W3** = 11.1a.
- If the code proves a rule here wrong, the worker says so in its report; nobody invents a second design (WORKER-RULES, "v2 theme A").

## 0. Decisions in one page

1. **Positions are mapped by the restore, applied by a `Switchover`.** The restore's complete verification (phase 7) already reads every restored record and its `x-original-offset`; it also maps every captured position of the bound point onto the target and writes a **positions-mapping document** bound by digest into the signed scorecard (§2). Nothing is applied. A new kind, **`Switchover`** (the name ADR 0008 Amendment A reserved, `docs/architecture.md:269`), applies a reviewed subset of that mapping to stopped groups, with its own approval subject (its kind), and writes a signed **switchover record** (§3).
2. **Positions come only from the restored point.** The positions document must be the one the restore's bound receipt names. That makes PROD-01.4's `GenerationMismatch` and `ContinuityUnverified` unreachable by construction (§2.1).
3. **Translation only over a passing complete verification** (`decisions/PROD-08.1-integrity-contract.md:123`, row 08.1-A4). Classes: `exact`, `approximate` (the restore's own selection excluded records the consumer had not read; the count is stated), `unavailable` (with a reason). Unavailable is never applied (§2.2).
4. **The commit is Logweir's own safe consumer API from a non-member** (`crates/logweir-kafka/src/rdkafka_reader.rs:384`, PROD-04.0a), never the engine (Amendment D, OD-14). The broker refuses it while the group has members (measured again here, §11). Each partition is compare-and-set against the plan: the current committed position must be absent (or the plan's stated prior), else the group is refused (§3.5).
5. **Topic configuration is the Restore's, end to end** (§4). An opt-in plan block creates each topic from the bound point's configuration model with the settings that cannot drop, reject or re-stamp restored records, and, after a **passing** verdict, phase 9 applies the deferred settings (compaction, retention, timestamp bounds) with one full-set `AlterConfigs` per topic. An owned topic's deferred settings are emitted as `KafkaTopic` YAML instead.
6. **A clone is a scratch restore whose teardown waits for a declared TTL** (§5). Scratch mode already holds the `AllowedClusters` check, target ≠ source and the marker topic; the cleanup deletes only the names the signed scorecard records as created, and only while each topic's ID is the one recorded at creation.
7. **One identity block for every created topic.** The scorecard records each created topic's name and topic ID (`target.creation`, W3). The switchover (`TargetGenerationChanged`) and the clone cleanup (`CloneTargetReplaced`) both read it.
8. **No console work** (OD-16): API, CLI, CRD and evidence surfaces only; §9 lists the screens PROD-18.1 must build.
9. **One ADR amendment, K** (§3.9), records what theme A narrows: the first offset commit, the first configuration change, a deferred teardown and a read of a vendor resource. Rule 8 needs the owner's yes before it lands (§10, point 1).

## 1. The operator's flow

| # | Step | Object and surface | confirm (OD-8 default) | two-person | strict |
|---|---|---|---|---|---|
| 1 | Back up with consumer groups | `Backup`/`BackupSchedule` `spec.consumerGroups` (PROD-04.1); 05.1a adds the source's `KafkaTopic` owners | none (backups need no approval) | none | none |
| 2 | Preview the restore | `Preflight` `operation: Restore` (exists); W2 adds the configuration diff, W3 the empty-in-window partitions | none (advisory) | none | none |
| 3 | Restore into new topics with `sample.coverage: complete`, optionally `target.topic_configuration` | `Restore` (exists) | the requester confirms; the scope shows the configuration choice | second person | personal key |
| 3′ | …under the original names (PROD-15.1) | `Restore` + `approvalSubject: originalName` | + typed topic names (OD-10) | second person | personal key |
| 3″ | …as a clone: scratch mode, `target.teardown: {ttl_seconds}` | `Restore` | the requester confirms; the scope shows the TTL | second person | personal key |
| 4 | Verify (phase 7, complete); the mapping document is written; phase 9 applies the configuration transition if the verdict is `pass` | runner | covered by step 3's approval | same | same |
| 5 | Review the mapping and the target groups' state | `Preflight` `operation: Switchover` (new, advisory, §3.8) | none | none | none |
| 6 | Switch the stopped groups over | `Switchover` (new) | the requester confirms (see §10, point 2) | second person, whole scope shown | personal key, `drill approve --subject-kind Switchover` |
| 7 | (If needed) retry the untouched groups, or roll back | a new `Switchover` from the plan or from `switchover rollback-plan` | as step 6 | as step 6 | as step 6 |
| 8 | A clone expires | the controller's cleanup Job at `expiresAt`, or at once when the `Restore` is deleted | none: step 3″'s approval covers the TTL | none | none |

A restore never creates a `Switchover`, and nothing commits a position automatically (tracker rule 5).

## 2. Position translation (W1)

### 2.1 Inputs and preconditions

- **The positions document** is the bound point's (`source.point` in the plan; the runner already verifies that receipt, `crates/logweir/src/drill/binding.rs`). The runner fetches the document the receipt's `consumer_positions.document` names and checks its sha256 and length (arms CP-1 to CP-14 already define it). No bound point, or no captured group: no mapping.
- **Only over a passing complete run:** `outcome: pass`, `integrity.verification.coverage: complete`, `complete.covered: true`. Otherwise no mapping document is written, and a `Switchover` is refused `VerificationNotComplete`, naming the field (08.1-A4).
- **A transactional archive** (`restore.allow_transaction_markers: true`) maps every position `unavailable: TransactionalArchive`, so a position can never land on a restored marker or aborted record (PROD-01.1 row 04-1).

### 2.2 The rule and the classes

Per captured position `P` of group `g` on source partition `(t, p)`, restored as `(M(t), p)`:

1. `found` := the first target record whose LAST `x-original-offset` (`phase7_verify/complete.rs:94`) is `≥ P`, read in the same in-order scan as the replay comparison (`complete.rs:363`); else `end` := the target's high watermark.
2. `skipped` := archived records with a source offset in `[P, lineage(found))` (or `[P, ∞)` for `end`) that the restore's selection excluded (window start, window end, non-monotonic timestamps). An offset hole (compaction, `offset_holes`) is not a record and does not count.
3. The class:

| Capture relation of `P` (`consumer_positions.rs:538`) | Condition | Class | Target offset |
|---|---|---|---|
| `withinArchive`, `atArchiveEnd` | `[P, found)` overlaps no recorded gap or pruned range, and `skipped = 0` | `exact` | `offset(found)` or `end` |
| same | no recorded gap or pruned range, and `skipped > 0` | `approximate` (`selectionExcluded`, with `skipped`) | `offset(found)` or `end` |
| same | `[P, found)` overlaps a recorded gap or pruned range | `unavailable: ArchiveGap` | — |
| `noArchivedData` | the capture marks show `log_start = high_watermark = P` (nothing to read) and the target partition is empty | `exact` | `0` |
| `noArchivedData` otherwise; `beforeLogStart`; `beforeArchive`; `beyondArchive`; `PositionBeyondEnd`; a group not `captured` | — | `unavailable` with that name (`NoArchivedData`, `BeforeLogStart`, `BeforeArchive`, `BeyondArchive`, `PositionBeyondEnd`, `NotCaptured: <capture reason>`) | — |
| any | the partition was not restored (a partition subset, PROD-11.1b) | `unavailable: PartitionNotRestored` | — |

A partition the group never committed stays without a position on the target (counted `no_committed_position`), so the consumer's `auto.offset.reset` behaves as it did on the source.

**Why this never skips silently.** Positions are read before the engine reads records (`consumer_positions.rs`, "not atomic"), so a captured `P` is at or behind what the group had read when the data was captured: the error is re-delivery, never loss. A group `active` at capture carries `active_at_capture: true`, and the review says so.

### 2.3 Edge cases (each is a row in §8)

| Case | Outcome |
|---|---|
| Compaction hole at `P` | `exact` at the next surviving record (holes are not records) |
| `P` at the partition end (consumer caught up) | `exact` at the target's high watermark |
| Empty source partition, empty target | `exact` at 0 |
| Duplicate provenance | a passing verification has none; the mapping function maps to the FIRST copy (PROD-01.1 04-4), proven in unit rows only |
| Recreated SOURCE topic | positions and data come from one backup run; a topic that changed during capture already fails its groups (`GenerationChangedDuringCapture`, arm CP-9) |
| Recreated TARGET topic | the switchover compares each target topic ID with `target.creation` and refuses `TargetGenerationChanged` before any commit (TI-04.2-2) |
| Original-name target (PROD-15.1) | maps through the lineage like any other (TI-04.2-3); after the topic's deletion the group holds no position on it (measured, §11), so `expected: none` holds |
| Filtered restore (PROD-11.1 window, subset) | `approximate` with `skipped`, or `PartitionNotRestored`; never `exact` over excluded records |
| Configuration transition already applied (§4.4) | offsets do not move under compaction or retention; a mapped offset below the target's current log start is refused `BelowTargetLogStart` at apply time |
| `LogAppendTime` source (04-5) | not applicable: there is no time-based translation |

### 2.4 Where the mapping lives

- **Document:** `logweir/drills/<run_id>.positions-mapping.json`, format **1.0.0**, unsigned, beside the scorecard; at most **4 MiB** (over it: no document, the block says `omitted: tooLarge` with the counts, and a `Switchover` is refused `NoPositionsMapping`). Shape: `{format_version, run_id, point_id, positions_sha256, rule: "firstTargetRecordAtOrAboveLineage", groups: {<g>: {group_type, active_at_capture, entries: [{topic, partition, position, target_topic, class, target_offset?, skipped?, reason?}]}}}`.
- **Signed binding:** scorecard block `integrity.verification.positions_mapping {document {key, sha256, bytes} | omitted, counts {exact, approximate, unavailable}, groups}`, a nested optional block (Global Constraint 12), **MINOR** under OD-7 (a) on both the 1.x line and the 2.x subset line. Both readers check the block's arms; `--positions-mapping <file>` checks the document against the digest, as `--consumer-positions` does for receipts.

## 3. Applying positions: the `Switchover` kind (W1)

### 3.1 Why a kind, and not a mode of `Restore`

It has its own approval subject, its own Job, its own lifecycle (it starts long after the restore, when the applications are stopped) and its own failure states; a `Restore` spec is immutable and its Job is finished. The kind is already in Amendment A's list, and the code anticipates it (`crates/weirkeeper/src/crds/approval.rs:16`, `crds/restore.rs:235`, `crates/logweir/src/schema.rs:48`). The controller never dials Kafka (`crates/weirkeeper/Cargo.toml` has no `logweir-kafka`), so the commit runs in a runner Job.

### 3.2 The plan (what the approver signs)

```yaml
format_version: "1.0.0"
restore: {name: incident-4471, uid: 6f1c…, plan_hash: "sha256:…", scorecard_sha256: "sha256:…", mapping_sha256: "sha256:…"}
groups:
  - source: orders-app            # the captured group
    target: orders-app            # the target group id (default: the same)
    topics:
      restore-20261013T080000Z-orders:
        offsets: {0: 1200, 1: 50, 2: 0}   # partition -> target offset to commit
        approximate: [1]                   # partitions whose class is approximate; the rest are exact
        expected: none                     # or {0: 1180, …}: the committed positions the apply requires (rollback plans)
```

Bounds: at most 100 groups (`consumer_positions.rs:248`), 8192 positions (`approval_scope.rs:60`) and 256 KiB (the API's `MAX_PLAN_BYTES`). Every entry must equal the mapping document's entry and never be `unavailable`. The target cluster, credentials and evidence location are the restore's, through `plan_hash`. Plans are built offline from the mapping: `logweir switchover plan` from a scorecard and mapping, or from a Switchover preflight's details; `logweir switchover rollback-plan` from a record. Both are pure functions in `logweir-core`.

### 3.3 Spec and status

- **spec:** `planBytes`, `approvalRef`, `restoreRef` (a declaration the controller holds to the plan), `deadlineSeconds`. Immutable (`SPEC_IMMUTABLE_RULE`).
- **status:** `phase` (`Pending`, `Running`, `Succeeded`, `PartiallyApplied`, `Failed`, `Refused`), `exitCode`, `exitReason`, `reason`, `jobRef`, `groups[]` (at most 100: `{source, target, outcome, reason, created, partitions}`), `counts {applied, alreadyApplied, refused, failed, untouched}`, `evidence {recordKey, verification}`, `conditions`.

### 3.4 Admission and the Job, in order

1. **Controller:** the plan parses and agrees with `restoreRef`. The `Restore` exists with that UID, is `Succeeded`, and its evidence verdict is `Valid`. It is not scratch (`RestoreIsScratch`: a clone is deleted at its TTL). No other non-terminal `Switchover` names it (`SwitchoverInProgress`). The approval is verified for subject kind `Switchover` (check 8 compares the kind, `crds/approval.rs:16`). Then the controller creates one Job: `logweir switchover run`, with the restore's credential projection and evidence read/write.
2. **Runner, before any commit:** it re-verifies the approval, the scorecard's signature and plan hash, and the mapping document's digest. Every plan entry must equal the mapping's (`PlanDisagreesWithMapping`). The target cluster ID must equal the scorecard's `target.cluster_id` (`TargetClusterChanged`). Every target topic ID must equal `target.creation`'s (`TargetGenerationChanged`; a null or unreadable ID refuses `TargetIdentityUnknown`). Every planned offset must lie within `[log start, high watermark]` now (`BelowTargetLogStart`, `AboveTargetEnd`; the group is refused).
3. **Per group, in plan order, independently:** classify the target group with PROD-04.0's rules (FFI listing and describe). Then read its committed positions on the planned partitions (RequireStable), compare (§3.5), commit through `commit_positions` (leader epoch −1, metadata `logweir:positions`), read back, and print one `switchover-group=` line.
4. **Record:** a signed switchover record. The last lines carry the counts and `failure-reason=`, lifted from the last two lines only, as PROD-15.1 does.

### 3.5 Per-group outcome

| Target state | Outcome |
|---|---|
| live members, or a state librdkafka maps to Unknown | `refused: GroupActive`; no commit attempt (AP-04.2-1, -5) |
| share, streams or another protocol | `refused: TargetGroupTypeUnsupported` (AP-04.2-2: an Empty streams group would accept the commit) |
| a broker that cannot type groups (ListGroups < v5) and the group exists | `refused: TargetGroupTypeUnproven` |
| hidden from the principal (a targeted 30) | `refused: TargetNotVisible`; never `created` (AP-04.2-7) |
| absent (`GroupNotFound`) | committed; `created: true` (AP-04.2-6) |
| every planned partition's current = `expected` (absent when `expected: none`) | `applied` |
| every planned partition's current = the planned offset | `alreadyApplied`; no commit |
| anything else | `refused: TargetPositionsChanged`; no commit for the group |
| the commit refused by the broker | `refused: GroupActive` (25: a member joined after the check; nothing applied, read back) or `NotAuthorized` (30) |
| any other commit error | `failed: CommitFailed` with `applied` from the readback (trap T16) |

The broker's refusal is the atomic liveness guard: a non-member commit is refused for the whole request while the group has any member, even one on another topic (§11, P3).

### 3.6 Partial application, re-run, rollback

- **Visible:** every group has exactly one outcome in the status, the Job's lines and the record; `PartiallyApplied` names them.
- **Recoverable:** the same plan in a NEW `Switchover` (and a new approval, which binds the object's UID) is safe. Applied groups read as `alreadyApplied`, refused ones are tried again, and a group that moved since is `TargetPositionsChanged`. This replaces AP-04.2-3's "under the same approval" (§12).
- **Rollback:** `switchover rollback-plan` builds `offsets := prior` and `expected := applied` from a record. Its limits: a group whose consumers ran since is refused `TargetPositionsChanged`, with the records consumed per partition; a prior that was absent cannot be restored through the safe API, so it is listed and left (`kafka-consumer-groups.sh --delete-offsets`).

### 3.7 The audit record

`logweir/switchovers/<run_id>.record.json` + `.sig`, format **1.0.0**, payload type `application/vnd.logweir.switchover-record+json;version=1.0.0`, schema `schemas/logweir-switchover-record-1.0.0.json` (`logweir schema switchover-record`). Fields: the switchover (name, UID, plan hash), the restore (name, UID, scorecard and mapping digests), the approval mode and principals, the target cluster ID, each topic ID checked, and per group its classification, outcome, reason, `created`, and per partition `{prior, planned, applied: true|false|unknown, readback}`, with start and end times. Both readers verify it signature-only, like the teardown attestation (`docs/verify_scorecard.py:5805`). The API's audit log also records create and approve.

### 3.8 The review: `Preflight` operation `Switchover`

Advisory, one check Job (kind `switchoverReadiness`, D-SEAMS S1: one check runner). Request: `{restoreRef, groups?}`. It verifies the scorecard and mapping as §3.4 step 2 does, then reads each requested target group's type, state, members and current committed positions on the mapped partitions. Results (≤ 8192 positions) go in the existing details chunks (`GET …/preflights/{id}/details`). It authorises nothing; the Switchover Job repeats every check.

### 3.9 Amendment K (proposed text; lands with W1, after the owner's yes, rule 8)

> **Amendment K — cutover (PROD-04.2, 05.2, 05.1a, 11.1a; OD-15).** (1) `Switchover`, Amendment A's reserved kind, is built: a namespaced kind that applies reviewed consumer positions, taken from a verified `Restore`'s signed mapping, to stopped consumer groups on that restore's target. It is the only path that commits a consumer-group offset. The commit is Logweir's own client from a non-member (ADR 0004; PROD-04.0), never the engine (Amendment D). It writes and deletes no topic, and no `Restore` creates one. (2) A `Restore` may create its topics with the source's configuration and, after a passing verdict, apply the settings that would have dropped or re-stamped restored records. It alters only topics it created, and only while their overrides are the ones it set. (3) A scratch `Restore` may keep its topics for a declared time: the same deletion phase 9 performs, of the same names, deferred, and only while each topic's ID is the one recorded at creation. (4) `weirkeeper` may read (get, list) Strimzi `KafkaTopic` resources where a `KafkaCluster` names its Strimzi cluster and the chart grants it. It never creates or changes one. No other kind is added.

## 4. Target topic configuration (W2)

### 4.1 The plan block (opt-in)

`target.topic_configuration: {transition: afterVerification | none, owners?: [{topic, kind, reference}]}`. Absent: today's behaviour, byte for byte (default replication factor, `TARGET_TOPIC_CONFIGS` only, `crates/logweir-kafka/src/reader.rs:859`). Present: the bound point must carry the configuration model (receipt ≥ 1.3.0; else `TopicConfigurationNotRecorded`). `owners` is the approver's statement where the point's `owner_detection` is empty (PROD-05.1 row 5).

### 4.2 Creation (phase 0), per topic

| Stage | Keys | Value |
|---|---|---|
| **during restore** (Logweir's) | `message.timestamp.type=CreateTime`, `retention.ms=-1`, `retention.bytes=-1`, `cleanup.policy=delete`, `message.timestamp.before.max.ms` and `…after.max.ms` = Long.MAX | so nothing restored is deleted, compacted, rejected or re-stamped before the verdict |
| **at creation** (the source's) | every other `portable` override; `removedInKafka4` only to a 3.9 target; `requiresTieredStorage` only where `validate_only` accepts it; `min.insync.replicas` capped at the replication factor (stated) | applied with the create |
| **after verification** (the source's) | `cleanup.policy`, `retention.ms`, `retention.bytes`, `local.retention.*`, `message.timestamp.type`, `message.timestamp.before/after/difference.max.ms` | phase 9, §4.4 |
| **never** | `inherited`, `clusterBound`, `providerOnly`, `secret` | listed in the diff with the source's value (05.1 row 2) |

Partitions: the model's (= the manifest's). Replication factor: `min(model, target brokers)`, with the reason recorded; the model's absence falls back to the plan's `default_replication_factor`, saying so. Phase 0 runs `CreateTopics validate_only` for every topic first and refuses before creating anything: `TopicConfigRefusedByTarget` names the topic and key; a class W2's re-derived table disagrees with refuses `TopicClassDisagrees` (05.1 row 8). Evidence: scorecard block `target.topic_configuration {transition, topics: {<target>: {source_topic, partitions, replication_factor {source, target, reason}, at_creation, deferred, not_applied, route}}, desired_state?}`, MINOR on both lines.

### 4.3 Preview

The restore preflight (`check/kinds/restore.rs`) runs the same pure function as phase 0 (`logweir-core`), over the verified receipt and the target's line and broker count. It returns the per-topic diff: the replication change and its reason, the during/at/after/never sets, the inherited differences (for example a 7-day `retention.ms` inherited at the source against the target's default, which the transition would apply), owner routes and refusals. It is advisory; phase 0 recomputes and fails closed.

### 4.4 The transition (phase 9, after the signed verdict)

- **When:** only with `transition: afterVerification`, only when `outcome: pass`, and only for topics the run keeps (newTopic, or a TTL clone). Otherwise it reports `notRun` with the reason.
- **What:** per `adminApi`-route topic, the run first reads its dynamic overrides. If they are not exactly what phase 0 set, it skips the topic (`ConfigChangedSinceCreation`). Then one legacy `AlterConfigs` (rdkafka 0.36.2's safe `alter_configs`) carries the topic's FULL desired override set: at-creation ∪ after-verification. That replaces the whole set, and Logweir's during-restore keys go with it (measured on 3.9.2 and 4.3.1, §11, P1; 4.3.1 still serves AlterConfigs, `KafkaApis.scala:2158` in the PROD-04.0 artifact, sha256 `f93449b5…`). It touches no other topic.
- **Owned topics** (`desiredStateExport`) are never altered through the admin API. The run emits `logweir/drills/<run_id>.desired-state.yaml`: one `KafkaTopic` per owned topic (`spec.topicName` = the target name, partitions, replicas, the full config), or a neutral JSON document for an `external` owner. The scorecard block binds it by digest.
- **Evidence:** an optional `topic_configuration {applied, failed: [{topic, error, desired}], skipped, exported}` block in the phase-9 attestation (signed; readers signature-only), and `Restore.status.topicConfiguration`. A partial transition names every unapplied topic with its full desired set. Logweir never retries on its own; the remaining step is that set, applied by the operator.

### 4.5 Owners and PROD-05.1a

- **Route:** `apply_route` of PROD-05.1 (`desiredStateExport`, `adminApi`, `unknown`). For `unknown` with no `owners` statement the restore refuses before creation (`TopicOwnerNotChecked`). The TARGET topic is still created by the restore, exclusively, in every route: phase 0 refuses an existing target, and PROD-15.1's owner path already creates the topic itself (`PROD-15.1-original-name.md:25`). Only the configuration of an owned topic is exported instead of applied (§12).
- **Detection (05.1a):** `KafkaCluster.spec.strimzi {namespace, cluster}` (optional). When it is set, the controller lists `kafkatopics.kafka.strimzi.io` in that namespace with the label `strimzi.io/cluster=<cluster>`, keeps those naming the plan's topics (a backup's source topics; a restore's target names, for PROD-15.1), and passes them as an additive `kafka_topic_resources` block of `execution-inputs.json` (D-SEAMS S4). The runner reads them as it reads `--kafka-topic-resources` today (`crates/logweir/src/cli.rs:615`, `:732`). The receipt then records `owner_detection: [kafkaTopicResources]`.
- **RBAC:** chart value `strimzi.readKafkaTopics` (default `false`). When true, the chart grants the controller `get, list` on `kafkatopics` in `kafka.strimzi.io` through a Role and RoleBinding in each namespace listed in `strimzi.namespaces`, and nothing else; the controller refuses a `spec.strimzi.namespace` outside that list before any API call (`OwnerDetectionFailed`). (Amended at integration: the first design said ClusterRole; an automated security review showed that let any KafkaCluster author read KafkaTopics in every namespace.) When `spec.strimzi` is set and the list is refused or the resource is not served, the run fails closed (`OwnerDetectionFailed`), never "looked, none found". `crd_shape.rs:333` (`no_vendor_crd_group_is_named_anywhere`) gains a one-file exemption for the read.

## 5. Safe clones (W3)

- **Shape:** `target.mode: scratch` with `target.teardown: {ttl_seconds: N}`, 300 ≤ N ≤ 2,592,000 (`CloneTtlOutOfRange`). The map form is refused outside scratch mode (`CloneTtlNeedsScratch`). It is also refused by every runner before it, because `teardown` there is a string (`crates/logweir-core/src/spec.rs:603`), so an older runner restores nothing instead of keeping a clone forever.
- **`AllowedClusters`:** scratch mode's own checks, unchanged: the target cluster ID is in the `TrustPolicy` `allowedTargetClusterIds`, it is not the source, and the marker topic exists (`spec.rs:641-644`).
- **Phase 9:** a TTL clone whose run passed keeps its topics. One that did not pass is torn down at once, as a scratch drill is (`phase9_teardown.rs:86`).
- **Identity at creation (every run that creates topics):** right after the create, `ClusterReader::topic_ids` (PROD-01.4a). The result is the scorecard block `target.creation {topics: [{name, partitions, topic_id | null, topic_id_reason?}], lineage_headers: "kept", teardown_after_seconds?}`, MINOR on both lines. This also records the "created target's identity" TI-15.1-1 asked for.
- **Cleanup:** the controller sets `status.clone.expiresAt` (the scorecard's end time + N). At that instant, or when the `Restore` is deleted first (finalizer `logweir.dev/clone-cleanup`, added at admission to TTL clones only), it runs one Job, `logweir restore cleanup`. The Job verifies the scorecard and checks that the target cluster ID is the scorecard's (`CloneTargetClusterChanged`, so a repointed `KafkaCluster` deletes nothing). It then deletes exactly `target.creation.topics` whose current ID is the recorded one: a changed ID is skipped as `CloneTargetReplaced` (TI-11.1-4), a null one as `CloneTargetIdentityUnknown`, a missing topic counts as `alreadyAbsent`. It uses the prefix-scoped `TopicDeleter` (`reader.rs:399`), never an original name.
- **Attempts:** at most 3, 5 minutes apart. After that, `status.clone.phase: RemovalFailed` names the topics, and the finalizer stays until an operator removes it (documented).
- **Header handling:** restored records always keep `x-original-offset` and `x-original-timestamp` (`render_restore.rs:321`); stripping makes the restore unverifiable. The scorecard states `lineage_headers: kept`, and the approval scope says so (§10, point 4).
- **After cleanup:** the scorecard, the phase-9 attestation and the cleanup attestation (the existing `TeardownAttestation`, `teardown_policy: "ttl"`, key `logweir/drills/<run_id>.cleanup.json`) stay under `logweir/`. `status.teardown` (exists) lists what was deleted. The `Restore` remains (unless it was deleted).
- **Preview row** (PROD-11.1 §9): the restore preflight names each selected partition with no record in the chosen window.

## 6. Interfaces and versions, decided once

Version steps are named; integration assigns the numbers (OD-7: MINOR = a new optional block whose arms read only it).

| Surface | Item | Change | Owner |
|---|---|---|---|
| CRD | `Switchover` (new kind, `logweir.dev/v1alpha1`) | §3.3 | W1 |
| CRD | `Approval.spec.subjectRef.kind` | + `Switchover` | W1 |
| CRD | `Preflight.spec.request.operation` | + `Switchover`, block `switchover {restoreRef, groups?}`; P3 rule extended | W1 |
| CRD | `KafkaCluster.spec.strimzi {namespace, cluster}` | optional | W2 |
| CRD | `Restore.status.topicConfiguration {transition, applied, failed, exported, desiredStateKey}` | optional | W2 |
| CRD | `Restore.status.clone {ttlSeconds, expiresAt, phase, attempts, jobRef}`; finalizer `logweir.dev/clone-cleanup` | optional | W3 |
| Chart | ClusterRole `switchovers`, `switchovers/status`; human roles | additive | W1 |
| Chart | `strimzi.readKafkaTopics` → `kafkatopics` get, list | default off | W2 |
| Chart | ClusterRole `patch` on `restores` (finalizer) | additive | W3 |
| API | `GET, POST /api/v1/namespaces/{ns}/switchovers`; `GET …/switchovers/{name}`; `POST …/{name}/approval`; `POST …/{name}/approval-request`; `GET, POST …/{name}/console-approval` | new, mirroring the restore routes | W1 |
| API | `POST …/preflights` with `operation: switchover` | additive | W1 |
| API | Restore view `topicConfiguration`, `clone` | additive | W2, W3 |
| API | approval scope: `switchover_scope` (whole plan, ≤ 100 groups, ≤ 8192 positions, complete or nothing); plan scope + `topicConfiguration`, + `teardownAfterSeconds`, `lineageHeaders` | additive | W1; W2; W3 |
| CLI | `switchover plan`, `switchover rollback-plan`, `switchover run` | new | W1 |
| CLI | `drill approve --subject-kind Switchover`; `schema switchover-record`, `schema positions-mapping`; `drill verify --payload-type switchover-record`; `drill verify --positions-mapping <file>` | additive | W1 |
| CLI | `restore cleanup` | new (the cleanup Job's entry point) | W3 |
| Plan | restore `target.topic_configuration` | additive; an older runner ignores it (no configuration applied, none claimed) | W2 |
| Plan | restore `target.teardown: {ttl_seconds}` | map form; refused by older runners | W3 |
| Plan | switchover plan grammar 1.0.0 | new | W1 |
| Inputs | `execution-inputs.json` block `kafka_topic_resources` | additive (S4) | W2 |
| Check | kind `switchoverReadiness` | new kind in the closed set | W1 |
| Check | restore preflight result: `topicConfiguration` diff; `emptyInWindow` | additive | W2; W3 |
| Evidence | scorecard `integrity.verification.positions_mapping` | MINOR (1.x and 2.x) | W1 |
| Evidence | scorecard `target.topic_configuration` | MINOR (1.x and 2.x) | W2 |
| Evidence | scorecard `target.creation` | MINOR (1.x and 2.x) | W3 |
| Evidence | positions-mapping document 1.0.0 + schema | new | W1 |
| Evidence | switchover record 1.0.0 + payload type + schema | new; signature-only readers | W1 |
| Evidence | desired-state document (KafkaTopic YAML / JSON) | new, bound by digest | W2 |
| Evidence | phase-9 attestation `topic_configuration`; `teardown_policy: "ttl"`; cleanup attestation key | optional field; new value; new key | W2; W3 |
| Approval | subject kind `Switchover` in the v1 document's `subject_kind` and the v2 document's `subject.kind` | new value; no format bump (every older reader refuses it, `approval_policy.rs:1857`) | W1 |
| Readers | `verify_scorecard.py` and `logweir drill verify` | one MINOR per block; payload type `switchover-record` | each |

**Refusal and outcome codes.** These are the names in §2.2, §3.4, §3.5, §4 and §5. W1: `RestoreNotSucceeded`, `RestoreIsScratch`, `SwitchoverInProgress`, `NoPositionsMapping`, `VerificationNotComplete`, `MappingDigestMismatch`, `PlanDisagreesWithMapping`, `TargetClusterChanged`, `TargetGenerationChanged`, `TargetIdentityUnknown`, `BelowTargetLogStart`, `AboveTargetEnd`, `GroupActive`, `TargetGroupTypeUnsupported`, `TargetGroupTypeUnproven`, `TargetNotVisible`, `TargetPositionsChanged`, `NotAuthorized`, `CommitFailed`. W2: `TopicConfigurationNotRecorded`, `TopicConfigRefusedByTarget`, `TopicClassDisagrees`, `TopicOwnerNotChecked`, `OwnerDetectionFailed`, `ConfigChangedSinceCreation`. W3: `CloneTtlOutOfRange`, `CloneTtlNeedsScratch`, `CloneTargetClusterChanged`, `CloneTargetReplaced`, `CloneTargetIdentityUnknown`.

**Compatibility.**
- Old objects and documents read as before.
- An older controller ignores `Switchover` objects, so nothing is applied. A Helm rollback leaves the CRD and its objects inert.
- An older verifier ignores the three new blocks (OD-7 (a)) and refuses the new payload type.
- An older runner refuses a TTL clone plan and ignores `topic_configuration`, applying the configuration it always did. The scorecard then has no block, so nothing is claimed.
- `docs/kubernetes.md`, `docs/stability.md` and the chart README carry each worker's migration notes.

## 7. Work split

| Worker | Owns (new files) |
|---|---|
| **W1** (04.2, Tier A) | `crates/weirkeeper/src/crds/switchover.rs`, `controllers/switchover.rs`; `crates/logweir-core/src/{positions_mapping,switchover}.rs`; `crates/logweir/src/switchover/`; `crates/logweir/src/drill/phase7_verify/positions.rs`; `crates/logweir/src/check/kinds/switchover.rs`; `crates/logweir-api/src/routes/switchovers.rs`; `schemas/logweir-{positions-mapping,switchover-record}-1.0.0.json`; `e2e/tests/switchover.rs`; Amendment K |
| **W2** (05.2 + 05.1a, Tier A) | `crates/logweir-core/src/topic_transition.rs` (stages, desired set, diff); `crates/logweir/src/drill/phase9_transition.rs`; `crates/weirkeeper/src/strimzi_topics.rs`; `e2e/tests/topic_configuration_apply.rs`; the chart value |
| **W3** (11.1a, Tier A: deletion) | `crates/logweir-core/src/target_creation.rs` (block, TTL shape); `crates/logweir/src/restore_cleanup.rs`; `crates/weirkeeper/src/controllers/restore_clone.rs`; `e2e/tests/clone_ttl.rs` |

**Shared files.** The rule: each worker adds its own struct or function in its own new file, and touches a shared file only at the named insertion point.

| Shared file | W1 | W2 | W3 |
|---|---|---|---|
| `weirkeeper/src/crds/{mod,approval,preflight}.rs`, `controllers/approval.rs`, `controllers/mod.rs`, `main.rs` | alone | — | — |
| `crds/restore.rs` | doc wording only (`:235`, `:1015`) | `status.topic_configuration` + struct | `status.clone` + struct |
| `crds/kafka_cluster.rs`, `controllers/{backup,backup_schedule}.rs` | — | alone | — |
| `controllers/restore.rs` | — | one call: lift `topic-configuration=` | one call into `restore_clone.rs`; finalizer |
| `logweir-core/src/spec.rs` | — | one field `TargetSpec.topic_configuration` | `TargetSpec.teardown`'s type |
| `logweir-core/src/scorecard.rs` | one field under `integrity.verification`, one `FORMAT_VERSION_WITH_*` pair, one arms call | one field under `target`, same | one field under `target`, same |
| `logweir-core/src/{approval_policy,approval_scope}.rs` | kind `Switchover`; `switchover_scope` | one plan-scope field | one plan-scope field |
| `logweir/src/drill/mod.rs`, `binding.rs` | positions document fetch; phase-7 hook; mapping write | one call to `phase9_transition` | identity read after creation; TTL keep/delete |
| `drill/phase0_admit.rs` | — | `create_target_topics`'s configs and RF (`:962`), `validate_only`, owner route | one new function, called once after creation |
| `drill/phase9_teardown.rs` | — | one optional attestation field | the TTL policy at `:86` |
| `check/kinds/restore.rs`, `check_contract.rs` | new kind and types | diff frame | `emptyInWindow` |
| `logweir/src/{cli,main,schema,verify}.rs`, `docs/verify_scorecard.py`, parity tests, corpus | `switchover` subcommands, schema arms, arms PM-* | arms TC-* | `restore cleanup`, arms TCR-* |
| `logweir-api/src/{app.rs,routes/restores.rs}` | routes | `topicConfiguration` view | `clone` view |
| `charts/logweir/templates/clusterrole.yaml` | its rule block | its conditional block | its rule block |
| `crates/weirkeeper/tests/crd_shape.rs` | kind list, subject enum, `schema` test | vendor-group exemption | — |
| `docs/kubernetes.md`, `docs/formats/drill-scorecard.md`, `docs/stability.md` | own sections | own sections; rewrites `stability.md` "A `newTopic` restore does not reconstruct…" | own sections |
| Generated: `charts/logweir/crds/*.yaml`, `schemas/logweir-api-v1.openapi.json`, scorecard schemas | regenerate locally | same | same |

At integration the orchestrator regenerates the generated files once, renumbers the three scorecard MINORs and the verifier versions (monotonic, W3 → W2 → W1 is fine), resolves the adjacent-field conflicts, and numbers the release notes. W1 reads W3's `target.creation` through the interface above and tests with fixtures until W3 lands.

## 8. The combined acceptance pass

Workers run their own rows once on their slot (OD-15). The orchestrator runs every row once on the integration branch. "4.3" / "3.9": a PROD-01.5 slot (`stack-env.sh --kafka …`). `groups`: PROD-04.0d's helper.

### 8.1 Compose rows

| # | Owner | Pass predicate | Negative control | Fixture |
|---|---|---|---|---|
| C1 | W1 | **Headline.** A paused fixture consumer (`app`, 40 of 100 records per partition committed, stopped) is backed up with `--consumer-group app`, restored (complete), switched over; restarted on the target topic with `auto.offset.reset=none`, its first record on every partition carries `x-original-offset = P` and the source record's key and value | Committing `P` unmapped, on a point whose log start is 5, lands 5 records off and fails the first-record check (TI-04.2-3) | 4.3; `groups` |
| C2 | W1 | **A live group cannot be reset silently.** With a live member (classic, then KIP-848), the group is `refused: GroupActive`, readback unchanged, the status names it; after the member stops, a new Switchover applies | A commit carrying the member's own id passes the broker and fails the unchanged readback (AP-04.2-1) | 4.3, 3.9; `groups` |
| C3 | W1 | **Restoring never resets.** After the restore (mapping written), every captured group's positions on the target are absent | A build that applies in phase 9 leaves positions; the row fails | 4.3 |
| C4 | W1 | **Partial, visible, recoverable.** Three groups, one live: `PartiallyApplied`; status, lines and record agree (2 applied, 1 refused); priors recorded before any commit; the same plan in a new Switchover gives `alreadyApplied` ×2 and applies the third after its member stops | A mutant that skips the compare commits the applied groups twice (commit count in the record); a mutant that skips the prior read leaves priors empty (AP-04.2-3) | 4.3; `groups` |
| C5 | W1 | A group whose target position moved since the plan is `refused: TargetPositionsChanged`; nothing committed for it | Without the compare it is overwritten | 4.3 |
| C6 | W1 | **Filtered restore × positions:** a window start past unread records gives `approximate` with the exact `skipped`; a partition subset gives `PartitionNotRestored` | A mapping that ignores the selection reports `exact` | 4.3 |
| C7 | W1 | A compaction hole at `P` maps `exact` to the next record | A lookup for lineage `= P` finds nothing | 4.3; compacted topic |
| C8 | W1 | A caught-up group maps to the target's end; after cutover a newly produced record is its first | Mapping to end − 1 re-delivers the last record | 4.3 |
| C9 | W1 | An emptied source partition (`log_start = HW = 5`, `P = 5`) maps `exact` to 0 | Copying 5 is `AboveTargetEnd` | 4.3 |
| C10 | W1 | A target topic deleted and recreated between restore and switchover: `TargetGenerationChanged`, nothing applied | Unchanged target: applies (TI-04.2-2) | 4.3 |
| C11 | W1 | A sampled restore is refused naming `coverage`; a complete restore with a record planted before verification is refused naming `unexpected` (08.1-A4) | The passing restore proceeds | 4.3 |
| C12 | W1 | An Empty streams group with the target id: `TargetGroupTypeUnsupported` before any commit (AP-04.2-2) | A classic Empty group applies | 4.3; `streams-protocol` |
| C13 | W1 | A hidden target group: `TargetNotVisible`, never `created` (AP-04.2-7) | Listing-only classification records `created` | 4.3; `acl` visibility |
| C14 | W1 | An absent target group is created and the record says `created` (AP-04.2-6) | — (the predicate is the record and the listing) | 3.9 |
| C15 | W1 | **Rollback limits.** With no consumption since, the rollback plan restores the priors and lists the absent ones as not restorable; after the consumer read 10 records, it is refused `TargetPositionsChanged` naming the 10 | A rollback without `expected` re-delivers the 10 | 4.3 |
| C16 | W1 | A transactional archive maps every position `unavailable: TransactionalArchive` (04-1) | The naive rule lands on a marker | PROD-01.1a fixture |
| C17 | W1 | An approval for the `Restore` cannot authorise the `Switchover` (`SubjectKindMismatch`) | The Switchover's own approval does | 4.3 |
| C20 | W2 | **From the model.** 3.9 → 4.3, compacted and delete topics: source partitions; `RF = min(model, brokers)` with its reason; at-creation overrides set; before phase 9 the topic reads `cleanup.policy=delete`, `retention.ms=-1` (05.1 row 1) | No block: created exactly as today | 3.9 source, 4.3 target |
| C21 | W2 | **Fewer brokers:** RF 3 → 1 with `reason: targetBrokers`; `min.insync.replicas` capped and stated | Applying `min.insync.replicas=2` at RF 1 fails the restore (`NOT_ENOUGH_REPLICAS`) | `cluster3` → 4.3 |
| C22 | W2 | **Compaction after verification:** complete coverage passes with `offset_holes` disclosed, then `cleanup.policy=compact`; the topic is readable before and after (08.1-A5) | Transition before verification: `missing`, fail | 4.3 |
| C23 | W2 | **Old timestamps:** records older than the target's timestamp bound and retention restore and pass; the preview names what the transition's retention will delete | Without the during-restore keys, phase 0's timestamp refusal (today's) | 4.3 |
| C24 | W2 | `requiresTieredStorage` is refused before any topic is created (row 4); `removedInKafka4` is applied on 3.9 and a named difference on 4.3 (row 3) | — | 3.9, 4.3 |
| C25 | W2 | `inherited`, `clusterBound`, `providerOnly`, `secret` are never applied and are listed (row 2); a captureDenied topic gets safe settings, NOT RECORDED (row 6); the 4.0 timestamp default change is named before Create (row 7) | A topic with no override lists only inherited differences | 4.3 |
| C26 | W2 | **Owners:** a Strimzi-owned point's topic is created and its desired-state `KafkaTopic` YAML is emitted, bound by digest, not altered; its unowned neighbour is transitioned. `owner_detection: []` with no statement: `TopicOwnerNotChecked`; with `owners: []` it proceeds (row 5) | — | 4.3; `--kafka-topic-resources` at backup |
| C27 | W2 | **Partial transition:** with AlterConfigs denied on one topic, the attestation and the status list `applied [A]`, `failed [B]` with B's full desired set; a topic the run did not create keeps its overrides byte for byte | A transition that touches the unrelated topic changes its overrides | `acl` |
| C28 | W2 | **05.1a (mock API):** with `spec.strimzi` set, the listed `KafkaTopic`s reach the execution inputs and the receipt records `kafkaTopicResources`; a 403 fails `OwnerDetectionFailed`; without the field nothing is listed | — | weirkeeper's mock client |
| C40 | W3 | **TTL clone (CLI):** with `ttl_seconds: 300` phase 9 keeps the topics; `restore cleanup` after expiry deletes exactly `target.creation.topics` and attests them | A topic with the clone's prefix that the run did not create survives | 4.3 |
| C41 | W3 | A clone topic recreated before cleanup is skipped `CloneTargetReplaced`; the others are deleted (TI-11.1-4) | Unchanged: deleted | 4.3 |
| C42 | W3 | A TTL clone to a cluster outside `allowedClusterIds`, or to the source, is refused before creation; cleanup against a repointed cluster refuses `CloneTargetClusterChanged` | The allowed cluster proceeds | 4.3, `cluster2` |
| C43 | W3 | `target.creation` (names, IDs, `lineage_headers: kept`) is in every topic-creating run's scorecard; a clone record carries both lineage headers | — | 4.3 |
| C44 | W3 | Main's runner (`e53ce68e`) refuses the map-form teardown (exit 3, nothing created) | A string `teardown` still runs | 4.3 |
| C45 | W3 | A TTL clone whose run failed is torn down at once | — | 4.3 |
| C46 | W3 | The restore preflight names a selected partition with nothing in the window | A partition with records is not named | 4.3 |

**Unit rows (no stack).** AP-04.2-4 (epoch −1), AP-04.2-5 (Unknown state is active), 04-4 (first copy of a duplicate), every row of §2.2's table, the §3.5 compare, plan ↔ mapping agreement, rollback-plan construction, 05.1 row 8 (re-derived class), `ConfigChangedSinceCreation`, the TTL bounds, the cleanup's ID rule. Mutants only on the mapping rule, the compare, the stage table and the cleanup rule.

### 8.2 PoC rows (one combined pass)

Drive the product API with a Dex session (the console's own calls: cookie and CSRF token); use `kubectl` for reads and for the fixture consumer inside the PoC's Kafka pods.

| # | Pass predicate | Negative control |
|---|---|---|
| K1 | **Headline:** a paused consumer group on the PoC source; a Backup with `consumerGroups`; a complete Restore (confirm); target positions absent after it; a Switchover preflight shows every partition `exact`; a Switchover (confirm) is `Succeeded`; the consumer started on the restored topic first reads the record with `x-original-offset = P` | — |
| K2 | A live member on the target group: `refused: GroupActive`, shown on the Switchover; after it stops, a new Switchover applies | — |
| K3 | Two-person namespace: `alice` creates, `bob` sees every group and position and approves; `alice` cannot approve her own (403) | A plan over 8192 positions is refused `scope_incomplete` |
| K4 | The Restore's own `Approval` cannot authorise the Switchover (`SubjectKindMismatch`) | — |
| K5 | The restored topic recreated before the switchover: `TargetGenerationChanged`, nothing applied | — |
| K6 | `transition: afterVerification`: `Restore.status.topicConfiguration` shows the RF bounded by the PoC target's broker count with its reason, and the overrides before (Logweir's) and after (the source's) | — |
| K7 | TTL clone (`ttl_seconds: 300`) to the allowed target: `status.clone.expiresAt` set; after it, the cleanup Job deletes the topics; `status.teardown.deleted` and the attestation key are present; the Restore remains | A topic with the prefix the run did not create survives |
| K8 | Deleting a TTL clone's Restore before expiry runs the cleanup first, then the object goes | — |
| K9 | Upgrade: existing Restores and Approvals decode; the Switchover CRD is served; old scorecards still verify; a Helm rollback leaves Switchovers inert | — |
| K10 | 05.1a live, only if the owner allows the KafkaTopic CRD on docker-desktop (§10, point 3): a KafkaTopic for a source topic makes the receipt record owner `strimzi`, and the restore emits its YAML instead of altering it | `strimzi.readKafkaTopics: false`: `OwnerDetectionFailed` |

## 9. Console screens for PROD-18.1

1. **Restore wizard additions:** a configuration step (from the point or Logweir's defaults; transition after verification or none; the owner statement when not checked; the per-topic diff with the replication change, the during/at/after/never sets, inherited differences, owned topics). A clone choice in scratch mode (TTL, the allowlist status, the lineage-header statement). The advanced selection (window start, partition subset) behind an explicit choice, with the empty-in-window rows.
2. **Restore detail:** configuration before/after with partial outcomes and the desired-state download; the clone's expiry countdown, cleanup outcome and attestations; a positions-mapping summary with "Plan switchover".
3. **Switchover planning:** pick groups and target ids; a per-partition table (`exact` / `approximate` with skipped counts / `unavailable` with reason); `active_at_capture` warnings; the live target state from the preflight (members, existing positions); the plan hash.
4. **Switchover approval:** confirm, two-person and strict, each showing the whole scope.
5. **Switchover detail:** per-group outcomes with priors and applied positions, the record's verification, and "retry remaining" and "build rollback" actions with their limits stated.
6. **States:** no positions captured; sampled coverage (why no switchover); preflight running; refused with reason; partial; expired clone.

## 10. Open points for the owner

1. **Amendment K (§3.9) under rule 8.** It narrows "no offset commit anywhere" and "phase 9 is the only deletion", and lets the controller read one vendor resource. *Recommendation: yes*; OD-15 chose the rows that need it. W1 lands it only after the yes.
2. **A Switchover under one-person confirmation:** (a) the namespace's normal mode; (b) OD-10-style typed confirmation of the target group ids; (c) always two people. *Recommendation: (a).* The broker refuses live groups, every partition is compare-and-set, and the approver sees every position. W1 builds (a); (b) is a fix-round addition (one signed field, one API check).
3. **05.1a live proof:** the PoC has no Strimzi, and a CRD is cluster-scoped. *Recommendation:* let the combined pass install only the `kafkatopics.kafka.strimzi.io` CRD (no operator), run K10, and delete it. Otherwise 05.1a is proven against the mock API only (C28). The chart grant stays opt-in (`strimzi.readKafkaTopics: false`), which settles PROD-05.1 §9's "owner call" unless you prefer it on.
4. **Clone header stripping:** *Recommendation:* keep lineage headers always, as designed. Stripping makes a restore unverifiable and its positions unmappable; a strip option would be a later row, and only for unverified copies.

## 11. Prototype (what was run)

Compose slot 1, broker only (`docker compose … up -d --wait kafka-broker-1` after `eval "$(e2e/compose/stack-env.sh --slot 1 --kafka 4.3)"`, then `--kafka 3.9`). Brokers: `apache/kafka:4.3.1@sha256:77e3df90…6532837` and `apache/kafka:3.9.2@sha256:05b4616e…7fb7b02`. Client: rdkafka 0.36.2 / librdkafka 2.12.1, a throwaway example built against the worktree (not committed). `just e2e-down` after each line. The source and outputs are in `claude/artifacts/v2a-design/`.

| Fact | 4.3.1 | 3.9.2 |
|---|---|---|
| P1: legacy `alter_configs` with `{cleanup.policy=compact, retention.ms=604800000, max.message.bytes=2000000}` on a topic created with `{message.timestamp.type=CreateTime, retention.ms=-1, max.message.bytes=2000000}` | accepted; overrides are exactly the three sent; `message.timestamp.type` back to `Default` (the whole set is replaced) | same |
| P2: offset 3 committed (non-member), topic deleted, then recreated under the same name | committed reads `Invalid` after the delete and after the re-create | same |
| P3: non-member commit to topic B while the group has a live member on topic A | `UnknownMemberId` (classic and KIP-848), readback unchanged; applies after the member leaves | `UnknownMemberId` (classic); applies after |

## 12. Corrections to the tracker and earlier records

- **PROD-04.2 "approximate (partition end proven within coverage)":** the partition end is `exact` when nothing unread lies beyond `P` in the archive. `approximate` is reserved for records the restore's own selection excluded, which 08.1's "no labelled partial translation" permits because verification still passed.
- **AP-04.2-3 "a re-run … under the same approval":** an approval binds one object (UID). A re-run is a new Switchover with a new approval, made safe by the compare (§3.6).
- **TI-04.2-1 (`GenerationMismatch`):** unreachable. Positions come only from the restored point; the row becomes "a mapping document not bound to the restore's scorecard is refused" (`MappingDigestMismatch`).
- **PROD-05.1 row 5 "an owned topic is not created through the admin API":** it conflicts with phase 0's refusal of existing targets and with PROD-15.1's owner path. The restore creates the topic; only an owned topic's configuration is exported.
- **PROD-11.1 §9 (clones as new-topic targets):** a clone is a scratch restore; that is where `AllowedClusters` already lives.
- **The brief's flow places the configuration transition after the switchover:** it runs in phase 9 after the signed verdict. Offsets do not move under compaction or retention, and a mapped offset below the new log start is refused at apply time.
- **PROD-11.1a's ledger row includes the console's advanced selection:** it moves to PROD-18.1 (OD-16).
- **TI-15.1-1's "created target's identity" was never recorded:** `target.creation` (W3) records it.
- **Code that encodes "tag 2":** `crds/mod.rs:8` and `:70`, `crds/approval.rs:16`, `crd_shape.rs:2328` (`the_subject_kind_enum_has_no_switchover`), `crates/logweir/tests/cli_verify_gc12.rs:116`, `crds/restore.rs:235` and `:1015` / `charts/logweir/crds/restores.yaml:903` (a Switchover "retiring an old topic": this design retires and deletes no topic). W1 updates them.

---

Documentation is licensed [CC-BY-4.0](../../LICENSE-docs).

Apache Kafka® and Kafka® are registered trademarks of the Apache Software
Foundation. Logweir is not affiliated with or endorsed by the ASF.
